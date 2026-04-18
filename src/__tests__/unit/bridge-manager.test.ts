/**
 * Unit tests for bridge-manager.
 *
 * Tests cover:
 * - Session lock concurrency: same-session serialization
 * - Session lock concurrency: different-session parallelism
 * - Bridge start/stop lifecycle
 * - Auto-start idempotency
 */

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { initBridgeContext } from '../../lib/bridge/context';
import type { BridgeStore, LifecycleHooks } from '../../lib/bridge/host';
import type { ChannelBinding, InboundMessage, OutboundMessage, SendResult } from '../../lib/bridge/types';
import { BaseChannelAdapter } from '../../lib/bridge/channel-adapter';

// ── Test the session lock mechanism directly ────────────────
// We test the processWithSessionLock pattern by extracting its logic.

function createSessionLocks() {
  const locks = new Map<string, Promise<void>>();

  function processWithSessionLock(sessionId: string, fn: () => Promise<void>): Promise<void> {
    const prev = locks.get(sessionId) || Promise.resolve();
    const current = prev.then(fn, fn);
    locks.set(sessionId, current);
    // Suppress unhandled rejection on the cleanup chain — callers handle the error on `current` directly
    current.finally(() => {
      if (locks.get(sessionId) === current) {
        locks.delete(sessionId);
      }
    }).catch(() => {});
    return current;
  }

  return { locks, processWithSessionLock };
}

describe('bridge-manager session locks', () => {
  it('serializes same-session operations', async () => {
    const { processWithSessionLock } = createSessionLocks();
    const order: number[] = [];

    const p1 = processWithSessionLock('session-1', async () => {
      await new Promise(r => setTimeout(r, 50));
      order.push(1);
    });

    const p2 = processWithSessionLock('session-1', async () => {
      order.push(2);
    });

    await Promise.all([p1, p2]);
    assert.deepStrictEqual(order, [1, 2], 'Same-session operations should be serialized');
  });

  it('allows different-session operations to run concurrently', async () => {
    const { processWithSessionLock } = createSessionLocks();
    const started: string[] = [];
    const completed: string[] = [];

    const p1 = processWithSessionLock('session-A', async () => {
      started.push('A');
      await new Promise(r => setTimeout(r, 50));
      completed.push('A');
    });

    const p2 = processWithSessionLock('session-B', async () => {
      started.push('B');
      await new Promise(r => setTimeout(r, 10));
      completed.push('B');
    });

    await Promise.all([p1, p2]);
    // Both should start before either completes (concurrent)
    assert.equal(started.length, 2);
    // B should complete first since it has shorter delay
    assert.equal(completed[0], 'B');
    assert.equal(completed[1], 'A');
  });

  it('continues after errors in locked operations', async () => {
    const { processWithSessionLock } = createSessionLocks();
    const order: number[] = [];

    const p1 = processWithSessionLock('session-1', async () => {
      order.push(1);
      throw new Error('test error');
    });

    const p2 = processWithSessionLock('session-1', async () => {
      order.push(2);
    });

    await p1.catch(() => {});
    await p2;
    assert.deepStrictEqual(order, [1, 2], 'Should continue after error');
  });

  it('cleans up completed locks', async () => {
    const { locks, processWithSessionLock } = createSessionLocks();

    await processWithSessionLock('session-1', async () => {});

    // Allow microtask to complete for finally() cleanup
    await new Promise(r => setTimeout(r, 0));
    assert.equal(locks.size, 0, 'Lock should be cleaned up after completion');
  });
});

// ── Lifecycle tests ─────────────────────────────────────────

describe('bridge-manager lifecycle', () => {
  beforeEach(() => {
    // Clear bridge manager state
    delete (globalThis as Record<string, unknown>)['__bridge_manager__'];
    delete (globalThis as Record<string, unknown>)['__bridge_context__'];
  });

  it('getStatus returns not running when bridge has not started', async () => {
    const store = createMinimalStore({ remote_bridge_enabled: 'false' });
    initBridgeContext({
      store,
      llm: { streamChat: () => new ReadableStream() },
      permissions: { resolvePendingPermission: () => false },
      lifecycle: {},
    });

    // Import dynamically to get fresh module state
    const { getStatus } = await import('../../lib/bridge/bridge-manager');
    const status = getStatus();
    assert.equal(status.running, false);
    assert.equal(status.adapters.length, 0);
  });
});

describe('bridge-manager /sessions output', () => {
  beforeEach(() => {
    delete (globalThis as Record<string, unknown>)['__bridge_manager__'];
    delete (globalThis as Record<string, unknown>)['__bridge_context__'];
  });

  it('shows the latest user message preview for bridge sessions', async () => {
    const binding: ChannelBinding = {
      id: 'binding-1',
      channelType: 'telegram',
      chatId: 'chat-1',
      codepilotSessionId: 'session-1',
      sdkSessionId: '',
      workingDirectory: '/Users/bytedance/Downloads/4.my_projects/260407_claude_im_skill',
      model: '',
      mode: 'code',
      active: true,
      createdAt: '2026-04-17T12:00:00.000Z',
      updatedAt: '2026-04-17T12:00:00.000Z',
    };

    const store = createMinimalStore({ remote_bridge_enabled: 'false' });
    store.getChannelBinding = (channelType: string, chatId: string) =>
      channelType === 'telegram' && chatId === 'chat-1' ? binding : null;
    store.listChannelBindings = () => [binding];
    store.getSession = (id: string) => id === 'session-1'
      ? {
          id: 'session-1',
          working_directory: binding.workingDirectory,
          model: '',
          created_at: '2026-04-17T12:00:00.000Z',
        } as any
      : null;
    store.getMessages = (sessionId: string) => ({
      messages: sessionId === 'session-1'
        ? [
            { role: 'user', content: '第一句提问' },
            { role: 'assistant', content: '第一句回复' },
            { role: 'user', content: '请帮我把 session 列表里最后一句用户消息也显示出来，最好简短一点方便回忆' },
          ]
        : [],
    });

    initBridgeContext({
      store,
      llm: { streamChat: () => new ReadableStream() },
      permissions: { resolvePendingPermission: () => false },
      lifecycle: {},
    });

    const sentMessages: OutboundMessage[] = [];
    const adapter = new TestAdapter(sentMessages);
    const { _testOnly } = await import('../../lib/bridge/bridge-manager');

    await _testOnly.handleMessage(adapter, {
      messageId: 'cmd-1',
      address: { channelType: 'telegram', chatId: 'chat-1', userId: 'user-1' },
      text: '/sessions',
      timestamp: Date.now(),
    } satisfies InboundMessage);

    assert.equal(sentMessages.length, 1);
    assert.match(sentMessages[0].text, /💬 请帮我把 session 列表里最后一句用户消息也显示出来/);
    assert.match(sentMessages[0].text, /\.\.\./);
  });

  it('shows attachment placeholder when the latest user message only has files', async () => {
    const binding: ChannelBinding = {
      id: 'binding-2',
      channelType: 'telegram',
      chatId: 'chat-2',
      codepilotSessionId: 'session-2',
      sdkSessionId: '',
      workingDirectory: '/tmp/demo',
      model: '',
      mode: 'code',
      active: true,
      createdAt: '2026-04-17T12:00:00.000Z',
      updatedAt: '2026-04-17T12:00:00.000Z',
    };

    const store = createMinimalStore();
    store.getChannelBinding = (channelType: string, chatId: string) =>
      channelType === 'telegram' && chatId === 'chat-2' ? binding : null;
    store.listChannelBindings = () => [binding];
    store.getSession = (id: string) => id === 'session-2'
      ? {
          id: 'session-2',
          working_directory: binding.workingDirectory,
          model: '',
          created_at: '2026-04-17T12:00:00.000Z',
        } as any
      : null;
    store.getMessages = (sessionId: string) => ({
      messages: sessionId === 'session-2'
        ? [
            {
              role: 'user',
              content: '<!--files:[{"id":"f1","name":"demo.png"}]-->',
            },
          ]
        : [],
    });

    initBridgeContext({
      store,
      llm: { streamChat: () => new ReadableStream() },
      permissions: { resolvePendingPermission: () => false },
      lifecycle: {},
    });

    const sentMessages: OutboundMessage[] = [];
    const adapter = new TestAdapter(sentMessages);
    const { _testOnly } = await import('../../lib/bridge/bridge-manager');

    await _testOnly.handleMessage(adapter, {
      messageId: 'cmd-2',
      address: { channelType: 'telegram', chatId: 'chat-2', userId: 'user-2' },
      text: '/sessions',
      timestamp: Date.now(),
    } satisfies InboundMessage);

    assert.equal(sentMessages.length, 1);
    assert.match(sentMessages[0].text, /💬 \[附件\]/);
  });

  it('switches to a Claude CLI session from the displayed list without re-querying getCliSession', async () => {
    const store = createMinimalStore();
    let cliListCalls = 0;
    store.listCliSessions = () => {
      cliListCalls += 1;
      return [
        {
          sessionId: '122499bf-1111-2222-3333-444444444444',
          pid: 123,
          cwd: '/Users/bytedance/Downloads/4.my_projects/invest/260416_openbb',
          startedAt: 100,
          kind: 'interactive',
          entrypoint: 'cli',
          isActive: true,
          agent: 'claude',
        },
      ];
    };
    store.getCliSession = () => {
      throw new Error('this.getSessionsDir is not a function');
    };

    initBridgeContext({
      store,
      llm: { streamChat: () => new ReadableStream() },
      permissions: { resolvePendingPermission: () => false },
      lifecycle: {},
    });

    const sentMessages: OutboundMessage[] = [];
    const adapter = new TestAdapter(sentMessages);
    const { _testOnly } = await import('../../lib/bridge/bridge-manager');

    await _testOnly.handleMessage(adapter, {
      messageId: 'sessions-claude',
      address: { channelType: 'telegram', chatId: 'chat-switch-claude', userId: 'user-1' },
      text: '/sessions',
      timestamp: Date.now(),
    } satisfies InboundMessage);

    await _testOnly.handleMessage(adapter, {
      messageId: 'switch-claude',
      address: { channelType: 'telegram', chatId: 'chat-switch-claude', userId: 'user-1' },
      text: '/switch #1',
      timestamp: Date.now(),
    } satisfies InboundMessage);

    assert.equal(cliListCalls >= 1, true);
    assert.equal(sentMessages.length, 2);
    assert.match(sentMessages[1].text, /已切换到会话 #1/);
    assert.match(sentMessages[1].text, /Agent: <b>Claude<\/b>/);
    assert.match(sentMessages[1].text, /260416_openbb/);
    assert.doesNotMatch(sentMessages[1].text, /命令执行出错/);
  });

  it('uses the last displayed /sessions snapshot for switch indexes', async () => {
    const store = createMinimalStore();
    let cliListCalls = 0;
    const claudeSession = {
      sessionId: 'claude-1111-2222-3333-4444444444444444',
      pid: 201,
      cwd: '/Users/bytedance/Downloads/4.my_projects/invest/260416_openbb',
      startedAt: 200,
      kind: 'interactive',
      entrypoint: 'cli',
      isActive: true,
      agent: 'claude' as const,
    };
    const codexSession = {
      sessionId: '019d9c18-9a23-7c01-8c54-464dfe45b1d9',
      pid: 0,
      cwd: '/Users/bytedance/Downloads/4.my_projects/drama/ai_drama',
      startedAt: 100,
      kind: 'interactive',
      entrypoint: 'cli',
      isActive: false,
      agent: 'codex' as const,
    };
    store.listCliSessions = () => {
      cliListCalls += 1;
      return cliListCalls === 1
        ? [claudeSession, codexSession]
        : [codexSession, claudeSession];
    };
    store.getCliSession = () => {
      throw new Error('getCliSession should not be called when switching by displayed index');
    };

    initBridgeContext({
      store,
      llm: { streamChat: () => new ReadableStream() },
      permissions: { resolvePendingPermission: () => false },
      lifecycle: {},
    });

    const sentMessages: OutboundMessage[] = [];
    const adapter = new TestAdapter(sentMessages);
    const { _testOnly } = await import('../../lib/bridge/bridge-manager');

    await _testOnly.handleMessage(adapter, {
      messageId: 'sessions-order',
      address: { channelType: 'telegram', chatId: 'chat-switch-order', userId: 'user-2' },
      text: '/sessions',
      timestamp: Date.now(),
    } satisfies InboundMessage);

    await _testOnly.handleMessage(adapter, {
      messageId: 'switch-order',
      address: { channelType: 'telegram', chatId: 'chat-switch-order', userId: 'user-2' },
      text: '/switch #2',
      timestamp: Date.now(),
    } satisfies InboundMessage);

    assert.equal(sentMessages.length, 2);
    assert.match(sentMessages[0].text, /#1 .*260416_openbb/s);
    assert.match(sentMessages[0].text, /#2 .*ai_drama/s);
    assert.match(sentMessages[1].text, /已切换到会话 #2/);
    assert.match(sentMessages[1].text, /Agent: <b>Codex<\/b>/);
    assert.match(sentMessages[1].text, /ai_drama/);
    assert.match(sentMessages[1].text, /codex --resume 019d9c18-9a23-7c01-8c54-464dfe45b1d9/);
  });
});

class TestAdapter extends BaseChannelAdapter {
  readonly channelType = 'telegram';

  constructor(private readonly sentMessages: OutboundMessage[]) {
    super();
  }

  async start(): Promise<void> {}
  async stop(): Promise<void> {}
  isRunning(): boolean { return false; }
  async consumeOne(): Promise<InboundMessage | null> { return null; }
  validateConfig(): string | null { return null; }
  isAuthorized(): boolean { return true; }

  async send(message: OutboundMessage): Promise<SendResult> {
    this.sentMessages.push(message);
    return { ok: true, messageId: `sent-${this.sentMessages.length}` };
  }
}

function createMinimalStore(settings: Record<string, string> = {}): BridgeStore {
  const bindings = new Map<string, ChannelBinding>();
  const sessions = new Map<string, { id: string; working_directory: string; model: string }>();
  let nextSessionId = 1;
  let nextBindingId = 1;

  return {
    getSetting: (key: string) => settings[key] ?? null,
    getChannelBinding: (channelType: string, chatId: string) =>
      bindings.get(`${channelType}:${chatId}`) ?? null,
    upsertChannelBinding: (data) => {
      const key = `${data.channelType}:${data.chatId}`;
      const existing = bindings.get(key);
      const binding: ChannelBinding = {
        id: existing?.id ?? `binding-${nextBindingId++}`,
        channelType: data.channelType,
        chatId: data.chatId,
        codepilotSessionId: data.codepilotSessionId,
        sdkSessionId: data.sdkSessionId ?? existing?.sdkSessionId ?? '',
        codexSessionId: data.codexSessionId ?? existing?.codexSessionId,
        workingDirectory: data.workingDirectory ?? existing?.workingDirectory ?? '',
        model: data.model ?? existing?.model ?? '',
        mode: (data.mode as ChannelBinding['mode']) ?? existing?.mode ?? 'code',
        agent: data.agent ?? existing?.agent,
        active: existing?.active ?? true,
        createdAt: existing?.createdAt ?? new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      };
      bindings.set(key, binding);
      return binding;
    },
    updateChannelBinding: (id: string, updates: Partial<ChannelBinding>) => {
      for (const [key, binding] of bindings.entries()) {
        if (binding.id === id) {
          bindings.set(key, { ...binding, ...updates, updatedAt: new Date().toISOString() });
        }
      }
    },
    listChannelBindings: (channelType?: string) =>
      Array.from(bindings.values()).filter(binding => !channelType || binding.channelType === channelType),
    getSession: (id: string) => sessions.get(id) ?? null,
    createSession: (_name: string, model: string, _systemPrompt?: string, cwd?: string) => {
      const session = {
        id: `session-${nextSessionId++}`,
        working_directory: cwd || '',
        model,
      };
      sessions.set(session.id, session);
      return session;
    },
    updateSessionProviderId: () => {},
    addMessage: () => {},
    getMessages: () => ({ messages: [] }),
    acquireSessionLock: () => true,
    renewSessionLock: () => {},
    releaseSessionLock: () => {},
    setSessionRuntimeStatus: () => {},
    updateSdkSessionId: () => {},
    updateSessionModel: () => {},
    syncSdkTasks: () => {},
    getProvider: () => undefined,
    getDefaultProviderId: () => null,
    insertAuditLog: () => {},
    checkDedup: () => false,
    insertDedup: () => {},
    cleanupExpiredDedup: () => {},
    insertOutboundRef: () => {},
    insertPermissionLink: () => {},
    getPermissionLink: () => null,
    markPermissionLinkResolved: () => false,
    listPendingPermissionLinksByChat: () => [],
    getChannelOffset: () => '0',
    setChannelOffset: () => {},
  };
}
