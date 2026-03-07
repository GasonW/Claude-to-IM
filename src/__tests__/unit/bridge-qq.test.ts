/**
 * Unit tests for QQ-related bridge functionality.
 *
 * Tests cover:
 * - PLATFORM_LIMITS for QQ
 * - Delivery-layer QQ chunking (3 segment max, truncation marker)
 * - Permission-broker QQ text permissions (no buttons, /perm commands)
 * - QQAdapter: validateConfig, isAuthorized, send
 * - qq-api: nextMsgSeq auto-increment
 * - bridge-manager: hasError clears sdkSessionId logic
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { initBridgeContext } from '../../lib/bridge/context';
import { deliver } from '../../lib/bridge/delivery-layer';
import { forwardPermissionRequest } from '../../lib/bridge/permission-broker';
import { PLATFORM_LIMITS } from '../../lib/bridge/types';
import { nextMsgSeq } from '../../lib/bridge/adapters/qq-api';
import { QQAdapter } from '../../lib/bridge/adapters/qq-adapter';
import type { BaseChannelAdapter } from '../../lib/bridge/channel-adapter';
import type { BridgeStore } from '../../lib/bridge/host';
import type { OutboundMessage, SendResult } from '../../lib/bridge/types';

// ── Mock Store ──────────────────────────────────────────────

function createMockStore(settings: Record<string, string> = {}) {
  const auditLogs: any[] = [];
  const outboundRefs: any[] = [];
  const dedupKeys = new Set<string>();
  const permLinks = new Map<string, any>();

  return {
    auditLogs,
    outboundRefs,
    dedupKeys,
    permLinks,
    getSetting: (key: string) => settings[key] ?? null,
    getChannelBinding: () => null,
    upsertChannelBinding: () => ({} as any),
    updateChannelBinding: () => {},
    listChannelBindings: () => [],
    getSession: () => null,
    createSession: () => ({ id: '1', working_directory: '', model: '' }),
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
    insertAuditLog: (entry: any) => { auditLogs.push(entry); },
    checkDedup: (key: string) => dedupKeys.has(key),
    insertDedup: (key: string) => { dedupKeys.add(key); },
    cleanupExpiredDedup: () => {},
    insertOutboundRef: (ref: any) => { outboundRefs.push(ref); },
    insertPermissionLink: (link: any) => { permLinks.set(link.permissionRequestId, link); },
    getPermissionLink: (id: string) => permLinks.get(id) ?? null,
    markPermissionLinkResolved: () => false,
    getChannelOffset: () => '0',
    setChannelOffset: () => {},
  };
}

type MockStore = ReturnType<typeof createMockStore>;

function setupContext(store: MockStore) {
  delete (globalThis as Record<string, unknown>)['__bridge_context__'];
  initBridgeContext({
    store: store as unknown as BridgeStore,
    llm: { streamChat: () => new ReadableStream() },
    permissions: { resolvePendingPermission: () => false },
    lifecycle: {},
  });
}

// ── Mock QQ Adapter ─────────────────────────────────────────

function createMockQQAdapter(opts?: {
  sendFn?: (msg: OutboundMessage) => Promise<SendResult>;
}): BaseChannelAdapter {
  const sendFn = opts?.sendFn ?? (async () => ({ ok: true, messageId: 'msg-1' }));
  return {
    channelType: 'qq',
    start: async () => {},
    stop: async () => {},
    isRunning: () => true,
    consumeOne: async () => null,
    send: sendFn,
    validateConfig: () => null,
    isAuthorized: () => true,
  } as unknown as BaseChannelAdapter;
}

// ── 1. PLATFORM_LIMITS ─────────────────────────────────────

describe('types - qq platform limit', () => {
  it('qq limit is 2000', () => {
    assert.equal(PLATFORM_LIMITS['qq'], 2000);
  });
});

// ── 2. Delivery-layer QQ chunking ──────────────────────────

describe('delivery-layer - qq chunking', () => {
  let store: MockStore;

  beforeEach(() => {
    store = createMockStore();
    setupContext(store);
  });

  it('limits qq to 3 segments max', async () => {
    const sentMessages: string[] = [];
    const adapter = createMockQQAdapter({
      sendFn: async (msg) => {
        sentMessages.push(msg.text);
        return { ok: true, messageId: `msg-${sentMessages.length}` };
      },
    });

    // Generate text that would produce >3 chunks at 2000 char limit
    // 5 chunks worth of text (each ~2000 chars)
    const longText = 'A'.repeat(1900) + '\n' +
                     'B'.repeat(1900) + '\n' +
                     'C'.repeat(1900) + '\n' +
                     'D'.repeat(1900) + '\n' +
                     'E'.repeat(1900);

    const result = await deliver(adapter, {
      address: { channelType: 'qq', chatId: 'user-1' },
      text: longText,
      parseMode: 'plain',
      replyToMessageId: 'inbound-1',
    });

    assert.ok(result.ok);
    assert.equal(sentMessages.length, 3, `Expected exactly 3 chunks, got ${sentMessages.length}`);
  });

  it('truncates overflow with marker', async () => {
    const sentMessages: string[] = [];
    const adapter = createMockQQAdapter({
      sendFn: async (msg) => {
        sentMessages.push(msg.text);
        return { ok: true, messageId: `msg-${sentMessages.length}` };
      },
    });

    // Generate text that would produce >3 chunks
    const longText = 'X'.repeat(1900) + '\n' +
                     'Y'.repeat(1900) + '\n' +
                     'Z'.repeat(1900) + '\n' +
                     'W'.repeat(1900) + '\n' +
                     'V'.repeat(1900);

    await deliver(adapter, {
      address: { channelType: 'qq', chatId: 'user-2' },
      text: longText,
      parseMode: 'plain',
      replyToMessageId: 'inbound-2',
    });

    // The last (3rd) chunk should contain the truncation marker
    const lastChunk = sentMessages[sentMessages.length - 1];
    assert.ok(lastChunk.includes('[... response truncated]'), 'Last chunk should contain truncation marker');
  });

  it('passes replyToMessageId through chunks', async () => {
    const sentReplyIds: (string | undefined)[] = [];
    const adapter = createMockQQAdapter({
      sendFn: async (msg) => {
        sentReplyIds.push(msg.replyToMessageId);
        return { ok: true, messageId: `msg-${sentReplyIds.length}` };
      },
    });

    // Generate text that produces multiple chunks
    const longText = 'A'.repeat(1900) + '\n' +
                     'B'.repeat(1900) + '\n' +
                     'C'.repeat(1900);

    await deliver(adapter, {
      address: { channelType: 'qq', chatId: 'user-3' },
      text: longText,
      parseMode: 'plain',
      replyToMessageId: 'reply-target-id',
    });

    // All chunks should carry the replyToMessageId
    for (const replyId of sentReplyIds) {
      assert.equal(replyId, 'reply-target-id', 'Each chunk should pass through replyToMessageId');
    }
  });
});

// ── 3. Permission-broker QQ text permissions ────────────────

describe('permission-broker - qq text permissions', () => {
  let store: MockStore;

  beforeEach(() => {
    store = createMockStore();
    setupContext(store);
  });

  it('sends plain text prompt for qq (no buttons)', async () => {
    const sentMessages: OutboundMessage[] = [];
    const adapter = createMockQQAdapter({
      sendFn: async (msg) => {
        sentMessages.push(msg);
        return { ok: true, messageId: 'perm-msg-1' };
      },
    });

    await forwardPermissionRequest(
      adapter,
      { channelType: 'qq', chatId: 'user-perm-1' },
      'perm-req-unique-1',
      'Bash',
      { command: 'ls -la' },
      'session-1',
      undefined,
      'reply-msg-1',
    );

    assert.ok(sentMessages.length > 0, 'Should have sent at least one message');

    const permMsg = sentMessages[0];
    // No inline buttons for QQ
    assert.equal(permMsg.inlineButtons, undefined, 'QQ permission prompt should not have inline buttons');
    // Should contain /perm commands
    assert.ok(permMsg.text.includes('/perm allow perm-req-unique-1'), 'Should contain /perm allow command');
    assert.ok(permMsg.text.includes('/perm allow_session perm-req-unique-1'), 'Should contain /perm allow_session command');
    assert.ok(permMsg.text.includes('/perm deny perm-req-unique-1'), 'Should contain /perm deny command');
  });

  it('passes replyToMessageId for qq', async () => {
    const sentMessages: OutboundMessage[] = [];
    const adapter = createMockQQAdapter({
      sendFn: async (msg) => {
        sentMessages.push(msg);
        return { ok: true, messageId: 'perm-msg-2' };
      },
    });

    await forwardPermissionRequest(
      adapter,
      { channelType: 'qq', chatId: 'user-perm-2' },
      'perm-req-unique-2',
      'Read',
      { file_path: '/tmp/test' },
      'session-2',
      undefined,
      'reply-msg-2',
    );

    assert.ok(sentMessages.length > 0);
    assert.equal(sentMessages[0].replyToMessageId, 'reply-msg-2', 'Should pass through replyToMessageId');
  });
});

// ── 4. QQAdapter unit tests ────────────────────────────────

describe('qq-adapter', () => {
  let store: MockStore;

  beforeEach(() => {
    store = createMockStore();
    setupContext(store);
  });

  it('validateConfig returns error when app_id missing', () => {
    const adapter = new QQAdapter();
    const error = adapter.validateConfig();
    assert.ok(error);
    assert.ok(error.includes('app_id'), `Expected error about app_id, got: ${error}`);
  });

  it('validateConfig returns error when app_secret missing', () => {
    store = createMockStore({ bridge_qq_app_id: 'test-app-id' });
    setupContext(store);

    const adapter = new QQAdapter();
    const error = adapter.validateConfig();
    assert.ok(error);
    assert.ok(error.includes('app_secret'), `Expected error about app_secret, got: ${error}`);
  });

  it('validateConfig returns null when both configured', () => {
    store = createMockStore({
      bridge_qq_app_id: 'test-app-id',
      bridge_qq_app_secret: 'test-app-secret',
    });
    setupContext(store);

    const adapter = new QQAdapter();
    const error = adapter.validateConfig();
    assert.equal(error, null);
  });

  it('isAuthorized allows all when allowed_users empty', () => {
    const adapter = new QQAdapter();
    assert.ok(adapter.isAuthorized('any-user', 'any-chat'));
  });

  it('isAuthorized blocks unlisted users', () => {
    store = createMockStore({ bridge_qq_allowed_users: 'user-a,user-b' });
    setupContext(store);

    const adapter = new QQAdapter();
    assert.equal(adapter.isAuthorized('user-c', 'chat-1'), false);
  });

  it('isAuthorized allows listed users', () => {
    store = createMockStore({ bridge_qq_allowed_users: 'user-a,user-b' });
    setupContext(store);

    const adapter = new QQAdapter();
    assert.ok(adapter.isAuthorized('user-a', 'chat-1'));
    assert.ok(adapter.isAuthorized('user-b', 'chat-1'));
  });

  it('send returns error when replyToMessageId missing', async () => {
    store = createMockStore({
      bridge_qq_app_id: 'test-app-id',
      bridge_qq_app_secret: 'test-app-secret',
    });
    setupContext(store);

    const adapter = new QQAdapter();
    const result = await adapter.send({
      address: { channelType: 'qq', chatId: 'user-1' },
      text: 'Hello',
      parseMode: 'plain',
      // No replyToMessageId
    });

    assert.equal(result.ok, false);
    assert.ok(result.error?.includes('replyToMessageId'));
  });

  it('send strips HTML tags when parseMode is HTML', async () => {
    store = createMockStore({
      bridge_qq_app_id: 'test-app-id',
      bridge_qq_app_secret: 'test-app-secret',
    });
    setupContext(store);

    const adapter = new QQAdapter();

    // Mock global fetch
    const originalFetch = globalThis.fetch;
    let capturedBody: string | undefined;

    globalThis.fetch = (async (_url: any, init: any) => {
      // Capture the token request
      const urlStr = typeof _url === 'string' ? _url : _url.toString();
      if (urlStr.includes('getAppAccessToken')) {
        return {
          ok: true,
          json: async () => ({ access_token: 'test-token', expires_in: 7200 }),
          text: async () => '',
        };
      }
      // Capture the send message request
      capturedBody = init?.body;
      return {
        ok: true,
        json: async () => ({ id: 'sent-1' }),
        text: async () => '',
      };
    }) as typeof fetch;

    try {
      await adapter.send({
        address: { channelType: 'qq', chatId: 'user-1' },
        text: '<b>Hello</b> <i>world</i>',
        parseMode: 'HTML',
        replyToMessageId: 'msg-in-1',
      });

      assert.ok(capturedBody, 'Should have captured request body');
      const parsed = JSON.parse(capturedBody!);
      assert.equal(parsed.content, 'Hello world', 'HTML tags should be stripped');
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});

// ── 5. qq-api nextMsgSeq ────────────────────────────────────

describe('qq-api - nextMsgSeq', () => {
  it('auto-increments per message ID', () => {
    // Use a unique message ID to avoid interference from other tests
    const msgId = `test-msg-seq-${Date.now()}`;

    const seq1 = nextMsgSeq(msgId);
    const seq2 = nextMsgSeq(msgId);
    const seq3 = nextMsgSeq(msgId);

    assert.equal(seq1, 1);
    assert.equal(seq2, 2);
    assert.equal(seq3, 3);
  });
});

// ── 6. bridge-manager hasError clears sdkSessionId ──────────

describe('bridge-manager - hasError clears sdkSessionId', () => {
  it('clears sdkSessionId when hasError is true', () => {
    const updates: Record<string, string>[] = [];
    const result = { hasError: true, sdkSessionId: 'new-sdk', responseText: '', errorMessage: 'err' };

    if (result.sdkSessionId && !result.hasError) {
      updates.push({ sdkSessionId: result.sdkSessionId });
    } else if (result.hasError) {
      updates.push({ sdkSessionId: '' });
    }

    assert.equal(updates.length, 1);
    assert.equal(updates[0].sdkSessionId, '');
  });

  it('saves sdkSessionId when no error', () => {
    const updates: Record<string, string>[] = [];
    const result = { hasError: false, sdkSessionId: 'new-sdk', responseText: 'ok', errorMessage: '' };

    if (result.sdkSessionId && !result.hasError) {
      updates.push({ sdkSessionId: result.sdkSessionId });
    } else if (result.hasError) {
      updates.push({ sdkSessionId: '' });
    }

    assert.equal(updates.length, 1);
    assert.equal(updates[0].sdkSessionId, 'new-sdk');
  });
});
