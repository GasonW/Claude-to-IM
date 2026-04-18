import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { buildFinalCardJson, buildStreamingContent } from '../../lib/bridge/markdown/feishu.js';

describe('feishu markdown card builders', () => {
  it('builds a full streaming card json payload', () => {
    const cardJson = buildStreamingContent('处理中', [
      { id: 'tool-1', name: 'Bash', status: 'running' },
    ]);

    const card = JSON.parse(cardJson);
    assert.equal(card.schema, '2.0');
    assert.equal(card.body.elements[0].tag, 'markdown');
    assert.match(card.body.elements[0].content, /处理中/);
    assert.match(card.body.elements[0].content, /Bash/);
  });

  it('builds a final card with footer notation', () => {
    const cardJson = buildFinalCardJson('完成了', [], {
      status: '✅ Completed',
      elapsed: '1.2s',
    });

    const card = JSON.parse(cardJson);
    assert.equal(card.schema, '2.0');
    assert.equal(card.body.elements.at(-1).text_size, 'notation');
    assert.match(card.body.elements.at(-1).content, /Completed/);
    assert.match(card.body.elements.at(-1).content, /1\.2s/);
  });
});
