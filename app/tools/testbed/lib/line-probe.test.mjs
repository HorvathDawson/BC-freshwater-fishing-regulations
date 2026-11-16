// (T760) node --test app/tools/testbed/lib/line-probe.test.mjs
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { lineVerdict, parseCurl, pingReceived } from './line-probe.mjs';

test('a probe needs HTTP 200 and bytes: a fast 404 or an empty 200 is no proof', () => {
  assert.equal(parseCurl('200 1234 0.412').ok, true);
  assert.equal(parseCurl('404 150 0.05').ok, false);
  assert.equal(parseCurl('200 0 0.3').ok, false);
  assert.equal(parseCurl('000 0 15.001').ok, false);
  assert.equal(parseCurl('').ok, false);
});

test('ping summaries are read', () => {
  assert.equal(pingReceived('3 packets transmitted, 3 received, 0% packet loss'), 3);
  assert.equal(pingReceived('3 packets transmitted, 0 packets received, 100% packet loss'), 0);
  assert.equal(pingReceived('ping: unknown host'), 0);
});

test('the verdict names which side was dead', () => {
  const good = parseCurl('200 1234 0.4'), bad = parseCurl('000 0 15.0');
  assert.match(lineVerdict(good, { fetched: 0, pinged: 3 }), /^line dead on the emulator only \(Mac fetched 1234 bytes in 0\.4 s\)/);
  assert.match(lineVerdict(bad, { fetched: 0, pinged: 0 }), /^line dead on the Mac too/);
  assert.match(lineVerdict(good, { fetched: 1234, pinged: 3 }), /^line up on both/);
  // no fetch tool on the image: ping decides
  assert.match(lineVerdict(good, { fetched: null, pinged: 0 }), /emulator only/);
  assert.match(lineVerdict(good, { fetched: null, pinged: 3 }), /up on both/);
});
