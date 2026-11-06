// (T442) node --test app/tools/testbed/lib/channel.test.mjs
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { channelProblem, checkChannel } from './channel.mjs';

const live = { data_release: 'r-4230bc0c1dd5' };

test('test/ on the live release: no problem', () => {
  assert.equal(channelProblem({ data_release: 'r-4230bc0c1dd5' }, live), null);
});

test('test/ left on a cleared release (walk-063: r-5a5999c94588) is refused, naming both', () => {
  const p = channelProblem({ data_release: 'r-5a5999c94588' }, live);
  assert.match(p, /r-5a5999c94588/);
  assert.match(p, /r-4230bc0c1dd5/);
});

test('an unreadable manifest is a problem, never a pass', () => {
  assert.match(channelProblem(null, live), /test\/release\/manifest\.json/);
  assert.match(channelProblem({ data_release: 'r-1' }, null), /live manifest/);
});

test('checkChannel reads both manifests and throws on a stale test/', async () => {
  const fake = (byKey) => async (url) => {
    const m = byKey[url.replace(/^https:\/\/[^/]+\//, '')];
    return m ? { ok: true, json: async () => m } : { ok: false, json: async () => null };
  };
  await assert.rejects(checkChannel(fake({ 'test/release/manifest.json': { data_release: 'r-old' }, 'release/manifest.json': live })),
                       /r-old/);
  await assert.rejects(checkChannel(fake({ 'release/manifest.json': live })), /could not be read/);
  const ok = await checkChannel(fake({ 'test/release/manifest.json': live, 'release/manifest.json': live }));
  assert.equal(ok.release.data_release, 'r-4230bc0c1dd5');
});
