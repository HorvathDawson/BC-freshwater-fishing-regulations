// (T810 V-py) the `side <dir>` scenario's pure half: no upload, no network. Run: node --test deploy/staging/test-updates.test.mjs
// (pipeline/tests/test_trout_vectors.py runs it). What it pins: a side manifest's data files are reached from the app's
// one release base (`${origin}/release/`, on the test channel too) only through "../test/release/<id>/…"; every key the
// scenario writes is under test/; a release without the trout section is refused; the staging data worker resolves the
// dot segment itself (a client that sends the url as written still reads the test/ key).
import assert from 'node:assert/strict';
import { test } from 'node:test';
import worker from './data-worker.js';
import { keyOf, sideManifest, sideUploads, SIDE_PREFIX } from './test-updates.mjs';

const man = (trout = true) => ({
  format: 'app-manifest/2', data_release: 'r-0123456789ab',
  regulations: { versions: { rows: 33, ...(trout ? { trout: 1 } : {}) } },
  files: { water: { url: 'r-0123456789ab/water-aaaa.sqlite.gz', sha256: 'a' },
           'regs-1': { url: 'r-0123456789ab/regs-1-bbbb.sqlite.gz', sha256: 'b' } },
  tiles: { compatible: ['h.*'], current: { id: 'h.t', url: 'tiles/h.t/atlas.pmtiles' } },
});

test('a side manifest names test/release/<id>/ through the one release base the app reads', () => {
  const t = sideManifest(man());
  for (const [name, f] of Object.entries(t.files)) {
    assert.equal(keyOf(f.url), `${SIDE_PREFIX}${man().files[name].url}`);
    assert.equal(f.url.split('/').pop(), man().files[name].url.split('/').pop());   // the app stores it by its last segment
  }
  assert.deepEqual(t.tiles, man().tiles);                                            // the live tiles, read only
  assert.equal(keyOf(man().files.water.url), 'release/r-0123456789ab/water-aaaa.sqlite.gz');  // a plain url: release/
});

test('every key the scenario writes is under test/', () => {
  for (const [, key] of sideUploads(man())) assert.ok(key.startsWith('test/release/r-0123456789ab/'), key);
});

test('a release without the trout section is refused', () => {
  assert.throws(() => sideManifest(man(false)), /no trout section/);
});

test('the staging data worker reads the dot-segment url as the test/ key, whatever the client sent', async () => {
  const keyed = [];
  const env = { BUCKET: { async get(k) { keyed.push(k); return null; }, async head() { return null; } } };
  // a client that sends the url as written (no normalising): the worker's own URL parse resolves "../"
  await worker.fetch({ url: 'https://d.example/release/../test/release/r-1/f.gz', method: 'GET', headers: new Headers() }, env);
  assert.deepEqual(keyed, ['test/release/r-1/f.gz']);
});
