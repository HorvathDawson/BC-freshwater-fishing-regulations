// (T810 V-py) the `side <dir>` scenario's pure half: no upload, no network. Run: node --test deploy/staging/test-updates.test.mjs
// (pipeline/tests/test_trout_vectors.py runs it). What it pins: a side manifest's data files are reached from the app's
// one release base (`${origin}/release/`, on the test channel too) only through "../test/release/<id>/…"; every key the
// scenario writes is under test/; a release without the trout section is refused; the staging data worker resolves the
// dot segment itself (a client that sends the url as written still reads the test/ key).
import assert from 'node:assert/strict';
import { test } from 'node:test';
import worker from './data-worker.js';
import { bumpPdf, bumpSqlite, dataBookManifest, keyOf, sideManifest, sideUploads, SIDE_PREFIX } from './test-updates.mjs';

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

test('the side release\'s own synopsis goes to test/ with its data; a synopsis outside its folder keeps its url', () => {
  // (T810 bench 2026-11-18) the synopsis was left on release/, where staging never held it: a 404 on first install
  const own = { ...man(), synopsis: { url: 'r-0123456789ab/synopsis-cccc.pdf', sha256: 'c', bytes: 1 } };
  assert.equal(keyOf(sideManifest(own).synopsis.url), 'test/release/r-0123456789ab/synopsis-cccc.pdf');
  assert.ok(sideUploads(own).some(([local, key]) => local === 'r-0123456789ab/synopsis-cccc.pdf'
                                                   && key === 'test/release/r-0123456789ab/synopsis-cccc.pdf'));
  const live = { ...man(), synopsis: { url: 'r-live00000000/synopsis-dddd.pdf', sha256: 'd', bytes: 1 } };
  assert.deepEqual(sideManifest(live).synopsis, live.synopsis);
  assert.ok(!sideUploads(live).some(([local]) => local.includes('synopsis')));
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

// (T912) `data-book`: one data file and the rule book with other bytes, the same content
test('data-book: the SQLite change counter moves (the header stays trusted); the PDF gains a comment; the manifest names the test/ copies', () => {
  const head = new Uint8Array(100);
  head.set(new TextEncoder().encode('SQLite format 3\0'));
  new DataView(head.buffer).setUint32(24, 7); new DataView(head.buffer).setUint32(92, 7);
  const out = bumpSqlite(head), v = new DataView(out.buffer);
  assert.deepEqual([v.getUint32(24), v.getUint32(92)], [8, 8]);
  assert.equal(new DataView(head.buffer).getUint32(24), 7);                          // the input is not changed
  assert.throws(() => bumpSqlite(new Uint8Array(100)), /not a SQLite file/);
  const pdf = new TextEncoder().encode('%PDF-1.7\n...%%EOF\n');
  const b = bumpPdf(pdf);
  assert.deepEqual(b.subarray(0, pdf.length), pdf);
  assert.ok(b.length > pdf.length);
  assert.throws(() => bumpPdf(new Uint8Array(10)), /not a PDF/);
  const padded = bumpPdf(pdf, 5000);
  assert.equal(padded.length, b.length + 5000);
  assert.deepEqual(padded.subarray(0, b.length), b);
  assert.ok(new TextDecoder().decode(padded.subarray(b.length)).split('\n').every((l) => l === '' || l.startsWith('%')));
  const live = { ...man(), synopsis: { url: 'r-0123456789ab/synopsis-eeee.pdf', sha256: 'e', bytes: 1 },
                 files: { ...man().files, climate: { url: 'r-0123456789ab/climate-cccc.sqlite.gz', sha256: 'c', bytes: 1 } } };
  const m = dataBookManifest(live, 'r-x-book5', 'climate', { url: '../test/release/r-x-book5/climate-dddd.sqlite.gz', sha256: 'd', bytes: 2 },
                             { url: '../test/release/r-x-book5/synopsis-ffff.pdf', sha256: 'f', bytes: 3 });
  assert.equal(keyOf(m.files.climate.url), 'test/release/r-x-book5/climate-dddd.sqlite.gz');
  assert.equal(keyOf(m.synopsis.url), 'test/release/r-x-book5/synopsis-ffff.pdf');
  assert.deepEqual(m.files.water, live.files.water);                                 // every other file: the live one
  assert.equal(live.synopsis.sha256, 'e');                                           // the live manifest is not changed
});
