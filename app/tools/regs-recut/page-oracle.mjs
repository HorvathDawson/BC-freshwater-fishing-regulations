// THE PAGE ORACLE ON THE RELEASE (T667 slice 5b, owner D1 2026-11-11): what page v36's OWN code — page_v36.js up to
// `function segBar` (makePlace, settle, buildModel, dayStatus/strip), frozen, never the app's port — says of every golden
// state (taps.json's tapped states) when it reads the RELEASE UNDER TEST:
//   node app/tools/regs-recut/page-oracle.mjs --release data/release/r-… [--out dir]
//   -> <out>/page-oracle.json   { meta, states: {<id>: {s, via, ranks, settle, model, dayStatus}} }
// The page's data blocks come from the release through the PYTHON reference decoder (page_blocks.py:
// `province_store` -> the answers file + the export slice), never through the app's TypeScript read path, which is what
// the parities (settle / card) test. So the oracle is two independent paths — the page's code over the Python's read —
// against the app's code over the app's read, as taps.json is.
// It replaces hydrate.json, which ran the same code over page v36's FROZEN data blocks and so needed a hand amendment per
// approved ruling (v36-rulings.json); here every ruling is already in the release's answers.
// Every rule is named by its DURABLE KEY ("entry::rule"), never a rix: a release that only renumbers rewrites nothing
// here, and a change in this file is a change in what a golden state answers (pipeline/release_follows.py: the
// `page-oracle` row STOPS the release for --accept-fixtures, T666 / owner D5). Node + .venv; no network.
import fs from 'node:fs'; import path from 'node:path'; import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(HERE, '../../..');
const argv = process.argv.slice(2), opt = (k) => { const i = argv.indexOf(k); return i >= 0 ? argv[i + 1] : undefined; };
const release = opt('--release') ?? process.env.RELEASE_DIR;
if (!release) { console.error('page-oracle: --release <dir> (or RELEASE_DIR) is required'); process.exit(1); }
const OUT = opt('--out') ? path.resolve(opt('--out')) : path.join(REPO, 'app/fixtures/golden');
const REF = path.join(REPO, 'app/fixtures/reference/');
const TAPS = path.join(OUT, 'taps.json');

const taps = JSON.parse(fs.readFileSync(TAPS, 'utf8'));
const raw = execFileSync(path.join(REPO, '.venv/bin/python'), [path.join(HERE, 'page_blocks.py'), '--release', path.resolve(REPO, release), '--taps', TAPS],
  { cwd: REPO, encoding: 'utf8', maxBuffer: 1 << 30 });
const B = JSON.parse(raw);
const text = { data: JSON.stringify(B.data), answers: JSON.stringify(B.answers), cases: JSON.stringify({ about: {} }) };
const cal = fs.readFileSync(REF + 'calendar.generated.js', 'utf8');
const js = fs.readFileSync(REF + 'page_v36.js', 'utf8').split('\n');
const end = js.findIndex((l) => /^function segBar\(/.test(l));
if (end < 0) throw new Error('page_v36.js: no `function segBar` — the decode layer moved; update page-oracle.mjs');
const YEAR = taps.meta.year;
const body = `${cal}\n${js.slice(0, end).join('\n')}\nYEAR = ${YEAR};\nreturn { D, makePlace, buildModel, strip };`;
const P = new Function('document', body)({ getElementById: (id) => ({ textContent: text[id] }) });

const inf = (x) => (x === Infinity ? 'Infinity' : x);
const key = (r) => (r == null ? null : r.key);
const keys = (l) => l.map(key);
const states = {}, bad = [];
for (const [id, t] of Object.entries(taps.states)) {
  const wi = P.D.waters.findIndex((w) => w.id === t.item);
  if (wi < 0) { bad.push(`${id}: no water ${t.item} in the blocks`); continue; }
  const w = P.makePlace(wi, Number(t.partIx));
  if (!w.key) { bad.push(`${id}: part ${t.partIx} of ${t.item} has no key`); continue; }
  const m = P.buildModel(w, Number(t.md)), c = m.ctx;
  if (c.s !== t.s) bad.push(`${id}: the page reads segment ${c.s}, taps.json ${t.s}`);
  const via = {}, ranks = {};
  for (const r of w.cands) { if (r.via === 'trib') via[r.key] = 'trib'; if (r.rank !== r.baseRank) ranks[r.key] = r.rank; }
  const res = (h) => h && { status: h.status, daily: inf(h.daily), win: key(h.win), narrow: key(h.narrow) };
  states[id] = {
    s: c.s, via, ranks,
    settle: { status: c.status?.status ?? null, broad: keys(c.broad), speaks: keys(c.speaks), beside: keys(c.beside), shown: keys(c.shown),
      inpart: c.inpart.map((r) => [r.key, r.now]), lifted: [...c.lifted].map(([a, b]) => [P.D.rid[a], b == null ? null : P.D.rid[b]]),
      timed: keys(c.timed), side: keys(c.side), standing: keys(c.standing), anglerclosed: keys(c.anglerclosed) },
    model: { spp: m.spp, line: m.line, missingRows: m.missingRows, status: m.status, broad: keys(m.broad),
      rows: m.rows.map((r) => ({ kind: r.kind, pool: key(r.pool), win: key(r.win), daily: inf(r.daily), narrow: key(r.narrow), members: r.members })),
      R: Object.fromEntries(Object.entries(m.R).map(([k, v]) => [k, { H: res(v.H), W: res(v.W) }])) },
    dayStatus: m.rows.map((r) => P.strip(w, r.members)),
  };
}
if (bad.length) { console.error(bad.join('\n')); process.exit(1); }
// meta: what the content depends on and nothing that moves per build (no release id, no digest), so a renumber-only
// release leaves the file byte-identical
const meta = { generator: 'app/tools/regs-recut/page-oracle.mjs', page: 'page_v36.js', decodeLines: `1-${end}`,
  golden: taps.meta.golden, version: taps.meta.version, year: YEAR };
fs.writeFileSync(path.join(OUT, 'page-oracle.json'), JSON.stringify({ meta, states }, null, 1) + '\n');
console.log(`page-oracle: page v36's code over ${path.basename(release)}: ${Object.keys(states).length} golden states -> ${path.relative(REPO, path.join(OUT, 'page-oracle.json'))}`);
