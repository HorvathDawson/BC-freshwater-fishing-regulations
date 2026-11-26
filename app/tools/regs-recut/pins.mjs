// (T1135) RECORD THE RELEASE PINS on a release: sync.mjs's last step (it sets RELEASE_DIR to the release it re-cuts).
//   node app/tools/regs-recut/pins.mjs
// The pin-bearing tests (a test file that imports `pin` from the pins helper: app/conformance/src/pins.ts,
// pipeline/tests/pins.py) run with PIN_RECORD=<a scratch dir> — each `pin(name, value)` records instead of comparing —
// then each pins file is written from the records:
//   app/fixtures/golden/release-pins.json       (the app suite's)
//   pipeline/tests/fixtures/release-pins.json   (the pipeline suite's)
// A pin keeps its `answers` flag (typed by hand when the pin is born); its value is the release's. A recorded name the
// file does not hold is refused (add its row by hand). A pin in the file that no test recorded is DROPPED and named,
// when its suite's run passed; when the run failed, it may not have been reached, so it is KEPT and named: the release
// runs the same files after the re-cut, comparing, and lists their failures with every other (one pass). The files
// are deterministic: keys sorted, one pin per line, no clock.
import { execFileSync } from 'node:child_process';
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(HERE, '../../..');
export const ABOUT = '(T1135) values copied from a release into tests, read by pin(name, value); re-cut by '
  + 'node app/tools/regs-recut/sync.mjs. answers: true = words or pages an angler reads (a moved one stops the release); '
  + 'false = counts and ids (committed with it).';
export const SUITES = {
  app: { file: 'app/fixtures/golden/release-pins.json', root: 'app', test: /\.test\.tsx?$/,
         uses: /^import\s*\{[^}]*\bpin\b[^}]*\}\s*from\s*["'][^"']*\/pins(\.ts)?["']/m },
  pipeline: { file: 'pipeline/tests/fixtures/release-pins.json', root: 'pipeline/tests', test: /^test_.*\.py$/,
              uses: /^from pipeline\.tests\.pins import [^\n]*\bpin\b/m },
};
const SKIP = new Set(['node_modules', 'dist', '.expo', 'ios', 'android']);

/** Every value sorted by key, recursively: one spelling for one value. */
const sorted = (v) => Array.isArray(v) ? v.map(sorted)
  : v && typeof v === 'object' ? Object.fromEntries(Object.keys(v).sort().map((k) => [k, sorted(v[k])])) : v;

/** The pins file's one text for `pins` ({name: {answers, value}}). */
export function format(pins) {
  const names = Object.keys(pins).sort();
  return `{\n  "about": ${JSON.stringify(ABOUT)},\n  "pins": {\n`
    + names.map((n) => `    ${JSON.stringify(n)}: ${JSON.stringify(sorted(pins[n]))}`).join(',\n')
    + `${names.length ? '\n' : ''}  }\n}\n`;
}

/** The test files of a suite that use its pin helper, repo-relative, sorted. */
export function pinTests(suite, repo = REPO) {
  const s = SUITES[suite], out = [];
  const walk = (d) => {
    for (const n of fs.readdirSync(d)) {
      if (SKIP.has(n)) continue;
      const p = path.join(d, n);
      if (fs.statSync(p).isDirectory()) walk(p);
      else if (s.test.test(n) && s.uses.test(fs.readFileSync(p, 'utf8'))) out.push(path.relative(repo, p));
    }
  };
  walk(path.join(repo, s.root));
  return out.sort();
}

/** {name: value} recorded into `dir` by one suite. */
export function recorded(dir) {
  if (!fs.existsSync(dir)) return {};
  return Object.fromEntries(fs.readdirSync(dir).filter((n) => n.endsWith('.json')).sort().map((n) => {
    const r = JSON.parse(fs.readFileSync(path.join(dir, n), 'utf8'));
    return [r.name, r.value];
  }));
}

/** The new pins of a file from its old pins and the records: { pins, said, refused }. `passed`: the suite's run passed. */
export function merge(old, rec, passed) {
  const pins = {}, said = [], refused = [];
  for (const [n, value] of Object.entries(rec)) {
    if (!Object.hasOwn(old, n)) { refused.push(n); continue; }
    pins[n] = { answers: old[n].answers, value };
    if (JSON.stringify(sorted(old[n].value)) !== JSON.stringify(sorted(value))) said.push(`moved: ${n}`);
  }
  for (const n of Object.keys(old).filter((x) => !Object.hasOwn(rec, x)).sort()) {
    if (passed) said.push(`STALE, dropped: ${n} (no test asked for it)`);
    else { pins[n] = old[n]; said.push(`not recorded, KEPT: ${n} (its suite's run failed before it, above)`); }
  }
  return { pins, said, refused };
}

function runs(suite, files, dir) {
  if (!files.length) return 0;
  const env = { ...process.env, PIN_RECORD: dir, REGS_STORE_REQUIRED: '1' };
  const [cmd, args] = suite === 'app'
    ? ['pnpm', ['-C', path.join(REPO, 'app'), 'exec', 'vitest', 'run', ...files.map((f) => path.relative('app', f))]]
    : [path.join(REPO, '.venv/bin/python'), ['-m', 'pytest', '-q', '-p', 'no:cacheprovider', ...files]];
  try {
    execFileSync(cmd, args, { cwd: REPO, stdio: ['ignore', 'inherit', 'inherit'], env });
    return 0;
  } catch (e) {
    if (typeof e.status !== 'number') throw e;          // it did not run at all: no pins, never a pass
    return e.status;
  }
}

export function main() {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'release-pins-'));
  let bad = false;
  try {
    for (const suite of Object.keys(SUITES)) {
      const { file } = SUITES[suite], files = pinTests(suite);
      const rc = runs(suite, files, tmp);
      const abs = path.join(REPO, file);
      const old = JSON.parse(fs.readFileSync(abs, 'utf8')).pins;
      const { pins, said, refused } = merge(old, recorded(path.join(tmp, suite)), rc === 0);
      console.log(`release pins, ${suite} (${files.length} file(s)${rc ? `, the run FAILED rc=${rc}: those failures are not pins` : ''}):`
        + (said.length ? said.map((x) => `\n  ${x}`).join('') : ' nothing moved'));
      if (refused.length) {
        console.error(`release pins: ${file} has no row for ${refused.join(', ')}: add each by hand with its answers flag`);
        bad = true;
        continue;
      }
      fs.writeFileSync(abs, format(pins));
    }
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
  if (bad) process.exit(1);
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) main();
