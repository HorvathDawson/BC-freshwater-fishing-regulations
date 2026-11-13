// (T441) RE-CUT EVERY RELEASE-BACKED TEST FIXTURE for a release, in one step, in order:
//   node app/tools/regs-recut/sync.mjs [--release data/release/r-…]
// WHICH RELEASE (T666 item 2): `--release <dir>`, else $RELEASE_DIR (the release under test, the variable both test
// suites read), else the one data/release/LATEST names. `python -m pipeline release --staging` runs this against its
// CANDIDATE before the promote and commits the result (`pipeline/release_follows.py`), so no hand re-cut follows a
// release. A release NAMED and not there is an error, never passed over for LATEST.
//   1. taps.py      app/fixtures/golden/taps.json from app/fixtures/golden/states.json (the tap QUESTIONS: water, part,
//                   date; no answers), on the release's regs store (data/release/LATEST; P20: no regs.sqlite is kept)
//   2. page-oracle.mjs app/fixtures/golden/page-oracle.json: page v36's CODE over this release (page_blocks.py reads it
//                   through the Python reference decoder), every tapped golden state, rules by durable key (T667 5b;
//                   it replaced hydrate.mjs / hydrate.json, page v36's frozen data amended per ruling)
//   3. regs-fixture the committed fixture release app/packages/data/dev/regs-release/ (the waters taps.json names) AND
//                   app/packages/data/dev/section-lines.json, cut from the same release's own `lines` file
//   4. VM fixtures  app/packages/ui-native/src/regs/fixtures/*.json (UPDATE_REGS_FIXTURES=1, the conformance harness)
// Each step reads what the one before it wrote. Then `pnpm check`; commit the regenerated files with the release id in
// the message. Nothing else is frozen: golden.json, pins.json, tokens.json and inventory.json were retired in T439.
import { execFileSync } from 'node:child_process';
import fs from 'node:fs'; import path from 'node:path'; import { fileURLToPath } from 'node:url';
import { latest as latestRelease } from '../../conformance/src/latestRelease.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(HERE, '../../..');
const APP = path.join(REPO, 'app');
const py = path.join(REPO, '.venv/bin/python');
const argv = process.argv.slice(2), ri = argv.indexOf('--release');
if (ri >= 0 && !argv[ri + 1]) { console.error('regs-recut: --release needs a release folder'); process.exit(1); }
const named = ri >= 0 ? argv[ri + 1] : process.env.RELEASE_DIR || null;
let release;
if (named) {
  release = path.resolve(REPO, named);
  if (!fs.existsSync(path.join(release, 'manifest.json'))) {
    console.error(`regs-recut: no release at ${release} (${ri >= 0 ? '--release' : 'RELEASE_DIR'}): no manifest.json there`);
    process.exit(1);
  }
} else {
  release = latestRelease(path.join(REPO, 'data/release'));   // (T601) the one latest(): LATEST names it, never an mtime
}
if (!fs.existsSync(py) || release === null) {
  console.error('regs-recut: needs .venv and a release (--release <dir>, RELEASE_DIR, or data/release/LATEST)'); process.exit(1);
}
// every step reads THIS release: the Python steps by --release, the vitest step by the suites' own variable
const run = (cmd, a, env = {}) =>
  execFileSync(cmd, a, { cwd: REPO, stdio: ['ignore', 'inherit', 'inherit'], env: { ...process.env, RELEASE_DIR: release, ...env } });
const step = (n, what) => console.log(`\nregs-recut ${n}/4: ${what} (${path.basename(release)})`);
step(1, 'taps.json'); run(py, [path.join(HERE, 'taps.py'), '--release', release]);
step(2, 'page-oracle.json'); run('node', [path.join(HERE, 'page-oracle.mjs'), '--release', release]);
step(3, 'the fixture release + section-lines.json');
run(py, ['-m', 'pipeline.deliver.release.regs_fixture', '--release', release]);
step(4, 'the regs VM fixtures');
run('pnpm', ['-C', APP, 'exec', 'vitest', 'run', 'conformance/src/regs/harness'], { UPDATE_REGS_FIXTURES: '1' });
console.log('\nregs-recut: done. Next: pnpm -C app check, then commit the fixtures with the release id.');
