// Re-cut the release-backed regulations oracles after a release: taps.py, verify_map.py, then hydrate.mjs.
//   node app/tools/regs-recut/sync.mjs
// Writes app/fixtures/golden/taps.json and hydrate.json. golden.json, pins.json, tokens.json and inventory.json are FROZEN:
// they were extracted from the regulations prototype (app/design/regs-v11.html, v38 @ 7fb6809f), which was deleted in T87
// (git keeps it). Nothing re-cuts them; the parity tests read them as fixtures.
// Commit the regenerated taps.json / hydrate.json with the release id in the message.
import { execFileSync } from 'node:child_process';
import fs from 'node:fs'; import path from 'node:path'; import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(HERE, '../../..');
const run = (cmd, a) => execFileSync(cmd, a, { cwd: REPO, stdio: ['ignore', 'inherit', 'inherit'] });
const py = path.join(REPO, '.venv/bin/python');
// taps.py and verify_map.py read the RELEASE's regs store (data/release/LATEST; P20: no regs.sqlite is kept)
if (!fs.existsSync(py) || !fs.existsSync(path.join(REPO, 'data/release/LATEST'))) {
  console.error('regs-recut: needs .venv and data/release/LATEST'); process.exit(1);
}
run(py, [path.join(HERE, 'taps.py')]);
run(py, [path.join(HERE, 'verify_map.py')]);
// after taps.py: hydrate.mjs reads golden/taps.json (its states and its meta.golden/version)
run('node', [path.join(HERE, 'hydrate.mjs')]);
