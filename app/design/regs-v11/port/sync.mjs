// Re-sync the app port with the prototype: regenerate every port artifact at a ref and say what moved.
//   node app/design/regs-v11/port/sync.mjs [--ref HEAD|<sha>|WORKTREE]
// Runs golden.mjs, pinreplay.mjs, tokens.mjs, inventory.mjs, taps.py and then hydrate.mjs into app/design/regs-v11/port/golden/, then prints
//   - the functions whose bodies changed (inventory diff: the tickets that port them need a look),
//   - per state and per tab, which golden blocks changed (the app tests that will now fail, and why),
//   - token values that changed (tokens.md / the theme JSON need a look).
// Commit the regenerated golden/ with the prototype version in the message; the app's parity tests read these files.
import { execFileSync } from 'node:child_process';
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path';
import { HERE, REPO, args } from './stage.mjs';

const A = args(), OUT = path.join(HERE, 'golden'), TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'regs-sync-'));
const keep = f => fs.existsSync(path.join(OUT, f)) && fs.copyFileSync(path.join(OUT, f), path.join(TMP, f));
['golden.json', 'tokens.json', 'inventory.json'].forEach(keep);
const run = (cmd, a) => execFileSync(cmd, a, { cwd: REPO, stdio: ['ignore', 'inherit', 'inherit'] });
run('node', [path.join(HERE, 'golden.mjs'), '--ref', A.ref]);
run('node', [path.join(HERE, 'pinreplay.mjs')]);
run('node', [path.join(HERE, 'tokens.mjs'), '--ref', A.ref]);
if (fs.existsSync(path.join(TMP, 'inventory.json'))) run('node', [path.join(HERE, 'inventory.mjs'), '--ref', A.ref, '--diff', path.join(TMP, 'inventory.json')]);
run('node', [path.join(HERE, 'inventory.mjs'), '--ref', A.ref]);
const py = path.join(REPO, '.venv/bin/python');
if (fs.existsSync(py) && fs.existsSync(path.join(REPO, 'data/generated/bundle/regs.sqlite'))) { run(py, [path.join(HERE, 'taps.py')]); run(py, [path.join(HERE, 'verify_map.py')]); }
else console.log('taps: skipped (no .venv or no data/generated/bundle/regs.sqlite)');
// after taps.py: hydrate.mjs reads golden/taps.json (its states and its meta.golden/version), so it must see this sync's taps
run('node', [path.join(HERE, 'hydrate.mjs'), '--ref', A.ref]);

const J = f => JSON.parse(fs.readFileSync(f, 'utf8'));
if (fs.existsSync(path.join(TMP, 'golden.json'))) {
  const a = J(path.join(TMP, 'golden.json')), b = J(path.join(OUT, 'golden.json'));
  console.log(`\ngolden diff ${a.meta.version}@${a.meta.sha} -> ${b.meta.version}@${b.meta.sha}`);
  const blocks = ['header', 'keep', 'gear', 'methods', 'licence', 'sheets', 'pins'], tally = {};
  for (const id of new Set([...Object.keys(a.states), ...Object.keys(b.states)])) {
    const x = a.states[id], y = b.states[id];
    if (!x || !y) { console.log(`  ${id}: ${x ? 'removed' : 'added'}`); continue; }
    const ch = blocks.filter(k => JSON.stringify(x[k]) !== JSON.stringify(y[k]));
    ch.forEach(k => { tally[k] = (tally[k] || 0) + 1; });
  }
  console.log('  states changed per block: ' + (Object.entries(tally).map(([k, n]) => `${k} ${n}`).join(', ') || 'none'));
  const gs = new Set([...Object.keys(a.sheets), ...Object.keys(b.sheets)]);
  console.log(`  global sheets changed: ${[...gs].filter(k => JSON.stringify(a.sheets[k]) !== JSON.stringify(b.sheets[k])).length}`);
  if (JSON.stringify(a.meta.misses) !== JSON.stringify(b.meta.misses)) console.log(`  selector misses now: ${b.meta.misses.join(', ')}  (was ${a.meta.misses.join(', ')})`);
}
if (fs.existsSync(path.join(TMP, 'tokens.json'))) {
  const a = J(path.join(TMP, 'tokens.json')), b = J(path.join(OUT, 'tokens.json'));
  for (const th of ['light', 'dark']) for (const k of new Set([...Object.keys(a.tokens[th]), ...Object.keys(b.tokens[th])]))
    if (a.tokens[th][k] !== b.tokens[th][k]) console.log(`  token ${th} ${k}: ${a.tokens[th][k]} -> ${b.tokens[th][k]}`);
}
fs.rmSync(TMP, { recursive: true, force: true });
