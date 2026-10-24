// Function inventory of the code the app ports: the prototype's inline script and page v36's logic, with line ranges and a
// hash of each body, so a ticket can name "shortOf (regs-v11.html)" and a re-sync can say which ported functions changed.
//   node app/design/regs-v11/port/inventory.mjs [--ref HEAD|<sha>|WORKTREE] [--out dir]
//   -> <out>/inventory.json { meta, files: {<path>: [{name, kind, start, end, sha}]} }
//   node app/design/regs-v11/port/inventory.mjs --diff <old inventory.json> [--ref …]   prints added / removed / changed bodies
// A top-level function or const arrow/function; nested helpers of buildKeep/buildGear/licence (const x = … => inside them)
// are listed too, prefixed by their parent ("buildKeep.shortOf"), because the port names those.
import fs from 'node:fs'; import path from 'node:path'; import crypto from 'node:crypto';
import { stage, args } from './stage.mjs';

const argv = process.argv.slice(2), di = argv.indexOf('--diff'), old = di >= 0 ? argv.splice(di, 2)[1] : null;
const A = args(argv); fs.mkdirSync(A.out, { recursive: true });
const S = stage(A.ref);

const TARGETS = [['app/design/regs-v11.html', 'script'], ['app/design/regs-v11/reference/page_v36.js', 'js']];
function scan(text, mode) {
  // top-level declarations start at column 0 in both files; a nested helper is an indented `const x = … =>` / `function x(`
  // inside a top-level body. A body ends where the next column-0 declaration (or the script's end) starts.
  const lines = text.split('\n'); let from = 0, to = lines.length;
  if (mode === 'script') { from = lines.findIndex(l => /^<script>\s*$/.test(l)); to = lines.findIndex((l, i) => i > from && /^<\/script>/.test(l)); }
  const TOP = /^(?:async\s+)?function\s+([A-Za-z_$][\w$]*)\s*\(|^(?:const|let)\s+([A-Za-z_$][\w$]*)\s*=/;
  const NEST = /^\s+(?:(?:async\s+)?function\s+([A-Za-z_$][\w$]*)\s*\(|(?:const|let)\s+([A-Za-z_$][\w$]*)\s*=\s*(?:async\s*)?(?:function\b|\([^)]*\)\s*=>|[A-Za-z_$][\w$]*\s*=>))/;
  const out = []; let top = null;
  for (let i = from + 1; i < to; i++) { const l = lines[i]; let m = l.match(TOP);
    if (m) { top = { name: m[1] || m[2], kind: m[1] ? 'function' : 'const', start: i + 1 }; out.push(top); continue; }
    m = top && l.match(NEST); if (m) out.push({ name: `${top.name}.${m[1] || m[2]}`, kind: 'nested', start: i + 1, parent: top.name }); }
  const tops = out.filter(o => o.kind !== 'nested');
  tops.forEach((o, j) => { o.end = (tops[j + 1]?.start ?? to + 1) - 1; while (o.end > o.start && !lines[o.end - 1].trim()) o.end--; });
  const byName = Object.fromEntries(tops.map(o => [o.name, o]));
  out.filter(o => o.kind === 'nested').forEach((o, j, ns) => { const nx = ns[j + 1]; o.end = nx && nx.parent === o.parent ? nx.start - 1 : byName[o.parent].end; });
  for (const o of out) o.sha = crypto.createHash('sha1').update(lines.slice(o.start - 1, o.end).join('\n')).digest('hex').slice(0, 10);
  return out;
}
const inv = { meta: { generator: 'app/design/regs-v11/port/inventory.mjs', ref: S.ref, sha: S.sha, version: S.version }, files: {} };
for (const [f, mode] of TARGETS) inv.files[f] = scan(fs.readFileSync(path.join(S.root, f), 'utf8'), mode);
S.cleanup();
if (old) { const o = JSON.parse(fs.readFileSync(old, 'utf8'));
  for (const f of Object.keys(inv.files)) { const a = Object.fromEntries((o.files[f] || []).map(x => [x.name, x])), b = Object.fromEntries(inv.files[f].map(x => [x.name, x]));
    const add = Object.keys(b).filter(k => !a[k]), rem = Object.keys(a).filter(k => !b[k]), chg = Object.keys(b).filter(k => a[k] && a[k].sha !== b[k].sha);
    console.log(`${f}  (${o.meta.version}@${o.meta.sha} -> ${inv.meta.version}@${inv.meta.sha})\n  changed: ${chg.join(', ') || '—'}\n  added:   ${add.join(', ') || '—'}\n  removed: ${rem.join(', ') || '—'}`); }
} else { const file = path.join(A.out, 'inventory.json'); fs.writeFileSync(file, JSON.stringify(inv, null, 1));
  console.log(`inventory: ${Object.entries(inv.files).map(([f, xs]) => `${path.basename(f)} ${xs.length}`).join(', ')} (regs ${S.version} @ ${S.sha}) -> ${path.relative(process.cwd(), file)}`); }
