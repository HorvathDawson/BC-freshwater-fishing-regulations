// Stage the regulations prototype at a git ref so the port generators read the COMMITTED design, not a working copy
// another agent is still editing.
//   import { stage } from './stage.mjs'; const s = stage('HEAD'); ... s.page (absolute path to regs-v11.html); s.cleanup()
//   ref 'WORKTREE' reads the files in place (no copy).
// The prototype needs only these files to render (the fish pictures are file:// images and are not needed for strings).
import { execFileSync } from 'node:child_process';
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { fileURLToPath } from 'node:url';

export const HERE = path.dirname(fileURLToPath(import.meta.url));
export const REPO = path.resolve(HERE, '../../../..');
export const FILES = ['app/design/regs-v11.html', 'app/design/regs-v11/v36snap.js', 'app/design/regs-v11/watermap.js',
  'app/design/regs-v11/UI-NOTES.md', 'app/design/regs-v10/fish.js', 'app/design/regs-v10/fishart.js',
  'app/design/regs-v11/reference/page_v36.js'];

const git = (...a) => execFileSync('git', ['-C', REPO, ...a], { encoding: 'utf8', maxBuffer: 1 << 30 });

export function stage(ref = 'HEAD') {
  if (ref === 'WORKTREE') return { ref, sha: 'WORKTREE', root: REPO, page: path.join(REPO, FILES[0]), version: versionOf(path.join(REPO, FILES[3])), cleanup() {} };
  // the prototype's own last commit at that ref (what "regs vNN @ sha" means), not the ref's tip
  const sha = git('log', '-1', '--format=%h', ref, '--', FILES[0]).trim() || git('rev-parse', '--short', ref).trim();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'regs-port-'));
  for (const f of FILES) {
    const out = path.join(root, f); fs.mkdirSync(path.dirname(out), { recursive: true });
    fs.writeFileSync(out, execFileSync('git', ['-C', REPO, 'show', `${ref}:${f}`], { maxBuffer: 1 << 30 }));
  }
  return { ref, sha, root, page: path.join(root, FILES[0]), version: versionOf(path.join(root, FILES[3])),
    cleanup() { fs.rmSync(root, { recursive: true, force: true }); } };
}

// "# regs v32 (round 20h, …" -> "v32"
export function versionOf(notes) { const m = fs.readFileSync(notes, 'utf8').match(/^#\s*regs\s+(v\d+)/im); return m ? m[1] : 'v?'; }

export function args(argv = process.argv.slice(2)) {
  const o = { ref: 'HEAD', out: path.join(HERE, 'golden') };
  for (let i = 0; i < argv.length; i++) { const a = argv[i];
    if (a === '--ref') o.ref = argv[++i]; else if (a === '--out') o.out = path.resolve(argv[++i]);
    else if (a === '--only') o.only = argv[++i]; else if (a === '--quick') o.quick = true; }
  return o;
}
