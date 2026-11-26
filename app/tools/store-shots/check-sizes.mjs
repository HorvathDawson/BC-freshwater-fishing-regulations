#!/usr/bin/env node
// (T1131) THE SIZE CHECK of the store-shots workflow: every framed PNG of every target in store-shots.json (or of `--only` targets) is
// exactly the target's declared pixel size, and each target holds its list's count. Exit 1 names each wrong file.
//   node app/tools/store-shots/check-sizes.mjs [--theme light] [--only a,b] [--dir <store graphics dir>]
import fs from 'node:fs'; import path from 'node:path';

const HERE = path.dirname(new URL(import.meta.url).pathname), APP = path.resolve(HERE, '../..');
const pngSize = (f) => { const b = fs.readFileSync(f); return [b.readUInt32BE(16), b.readUInt32BE(20)]; };
/** problems (strings) for the framed folders under `root` = <graphics>/store/<theme>; pure but for the reads */
export function sizeProblems(root, { shots, targets }, only = null) {
  const out = [];
  for (const [name, t] of Object.entries(targets)) {
    if (only && !only.includes(name)) continue;
    const dir = path.join(root, name), want = shots.filter((s) => !s.only || s.only.includes(name)).length;
    const files = fs.existsSync(dir) ? fs.readdirSync(dir).filter((f) => f.endsWith('.png')).sort() : [];
    if (files.length !== want) out.push(`${name}: ${files.length} pictures, the list has ${want}`);
    for (const f of files) { const [w, h] = pngSize(path.join(dir, f)); if (w !== t.w || h !== t.h) out.push(`${name}/${f}: ${w}x${h}, the target declares ${t.w}x${t.h}`); }
  }
  return out;
}
if (process.argv[1] && path.resolve(process.argv[1]) === new URL(import.meta.url).pathname) {
  const a = process.argv.slice(2), val = (k, d) => (a.includes(k) ? a[a.indexOf(k) + 1] : d);
  const graphics = path.resolve(val('--dir', path.resolve(APP, '../handoff/PLAN/app-integration/store/graphics')));
  const cfg = JSON.parse(fs.readFileSync(path.join(APP, 'tools/store-shots.json'), 'utf8'));
  const p = sizeProblems(path.join(graphics, 'store', val('--theme', 'light')), cfg, val('--only', null)?.split(','));
  if (p.length) { console.error(`store-shots sizes: ${p.length} problem(s)\n  ${p.join('\n  ')}`); process.exit(1); }
  console.log('store-shots sizes: every picture has its target\'s exact size');
}
