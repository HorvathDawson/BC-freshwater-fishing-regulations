#!/usr/bin/env node
// (T255) A TOUR for writing flows: walks the screens the scenarios use and saves, for each, a screenshot and every
// word on it (out/explore/NN-name.{png,txt}). Tolerant: a step that cannot be reached is noted and the tour goes on.
// Run inside a bounded session: scripts/heavy.sh testbed 30 -- bash app/tools/testbed/session.sh -- node app/tools/testbed/explore.mjs
import fs from 'node:fs';
import path from 'node:path';
import * as d from './lib/device.mjs';
import { flow } from './lib/maestro.mjs';

const OUT = path.join(path.dirname(new URL(import.meta.url).pathname), 'out', 'explore');
fs.mkdirSync(OUT, { recursive: true });
let n = 0;
function save(name) {
  const base = path.join(OUT, `${String(++n).padStart(2, '0')}-${name}`);
  d.screenshot(`${base}.png`);
  fs.writeFileSync(`${base}.txt`, d.screenText().join('\n'));
  console.log(`saved ${base}`);
}
function tryFlow(name, env) {
  const r = flow(name, env);
  if (!r.ok) console.log(`  flow ${name} ${JSON.stringify(env ?? {})} failed: ${r.said.split('\n').slice(-2).join(' / ')}`);
  return r.ok;
}
const tapText = (t) => tryFlow('tap-text', { TEXT: t });

const fresh = process.argv.includes('--fresh');
if (fresh) { d.stop(); d.clearData(); }
await d.net('wifi');
d.launch();
const t0 = Date.now();
for (let i = 0; i < 120; i++) {           // up to ~10 min for the first-run download
  const w = d.screenText();
  if (i % 6 === 0) console.log(`  [${Math.round((Date.now() - t0) / 1000)} s] ${w.slice(0, 6).join(' | ').slice(0, 200)}`);
  if (w.includes('More')) break;
  await d.sleep(5000);
}
save('first-screen');
tryFlow('dismiss-first-run'); await d.sleep(1500); save('after-dismiss');
tryFlow('open-updates-log'); await d.sleep(2000); save('updates-log');
tryFlow('updates-log-action', { ACTION: 'check' }); await d.sleep(6000); save('updates-log-checked');
d.sh('input keyevent 4'); await d.sleep(800); d.sh('input keyevent 4'); await d.sleep(800);
tapText('Map'); await d.sleep(1500);
tryFlow('tap-text', { TEXT: 'Choose layers' }); await d.sleep(1500); save('layers');
d.sh('input keyevent 4'); await d.sleep(800);
tryFlow('report-open'); await d.sleep(1500); save('report');
d.sh('input keyevent 4'); await d.sleep(800);
// the spot capture from "At my location": Cultus Lake's middle (a lake: any tap inside it picks it)
d.addPhoto(path.join(path.dirname(new URL(import.meta.url).pathname), 'media', 'testbed-photo.jpg'));
d.geo(-121.9870, 49.0535); await d.sleep(2000);
tapText('Spots'); await d.sleep(1200);
tapText('At my location'); await d.sleep(2500); save('mine-1');
tapText('(While using the app|Only this time|Allow)'); await d.sleep(4000); save('mine-2');
const [W, H] = d.sh('wm size').match(/(\d+)x(\d+)/).slice(1).map(Number);
for (let i = 3; i < 12; i++) {
  const words = d.screenText();
  if (words.some((x) => /^Save spot$/.test(x))) { save(`mine-${i}-save`); break; }
  const go = ['Continue', 'Next'].find((b) => words.includes(b));
  if (go) tapText(go);
  else if (words.some((x) => /Pick the water|Tap the stream|Now the exact spot|Tap the point/.test(x))) d.sh(`input tap ${W / 2} ${H / 2}`);
  await d.sleep(3000); save(`mine-${i}`);
}
tryFlow('tap-text', { TEXT: 'Add from photos' }); await d.sleep(4000); save('photo-picker');
console.log('tour done');
