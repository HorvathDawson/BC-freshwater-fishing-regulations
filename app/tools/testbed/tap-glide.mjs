// (T420) ANR HUNT: flings with a tap on the map right after each one (mid-glide), so water pages open while the map
// still moves — the path on which the patched MapLibre build hung once (t420-j3-fv11-r2-patched-long). Each opened page
// is closed with Back. After every fling: "isn't responding" on screen = an ANR (shot + the app's /data/anr files kept).
// Inside a session (stressed emulator for the A/B):
//
//   node app/tools/testbed/tap-glide.mjs <out dir>     TAPG_AT=lon,lat,zoom (FV z11)  TAPG_ROUNDS=6  TAPG_SWIPES=16
import fs from 'node:fs';
import path from 'node:path';
import * as d from './lib/device.mjs';
import { coldStart } from './scenarios/common.mjs';
import { cameraTo } from './scenarios/perf.mjs';

const OUT = path.resolve(process.argv[2] ?? 'tap-glide');
fs.mkdirSync(OUT, { recursive: true });
const [lon, lat, zoom] = (process.env.TAPG_AT ?? '-122.05,49.25,11').split(',').map(Number);
const ROUNDS = Number(process.env.TAPG_ROUNDS ?? 6), SWIPES = Number(process.env.TAPG_SWIPES ?? 16);
const ctx = { device: d, flow: (f) => { throw new Error(`tap-glide: no Maestro here (asked for ${f})`); } };
const log = [];
const say = (s) => { console.log(s); log.push(s); };
const anrFiles = () => d.sh("su 0 sh -c 'ls /data/anr/ 2>/dev/null'").split('\n').map((s) => s.trim()).filter((s) => /^anr_/.test(s));

const before = new Set(anrFiles());
await coldStart(ctx, { ms: 120_000 });
cameraTo(ctx, { lon, lat, zoom });
await d.sleep(6000);
const [w, h] = (/(\d+)x(\d+)/.exec(d.sh('wm size'))?.slice(1) ?? ['1080', '2400']).map(Number);
const cx = w >> 1, cy = Math.round(h * 0.45), dx = Math.round(w * 0.35), dy = Math.round(h * 0.18);
const loop = [[-dx, 0], [0, dy], [dx, 0], [0, -dy]];
const taps = [[0.3, 0.3], [0.6, 0.4], [0.45, 0.55], [0.7, 0.25], [0.25, 0.5]];
let opens = 0, anr = 0, n = 0;
outer: for (let r = 1; r <= ROUNDS; r++) {
  for (let i = 0; i < SWIPES; i++, n++) {
    const [mx, my] = loop[i % loop.length];
    d.sh(`input swipe ${cx} ${cy} ${cx + mx} ${cy + my} 120`);
    const [tx, ty] = taps[n % taps.length];
    d.sh(`input tap ${Math.round(w * tx)} ${Math.round(h * ty)}`);   // mid-glide
    await d.sleep(1500);
    let words = d.screenText();
    if (words.some((x) => /isn.t responding/.test(x))) {
      anr++; d.screenshot(path.join(OUT, `anr-r${r}-s${i}.png`));
      say(`ANR at round ${r} swipe ${i} (${opens} pages opened so far)`);
      break outer;
    }
    if (words.some((x) => /^What you can keep$|^Regulations$/.test(x))) {
      opens++;
      if (opens <= 2) d.screenshot(path.join(OUT, `page-${opens}.png`));
      for (let k = 0; k < 3 && !words.includes('MORE'); k++) { d.sh('input keyevent 4'); await d.sleep(1200); words = d.screenText(); }
      if (!words.includes('MORE')) { say(`lost the map after a page (round ${r} swipe ${i}): relaunch`); await coldStart(ctx, { ms: 120_000 }); cameraTo(ctx, { lon, lat, zoom }); await d.sleep(4000); }
    }
  }
  say(`round ${r}: ${opens} pages opened so far`);
}
const added = anrFiles().filter((f) => !before.has(f));
const app = added.filter((f) => /ca\.canifishthis/.test(d.sh(`su 0 sh -c 'head -c 2000 /data/anr/${f}'`)));
for (const f of app) fs.writeFileSync(path.join(OUT, f), d.sh(`su 0 cat /data/anr/${f}`));
say(`RESULT flings ${n} · pages opened ${opens} · ANR dialogs ${anr} · new app ANR files ${app.length} (${app.join(', ') || 'none'})`);
fs.writeFileSync(path.join(OUT, 'tap-glide.txt'), `${log.join('\n')}\n`);
