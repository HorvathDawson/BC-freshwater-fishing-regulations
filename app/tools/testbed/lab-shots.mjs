// (T420) THE MAP LAB, LOOKED AT: the card, then the map at one camera with each switch flipped alone (labels, roads,
// landuse off; streams in one bucket; colour batches ≥ 250 ms apart, shot after a pan so late colour shows), each
// compared with today's map at the same camera (mean |ΔRGB| per pixel, and the share of pixels moved > 40). Every switch
// flipped back at the end and the foot read ("Today's map."). Inside a session (a lab build):
//
//   scripts/heavy.sh testbed 30 -- bash app/tools/testbed/session.sh -- node app/tools/testbed/lab-shots.mjs [out dir]
//   LAB_AT=lon,lat,zoom (default Coquitlam z13)
import fs from 'node:fs';
import path from 'node:path';
import * as device from './lib/device.mjs';
import { readPng, changed } from './lib/png.mjs';
import { openLab, setLab, closeLab, labState } from './lib/lab.mjs';
import { coldStart } from './scenarios/common.mjs';
import { cameraTo } from './scenarios/perf.mjs';

const HERE = path.dirname(new URL(import.meta.url).pathname);
const OUT = path.resolve(process.argv[2] ?? path.join(HERE, 'out/runs', `t420-lab-shots-${Date.now().toString(36)}`));
fs.mkdirSync(OUT, { recursive: true });
const [lon, lat, zoom] = (process.env.LAB_AT ?? '-122.78,49.28,13').split(',').map(Number);
const ctx = { device, flow: (f) => { throw new Error(`lab-shots: no Maestro here (asked for ${f})`); } };
const d = device;
const lines = [];
const say = (s) => { console.log(s); lines.push(s); };

/** Mean |ΔRGB| per pixel over the map area (status bar and tab bar left out), and the share moved > 40. */
function diff(a, b) {
  const A = readPng(path.join(OUT, a)), B = readPng(path.join(OUT, b));
  const r = { x: 0, y: Math.round(A.h * 0.06), w: A.w, h: Math.round(A.h * 0.8) };
  let s = 0, n = 0;
  for (let y = r.y; y < r.y + r.h; y += 2) for (let x = 0; x < A.w; x += 2) {
    const i = (y * A.w + x) * A.ch, j = (y * B.w + x) * B.ch;
    s += Math.abs(A.px[i] - B.px[j]) + Math.abs(A.px[i + 1] - B.px[j + 1]) + Math.abs(A.px[i + 2] - B.px[j + 2]); n++;
  }
  return `mean |ΔRGB| ${(s / n).toFixed(2)} /765, moved>40 ${(changed(A, B, r) * 100).toFixed(2)} %`;
}
const shot = (name) => { d.screenshot(path.join(OUT, name)); return name; };
async function place(ms = 6000) { cameraTo(ctx, { lon, lat, zoom }); await d.sleep(ms); }
/** A few swipes away and back, then the camera put back: what late colour (a gap between batches) would show. */
async function panAndBack() {
  const [w, h] = (/(\d+)x(\d+)/.exec(d.sh('wm size'))?.slice(1) ?? ['1080', '2400']).map(Number);
  const cx = w >> 1, cy = Math.round(h * 0.45), dx = Math.round(w * 0.35);
  for (const m of [-dx, -dx, dx, dx]) { d.sh(`input swipe ${cx} ${cy} ${cx + m} ${cy} 120`); await d.sleep(500); }
  await place(1500);
}

await coldStart(ctx, { ms: 90_000 });
await place(8000);
shot('00-default.png');
await openLab(); say(`card open; foot: ${labState()}`); shot('01-card.png');
await closeLab();
await place(3000); shot('02-default-again.png');
say(`default vs default again (noise): ${diff('00-default.png', '02-default-again.png')}`);

for (const [k, v, label] of [['labels', false, 'labels off'], ['roads', false, 'roads off'], ['landuse', false, 'landuse off'],
                             ['streamsOneBucket', true, 'streams in one bucket'], ['hideInvisible', true, 'invisible layers skipped']]) {
  const foot = await setLab({ [k]: v }); await closeLab();
  await place(6000);
  const f = shot(`1-${k}.png`);
  say(`${label}: foot "${foot}"; vs default ${diff('00-default.png', f)}`);
  await setLab({ [k]: !v }); await closeLab();
}

// colour gap: today's map vs gap 250, each shot 1.5 s after the camera came back from a fast pan
await place(4000); await panAndBack(); shot('2-gap0-after-pan.png');
const gf = await setLab({ stateGapMs: 250 }); await closeLab();
await place(4000); await panAndBack(); shot('2-gap250-after-pan.png');
await d.sleep(3000); shot('2-gap250-after-pan-4s.png');
say(`gap 250 (foot "${gf}"): after a pan vs today after the same pan ${diff('2-gap0-after-pan.png', '2-gap250-after-pan.png')}; ` +
    `vs at rest default ${diff('00-default.png', '2-gap250-after-pan-4s.png')}`);
await setLab({ stateGapMs: 0 });
const foot = labState(); shot('3-card-reset.png'); await closeLab();
say(`all back: foot "${foot}"`);
fs.writeFileSync(path.join(OUT, 'lab-shots.txt'), `${lines.join('\n')}\n`);
if (foot !== "Today's map.") { console.error('foot is not "Today\'s map."'); process.exit(1); }
