// (T452) A FLING'S GLIDE, MEASURED FRAME BY FRAME: the patched binding's camera probe (`adb shell setprop log.tag.CifCam D`,
// MLRNMapView `camProbe`: `m` each camera sample MapLibre makes — time, centre, zoom; `f` each frame rendered; `v` each
// vsync while the camera moves; `u` the finger's lift; `d` each `onRegionDidChange` sent to JS) and SurfaceFlinger's
// present times for the map's surface. Per fling:
//   steps     the camera's travel per sample (dp) — a smooth glide's steps shrink evenly; "uneven" counts steps more than
//             25 % off the mean of their two neighbours (a judder even at 60 fps)
//   gaps      sample-to-sample intervals (ms) — over 20 ms is a missed frame
//   phase     where each sample falls after its vsync (ms): a wide spread means the camera is sampled at uneven moments
//   presents  the map surface's frame intervals during the glide (SurfaceFlinger)
//   settles   `onRegionDidChange` events during the glide (JS settle work while it moves)
// GLIDE_VARIANTS / log-tag switches were the T452 experiment build's (MapLibre's fling vs the binding's own glide, per-frame
// events off); the shipped patch has the own glide only, so a run is one variant (`base`) unless such a build is made.
//
//   node app/tools/testbed/run.mjs glide      (GLIDE_AT=lon,lat,zoom; GLIDE_N=flings per variant, default 8; GLIDE_MS=120)
import fs from 'node:fs';
import path from 'node:path';
import { coldStart, toMap } from './common.mjs';
import { cameraTo } from './perf.mjs';
import { mapLayer, presents } from './pan.mjs';

const AT = (process.env.GLIDE_AT ?? '-122.05,49.25,11').split(',').map(Number);
const N = Number(process.env.GLIDE_N ?? 8);
const MS = Number(process.env.GLIDE_MS ?? 120);
const VARIANTS = (process.env.GLIDE_VARIANTS ?? 'base').split(',');
const PROPS = { noev: 'CifNoEv', own: 'CifGlide' };

/** Web Mercator, in dp at zoom z (512 dp tiles, MapLibre's). */
const mx = (lon, z) => ((lon + 180) / 360) * 512 * 2 ** z;
const my = (lat, z) => { const p = (lat * Math.PI) / 180; return ((1 - Math.log(Math.tan(p) + 1 / Math.cos(p)) / Math.PI) / 2) * 512 * 2 ** z; };
const q = (a, p) => { const s = [...a].sort((x, y) => x - y); return s.length ? Math.round(s[Math.min(s.length - 1, Math.floor(p * s.length))] * 10) / 10 : null; };

/** One fling's probe lines → its numbers. */
export function glideStats(lines, presentNs = []) {
  const ev = lines.map((l) => /CifCam\s*:\s*(\w) (.*)$/.exec(l)).filter(Boolean).map((m) => [m[1], m[2].split(' ')]);
  const up = ev.find(([k]) => k === 'u');
  if (!up) return null;
  const tUp = Number(up[1][0]);
  const ms = ev.filter(([k, a]) => k === 'm' && Number(a[0]) >= tUp).map(([, a]) => ({ t: Number(a[0]), lon: Number(a[1]), lat: Number(a[2]), z: Number(a[3]) }));
  const vs = ev.filter(([k]) => k === 'v').map(([, a]) => Number(a[0])).sort((a, b) => a - b);
  const settles = ev.filter(([k, a]) => k === 'd' && Number(a[0]) >= tUp).map(([, a]) => Number(a[0]));
  if (ms.length < 4) return { samples: ms.length, settles: settles.length };
  const z = ms[0].z;
  const steps = [], gaps = [], phase = [];
  for (let i = 1; i < ms.length; i++) {
    steps.push(Math.hypot(mx(ms[i].lon, z) - mx(ms[i - 1].lon, z), my(ms[i].lat, z) - my(ms[i - 1].lat, z)));
    gaps.push((ms[i].t - ms[i - 1].t) / 1e6);
  }
  for (const m of ms) {
    let lo = -1;
    for (const v of vs) if (v <= m.t) lo = v; else break;
    if (lo >= 0) phase.push((m.t - lo) / 1e6);
  }
  // uneven: a step more than 25 % off its neighbours' mean, among steps still moving (> 1 dp)
  let uneven = 0, moving = 0;
  for (let i = 1; i < steps.length - 1; i++) {
    const ref = (steps[i - 1] + steps[i + 1]) / 2;
    if (ref < 1) continue;
    moving += 1;
    if (Math.abs(steps[i] - ref) / ref > 0.25) uneven += 1;
  }
  // speed per sample (dp/s), and how evenly it falls: the worst rise between samples (a smooth glide never speeds up)
  const speed = steps.map((s, i) => s / (gaps[i] / 1000));
  let rises = 0;
  for (let i = 1; i < speed.length; i++) if (speed[i] > speed[i - 1] * 1.25 && speed[i - 1] > 60) rises += 1;
  const tEnd = ms.at(-1).t;
  const pr = [...new Set(presentNs)].filter((t) => t >= tUp && t <= tEnd + 50e6).sort((a, b) => a - b);
  const pg = [];
  for (let i = 1; i < pr.length; i++) pg.push((pr[i] - pr[i - 1]) / 1e6);
  return {
    vDp: Math.round(Number(up[1][2])), glideMs: Math.round((tEnd - tUp) / 1e6), samples: ms.length,
    travelDp: Math.round(steps.reduce((a, b) => a + b, 0)),
    gap: { p50: q(gaps, 0.5), p95: q(gaps, 0.95), max: q(gaps, 1), over20: gaps.filter((g) => g > 20).length },
    uneven, moving, rises,
    phase: { p5: q(phase, 0.05), p50: q(phase, 0.5), p95: q(phase, 0.95) },
    presents: { n: pg.length, p50: q(pg, 0.5), p95: q(pg, 0.95), max: q(pg, 1), over20: pg.filter((g) => g > 20).length },
    settles: settles.length, settlesMid: settles.filter((t) => t < tEnd - 30e6).length,
    firstSpeed: Math.round(speed[0] ?? 0), speeds: speed.slice(0, 40).map((s) => Math.round(s)),
  };
}

export const glide = {
  id: 'glide',
  title: `A fling's glide frame by frame: ${N} flings × ${VARIANTS.join('/')} at ${AT.join(',')} (T452)`,
  async run(ctx) {
    const d = ctx.device;
    const [w, h] = (/(\d+)x(\d+)/.exec(d.sh('wm size'))?.slice(1) ?? ['1080', '2400']).map(Number);
    const cx = Math.round(w / 2), cy = Math.round(h * 0.45), dx = Math.round(w * 0.28), dy = Math.round(h * 0.13);
    const set = (on) => { for (const [k, tag] of Object.entries(PROPS)) d.sh(`setprop log.tag.${tag} ${on.includes(k) ? 'D' : 'INFO'}`); };
    await ctx.step(`the map up (cold start, Wi-Fi, camera probe on), the camera at ${AT.join(',')}`, async () => {
      d.sh('setprop log.tag.CifCam D');
      set([]);
      d.sh('logcat -G 16M');
      await d.net('wifi'); d.link('full', 'none');
      await coldStart(ctx, { ms: 90_000 });
      await toMap(ctx);
      cameraTo(ctx, { lon: AT[0], lat: AT[1], zoom: AT[2] });
      await d.sleep(6000);
      return `screen ${w}×${h}`;
    }, { stop: true });
    const out = { at: AT, ms: MS, variants: {} };
    const layer = mapLayer(ctx);
    // east, north, west, south: the camera comes back round, so every variant flies over the same ground
    const moves = [[-dx, 0], [0, dy], [dx, 0], [0, -dy]];
    for (const v of VARIANTS) {
      await ctx.step(`variant ${v}: ${N} flings of ${dx}/${dy} px in ${MS} ms`, async () => {
        set(v.split('+'));
        const rows = [];
        for (let i = 0; i < N; i++) {
          const [ox, oy] = moves[i % moves.length];
          d.logcatClear();
          d.sh(`input swipe ${cx - ox / 2} ${cy - oy / 2} ${cx + ox / 2} ${cy + oy / 2} ${MS}`);
          await d.sleep(3200);
          const lines = d.sh('logcat -d -s CifCam:D').split('\n');
          fs.appendFileSync(path.join(ctx.dir, `T452-glide-${v}.log`), `# fling ${i}\n${lines.join('\n')}\n`);
          const r = glideStats(lines, layer ? presents(ctx, layer) : []);
          if (r) rows.push(r);
        }
        out.variants[v] = rows;
        fs.writeFileSync(path.join(ctx.dir, 'T452-glide.json'), `${JSON.stringify(out, null, 1)}\n`);
        const all = (f) => rows.flatMap(f);
        const sum = (k) => rows.reduce((n, r) => n + (r[k] ?? 0), 0);
        return `${rows.length} flings · v ${q(rows.map((r) => r.vDp), 0.5)} dp/s, glide ${q(rows.map((r) => r.glideMs), 0.5)} ms, ` +
          `travel ${q(rows.map((r) => r.travelDp), 0.5)} dp · samples ${sum('samples')}, gaps >20 ms ${rows.reduce((n, r) => n + (r.gap?.over20 ?? 0), 0)}, ` +
          `gap p95 ${q(all((r) => [r.gap?.p95 ?? 0]), 0.5)} ms · uneven steps ${sum('uneven')}/${sum('moving')}, speed rises ${sum('rises')} · ` +
          `phase p5–p95 ${q(all((r) => [r.phase?.p5 ?? 0]), 0.5)}–${q(all((r) => [r.phase?.p95 ?? 0]), 0.5)} ms · ` +
          `presents ${rows.reduce((n, r) => n + (r.presents?.n ?? 0), 0)}, >20 ms ${rows.reduce((n, r) => n + (r.presents?.over20 ?? 0), 0)} · ` +
          `settles mid-glide ${sum('settlesMid')} (all ${sum('settles')}) · first-sample speed ${q(rows.map((r) => r.firstSpeed), 0.5)} dp/s`;
      });
    }
    set([]);
  },
};
