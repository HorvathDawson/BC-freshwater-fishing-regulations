// (T707, owner on build 29: "No zoom flick on map in app so zoom is clunky gesture wise") THE MAP'S ZOOM GESTURES, MEASURED
// BY THE CAMERA, not by pixels: the patched binding's camera probe (`setprop log.tag.CifCam D`, MLRNMapView `camProbe`: an
// `m <ns> lon lat zoom` line per camera move) read with logcat's monotonic stamps, and a `U` marker logged by the same
// shell call right after the fingers lift. Every gesture is written to the emulator's touchscreen as raw input events
// (multi-touch protocol B, `gestures.mjs` `touchscreen`; `touchFiles`), all of it in ONE shell call so its timing holds.
//
//   pinch out / in   a fast spread (or squeeze) and release: the zoom at the lift and where the camera comes to rest.
//                    MOMENTUM is the camera still zooming the same way after the lift (stock MapLibre's scale velocity
//                    animation: ≥ 300 ms of DecelerateInterpolator); none is a camera that stops dead at the lift.
//   double tap       zoom in by one about the tap
//   two-finger tap   zoom out by one
//   quick zoom       double-tap, hold the second press and drag down: zoom in as the finger moves
//
//   node app/tools/testbed/run.mjs zoom-flick        (FLICK_AT=lon,lat,zoom; FLICK_N=pinches each way, default 5)
import fs from 'node:fs';
import path from 'node:path';
import { coldStart, toMap } from './common.mjs';
import { cameraTo } from './perf.mjs';
import { touchscreen } from './gestures.mjs';

const AT = (process.env.FLICK_AT ?? '-121.95,49.17,11').split(',').map(Number);
const N = Number(process.env.FLICK_N ?? 5);

/** The probe's camera samples and the lift marker, on logcat's monotonic clock (s). */
export function probeLines(text) {
  const samples = [], marks = [], frames = [], gates = [];
  for (const l of text.split('\n')) {
    const t = /^\s*(\d+\.\d+)\s/.exec(l)?.[1];
    if (!t) continue;
    const m = /CifCam\s*:\s*m \d+ (\S+) (\S+) (\S+)\s*$/.exec(l);
    if (m) samples.push({ t: Number(t), z: Number(m[3]) });
    else if (/CifCam\s*:\s*U\s*$/.test(l)) marks.push(Number(t));
    else if (/CifCam\s*:\s*f /.test(l)) frames.push(Number(t));
    else {
      // (T707) the binding's decision at the second-last finger's lift: span rate dp/s, ms since the span last moved,
      // the zoom it carries, 1 = carries
      const g = /CifCam\s*:\s*p \d+ (-?\d+) (\d+) (-?[\d.]+) ([01])\s*$/.exec(l);
      if (g) gates.push({ rate: +g[1], stillMs: +g[2], add: +g[3], carries: g[4] === '1' });
    }
  }
  const up = marks.at(-1) ?? null;
  // how long the map went on drawing after the lift (ms): a camera that stops dead draws ~150 ms more (the settle's
  // frames); one still zooming draws as long as it moves
  const drawnAfter = up === null ? null : Math.round(Math.max(0, ...frames.filter((f) => f > up && f < up + 3).map((f) => f - up)) * 1000);
  return { samples, up, drawnAfter, gate: gates.at(-1) ?? null };
}

/**
 * One gesture's numbers: the zoom at the lift (the last sample at or before it), where it came to rest (the last sample),
 * how far it went on after the lift and for how long. `momentum` is a zoom that went on ≥ 0.05 the same way for ≥ 120 ms.
 */
export function afterLift({ samples, up }, z0) {
  if (up === null || !samples.length) return null;
  const before = samples.filter((s) => s.t <= up), after = samples.filter((s) => s.t > up);
  const zUp = (before.at(-1) ?? samples[0]).z, zEnd = samples.at(-1).z;
  const dir = Math.sign(zUp - z0);
  const carried = (zEnd - zUp) * (dir || 1);
  const ms = after.length ? Math.round((after.at(-1).t - up) * 1000) : 0;
  return { z0, zUp: +zUp.toFixed(3), zEnd: +zEnd.toFixed(3), carried: +carried.toFixed(3), ms, after: after.length,
           momentum: carried >= 0.05 && ms >= 120 };
}

/**
 * The gesture as raw `input_event`s (24 bytes each on a 64-bit kernel: timeval, type, code, value; the kernel stamps the
 * time), one file per report, written to the touchscreen node by `cat` with `frameMs` between reports — `sendevent` is a
 * process per event (~200 ms a two-finger report on the emulator), far too slow for a flick. Frames of [x, y] points
 * (screen px), 'up' (every finger lifts in one report) or 'pause' (`gapMs`); then the `U` marker.
 */
export function touchFiles(ts, size, frames, { gapMs = 0, frameMs = 0 } = {}) {
  const sx = ts.maxX / size.w, sy = ts.maxY / size.h;
  const files = [];
  let down = 0, evs = [];
  const ev = (t, c, v) => { const b = Buffer.alloc(24); b.writeUInt16LE(t, 16); b.writeUInt16LE(c, 18); b.writeInt32LE(v, 20); evs.push(b); };
  const flush = () => { files.push({ data: Buffer.concat(evs), sleep: frameMs }); evs = []; };
  frames.forEach((pts, i) => {
    if (pts === 'up') {
      for (let k = 0; k < down; k++) { ev(3, 47, k); ev(3, 57, -1); }
      ev(1, 330, 0); ev(0, 0, 0); flush();
      down = 0;
      return;
    }
    if (pts === 'pause') { if (files.length) files.at(-1).sleep = gapMs; return; }
    pts.forEach(([x, y], k) => {
      ev(3, 47, k);                                                         // ABS_MT_SLOT
      if (k >= down) ev(3, 57, 200 + i * 4 + k);                            // ABS_MT_TRACKING_ID: a new finger
      ev(3, 53, Math.round(x * sx)); ev(3, 54, Math.round(y * sy));
      if (ts.pressure) ev(3, 58, 50);
    });
    if (down === 0) ev(1, 330, 1);                                          // BTN_TOUCH down
    down = Math.max(down, pts.length);
    ev(0, 0, 0); flush();                                                   // SYN_REPORT
  });
  return files;
}

/** Push the gesture's files and play them in one shell call (root: the node is root's on the Play image). */
export function play(d, ts, files, dir) {
  fs.rmSync(dir, { recursive: true, force: true });
  fs.mkdirSync(dir, { recursive: true });
  files.forEach((f, i) => fs.writeFileSync(path.join(dir, String(i)), f.data));
  d.sh('rm -rf /data/local/tmp/t707; mkdir -p /data/local/tmp/t707');
  d.adb(['push', `${dir}/.`, '/data/local/tmp/t707/'], { timeout: 30_000 });
  // no `sleep` between reports unless asked: each `cat` is a process start (~15 ms on the emulator), so a sleep doubles the
  // gap, and over 40 ms between a pinch's moves reads as fingers that stopped (Android's VelocityTracker, the binding's gate)
  const steps = files.map((f, i) => `cat /data/local/tmp/t707/${i} > ${ts.dev}${f.sleep > 0 ? `; sleep ${(f.sleep / 1000).toFixed(3)}` : ''}`);
  d.sh(`${steps.join('; ')}; log -p d -t CifCam U`);
}

/**
 * (T707) The common one-hand pinch: the thumb (the FIRST finger down) holds still and the index finger flicks away from or
 * towards it. MapLibre's own gate reads the first pointer's speed only, so this one is where it fails.
 */
function anchoredFrames(cx, cy, d0, d1, steps = 6) {
  const f = [[[cx, cy + 300]]];                                             // the thumb lands first, alone
  for (let i = 0; i <= steps; i++) {
    const p = (i / steps) ** 1.6;
    const d = d0 + (d1 - d0) * p;
    f.push([[cx, cy + 300], [cx, cy + 300 - d]]);
  }
  f.push('up');
  return f;
}

/** A pinch from `d0` to `d1` px apart (along x) about (cx, cy), the span growing fastest at the end (a flick). */
function pinchFrames(cx, cy, d0, d1, steps = 6) {
  const f = [];
  for (let i = 0; i <= steps; i++) {
    const p = (i / steps) ** 1.6;
    const d = d0 + (d1 - d0) * p;
    f.push([[cx - d / 2, cy], [cx + d / 2, cy]]);
  }
  f.push('up');
  return f;
}

export const zoomFlick = {
  id: 'zoom-flick',
  title: `T707: the map's zoom gestures — pinch momentum after the lift, double tap, two-finger tap, quick zoom (${AT.join(',')})`,
  async run(ctx) {
    const d = ctx.device;
    const [w, h] = (/(\d+)x(\d+)/.exec(d.sh('wm size'))?.slice(1) ?? ['1080', '2400']).map(Number);
    const size = { w, h };
    const cx = Math.round(w / 2), cy = Math.round(h * 0.45);
    let ts = null;
    const out = { at: AT, pinches: [], taps: {} };
    const save = () => fs.writeFileSync(path.join(ctx.dir, 'T707-zoom-flick.json'), `${JSON.stringify(out, null, 1)}\n`);
    /** run one gesture script; the probe's lines after it settles → its numbers */
    const gesture = async (label, frames, z0, opts) => {
      d.logcatClear();
      play(d, ts, touchFiles(ts, size, frames, opts), path.join(ctx.dir, 'touch'));
      await d.sleep(2500);
      const text = d.sh('logcat -d -v monotonic -s CifCam:D');
      fs.appendFileSync(path.join(ctx.dir, 'T707-zoom-flick.log'), `# ${label}\n${text}\n`);
      const p = probeLines(text);
      return { p, r: afterLift(p, z0) };
    };
    /** the camera's zoom at rest: the last probe sample, or the camera put there */
    let zoom = AT[2];
    await ctx.step(`the map up (camera probe on), the camera at ${AT.join(',')}`, async () => {
      d.sh('setprop log.tag.CifCam D');
      d.sh('logcat -G 16M');
      await coldStart(ctx, { ms: 90_000 });
      await toMap(ctx);
      ts = touchscreen(ctx);
      if (!ts) throw new Error('no multi-touch screen found (getevent -pl)');
      // the touchscreen node is root's on the Play image (`sendevent: Permission denied` as shell): adbd as root, as
      // `crash-bed` does; put back at the end of the scenario
      d.adb(['root'], { timeout: 30_000 });
      await d.sleep(3000);
      d.adb(['wait-for-device'], { timeout: 60_000 });
      cameraTo(ctx, { lon: AT[0], lat: AT[1], zoom: AT[2] });
      await d.sleep(5000);
      return `screen ${w}×${h}, touchscreen ${ts.dev}`;
    }, { stop: true });

    try {
    const reset = async () => { cameraTo(ctx, { lon: AT[0], lat: AT[1], zoom: AT[2] }); await d.sleep(2500); zoom = AT[2]; };
    for (const [dir, d0, d1] of [['out', Math.round(w * 0.15), Math.round(w * 0.8)], ['in', Math.round(w * 0.8), Math.round(w * 0.15)]]) {
      await ctx.step(`pinch ${dir} ×${N}, fast, released: the zoom carries on after the lift`, async () => {
        const rows = [];
        for (let i = 0; i < N; i++) {
          await reset();
          const { p, r } = await gesture(`pinch ${dir} ${i}`, pinchFrames(cx, cy, d0, d1), zoom);
          rows.push({ ...(r ?? { none: true, drawnAfter: p.drawnAfter }), gate: p.gate });
        }
        out.pinches.push({ dir, rows });
        save();
        const ok = rows.filter((r) => r.momentum).length;
        const said = rows.map((r) => (r.none ? `no samples, drawn ${r.drawnAfter} ms after the lift` : `${r.zUp}→${r.zEnd} (+${r.carried} in ${r.ms} ms)`)).join('; ');
        // every fast release carries (MapLibre's own gate let 1 in 3 through, T707)
        if (ok < rows.length) throw new Error(`${rows.length - ok} of ${rows.length} pinches ${dir} stopped dead at the lift: ${said}`);
        return `${ok}/${rows.length} carried on: ${said}`;
      });
    }

    for (const [dir, d0, d1] of [['out', Math.round(h * 0.06), Math.round(h * 0.4)], ['in', Math.round(h * 0.4), Math.round(h * 0.06)]]) {
      await ctx.step(`one-hand pinch ${dir} ×${N} (thumb still, finger flicks), released: the zoom carries on after the lift`, async () => {
        const rows = [];
        for (let i = 0; i < N; i++) {
          await reset();
          const { p, r } = await gesture(`anchored ${dir} ${i}`, anchoredFrames(cx, cy, d0, d1), zoom);
          rows.push({ ...(r ?? { none: true, drawnAfter: p.drawnAfter }), gate: p.gate });
        }
        out.pinches.push({ dir: `anchored-${dir}`, rows });
        save();
        const ok = rows.filter((r) => r.momentum).length;
        const said = rows.map((r) => (r.none ? 'no samples' : `${r.zUp}→${r.zEnd} (+${r.carried} in ${r.ms} ms)`)).join('; ');
        if (ok < rows.length) throw new Error(`${rows.length - ok} of ${rows.length} one-hand pinches ${dir} stopped dead at the lift: ${said}`);
        return `${ok}/${rows.length} carried on: ${said}`;
      });
    }

    await ctx.step('control: a pinch out held still 400 ms before the lift stops where the fingers stopped', async () => {
      await reset();
      const f = pinchFrames(cx, cy, Math.round(w * 0.15), Math.round(w * 0.8));
      const last = f.at(-2);
      const { p, r } = await gesture('pinch held', [...f.slice(0, -1), 'pause', last, 'up'], zoom, { gapMs: 400 });
      out.control = { ...(r ?? { none: true, drawnAfter: p.drawnAfter }), gate: p.gate };
      save();
      if (!r) return `no samples, drawn ${p.drawnAfter} ms after the lift`;
      if (r.momentum) throw new Error(`a held pinch went on zooming: ${r.zUp}→${r.zEnd}`);
      return `${r.zUp}→${r.zEnd} (+${r.carried} in ${r.ms} ms)`;
    });

    await ctx.step('double tap zooms in by about one', async () => {
      await reset();
      const tap = [[[cx, cy]], 'up'];
      const { p } = await gesture('double tap', [...tap, 'pause', ...tap], zoom, { gapMs: 120 });
      const zEnd = p.samples.at(-1)?.z ?? zoom;
      out.taps.double = { z0: zoom, zEnd };
      save();
      if (!(zEnd - zoom > 0.6)) throw new Error(`double tap: zoom ${zoom} → ${zEnd}`);
      return `zoom ${zoom} → ${zEnd.toFixed(2)}`;
    });

    await ctx.step('two-finger tap zooms out by about one', async () => {
      await reset();
      const { p } = await gesture('two-finger tap', [[[cx - 150, cy], [cx + 150, cy]], 'up'], zoom);
      const zEnd = p.samples.at(-1)?.z ?? zoom;
      out.taps.twoFinger = { z0: zoom, zEnd };
      save();
      if (!(zoom - zEnd > 0.6)) throw new Error(`two-finger tap: zoom ${zoom} → ${zEnd}`);
      return `zoom ${zoom} → ${zEnd.toFixed(2)}`;
    });

    await ctx.step('quick zoom: double tap, hold and drag down zooms in', async () => {
      await reset();
      const drag = [];
      for (let i = 0; i <= 8; i++) drag.push([[cx, cy + i * 40]]);
      const { p } = await gesture('quick zoom', [[[cx, cy]], 'up', 'pause', ...drag, 'up'], zoom, { gapMs: 120 });
      const zEnd = p.samples.at(-1)?.z ?? zoom;
      out.taps.quick = { z0: zoom, zEnd };
      save();
      if (!(zEnd - zoom > 0.3)) throw new Error(`quick zoom: zoom ${zoom} → ${zEnd}`);
      return `zoom ${zoom} → ${zEnd.toFixed(2)}`;
    });
    } finally {
      d.sh('setprop log.tag.CifCam INFO');
      try { d.adb(['unroot'], { timeout: 30_000 }); } catch { /* was not root */ }
      try { d.adb(['wait-for-device'], { timeout: 60_000 }); } catch { /* the next scenario waits too */ }
    }
  },
};
