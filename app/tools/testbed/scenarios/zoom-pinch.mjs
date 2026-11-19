// (T876, owner 2026-11-18 on 0.8.1: "More laggy quick zoom in and out") QUICK PINCHES, MEASURED AS THE OWNER'S TRACE WAS:
// six quick two-finger pinch in/out cycles at one place (the Fraser Valley, z10–z12), and a quick zoom-out (z12 → ~z7,
// T861's case), each under a Perfetto trace (sched, the gesture marked in the trace, `linux.perf` samples of the app when
// the image allows). Frames are the binding's camera probe (`CifCam f`, one per rendered frame); a GAP is a frame interval
// over 100 ms while the camera was moving (a probe `m` / `v` line inside it). The render thread's CPU share is read off
// the trace on the host: `app/tools/perfetto/pinch.py <run dir>`. A measurement, no budget.
//
//   node app/tools/testbed/run.mjs zoom-pinch
//     PINCH_ROUNDS=n      rounds per arm (default 3)
//     PINCH_ARMS=a;b      arms, each a cold start: `base`, or `lab:<k=v,…>` (map-lab switches, e.g. lab:stateGapMs=250)
//     PINCH_MODE=salmon   the rail's Salmon map instead of Trout (the default)
//     PINCH_AT=lon,lat    (default -121.95,49.17)
import fs from 'node:fs';
import path from 'node:path';
import { coldStart, toMap } from './common.mjs';
import { cameraTo } from './perf.mjs';
import { touchscreen } from './gestures.mjs';
import { play, touchFiles } from './zoom-flick.mjs';
import { applyLab, parseLab } from '../lib/lab.mjs';

const ROUNDS = Number(process.env.PINCH_ROUNDS ?? 3);
const ARMS = (process.env.PINCH_ARMS ?? 'base').split(';').map((s) => s.trim()).filter(Boolean);
const MODE = process.env.PINCH_MODE ?? 'trout';
const AT = (process.env.PINCH_AT ?? '-121.95,49.17').split(',').map(Number);
const TRACE_S = 16;

/** The frames (s, logcat's monotonic clock), the camera's samples and vsyncs, and the gesture's marks. */
export function probeFrames(text) {
  const frames = [], moving = [];
  let start = null, end = null;
  for (const l of text.split('\n')) {
    const t = Number(/^\s*(\d+\.\d+)\s/.exec(l)?.[1]);
    if (!Number.isFinite(t)) continue;
    if (/CifCam\s*:\s*f /.test(l)) frames.push(t);
    else if (/CifCam\s*:\s*[mv] /.test(l)) moving.push(t);
    else if (/CifCam\s*:\s*S\s*$/.test(l)) start = t;
    else if (/CifCam\s*:\s*U\s*$/.test(l)) end = t;
  }
  return { frames, moving, start, end };
}

/** Frame intervals while the camera moved, from the gesture's start to 1.5 s after its last lift. */
export function gapStats({ frames, moving, start, end }) {
  if (start === null || end === null) return null;
  const fs_ = frames.filter((f) => f >= start && f <= end + 1.5);
  const gaps = [];
  for (let i = 1; i < fs_.length; i++) {
    const a = fs_[i - 1], b = fs_[i];
    if (moving.some((m) => m > a && m < b) || b - a < 0.1) gaps.push((b - a) * 1000);
  }
  const span = fs_.length > 1 ? fs_.at(-1) - fs_[0] : 0;
  return { frames: fs_.length, fps: span ? Math.round((fs_.length - 1) / span * 10) / 10 : 0,
           over50: gaps.filter((g) => g > 50).length, over100: gaps.filter((g) => g > 100).length,
           worst: Math.round(Math.max(0, ...gaps)), ms: Math.round((end - start) * 1000) };
}

const traceConfig = (app) => `
buffers { size_kb: 131072 fill_policy: DISCARD }
data_sources { config { name: "linux.ftrace" ftrace_config {
  ftrace_events: "sched/sched_switch" ftrace_events: "sched/sched_waking" ftrace_events: "power/cpu_frequency"
  ftrace_events: "ftrace/print" atrace_categories: "gfx" atrace_categories: "view" atrace_apps: "${app}" } } }
data_sources { config { name: "linux.process_stats" process_stats_config { scan_all_processes_on_start: true } } }
data_sources { config { name: "linux.perf" perf_event_config { timebase { frequency: 1000 }
  callstack_sampling { scope { target_cmdline: "${app}" } } } } }
duration_ms: ${TRACE_S * 1000}
`;

/** A pinch from `d0` to `d1` px apart (along x) about (cx, cy), the span growing fastest at the end, then the lift. */
function pinch(cx, cy, d0, d1, steps = 10) {
  const f = [];
  for (let i = 0; i <= steps; i++) {
    const p = (i / steps) ** 1.4, d = d0 + (d1 - d0) * p;
    f.push([[cx - d / 2, cy], [cx + d / 2, cy]]);
  }
  f.push('up');
  return f;
}

export const zoomPinch = {
  id: 'zoom-pinch',
  title: `T876: quick pinch zoom in/out ×6 and a quick zoom-out, frame gaps + render-thread CPU (arms ${ARMS.join(' / ')}, ${MODE})`,
  async run(ctx) {
    const d = ctx.device;
    const [w, h] = (/(\d+)x(\d+)/.exec(d.sh('wm size'))?.slice(1) ?? ['1080', '2400']).map(Number);
    const size = { w, h }, cx = Math.round(w / 2), cy = Math.round(h * 0.45);
    const out = { at: AT, mode: MODE, rounds: ROUNDS, arms: [] };
    const save = () => fs.writeFileSync(path.join(ctx.dir, 'T876-zoom-pinch.json'), `${JSON.stringify(out, null, 1)}\n`);
    let ts = null;
    const remote = '/data/misc/perfetto-traces/t876.pftrace';
    /** one traced gesture: the frames, the gaps, the trace pulled as T876-<label>.pftrace */
    const traced = async (label, frames, gapMs) => {
      d.logcatClear();
      d.sh(`rm -f ${remote}`);
      d.sh(`echo '${traceConfig(d.APP_ID)}' | perfetto --txt -c - -o ${remote} --background`);
      await d.sleep(2500);
      d.sh('log -p d -t CifCam S; echo "B|$$|T876gesture" > /sys/kernel/tracing/trace_marker 2>/dev/null || echo "B|$$|T876gesture" > /sys/kernel/debug/tracing/trace_marker');
      play(d, ts, touchFiles(ts, size, frames, { gapMs }), path.join(ctx.dir, 'touch'));
      // (each `sh` is its own shell, so an `E` here would close nothing: the end is a mark of its own, and the log's `U`)
      d.sh('echo "B|$$|T876end" > /sys/kernel/tracing/trace_marker 2>/dev/null || echo "B|$$|T876end" > /sys/kernel/debug/tracing/trace_marker');
      await d.sleep(2500);
      const text = d.sh('logcat -d -v monotonic -s CifCam:D');
      fs.writeFileSync(path.join(ctx.dir, `T876-${label}.log`), text);
      const end = Date.now() + (TRACE_S + 10) * 1000;
      while (Date.now() < end && d.sh('pidof perfetto || true').trim()) await d.sleep(1000);
      const local = path.join(ctx.dir, `T876-${label}.pftrace`);
      try { d.adb(['pull', remote, local], { timeout: 60_000 }); } catch (e) { return { ...gapStats(probeFrames(text)), trace: `not pulled: ${String(e.message).split('\n')[0]}` }; }
      return { ...gapStats(probeFrames(text)), trace: path.basename(local) };
    };
    const camera = async (zoom) => { cameraTo(ctx, { lon: AT[0], lat: AT[1], zoom }); await d.sleep(5000); };
    // six quick in/out cycles: out (span ×2.5, +1.3 zoom and its carry), then in; 250 ms between pinches
    const cycles = [];
    for (let i = 0; i < 6; i++) cycles.push(...pinch(cx, cy, w * 0.3, w * 0.75), 'pause', ...pinch(cx, cy, w * 0.75, w * 0.3), 'pause');
    // a quick zoom-out from z12: three fast squeezes (span ÷4 each)
    const outward = [];
    for (let i = 0; i < 3; i++) outward.push(...pinch(cx, cy, w * 0.8, w * 0.2), 'pause');

    for (const [block, arm] of ARMS.entries()) {
      const lab = arm.startsWith('lab:') ? parseLab(arm.slice(4)) : {};
      const row = { arm, pinch: [], zoomOut: [] };
      // (T903) an arm run in several blocks (interleaved A/B) names its traces by block, or the last block's overwrite the rest
      const tag = `${arm.replace(/[^a-z0-9]+/gi, '-')}${ARMS.filter((a) => a === arm).length > 1 ? `-b${block}` : ''}`;
      out.arms.push(row);
      await ctx.step(`${arm}: the map up (cold start, camera probe on, ${MODE})${Object.keys(lab).length ? `, lab ${arm.slice(4)}` : ''}`, async () => {
        d.sh('setprop log.tag.CifCam D');
        d.sh('logcat -G 16M');
        await coldStart(ctx, { ms: 90_000 });
        await toMap(ctx);
        ts = ts ?? touchscreen(ctx);
        if (!ts) throw new Error('no multi-touch screen found (getevent -pl)');
        d.adb(['root'], { timeout: 30_000 });
        await d.sleep(3000);
        d.adb(['wait-for-device'], { timeout: 60_000 });
        d.sh('setprop persist.traced_perf.enable 1 2>/dev/null; true');
        await camera(10.5);
        if (MODE === 'salmon') {
          ctx.flow('tap-text', { TEXT: 'Salmon' });
          await d.sleep(4000);
          for (let i = 0; i < 3; i++) {
            const t = d.screenText(), b = ['Skip', 'Got it'].find((s) => t.includes(s));
            if (!b) break;
            ctx.flow('tap-text', { TEXT: b }); await d.sleep(1500);
          }
        }
        const foot = Object.keys(lab).length ? await applyLab(lab, d) : '';
        await d.sleep(3000);
        return `screen ${w}×${h}, ${ts.dev}${foot ? ` · lab: ${foot}` : ''}`;
      }, { stop: true });
      try {
        await ctx.step(`${arm}: six quick pinch in/out cycles at z10.5, ×${ROUNDS}`, async () => {
          for (let r = 0; r < ROUNDS; r++) {
            await camera(10.5);
            row.pinch.push(await traced(`${tag}-pinch-${r}`, cycles, 250));
            save();
          }
          return row.pinch.map((p) => `${p.frames} frames ${p.fps} fps, >100 ms ${p.over100}, >50 ms ${p.over50}, worst ${p.worst}`).join(' | ');
        });
        await ctx.step(`${arm}: a quick zoom-out z12 → ~z7, ×${ROUNDS}`, async () => {
          for (let r = 0; r < ROUNDS; r++) {
            await camera(12);
            row.zoomOut.push(await traced(`${tag}-out-${r}`, outward, 250));
            save();
          }
          return row.zoomOut.map((p) => `${p.frames} frames ${p.fps} fps, >100 ms ${p.over100}, worst ${p.worst}`).join(' | ');
        });
      } finally {
        d.sh('setprop log.tag.CifCam INFO');
        try { d.adb(['unroot'], { timeout: 30_000 }); } catch { /* was not root */ }
        try { d.adb(['wait-for-device'], { timeout: 60_000 }); } catch { /* the next step waits too */ }
      }
    }
  },
};
