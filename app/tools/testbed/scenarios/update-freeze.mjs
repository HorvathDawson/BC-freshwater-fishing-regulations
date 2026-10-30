// (T287) NO FREEZE WHEN AN UPDATE LANDS: a new release on the test channel (same files, a new id: `test-updates.mjs
// release`, so the phone copies and re-checks every file it holds), checked for with "Check now", then the map panned
// while it downloads and stages, and again after it is applied at the next pause. The test-bed build logs every JS-thread
// stall over 50 ms (`[stall]`, apps/mobile index.js); each phase's stalls go into the step note, with the app's
// `[download]` / `[setup]` lines, and the whole log into `T287-<phase>.txt` beside the report. A benchmark: a stall over
// 100 ms fails the step (the ticket: no long task over 50 ms; 50–100 is reported).
//
//   node app/tools/testbed/run.mjs update-freeze          (FREEZE_PAN_S=n seconds of panning per phase, default 60)
import fs from 'node:fs';
import path from 'node:path';
import { coldStart, openLog, toMap } from './common.mjs';

const PAN_S = Number(process.env.FREEZE_PAN_S ?? 60);

/** The JS log with epoch times (`logcat -v epoch`): [{at, line}]. */
function jsLog(ctx) {
  return ctx.device.adb(['logcat', '-d', '-v', 'epoch', '-s', 'ReactNativeJS:*']).toString().split('\n')
    .map((l) => { const m = /^\s*(\d+)\.(\d{3})\s.*?ReactNativeJS:\s?(.*)$/.exec(l); return m ? { at: Number(m[1]) * 1000 + Number(m[2]), line: m[3] } : null; })
    .filter(Boolean);
}

/** Pan for `s` seconds (the T273 swipes), then the stalls logged meanwhile and what the app was doing around them. */
async function panAndRead(ctx, s, phase) {
  const [w, h] = (/(\d+)x(\d+)/.exec(ctx.device.sh('wm size'))?.slice(1) ?? ['1080', '2400']).map(Number);
  const cx = Math.round(w / 2), cy = Math.round(h * 0.45), dx = Math.round(w * 0.3), dy = Math.round(h * 0.15);
  const loop = [[-dx, 0], [0, dy], [dx, 0], [0, -dy]];
  const end = Date.now() + s * 1000;
  for (let i = 0; s > 0 && Date.now() < end; i++) {
    const [mx, my] = loop[i % loop.length];
    ctx.device.sh(`input swipe ${cx} ${cy} ${cx + mx} ${cy + my} 350`);
    await ctx.device.sleep(900);
  }
  const log = jsLog(ctx);
  fs.writeFileSync(path.join(ctx.dir, `T287-${phase}.txt`), log.map((x) => `${x.at} ${x.line}`).join('\n'));
  const stalls = log.filter((x) => x.line.startsWith('[stall] ')).map((x) => JSON.parse(x.line.slice(8)));
  // what the app said in the second before a stall ended (`[work]` timed decodes, `[swap]`, `[setup]`), MapLibre's noise left out
  const around = (t) => log.filter((x) => !/^\[(stall|pan)\]|Mbgl|MapLibre/.test(x.line) && x.at > t - 1000 && x.at <= t + 50)
    .map((x) => x.line.slice(0, 80)).slice(-3).join(' / ');
  const big = [...stalls].sort((a, b) => b.ms - a.ms).slice(0, 5);
  const worst = big[0]?.ms ?? 0;
  const note = `${stalls.length} stalls >50 ms (worst ${worst} ms, >100 ms: ${stalls.filter((x) => x.ms > 100).length})`
    + (big.length ? ` · ${big.map((b) => `${b.ms} ms [${around(b.at)}]`).join(' · ')}` : '');
  return { note, worst };
}

export const updateFreeze = {
  id: 'update-freeze',
  title: `An update landing while the map pans: JS stalls (${PAN_S} s per phase)`,
  async run(ctx) {
    await ctx.step('the map up (cold start, Wi-Fi), the test channel = live staging', async () => {
      ctx.updates('reset');
      await ctx.device.net('wifi'); ctx.device.link('full', 'none');
      await coldStart(ctx, { ms: 90_000 });
      await ctx.device.sleep(4000);
    });
    await ctx.step(`baseline: ${Math.min(PAN_S, 20)} s of panning, nothing downloading`, async () => {
      ctx.device.logcatClear();
      const r = await panAndRead(ctx, Math.min(PAN_S, 20), 'baseline');
      return r.note;
    });
    let id = null;
    await ctx.step('a new release (same files, new id) published; Check now; back to the map and pan while it stages', async () => {
      id = ctx.updates('release', `f${Date.now().toString(36).slice(-5)}`).release?.data_release ?? null;
      openLog(ctx);
      ctx.device.logcatClear();
      ctx.flow('updates-log-action', { ACTION: 'check' });
      await toMap(ctx);
      const r = await panAndRead(ctx, PAN_S, 'stage');
      // (T287) the map left still: the swap may land now (the Shell waits for a still map), where no one is panning
      ctx.device.logcatClear();
      await ctx.device.sleep(8000);
      const idle = await panAndRead(ctx, 0, 'still');
      openLog(ctx);
      const row = ctx.device.screenText().find((x) => /Release, (staged|applied|unchanged|failed)/.test(x)) ?? 'no release row';
      const note = `${row} · WHILE PANNING ${r.note} · THEN STILL ${idle.note}`;
      if (r.worst > 100) throw new Error(note);
      return note;
    });
    await ctx.step('applied at the next pause, then pan', async () => {
      await toMap(ctx);
      ctx.device.logcatClear();
      ctx.device.background(); await ctx.device.sleep(3000); ctx.device.launch();
      const r = await panAndRead(ctx, Math.min(PAN_S, 30), 'apply');
      openLog(ctx);
      const head = ctx.device.screenText().find((x) => /release r-/.test(x)) ?? '';
      if (id && !head.includes(id)) throw new Error(`not applied (${head}) · ${r.note}`);
      if (r.worst > 100) throw new Error(r.note);
      return `${head} · ${r.note}`;
    });
    await ctx.step('test channel back to live staging', async () => { ctx.updates('reset'); }, { shot: false });
  },
};
