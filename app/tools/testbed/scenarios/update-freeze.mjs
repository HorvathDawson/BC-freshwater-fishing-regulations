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
    // (T746 §4) THE CURTAIN ON THE SCREEN, light then dark. A release that changes a data file and the rule book (bg-update's
    // `data-book`) is a real apply; its way back (`reset`) is the dark look. After Check now the app's own log is polled every
    // ~0.4 s (the JS log: `[swap] curtain up` / `down`); at "up" the screen is photographed at once, three times. The curtain is
    // drawn only after 300 ms and kept at least 400 ms (T746), so a frame or two is what there is to catch. Frames are kept as
    // `T746-curtain-<mode>-<n>.png`, the swap lines as `T746-curtain-<mode>.txt`, beside the report.
    for (const mode of ['light', 'dark']) {
      await ctx.step(`T746 curtain, ${mode}: a new release applied, the curtain photographed when the log says it is up`, async () => {
        const d = ctx.device;
        d.sh(`cmd uimode night ${mode === 'dark' ? 'yes' : 'no'}`);
        await toMap(ctx);
        const rid = (mode === 'light' ? ctx.updates('data-book') : ctx.updates('reset'))?.data_release ?? null;
        d.logcatClear();
        openLog(ctx);
        ctx.flow('updates-log-action', { ACTION: 'check' });
        // the Developer page is a window of its own ABOVE the Shell, which draws the curtain: the map must be the screen
        // (the update stages for ~50 s, then the Shell applies it when the map has been still ~3 s)
        await toMap(ctx);
        const frames = [], swap = [];
        let up = null, down = null;
        const end = Date.now() + 300_000;
        while (Date.now() < end && !down) {
          const lines = d.adb(['logcat', '-d', '-v', 'epoch', '-t', '60', '-s', 'ReactNativeJS:*']).toString().split('\n')
            .map((l) => { const m = /^\s*(\d+)\.(\d{3})\s.*?ReactNativeJS:\s?(.*)$/.exec(l); return m ? { at: Number(m[1]) * 1000 + Number(m[2]), line: m[3] } : null; })
            .filter((x) => x && /^\[swap\] (apply|curtain|the new set)/.test(x.line));
          for (const x of lines) if (!swap.some((y) => y.at === x.at && y.line === x.line)) swap.push(x);
          if (!up && swap.some((x) => /curtain up/.test(x.line))) {
            up = swap.find((x) => /curtain up/.test(x.line));
            for (let i = 1; i <= 3; i++) { const f = path.join(ctx.dir, `T746-curtain-${mode}-${i}.png`); d.screenshot(f); frames.push(f); }
          }
          down = swap.find((x) => /curtain down/.test(x.line)) ?? null;
          await d.sleep(300);   // poll interval: the app's log
        }
        fs.writeFileSync(path.join(ctx.dir, `T746-curtain-${mode}.txt`), swap.map((x) => `${x.at} ${x.line}`).join('\n'));
        if (!up) throw new Error(`no "[swap] curtain up" within 5 min of Check now (swap lines: ${swap.map((x) => x.line.slice(0, 40)).join(' / ') || 'none'})`);
        return `${rid}: curtain up ${up.at}${down ? `, down ${down.at} (${down.at - up.at} ms)` : ', no down line'}; ${frames.length} frames`;
      });
    }
    ctx.device.sh('cmd uimode night no');
    await ctx.step('test channel back to live staging', async () => { ctx.updates('reset'); }, { shot: false });
  },
};
