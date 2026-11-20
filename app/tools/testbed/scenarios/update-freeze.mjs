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
    // (T746 §4) THE CURTAIN ON THE SCREEN, light then dark: a new release staged, the app backgrounded and returned to (the
    // next pause), a burst of device-side screencaps from the launch, and the frames read against the app's own
    // `[swap] curtain up` / `down` lines. Kept as `T746-curtain-<mode>-<n>.png` beside the report.
    for (const mode of ['light', 'dark']) {
      await ctx.step(`T746 curtain, ${mode}: a new release staged, the app returned to, the curtain photographed`, async () => {
        const d = ctx.device;
        d.sh(`cmd uimode night ${mode === 'dark' ? 'yes' : 'no'}`);
        await toMap(ctx);
        // a release that changes a data file and the rule book (bg-update's `data-book`) is a REAL apply: a same-files release
        // is swapped in too fast for the curtain (it is drawn only after 300 ms, T746); the dark look is the way back to the live release
        const rid = (mode === 'light' ? ctx.updates('data-book') : ctx.updates('reset'))?.data_release ?? null;
        d.logcatClear();
        openLog(ctx);
        ctx.flow('updates-log-action', { ACTION: 'check' });
        const end = Date.now() + 240_000;
        let row = '';
        while (Date.now() < end) {   // staging copies every file: wait until the row says staged
          row = d.screenText().find((x) => /Release, (staged|applied|unchanged|failed)/.test(x)) ?? '';
          if (/staged|applied|failed/.test(row)) break;
          await d.sleep(4000);
        }
        if (!/staged/.test(row)) throw new Error(`the release did not reach staged (${row || 'no release row'})`);
        // the map left still: the Shell applies at the safe moment (map still ~3 s) with no one touching the phone, so the
        // burst starts the moment the map is up and runs on the device (no adb round trip per frame)
        await toMap(ctx);
        d.sh('rm -f /sdcard/tbc-*.png; for i in $(seq 1 40); do screencap /sdcard/tbc-$i.png; done');
        const frames = [];
        for (let i = 1; i <= 40; i++) {
          const f = path.join(ctx.dir, `T746-curtain-${mode}-${String(i).padStart(2, '0')}.png`);
          try { d.adb(['pull', `/sdcard/tbc-${i}.png`, f]); frames.push(f); } catch { /* none */ }
        }
        d.sh('rm -f /sdcard/tbc-*.png');
        const log = jsLog(ctx).filter((x) => /^\[swap\]/.test(x.line));
        fs.writeFileSync(path.join(ctx.dir, `T746-curtain-${mode}.txt`), log.map((x) => `${x.at} ${x.line}`).join('\n'));
        const up = log.find((x) => /curtain up/.test(x.line)), down = log.find((x) => /curtain down/.test(x.line));
        if (!up) throw new Error(`no "[swap] curtain up" in the log (swap lines: ${log.map((x) => x.line.slice(0, 40)).join(' / ') || 'none'}); ${frames.length} frames kept`);
        return `${rid}: curtain up ${up.at}${down ? `, down ${down.at} (${down.at - up.at} ms)` : ', no down line'}; ${frames.length} frames kept`;
      });
    }
    ctx.device.sh('cmd uimode night no');
    await ctx.step('test channel back to live staging', async () => { ctx.updates('reset'); }, { shot: false });
  },
};
