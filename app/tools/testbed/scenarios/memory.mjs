// (T360) THE APP'S MEMORY, SCREEN BY SCREEN, and (T354) the rule book in the app's own viewer. After each step the app's
// `dumpsys meminfo` App Summary (TOTAL PSS, Java, Native, Graphics, Code, Private Other, System; MB) is taken; the rows go
// into the step notes and `T360-memory.json` beside the report. The steps: boot, 10 pans, zoom in, satellite on (and
// off), a water's page, a "Rule book p.N ↗" tap (the viewer by its testID, the page it opened at, the open time; back
// closes it), Conditions, Salmon, then `am send-trim-memory RUNNING_CRITICAL`. A measurement, no budget: T245's `perf`
// holds the budgets.
//
//   node app/tools/testbed/run.mjs memory          (a `session.sh --local` APK reads this machine's release)
import fs from 'node:fs';
import path from 'node:path';
import { coldStart, toMap } from './common.mjs';

/** The App Summary rows, MB (null when the app is not running). */
function summary(ctx) {
  const t = ctx.device.sh(`dumpsys meminfo ${ctx.device.APP_ID}`);
  const at = t.indexOf('App Summary');
  if (at < 0) return null;
  const s = t.slice(at);
  const mb = (label) => { const m = new RegExp(`${label}:\\s+(\\d+)`).exec(s); return m ? Math.round(Number(m[1]) / 102.4) / 10 : null; };
  return { total: mb('TOTAL PSS'), java: mb('Java Heap'), native: mb('Native Heap'), graphics: mb('Graphics'),
           code: mb('Code'), other: mb('Private Other'), system: mb('System') };
}
const line = (m) => m ? `PSS ${m.total} · java ${m.java} · native ${m.native} · graphics ${m.graphics} · code ${m.code} · other ${m.other}` : 'not running';

/** The resource-ids (RN testIDs) on screen. */
function ids(ctx) {
  ctx.device.sh('uiautomator dump /sdcard/tb-ids.xml >/dev/null 2>&1');
  return [...ctx.device.sh('cat /sdcard/tb-ids.xml').matchAll(/resource-id="([^"]+)"/g)].map((m) => m[1]);
}

export const memory = {
  id: 'memory',
  title: 'Memory per screen (T360) and the rule book in the app (T354)',
  async run(ctx) {
    const rows = [];
    const keep = () => fs.writeFileSync(path.join(ctx.dir, 'T360-memory.json'), `${JSON.stringify(rows, null, 1)}\n`);
    const measure = async (what, settle = 3000) => {
      await ctx.device.sleep(settle);
      const m = summary(ctx);
      rows.push({ step: what, ...m }); keep();
      return line(m);
    };
    const [w, h] = (/(\d+)x(\d+)/.exec(ctx.device.sh('wm size'))?.slice(1) ?? ['1080', '2400']).map(Number);
    const cx = Math.round(w / 2), cy = Math.round(h * 0.45);

    await ctx.step('boot: cold start → the map drawn', async () => {
      // a clean install of THIS build's release (an earlier staging install's data is not this release)
      if (process.env.MEMORY_CLEAR !== '0') ctx.device.clearData();
      await ctx.device.net('wifi'); ctx.device.link('full', 'none');   // a just-booted emulator's network comes up late
      ctx.device.stop(); ctx.device.launch();
      // the first run's "could not be loaded" card waits for its Try again (a reader's tap), it does not retry itself
      for (let i = 0; i < 5; i++) {
        await ctx.device.sleep(8000);
        const words = ctx.device.screenText();
        if (!words.some((x) => /could not be loaded/.test(x))) break;
        ctx.flow('tap-text', { TEXT: 'Try again' });
      }
      await coldStart(ctx, { ms: 600_000 });
      return measure('boot', 8000);
    }, { stop: true });
    await ctx.step('10 pans', async () => {
      const d = [[-0.35, 0], [-0.35, 0], [0, 0.18], [0, 0.18], [0.35, 0], [0.35, 0], [0, -0.18], [0, -0.18], [-0.35, 0], [0.35, 0]];
      for (const [dx, dy] of d) { ctx.device.sh(`input swipe ${cx} ${cy} ${cx + Math.round(w * dx)} ${cy + Math.round(h * dy)} 350`); await ctx.device.sleep(900); }
      return measure('pan x10');
    });
    await ctx.step('zoom in (4 double taps, ~+4 z)', async () => {
      for (let i = 0; i < 4; i++) { ctx.device.sh(`input tap ${cx} ${cy} && input tap ${cx} ${cy}`); await ctx.device.sleep(1500); }
      return measure('zoom +4', 5000);
    });
    await ctx.step('satellite on, 4 pans', async () => {
      ctx.flow('layers-satellite');
      ctx.device.sh('input keyevent KEYCODE_BACK');
      for (const dx of [-0.3, 0.3, -0.3, 0.3]) { ctx.device.sh(`input swipe ${cx} ${cy} ${cx + Math.round(w * dx)} ${cy} 350`); await ctx.device.sleep(1200); }
      const r = await measure('satellite on', 5000);
      ctx.flow('layers-satellite');
      ctx.device.sh('input keyevent KEYCODE_BACK');
      return r;
    });
    await ctx.step("a water's page (Cultus Lake)", async () => {
      await toMap(ctx);
      ctx.flow('search', { Q: 'Cultus' });
      ctx.flow('open-result', { LINE: 'Lake · .*Lindell Beach.*' });
      await ctx.waitText(/Regulations/, 30_000);
      return measure('water page');
    });
    await ctx.step('the rule book: in the app, at the cited page; back closes it', async () => {
      // a WATER row's link (the regional tables, printed p.20 on): only those carry a box to highlight (T215; a
      // provincial rule's p.9 opens unmarked)
      const LINK = process.env.BOOK_LINK ?? 'Rule book p\\.[2-9][0-9] ↗.*';
      ctx.flow('scroll-to', { TEXT: LINK });
      const link = ctx.device.screenText().find((x) => new RegExp(LINK).test(x)) ?? '';
      ctx.device.logcatClear();
      const t0 = Date.now();
      ctx.flow('tap-text', { TEXT: LINK });
      let page = null, failed = null;
      while (Date.now() - t0 < 60_000) {
        const words = ctx.device.screenText();
        failed = words.find((x) => /Couldn.t (show|load) the (rule book|synopsis)/.test(x)) ?? null;
        page = words.find((x) => /^Page \d+ of \d+$/.test(x)) ?? null;
        if (failed || page) break;
        await ctx.device.sleep(500);
      }
      const ms = Date.now() - t0;
      ctx.device.screenshot(path.join(ctx.dir, 'T354-viewer.png'));
      const on = ids(ctx);
      if (failed || on.includes('pdf-viewer-failed')) throw new Error(`the viewer said: ${failed} (${ctx.device.screenText().slice(0, 12).join(' | ')})`);
      if (!on.includes('pdf-viewer')) throw new Error(`no in-app viewer (pdf-viewer) after ${ms} ms; front: ${ctx.device.sh('dumpsys activity activities | grep -m1 topResumedActivity').trim()}`);
      if (!page) throw new Error(`the viewer never said its page in ${ms} ms`);
      const mem = await measure('rule book open', 2000);
      ctx.device.sh('input keyevent KEYCODE_BACK');
      await ctx.device.sleep(1500);
      if (ids(ctx).includes('pdf-viewer')) throw new Error('back did not close the viewer');
      if (!ctx.device.screenText().some((x) => /Regulations/.test(x))) throw new Error('back closed more than the viewer (the water page is gone)');
      const said = ctx.device.logcatJs().split('\n').filter((l) => l.includes('[synopsis] open')).map((l) => l.slice(l.indexOf('[synopsis]'))).at(-1) ?? 'no [synopsis] line';
      return `${link} → ${page} in ${ms} ms (${said}); back closed it · ${mem}`;
    });
    await ctx.step('Conditions', async () => { await toMap(ctx); ctx.flow('tab', { TAB: 'Conditions' }); return measure('conditions', 6000); });
    await ctx.step('Salmon', async () => {
      ctx.flow('tap-text', { TEXT: 'Salmon.*' });
      return measure('salmon', 6000);
    });
    await ctx.step('trim memory RUNNING_CRITICAL', async () => {
      const pid = ctx.device.pidOf();
      ctx.device.sh(`am send-trim-memory ${pid} RUNNING_CRITICAL`);
      return measure('after trim', 5000);
    });
  },
};
