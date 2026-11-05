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
import { closeWaters, coldStart, toMap } from './common.mjs';

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

/** One Layers row tapped by its id (a switch flips; a ground is chosen), the sheet closed. */
function layer(ctx, id) {
  ctx.flow('tap-text', { TEXT: 'Choose layers.*' });
  ctx.flow('tap-id', { ID: id.startsWith('ground.') ? `option-${id.replace('.', '\\.')}` : `layer-${id.replace('.', '\\.')}` });
  ctx.flow('tap-text', { TEXT: 'Done' });
}
const groundMap = (ctx) => layer(ctx, 'ground.map');

export const memory = {
  id: 'memory',
  title: 'Memory per screen (T360) and the rule book in the app (T354)',
  async run(ctx) {
    const rows = [];
    const keep = () => fs.writeFileSync(path.join(ctx.dir, 'T360-memory.json'), `${JSON.stringify(rows, null, 1)}\n`);
    const measure = async (what, settle = 3000) => {
      await ctx.device.sleep(settle);
      const m = summary(ctx);
      // (T360) the JS engine's own heap at this step (the build's `[heap]` probe, index.js): Native Heap less this is
      // the map, SQLite and the rest
      const h = ctx.device.logcatJs().split('\n').filter((l) => l.includes('[heap] ')).at(-1);
      let js = null, ext = null;
      try {
        const j = JSON.parse(h.slice(h.indexOf('[heap] ') + 7));
        js = Math.round(j.size / 104857.6) / 10;
        // Hermes' EXTERNAL bytes (typed arrays: the decoded release files) are malloc'd: they count as Native Heap
        ext = j.ext == null ? null : Math.round(j.ext / 104857.6) / 10;
      } catch { /* no probe line */ }
      rows.push({ step: what, ...m, js, ext }); keep();
      return `${line(m)} · js heap ${js ?? '?'} · js external ${ext ?? '?'}`;
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
      await closeWaters(ctx); await toMap(ctx);
      ctx.flow('layers-satellite');
      ctx.device.sh('input keyevent KEYCODE_BACK');
      for (const dx of [-0.3, 0.3, -0.3, 0.3]) { ctx.device.sh(`input swipe ${cx} ${cy} ${cx + Math.round(w * dx)} ${cy} 350`); await ctx.device.sleep(1200); }
      const r = await measure('satellite on', 5000);
      // the ground is a choice of Map or Satellite, not a toggle (tapping Satellite again left it on, and every later
      // step was measured over imagery): the Map base by its id
      groundMap(ctx);
      return r;
    });
    await ctx.step("a water's page (Cultus Lake)", async () => {
      await closeWaters(ctx); await toMap(ctx);
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
      await measure('before rule book', 1000);
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
      // (T360) what the viewer leaves behind once closed (the leak question): settled, then after a moderate trim
      await measure('rule book closed', 5000);
      ctx.device.sh(`am send-trim-memory ${ctx.device.pidOf()} RUNNING_MODERATE`);
      await measure('rule book closed + trim', 4000);
      if (!ctx.device.screenText().some((x) => /Regulations/.test(x))) throw new Error('back closed more than the viewer (the water page is gone)');
      const said = ctx.device.logcatJs().split('\n').filter((l) => l.includes('[synopsis] ')).map((l) => l.slice(l.indexOf('[synopsis]') + 11)).join('; ') || 'no [synopsis] line';
      return `${link} → ${page} in ${ms} ms (${said}); back closed it · ${mem}`;
    });
    await ctx.step('the rule book again (held): the open alone, timed', async () => {
      ctx.device.logcatClear();
      const t0 = Date.now();
      ctx.flow('tap-text', { TEXT: process.env.BOOK_LINK ?? 'Rule book p\\.[2-9][0-9] ↗.*' });
      let page = null;
      while (Date.now() - t0 < 60_000 && !page) { page = ctx.device.screenText().find((x) => /^Page \d+ of \d+$/.test(x)) ?? null; if (!page) await ctx.device.sleep(300); }
      const ms = Date.now() - t0;
      ctx.device.screenshot(path.join(ctx.dir, 'T354-viewer-2.png'));
      const said = ctx.device.logcatJs().split('\n').filter((l) => l.includes('[synopsis] ')).map((l) => l.slice(l.indexOf('[synopsis]') + 11)).join('; ') || 'no [synopsis] line';
      ctx.device.sh('input keyevent KEYCODE_BACK'); await ctx.device.sleep(1500);
      if (!page) throw new Error(`the viewer never said its page in ${ms} ms (${said})`);
      // (T360) a second open + close that keeps more than the first is a leak; the same is a one-time cost
      return `${page} in ${ms} ms (${said}) · closed again: ${await measure('rule book closed (2nd)', 5000)}`;
    });
    // (T360) CONDITIONS, SPLIT: as shown, then its switches off one at a time (gauges, arrows), then the Map tab again
    // (what leaving Conditions gives back). What is left over the Map tab is the flow colouring and the panel's strip.
    await ctx.step('Conditions', async () => {
      await closeWaters(ctx); await toMap(ctx); ctx.flow('tab', { TAB: 'Conditions' });
      await ctx.device.sleep(2000);
      if (ctx.device.screenText().includes('Skip')) ctx.flow('tap-text', { TEXT: 'Skip' });   // its first-use tour
      return measure('conditions', 6000);
    });
    for (const [id, what] of [['overlay.gauges', 'conditions, gauges off'], ['overlay.arrows', 'conditions, gauges + arrows off']]) {
      await ctx.step(`Conditions: ${what}`, async () => { layer(ctx, id); return measure(what, 6000); });
    }
    await ctx.step('the Map tab after Conditions', async () => { ctx.flow('tab', { TAB: 'Map' }); return measure('map after conditions', 6000); });
    await ctx.step('Conditions again (switches on again)', async () => {
      ctx.flow('tab', { TAB: 'Conditions' });
      layer(ctx, 'overlay.gauges'); layer(ctx, 'overlay.arrows');      // the choice is kept: put back for the next run
      return measure('conditions again', 6000);
    });
    await ctx.step('Salmon (the map coloured by salmon)', async () => {
      await toMap(ctx);
      // (T367) the Standard / Salmon switch is in the legend's fishery bar now. By its WORD: the strip is a labelled
      // radiogroup, which on Android hides its segments' ids from the accessibility tree (r050b: shown, id not found)
      ctx.flow('tap-text', { TEXT: 'Salmon' });
      return measure('salmon', 6000);
    });
    await ctx.step('trim memory RUNNING_CRITICAL', async () => {
      const pid = ctx.device.pidOf();
      ctx.device.logcatClear();
      ctx.device.sh(`am send-trim-memory ${pid} RUNNING_CRITICAL`);
      const r = await measure('after trim', 5000);
      // (T360) what answered: the maps told (MemoryTrim) and the databases shrunk ([trim])
      const said = ctx.device.adb(['logcat', '-d', '-s', 'MemoryTrim:*', 'ReactNativeJS:*']).toString().split('\n')
        .filter((l) => /MemoryTrim|\[trim\]/.test(l)).map((l) => l.replace(/^.*?: /, '')).join('; ') || 'nothing answered the trim';
      return `${r} · ${said}`;
    });
  },
};
