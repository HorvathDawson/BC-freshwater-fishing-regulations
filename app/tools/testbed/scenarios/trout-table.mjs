// (T810 R) THE TROUT TABLE ON A FRESH INSTALL. Since the cutover (T810 L + S) every app build draws the table, and every
// release carries the `trout` section; this scenario still installs a named release (a SIDE release, or the candidate)
// put on the staging test channel by `node deploy/staging/test-updates.mjs side <dir>` (test/ only) BEFORE this run. Its
// upload is long, so it is not done here; and `install` resets test/ to the live release, so this
// scenario runs alone (`run.mjs trout-table`), never after `install` in the same run.
//
// Steps: test/ names the side release; a clean app (uninstalled, installed again) downloads it whole and opens the map;
// the Updates log says that release; a water's Regulations (the Vedder by default); the trout fold (`glance:trout`) opens
// the table in place (`keep-trout`); the first row's picture inside the table (`pic:<row key>`) opens that fish's sheet
// (`sheet-panel`, heading = the row's fish); Done closes it. Each step is shot. The old card where the table should be
// (no `glance:trout`, no `keep-trout`) fails, naming the two causes (an APK or a release from before the cutover).
//
//   TROUT_SIDE_ID=r-…        (required) the side release's id, as test/release/manifest.json names it
//   TROUT_Q / TROUT_ROW      the water: search words and its result row (default the Vedder River, Region 2)
import { appUp, logStatus, openLog, openWater, toMap } from './common.mjs';

const SIDE = process.env.TROUT_SIDE_ID ?? null;
const Q = process.env.TROUT_Q ?? 'Vedder River';
const ROW = process.env.TROUT_ROW ?? '.* · Region 2 · .*';

export const troutTable = {
  id: 'trout-table',
  title: `T810 R: a fresh install of the side release (${SIDE ?? 'TROUT_SIDE_ID unset'}), ${Q}: the trout fold opens the table, a row's picture opens its sheet`,
  async run(ctx) {
    await ctx.step('test/ names the side release', async () => {
      if (!SIDE) throw new Error('TROUT_SIDE_ID is unset: put the side release on test/ (test-updates.mjs side <dir>) and name it');
      const s = ctx.updates('status');
      const id = s.release?.data_release ?? String(s.release);
      if (id !== SIDE) throw new Error(`test/ names ${id}, not ${SIDE}: run \`node deploy/staging/test-updates.mjs side <dir>\` first (and not \`install\` in the same run)`);
      return `test/ = ${id}`;
    }, { shot: false, stop: true });
    await ctx.step('a clean app downloads the side release and opens the map', async () => {
      ctx.device.stop(); ctx.device.cleanApp();
      await ctx.device.net('wifi'); ctx.device.link('full', 'none');
      const t0 = Date.now();
      await appUp(ctx, { ms: 20 * 60_000 });
      return `up in ${Math.round((Date.now() - t0) / 1000)} s`;
    }, { stop: true });
    await ctx.step('the Updates log names the side release', async () => {
      openLog(ctx);
      const line = await ctx.until(() => { const l = logStatus(ctx); return l.includes(SIDE) ? l : null; },
                                   { ms: 30_000, what: `the log's Release line naming ${SIDE}` });
      await toMap(ctx);
      return line;
    }, { stop: true });
    await ctx.step(`${Q}: Regulations, the trout fold`, async () => {
      await openWater(ctx, { q: Q, row: ROW, view: 'regs' });
      try { ctx.flow('scroll-to-id', { ID: 'glance:trout' }); } catch { /* said below */ }
      if (!ctx.hasId('glance:trout') && !ctx.hasId('keep-trout'))
        throw new Error('no trout fold and no trout table (the old card): is the APK from before the cutover (0.8.0), or the release cut before rows 34 (no `trout` section)?');
      return ctx.hasId('glance:trout') ? 'the fold is drawn' : 'the table is drawn in full (no at-a-glance fold)';
    }, { stop: true });
    await ctx.step('the fold opens the table in place', async () => {
      if (ctx.hasId('glance:trout') && !ctx.hasId('keep-trout')) ctx.flow('tap-id', { ID: 'glance:trout' });
      await ctx.until(() => ctx.hasId('keep-trout'), { ms: 8_000, what: 'the trout table (keep-trout) under its fold' });
      try { ctx.flow('scroll-to-id', { ID: 'trout-boxes' }); } catch { /* the boxes already in view */ }
      return ctx.device.screenText().filter((w) => /^(Keep|Release|Closed|Up to)\b/.test(w)).slice(0, 4).join(' | ');
    }, { stop: true });
    await ctx.step("a row's picture opens that fish's sheet", async () => {
      ctx.flow('tap-id-in', { ID: 'pic:.*', IN: 'keep-trout' });
      await ctx.until(() => ctx.hasId('sheet-panel') || ctx.hasId('sheet-dialog'), { ms: 8_000, what: 'the fish sheet (sheet-panel)' });
      const w = ctx.device.screenText();
      if (!w.some((x) => /^Here, today$/i.test(x))) throw new Error(`the sheet has no "Here, today" (saw: ${w.slice(0, 10).join(' | ').slice(0, 300)})`);
      return w.slice(0, 4).join(' | ');
    });
    await ctx.step('Done closes the sheet', async () => {
      ctx.flow('tap-text', { TEXT: 'Done' });
      await ctx.until(() => !ctx.hasId('sheet-panel') && !ctx.hasId('sheet-dialog'), { ms: 8_000, what: 'the sheet closing after Done' });
    });
  },
};
