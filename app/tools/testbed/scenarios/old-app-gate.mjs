// (T810, 0.8.0 release order) AN OLD APP MEETS THE NEW RELEASE. Run with an OLD test-bed APK (0.7.x, before the trout
// cutover) installed by session.sh: the app takes test/ = the live release and opens the map; then test/ names the 0.8.0
// candidate (regs-region/2, trout 5) — first UNSTAMPED, then stamped minAppVersion 0.8.0 as `pipeline release` stamps it
// (app_gate). What must hold: the old app never breaks on the new manifest; it keeps the set it holds, opens the map from
// it after a cold start, and says "Update the app to get new regulations" once the release names 0.8.0; a FRESH old
// install on that release says the same words on its first run (nothing older to keep).
//
//   OLD_GATE_CANDIDATE=<release dir>   (required) the 0.8.0 candidate folder (r-…/ with manifest.json), put on test/ by
//                                      `test-updates side`; its tiles are not on staging yet, and an old app must never
//                                      ask for them (it stops at the manifest)
import { appUp, checkNow, coldStart, openLog, toMap } from './common.mjs';

const CANDIDATE = process.env.OLD_GATE_CANDIDATE ?? null;
const releaseIn = (ctx) => ctx.device.screenText().find((w) => /release r-[0-9a-f]+/.test(w))?.match(/release (r-[0-9a-f]+)/)?.[1] ?? null;

export const oldAppGate = {
  id: 'old-app-gate',
  title: `An old app on the 0.8.0 release (${CANDIDATE ?? 'OLD_GATE_CANDIDATE unset'}): keeps its data, says "Update the app"`,
  async run(ctx) {
    let held = null;
    await ctx.step('test/ = the live release; a clean old app opens the map', async () => {
      if (!CANDIDATE) throw new Error('OLD_GATE_CANDIDATE is unset: the 0.8.0 candidate release folder');
      ctx.updates('reset');
      ctx.device.stop(); ctx.device.cleanApp();
      await ctx.device.net('wifi');
      await appUp(ctx, { ms: 20 * 60_000 });
      openLog(ctx);
      held = await ctx.until(() => releaseIn(ctx), { ms: 30_000, what: 'the release in use on the log page' });
      await toMap(ctx);
      return `holds ${held}`;
    }, { stop: true });
    await ctx.step('test/ names the 0.8.0 candidate, UNSTAMPED: Check now; the data in use is kept', async () => {
      const id = ctx.updates('side', CANDIDATE).data_release;
      openLog(ctx);
      await checkNow(ctx, /Release, /);
      const now = releaseIn(ctx);
      if (now !== held) throw new Error(`in use: ${now}, not ${held}`);
      return `test/ = ${id}; in use ${now}; ${ctx.device.screenText().filter((w) => /^Release: /.test(w) || /could not be used|Update the app/.test(w)).join(' | ').slice(0, 200)}`;
    });
    await ctx.step('stamped minAppVersion 0.8.0 (as `pipeline release` stamps it): "Update the app to get new regulations"', async () => {
      ctx.updates('min-app', '0.8.0');
      await checkNow(ctx, /Release, (changed|app too old|unchanged)/);
      await ctx.waitText(/Update the app to get new regulations/, 20_000);
      const now = releaseIn(ctx);
      if (now !== held) throw new Error(`in use: ${now}, not ${held}`);
      return `in use ${now}`;
    });
    await ctx.step('a cold start: the map opens from the held set; the log still names it', async () => {
      await coldStart(ctx);
      openLog(ctx);
      const now = await ctx.until(() => releaseIn(ctx), { ms: 30_000, what: 'the release in use on the log page' });
      if (now !== held) throw new Error(`in use: ${now}, not ${held}`);
      await toMap(ctx);
      return `in use ${now}`;
    });
    await ctx.step('a FRESH old install on the 0.8.0 release: its first run says "Update the app to get new regulations"', async () => {
      ctx.device.stop(); ctx.device.cleanApp();
      ctx.device.launch();
      await ctx.waitText(/Update the app to get new regulations/, 120_000);
      return ctx.device.screenText().filter((w) => w.trim()).slice(0, 6).join(' | ');
    });
  },
};
