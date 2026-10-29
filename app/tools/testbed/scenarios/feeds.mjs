// (T255) 1. NEW FEEDS IN R2: a feed index changes on the test channel → the app sees it (on a foreground return by
// itself, and on Check now), downloads it, and the Updates log + the sync button say so; a feed that 404s is a failure
// the sync button badges and the bell lists, and clears when it is back.
import { appUp, checkNow, logStatus, openLog, syncLabel, toMap, coldStart } from './common.mjs';

export const feeds = {
  id: 'feeds',
  title: 'New feeds in R2 are detected, downloaded and shown',
  async run(ctx) {
    await ctx.step('test channel reset; app up on Wi-Fi', async () => {
      ctx.updates('reset');
      await ctx.device.net('wifi');
      await coldStart(ctx);   // every scenario starts from a fresh launch (the data stays)
    });
    await ctx.step('Check now: every feed unchanged (304)', async () => {
      openLog(ctx);
      ctx.flow('updates-log-action', { ACTION: 'clear' });
      await checkNow(ctx, /Gauges, unchanged, 304/);
      await ctx.waitText(/In-season, unchanged/, 30_000);
      return logStatus(ctx).replace(/\n/g, ' · ');
    });
    await ctx.step('gauges bumped in R2 → detected on return to the foreground (no tap)', async () => {
      ctx.updates('feed-bump', 'gauges');
      ctx.device.background();
      await ctx.device.sleep(65_000);                      // past foregroundAfterMs.feeds (60 s)
      ctx.device.launch();
      const row = await ctx.waitText(/Gauges, changed, 200/, 60_000);
      if (/manual/.test(row)) throw new Error(`the change came from a manual check: ${row}`);
      return row;
    });
    await ctx.step('DFO + in-season bumped → Check now downloads both', async () => {
      ctx.updates('feed-bump', 'dfo');
      ctx.updates('feed-bump', 'inseason');
      await checkNow(ctx, /DFO.*changed, 200.*manual|Salmon.*changed, 200.*manual/);
      return ctx.waitText(/In-season, changed, 200/, 30_000);
    });
    await ctx.step('sync button: up to date after the check', async () => {
      await toMap(ctx);
      const l = syncLabel(ctx);
      if (!l || !/up to date/.test(l)) throw new Error(`sync label: ${l}`);
      return l;
    });
    await ctx.step('DFO index removed (404) → sync error badge + bell', async () => {
      ctx.updates('feed-404', 'dfo');
      ctx.flow('tap-sync');
      await ctx.device.sleep(4000);
      const words = ctx.device.screenText();
      const label = words.find((w) => /^Refresh data: could not update/.test(w));
      const bell = words.find((w) => /update failed/.test(w));
      if (!label) throw new Error(`no error state on the sync button (${syncLabel(ctx)})`);
      if (!bell) throw new Error('the bell does not list the failed update');
      return `${label} · bell: ${bell}`;
    });
    await ctx.step('the bell opens what failed, with Retry', async () => {
      ctx.flow('open-bell');
      await ctx.waitText(/Could not update|Notices/, 10_000);
      return ctx.waitText(/server did not answer|HTTP|Last succeeded/, 10_000);
    });
    await ctx.step('feed back → Retry clears the error', async () => {
      ctx.updates('reset');
      ctx.flow('tap-text', { TEXT: 'Retry' });
      await ctx.device.sleep(5000);
      await toMap(ctx);
      const l = syncLabel(ctx);
      if (!l || /could not update/.test(l)) throw new Error(`still failed: ${l}`);
      return l;
    });
  },
};
