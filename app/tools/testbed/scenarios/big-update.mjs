// (T255) 2. A BIG UPDATE: a new release whose download is over the metered line (the basemap's id moves: ~270 MB).
// On mobile data it waits for Wi-Fi (sync button, More, the bell); on Wi-Fi it downloads (progress), is staged beside
// the set in use and applied at the next pause ("Updated to …"). Then the release gate: a release that needs a newer
// app is refused with "Update the app"; a corrupt release fails its checksum and the data in use is kept.
import { appUp, checkNow, logStatus, openLog, syncLabel, toMap, coldStart } from './common.mjs';

export const bigUpdate = {
  id: 'big-update',
  title: 'A big release update: Wi-Fi hold, download, apply; app-too-old; corrupt',
  async run(ctx) {
    let target = null;
    await ctx.step('test channel reset; app up on Wi-Fi, current', async () => {
      ctx.updates('reset');
      await ctx.device.net('wifi');
      await coldStart(ctx);   // every scenario starts from a fresh launch (the data stays)
    });
    await ctx.step('a BIG release in R2; the phone on mobile data (metered)', async () => {
      target = ctx.updates('big').data_release;
      await ctx.device.net('cellular');
      return `${target} · network ${ctx.device.netState()}`;
    });
    await ctx.step('Check now → "waiting for Wi-Fi" (nothing downloaded)', async () => {
      openLog(ctx);
      await checkNow(ctx, /Release, changed|Release, unchanged/);
      await ctx.device.sleep(3000);
      const st = logStatus(ctx);
      if (!/waiting-wifi/.test(st)) throw new Error(`release phase: ${st}`);
      return st.split('\n').find((l) => /Release/.test(l));
    });
    await ctx.step('top bar says it: the sync button', async () => {
      await toMap(ctx);
      const l = syncLabel(ctx);
      if (!/waiting for Wi-Fi/.test(l ?? '')) throw new Error(`sync label: ${l}`);
      return l;
    });
    await ctx.step('the bell lists the update waiting', async () => {
      const bell = ctx.device.screenText().find((w) => /update/i.test(w) && /bell|Notices|waiting|available/i.test(w) && !/^Refresh data/.test(w));
      if (!bell) throw new Error('no bell entry for an update waiting for Wi-Fi (the bell lists in-season changes and failed updates only) — ticket T263');
      return bell;
    });
    await ctx.step('More → Data says it', async () => {
      ctx.flow('tab', { TAB: 'More' });
      return ctx.waitText(/waiting for Wi-Fi/, 10_000);
    });
    await ctx.step('Wi-Fi back → the download starts by itself, with progress', async () => {
      await ctx.device.net('wifi');
      ctx.flow('tab', { TAB: 'Map' });
      try { return await ctx.waitText(/^Refresh data: (downloading an update, \d+%|an update is downloaded)/, 120_000); }
      catch (e) { throw new Error(`${e.message.split(' (saw')[0]} — still held for Wi-Fi: ticket T265`); }
    });
    await ctx.step('(T265 until fixed) Check now on Wi-Fi → it downloads and is staged', async () => {
      openLog(ctx);
      ctx.flow('updates-log-action', { ACTION: 'check' });
      return ctx.waitText(/Release, staged/, 15 * 60_000);
    });
    await ctx.step('next pause (background → foreground) → applied; the new release in use', async () => {
      await toMap(ctx);
      ctx.device.background();
      await ctx.device.sleep(3000);
      ctx.device.launch();
      const toast = await ctx.waitText(/^Updated to |^Data updated$/, 20_000).catch(() => null);
      openLog(ctx);
      await ctx.waitText(/Release, applied/, 30_000);
      const head = ctx.device.screenText().find((w) => /release r-/.test(w)) ?? '';
      if (!head.includes(target)) throw new Error(`in use: ${head}`);
      return `${toast ? `toast "${toast}"` : 'no toast seen'} · ${head}`;
    });
    await ctx.step('a release that needs app 99.0.0 → "Update the app", data kept', async () => {
      ctx.updates('min-app', '99.0.0');
      await checkNow(ctx, /Release, (changed|app too old)/);
      await ctx.waitText(/Update the app to get new regulations/, 20_000);
      return logStatus(ctx).split('\n').find((l) => /Release/.test(l));
    });
    await ctx.step('More says "Update the app to get new regulations"', async () => {
      await toMap(ctx);
      ctx.flow('tab', { TAB: 'More' });
      return ctx.waitText(/Update the app to get new regulations/, 10_000);
    });
    await ctx.step('a corrupt release → sha-mismatch, the set in use kept', async () => {
      ctx.updates('corrupt');
      openLog(ctx);
      await checkNow(ctx, /Release, (changed|failed)/);
      const row = await ctx.waitText(/sha-mismatch/, 120_000);
      return row;
    });
    await ctx.step('reset: back to the live release (its basemap id differs again: a second big download, on Wi-Fi)', async () => {
      ctx.updates('reset');
      openLog(ctx);
      ctx.flow('updates-log-action', { ACTION: 'clear' });
      await checkNow(ctx, /Release, changed/);
      await ctx.waitText(/Release, staged/, 15 * 60_000);
      await toMap(ctx);
      ctx.device.background(); await ctx.device.sleep(3000); ctx.device.launch();
      openLog(ctx);
      await ctx.waitText(/Release, applied/, 30_000);
      return logStatus(ctx).replace(/\n/g, ' · ');
    });
  },
};
