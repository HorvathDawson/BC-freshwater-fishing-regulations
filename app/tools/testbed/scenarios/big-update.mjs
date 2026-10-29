// (T255) 2. A BIG UPDATE: a new release whose download is over the metered line (the basemap's id moves: ~270 MB).
// On mobile data it waits for Wi-Fi (sync button, More, the bell); on Wi-Fi it downloads (progress), is staged beside
// the set in use and applied at the next pause ("Updated to …"). Then the release gate: a release that needs a newer
// app is refused with "Update the app"; a corrupt release fails its checksum and the data in use is kept.
import { appUp, checkNow, logStatus, openLog, syncLabel, toMap } from './common.mjs';

export const bigUpdate = {
  id: 'big-update',
  title: 'A big release update: Wi-Fi hold, download, apply; app-too-old; corrupt',
  async run(ctx) {
    let target = null;
    await ctx.step('test channel reset; app up on Wi-Fi, current', async () => {
      ctx.updates('reset');
      await ctx.device.net('wifi');
      await appUp(ctx);
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
      if (!bell) throw new Error('no bell entry for an update waiting for Wi-Fi (the bell lists in-season changes and failed updates only)');
      return bell;
    });
    await ctx.step('More → Data says it', async () => {
      ctx.flow('tab', { TAB: 'More' });
      return ctx.waitText(/waiting for Wi-Fi/, 10_000);
    });
    await ctx.step('Wi-Fi back → the download starts by itself, with progress', async () => {
      await ctx.device.net('wifi');
      ctx.flow('tab', { TAB: 'Map' });
      const l = await ctx.waitText(/^Refresh data: (downloading an update, \d+%|an update is downloaded)/, 90_000);
      return l;
    });
    await ctx.step('downloaded and staged ("used at the next pause")', async () => {
      const l = await ctx.waitText(/^Refresh data: an update is downloaded/, 15 * 60_000);
      return l;
    });
    await ctx.step('next pause (background → foreground) → applied, "Updated to …"', async () => {
      ctx.device.background();
      await ctx.device.sleep(3000);
      ctx.device.launch();
      return ctx.waitText(/^Updated to |^Data updated$/, 60_000);
    });
    await ctx.step('Updates log: release staged then applied, the new release in use', async () => {
      openLog(ctx);
      await ctx.waitText(/Release, applied/, 20_000);
      const head = ctx.device.screenText().find((w) => /release r-/.test(w)) ?? '';
      if (!head.includes(target)) throw new Error(`in use: ${head}`);
      return head;
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
    await ctx.step('reset: back to the live release (a newer-looking set is never left on the device)', async () => {
      // the live set's basemap id differs from the big one's: this is a second big download (Wi-Fi), then applied
      ctx.updates('reset');
      await checkNow(ctx, /Release, changed/);
      await toMap(ctx);
      await ctx.waitText(/^Refresh data: an update is downloaded/, 15 * 60_000);
      ctx.device.background(); await ctx.device.sleep(3000); ctx.device.launch();
      await ctx.waitText(/^Updated to |^Data updated$/, 60_000);
      openLog(ctx);
      return logStatus(ctx).replace(/\n/g, ' · ');
    });
  },
};
