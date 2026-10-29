// (T255) 0. FIRST RUN: a clean app on Wi-Fi installs the whole staging release (the test channel's manifest, a copy of
// the live one) for offline use, then opens the map. Everything after builds on this install.
import { appUp, logStatus, openLog } from './common.mjs';

export const install = {
  id: 'install',
  title: 'First run downloads the release and opens the map',
  async run(ctx) {
    await ctx.step('test channel reset to the live release', () => {
      const s = ctx.updates('reset');
      return `test/ = ${s.release.data_release}`;
    }, { shot: false });
    await ctx.step('clean app on Wi-Fi', async () => {
      ctx.device.stop(); ctx.device.clearData();
      await ctx.device.net('wifi'); ctx.device.link('full', 'none');
      ctx.device.launch();
      await ctx.device.sleep(4000);
    });
    await ctx.step('first-run download, then the map', async () => {
      const t0 = Date.now();
      // the first-run notice / walkthrough may sit over the map: dismissed by the flow when present
      await appUp(ctx, { launch: false, ms: 15 * 60_000 });
      ctx.flow('dismiss-first-run');
      return `opened in ${Math.round((Date.now() - t0) / 1000)} s after launch`;
    }, { stop: true });
    await ctx.step('Updates log: every source checked on start', async () => {
      openLog(ctx);
      await ctx.device.sleep(3000);
      const st = logStatus(ctx);
      if (!/Gauges: ok/.test(st) || !/Release: ok/.test(st)) throw new Error(`status: ${st || 'not read'}`);
      return st.replace(/\n/g, ' · ');
    });
  },
};
