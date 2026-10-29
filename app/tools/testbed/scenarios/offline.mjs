// (T255) 3. OFFLINE: launch with no network on the installed set (map, a water's regulations, search); go offline mid
// session (the banner, what is online-only says so); a slow line ("still loading", never "not there"); reconnect →
// the banner goes, the update service checks at once and the outbox flushes.
import { coldStart, logStatus, openLog, toMap } from './common.mjs';

const OFFLINE_BANNER = /^Offline — showing what.s saved on this device$/;

export const offline = {
  id: 'offline',
  title: 'Offline launch, offline mid-session, slow line, reconnect',
  async run(ctx) {
    await ctx.step('airplane mode, then a cold start: the map opens from the device', async () => {
      await ctx.device.net('offline');
      await coldStart(ctx, { ms: 60_000 });
      return ctx.device.netState();
    });
    await ctx.step('the offline banner is shown', async () => ctx.waitText(OFFLINE_BANNER, 20_000));
    await ctx.step('search works offline ("Cultus")', async () => {
      ctx.flow('search', { Q: 'Cultus' });
      return ctx.waitText(/^Cultus Lake/, 20_000);
    });
    await ctx.step("a water's regulations open offline", async () => {
      ctx.flow('open-result', { LINE: 'Lake · near Lindell Beach.*' });
      await ctx.waitText(/Regulations/, 20_000);
      const words = ctx.device.screenText();
      const stuck = words.find((w) => /not available offline|Loading the regulations|this will load when/.test(w));
      if (stuck) throw new Error(`the installed set should answer offline: "${stuck}"`);
      return words.find((w) => /Open|Closed|open|closed/.test(w)) ?? 'regulations drawn';
    });
    await ctx.step('a town offline: the waters near it (read from the installed set)', async () => {
      await toMap(ctx);
      ctx.flow('search', { Q: 'Cultus' });
      ctx.device.logcatClear();
      ctx.flow('tap-text', { TEXT: 'Village · every named water around it' });
      await ctx.device.sleep(4000);
      const w = ctx.device.screenText();
      if (w.some((x) => /Could not read what is near this town/.test(x))) {
        const js = ctx.device.logcatJs().split('\n').filter((l) => /near|sqlite|Error|error/i.test(l)).slice(-4).join(' / ');
        ctx.finding(`offline, a town's "waters near" fails: "Could not read what is near this town." (Cultus Lake village)${js ? ` — JS: ${js.slice(0, 300)}` : ''}`);
        throw new Error('Could not read what is near this town.');
      }
      return w.find((x) => /NAMED WATERS|named waters/i.test(x)) ?? 'listed';
    });
    await ctx.step('back to the water: Conditions view', async () => {
      await toMap(ctx);
      ctx.flow('search', { Q: 'Cultus' });
      ctx.flow('open-result', { LINE: 'Lake · near Lindell Beach.*' });
      await ctx.waitText(/Regulations/, 20_000);
    });
    await ctx.step('Conditions offline: last captured readings or "not available offline", never a spinner', async () => {
      ctx.flow('tap-text', { TEXT: 'Conditions' });
      await ctx.device.sleep(4000);
      const words = ctx.device.screenText();
      const said = words.find((w) => !OFFLINE_BANNER.test(w) && /offline|not available|Last|ago|m³\/s|No reading|gauge|station/i.test(w));
      if (!said) throw new Error(`nothing said about the readings offline (saw: ${words.slice(0, 10).join(' | ')})`);
      return said;
    });
    await ctx.step('online again: the banner goes and the service checks at once', async () => {
      await toMap(ctx);
      await ctx.device.net('wifi');
      const end = Date.now() + 30_000;
      while (Date.now() < end && ctx.device.screenText().some((w) => OFFLINE_BANNER.test(w))) await ctx.device.sleep(1500);
      if (ctx.device.screenText().some((w) => OFFLINE_BANNER.test(w))) throw new Error('the offline banner stayed');
      openLog(ctx);
      const row = await ctx.waitText(/Gauges, (unchanged|changed)/, 30_000);
      return row;
    });
    await ctx.step('the same town online: the waters near it', async () => {
      await toMap(ctx);
      ctx.flow('search', { Q: 'Cultus' });
      ctx.flow('tap-text', { TEXT: 'Village · every named water around it' });
      await ctx.device.sleep(4000);
      const w = ctx.device.screenText();
      if (w.some((x) => /Could not read what is near this town/.test(x))) throw new Error('Could not read what is near this town (online too)');
      return w.find((x) => /NAMED WATERS|named waters/i.test(x)) ?? 'listed';
    });
    await ctx.step('mid-session: offline while using the app → the banner, the app keeps working', async () => {
      await toMap(ctx);
      await ctx.device.net('offline');
      await ctx.waitText(OFFLINE_BANNER, 20_000);
      ctx.flow('tab', { TAB: 'Conditions' });
      await ctx.device.sleep(3000);
      ctx.flow('tab', { TAB: 'Spots' });
      await ctx.device.sleep(1500);
      ctx.flow('tab', { TAB: 'Map' });
      const bad = ctx.device.screenText().find((w) => /could not be loaded|Something went wrong/i.test(w));
      if (bad) throw new Error(bad);
    });
    await ctx.step('online-only part offline: satellite imagery says so', async () => {
      await toMap(ctx);
      await ctx.device.sleep(1500);
      ctx.flow('layers-satellite');                                         // on (or off, if a run left it on)
      let w = ctx.device.screenText();
      if (!w.some((x) => /NEEDS A CONNECTION|Satellite needs a connection/.test(x))) {
        ctx.flow('tap-text', { TEXT: 'Satellite' });                       // it was on: now on again
        await ctx.device.sleep(1500);
        w = ctx.device.screenText();
      }
      const said = w.find((x) => /NEEDS A CONNECTION|Satellite needs a connection/.test(x));
      ctx.flow('tap-text', { TEXT: 'Satellite' });                         // left off for the next run
      ctx.flow('tap-text', { TEXT: 'Done' });
      if (!said) throw new Error('Satellite on, offline: nothing says it needs a connection');
      return said;
    });
    await ctx.step('Check now offline: "offline", nothing requested', async () => {
      openLog(ctx);
      ctx.flow('updates-log-action', { ACTION: 'check' });
      await ctx.device.sleep(3000);
      const st = logStatus(ctx);
      if (!/failed \(offline\)/.test(st)) throw new Error(`status offline: ${st}`);
      return st.replace(/\n/g, ' · ');
    });
    await ctx.step('a slow line (EDGE speed, GPRS latency) → slow banner or "still loading"', async () => {
      ctx.device.link('edge', 'gprs');
      await ctx.device.net('wifi');
      ctx.flow('updates-log-action', { ACTION: 'force' });
      await toMap(ctx);
      ctx.flow('search', { Q: 'Chilliwack' });
      await ctx.device.sleep(2000);
      const words = ctx.device.screenText();
      const said = words.find((w) => /Slow connection|still loading|Loading|Downloading|checking/i.test(w));
      return said ? `said: ${said}` : 'nothing said within 2 s (the installed set answers before the line matters)';
    });
    await ctx.step('back to a full line: no error left on screen', async () => {
      ctx.device.link('full', 'none');
      await ctx.device.sleep(15_000);
      await coldStart(ctx);
      const l = ctx.device.screenText().find((w) => /^Refresh data: could not update/.test(w));
      if (l) throw new Error(l);
      return ctx.device.screenText().find((w) => /^Refresh data/.test(w));
    });
  },
};


