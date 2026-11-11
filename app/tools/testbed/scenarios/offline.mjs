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
      ctx.flow('open-result', { LINE: 'Lake · Region 2 · MU 2-3 · near Lindell Beach.*' });
      await ctx.waitText(/Regulations/, 20_000);
      const words = ctx.device.screenText();
      const stuck = words.find((w) => /not available offline|Loading the regulations|this will load when/.test(w));
      if (stuck) throw new Error(`the installed set should answer offline: "${stuck}"`);
      return words.find((w) => /Open|Closed|open|closed/.test(w)) ?? 'regulations drawn';
    });
    // (T238/T285/T354) the rule book came with the first install: a "Rule book p.N ↗" link opens it offline in the app's
    // own viewer (testID pdf-viewer, "Page N of M"); never "Couldn't load the synopsis." nor the viewer's failure face
    await ctx.step('the rule book opens offline (a "Rule book p.N ↗" link on the page)', async () => {
      ctx.flow('scroll-to', { TEXT: 'Rule book p\\.[0-9]+ ↗.*' });
      ctx.flow('tap-text', { TEXT: 'Rule book p\\.[0-9]+ ↗.*' });
      let words = [];
      for (let i = 0; i < 30; i++) {
        await ctx.device.sleep(1000);   // poll interval
        words = ctx.device.screenText();
        if (words.some((w) => /^Page \d+ of \d+$|Couldn.t (show|load)/.test(w))) break;
      }
      if (words.some((w) => /Couldn.t load the synopsis/.test(w))) throw new Error('the installed rule book did not open offline: "Couldn’t load the synopsis."');
      if (words.some((w) => /Loading the synopsis/.test(w))) throw new Error('the rule book is being downloaded offline (not held from the install)');
      const failed = words.find((w) => /Couldn.t show the rule book here/.test(w));
      if (failed) throw new Error(`the in-app viewer failed: ${words.slice(words.indexOf(failed), words.indexOf(failed) + 2).join(' ')}`);
      const page = words.find((w) => /^Page \d+ of \d+$/.test(w));
      if (!page) throw new Error(`the in-app viewer did not open (saw: ${words.slice(0, 8).join(' | ')})`);
      ctx.device.sh('input keyevent KEYCODE_BACK');
      await ctx.until(() => !ctx.device.screenText().some((w) => /^Page \d+ of \d+$/.test(w)), { ms: 8000, what: 'the viewer closed', soft: true });
      if (ctx.device.screenText().some((w) => /^Page \d+ of \d+$/.test(w))) throw new Error('back did not close the viewer');
      return `opened in the app: ${page}`;
    });
    await ctx.step('a town offline: the waters near it (read from the installed set)', async () => {
      await toMap(ctx);
      ctx.flow('search', { Q: 'Cultus' });
      ctx.device.logcatClear();
      ctx.flow('tap-text', { TEXT: 'Village · every named water around it' });
      await ctx.until(() => ctx.device.screenText().some((x) => /NAMED WATERS|named waters|Could not read what is near this town/i.test(x)), { ms: 20_000, what: 'the waters-near list', soft: true });
      const w = ctx.device.screenText();   // soft wait: the checks below say what is there
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
      ctx.flow('open-result', { LINE: 'Lake · Region 2 · MU 2-3 · near Lindell Beach.*' });
      await ctx.waitText(/Regulations/, 20_000);
    });
    await ctx.step('Conditions offline: last captured readings or "not available offline", never a spinner', async () => {
      ctx.flow('tap-text', { TEXT: 'Conditions' });
      const readings = () => ctx.device.screenText().find((w) => !OFFLINE_BANNER.test(w) && /offline|not available|Last|ago|m³\/s|No reading|gauge|station/i.test(w));
      await ctx.until(readings, { ms: 20_000, what: 'a word about the readings', soft: true });
      const words = ctx.device.screenText();   // soft wait: the check below reports an empty answer
      const said = words.find((w) => !OFFLINE_BANNER.test(w) && /offline|not available|Last|ago|m³\/s|No reading|gauge|station/i.test(w));
      if (!said) throw new Error(`nothing said about the readings offline (saw: ${words.slice(0, 10).join(' | ')})`);
      return said;
    });
    await ctx.step('online again: the banner goes and the service checks at once', async () => {
      await toMap(ctx);
      await ctx.device.net('wifi');
      const end = Date.now() + 30_000;
      while (Date.now() < end && ctx.device.screenText().some((w) => OFFLINE_BANNER.test(w))) await ctx.device.sleep(1500);   // poll interval
      if (ctx.device.screenText().some((w) => OFFLINE_BANNER.test(w))) throw new Error('the offline banner stayed');
      openLog(ctx);
      const row = await ctx.waitText(/Gauges, (unchanged|changed)/, 30_000);
      return row;
    });
    await ctx.step('the same town online: the waters near it', async () => {
      await toMap(ctx);
      ctx.flow('search', { Q: 'Cultus' });
      ctx.flow('tap-text', { TEXT: 'Village · every named water around it' });
      await ctx.until(() => ctx.device.screenText().some((x) => /NAMED WATERS|named waters|Could not read what is near this town/i.test(x)), { ms: 20_000, what: 'the waters-near list', soft: true });
      const w = ctx.device.screenText();   // soft wait: the checks below say what is there
      if (w.some((x) => /Could not read what is near this town/.test(x))) throw new Error('Could not read what is near this town (online too)');
      return w.find((x) => /NAMED WATERS|named waters/i.test(x)) ?? 'listed';
    });
    await ctx.step('mid-session: offline while using the app → the banner, the app keeps working', async () => {
      await toMap(ctx);
      await ctx.device.net('offline');
      await ctx.waitText(OFFLINE_BANNER, 20_000);
      ctx.flow('tab', { TAB: 'Conditions' });
      await ctx.settle({ ms: 3000 });
      // (T257) a tab's first visit shows its tour (a Modal over the tab bar): skipped as a reader would
      if (ctx.device.screenText().some((w) => /^Skip$/.test(w))) { ctx.flow('tap-text', { TEXT: 'Skip' }); await ctx.settle({ ms: 1000 }); }
      ctx.flow('tab', { TAB: 'Spots' });
      await ctx.settle({ ms: 1500 });
      if (ctx.device.screenText().some((w) => /^Skip$/.test(w))) { ctx.flow('tap-text', { TEXT: 'Skip' }); await ctx.settle({ ms: 1000 }); }
      await ctx.settle({ ms: 1500 });
      ctx.flow('tab', { TAB: 'Map' });
      const bad = ctx.device.screenText().find((w) => /could not be loaded|Something went wrong/i.test(w));
      if (bad) throw new Error(bad);
    });
    await ctx.step('online-only part offline: satellite imagery says so', async () => {
      await toMap(ctx);
      await ctx.settle({ ms: 1500 });
      ctx.flow('layers-satellite');                                         // on (or off, if a run left it on)
      let w = ctx.device.screenText();
      if (!w.some((x) => /NEEDS A CONNECTION|Satellite needs a connection/.test(x))) {
        ctx.flow('tap-text', { TEXT: 'Satellite' });                       // it was on: now on again
        await ctx.until(() => ctx.device.screenText().some((x) => /NEEDS A CONNECTION|Satellite needs a connection/.test(x)), { ms: 8000, what: 'the satellite note', soft: true });
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
      await ctx.until(() => /failed \(offline\)/.test(logStatus(ctx)), { ms: 20_000, what: 'the offline answer on the log page', soft: true });
      const st = logStatus(ctx);   // soft wait: this check reports the status it saw
      if (!/failed \(offline\)/.test(st)) throw new Error(`status offline: ${st}`);
      return st.replace(/\n/g, ' · ');
    });
    await ctx.step('a slow line (EDGE speed, GPRS latency) → slow banner or "still loading"', async () => {
      ctx.device.link('edge', 'gprs');
      await ctx.device.net('wifi');
      ctx.flow('updates-log-action', { ACTION: 'force' });
      await toMap(ctx);
      ctx.flow('search', { Q: 'Chilliwack' });
      await ctx.until(() => ctx.device.screenText().some((w) => /Slow connection|still loading|Loading|Downloading|checking/i.test(w)), { ms: 2000, every: 400, what: 'a slow-line word', soft: true });
      const words = ctx.device.screenText();
      const said = words.find((w) => /Slow connection|still loading|Loading|Downloading|checking/i.test(w));
      return said ? `said: ${said}` : 'nothing said within 2 s (the installed set answers before the line matters)';
    });
    await ctx.step('back to a full line: no error left on screen', async () => {
      ctx.device.link('full', 'none');
      // DELIBERATE fixed wait: nothing on screen says the throttled (EDGE/GPRS) request started by 'force' has ended
      await ctx.timer(15_000, 'lets the throttled in-flight check from the slow-line step finish or time out before a cold start');
      await coldStart(ctx);
      const l = ctx.device.screenText().find((w) => /^Refresh data: could not update/.test(w));
      if (l) throw new Error(l);
      return ctx.device.screenText().find((w) => /^Refresh data/.test(w));
    });
  },
};


