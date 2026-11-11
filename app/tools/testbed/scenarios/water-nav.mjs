// (T345) STEPPING A WATER'S PARTS ON ITS MAP VIEW, and what stays lit. Owner 2026-11-03: on the Coquitlam River's Map
// view, sequences of ▲/▼ steps (and picks from "Show all N parts") left the "Lougheed bridge to watershed boundary"
// highlight running past the bridge, and "Maryhill bypass to Lougheed bridge" lit as a sliver. The data and the pane's
// parts were checked clean (T345 findings), so this drives the screen: after every step a screenshot and the part's
// name (`water-map-name`), into `T345-<n>-<what>.png` beside the report, for a person to compare. A measurement: the
// steps pass when the screen answered.
//
//   node app/tools/testbed/run.mjs water-nav        (NAV_Q / NAV_LINE pick another water)
import fs from 'node:fs';
import path from 'node:path';
import { appUp, coldStart, toMap } from './common.mjs';
import { settledTarget } from './regs-open.mjs';

const Q = process.env.NAV_Q ?? 'Coquitlam River';
const ROW = process.env.NAV_ROW ?? '.* · Region 2 · MU 2-8.*';
const TITLE = new RegExp(process.env.NAV_TITLE ?? '^Coquitlam River');
/** `tap` (default): the water tapped on the map, as the owner did; `open`: the search result's Open (no tapped section) */
const VIA = process.env.NAV_VIA ?? 'tap';
const SEQ = ['up', 'up', 'down', 'up', 'down', 'down', 'up', 'up', 'up', 'down', 'down', 'down'];

export const waterNav = {
  id: 'water-nav',
  title: `Stepping ${Q}'s parts on its Map view: ▲/▼ sequences and the parts list (T345)`,
  async run(ctx) {
    let n = 0;
    const name = () => {
      ctx.device.sh('uiautomator dump /sdcard/tb-nav.xml >/dev/null 2>&1');
      const x = ctx.device.sh('cat /sdcard/tb-nav.xml');
      return /resource-id="water-map-name"[^>]*?(?:text|content-desc)="([^"]*)"/.exec(x)?.[1]
        ?? /(?:text|content-desc)="([^"]*)"[^>]*resource-id="water-map-name"/.exec(x)?.[1] ?? '?';
    };
    const shot = async (what) => {
      await ctx.settle({ ms: 2500, frame: true });             // the camera's fly and the highlight settle
      const label = name();
      ctx.device.screenshot(path.join(ctx.dir, `T345-${String(++n).padStart(2, '0')}-${what}.png`));
      return `${n} ${what}: ${label}`;
    };
    const log = [];
    /** The whole crash buffer (the Java stack), beside the report: the runner keeps only its first line. */
    const died = (what) => {
      fs.writeFileSync(path.join(ctx.dir, 'T345-crash.txt'), ctx.device.adb(['logcat', '-d', '-b', 'crash']).toString());
      return new Error(`the app died ${what}: T345-crash.txt`);
    };
    /** The owner's way in (2026-11-03): the water TAPPED on the map (the frame then holds the tapped section), its
     *  regulations, then its Map view. */
    const tapIn = async () => {
      // a water still open from before (its screen hides the tab bar): closed by its own ✕
      for (let i = 0; i < 3; i++) {
        ctx.device.sh('uiautomator dump /sdcard/tb-nav.xml >/dev/null 2>&1');
        if (!ctx.device.sh('cat /sdcard/tb-nav.xml').includes('resource-id="water-close"')) break;
        ctx.flow('tap-id', { ID: 'water-close' }); await ctx.until(() => !ctx.hasId('water-close'), { ms: 6000, what: 'the water closed', soft: true });
      }
      await toMap(ctx);
      ctx.device.logcatClear();
      ctx.flow('search', { Q });
      const rows = ctx.device.screenText().filter((w) => / · /.test(w)).slice(0, 6);
      // (T340) both ways open the water from its result; a TAP then goes Back to the map, which comes back framed on it
      // (useShellSearch `arrive`) — closing search instead restores the camera from before search (T48)
      ctx.flow('open-result', { LINE: ROW });
      await ctx.until(() => ctx.hasId('water-view-regs'), { ms: 15_000, what: 'the water screen (water-view-regs)', soft: true });
      if (VIA !== 'open') { ctx.device.sh('input keyevent 4'); await ctx.settle({ ms: 3000, frame: true }); }
      if (VIA !== 'open') {
        const at = await settledTarget(ctx);
        if (!at) throw new Error('the app named no tap target for the water (no [target] line)');
        ctx.device.screenshot(path.join(ctx.dir, `T478-before-tap.png`));   // (T478) what the tap lands on
        ctx.device.sh(`input tap ${at.x} ${at.y}`);
        // (0.5.0 drift) beside the tap: the section the probe hit there (a confluence tap
        // opened the unnamed tributary)
        const hits = [...ctx.device.logcatJs().matchAll(/\[target\] (\{.*\})/g)].map((m) => m[1]);
        await ctx.settle({ ms: 2500, frame: true });
        console.log(`  opened: ${[...ctx.device.logcatJs().matchAll(/\[(?:open|tap)\] (\{.{0,260})/g)].map((m) => m[1]).join(', ') || '?'}`);
        console.log(`  tap ${at.x},${at.y} px; ${hits.length} probe line(s), the last (tapped) ${hits.at(-1) ?? '?'}; first ${hits[0] ?? '?'}`);
      }
      // the water's own screen (its view strip), and the water tapped is the one searched (not a neighbour; the search
      // field keeps the query, so the title alone proves nothing)
      const end = Date.now() + 30_000;
      let words = [];
      while (Date.now() < end) {
        ctx.device.sh('uiautomator dump /sdcard/tb-nav.xml >/dev/null 2>&1');
        const x = ctx.device.sh('cat /sdcard/tb-nav.xml');
        words = ctx.device.screenText();
        if (x.includes('resource-id="water-view-regs"') && words.some((w) => TITLE.test(w))) break;
        await ctx.device.sleep(1500);   // poll interval
      }
      if (!words.some((w) => TITLE.test(w)) || !ctx.device.sh('cat /sdcard/tb-nav.xml').includes('water-view-regs'))
        throw new Error(`the tap did not open ${Q} (rows: ${rows.join(' / ')}; saw: ${words.slice(0, 10).join(' | ')})`);
      const regs = await shot('regs');
      ctx.flow('tap-id', { ID: 'water-view-map' });
      await ctx.waitText(/Show all \d+ parts|Lowest part|Highest part|Upstream|Downstream/, 30_000);
      return `${regs} → ${await shot('map')}`;
    };
    await ctx.step('a cold start (nothing left open by an earlier scenario)', () => coldStart(ctx), { stop: true });
    await ctx.step(`${Q}: tapped on the map → its regulations → Map`, tapIn, { stop: true });
    await ctx.step('one ▲ (owner: the part runs past its bounds)', async () => {
      ctx.flow('tap-id', { ID: 'water-map-step-up' });
      return shot('up');
    });
    await ctx.step('"Show all N parts" → the lowest (owner: a sliver lit)', async () => {
      ctx.flow('tap-id', { ID: 'water-map-parts-toggle' });
      await ctx.until(() => ctx.device.screenText().some((w) => /^Mary/.test(w)), { ms: 6000, what: 'the parts list (Mary…)', soft: true });
      ctx.flow('tap-text', { TEXT: 'Mary.*' });
      return shot('pick-lowest');
    });
    await ctx.step('closed, the water opened again the same way, Map (owner: the app crashed)', async () => {
      ctx.device.sh('input keyevent 4'); await ctx.settle({ ms: 1000 });
      ctx.device.sh('input keyevent 4'); await ctx.settle({ ms: 1500 });
      let r;
      try { r = await tapIn(); } catch (e) { if (!ctx.device.pidOf()) throw died(e.message); throw e; }
      await ctx.timer(4000, 'a crash after opening the Map view arrives a beat late: watch before asserting the app is alive');
      if (!ctx.device.pidOf()) throw died(`opening the Map view again (${r})`);
      return r;
    });
    await ctx.step(`▲/▼ sequence (${SEQ.length} steps)`, async () => {
      for (const dir of SEQ) {
        try { ctx.flow('tap-id', { ID: `water-map-step-${dir}` }); } catch (e) { log.push(`${dir}: ${e.message}`); continue; }
        log.push(await shot(dir));
      }
      return log.join(' · ');
    });
    await ctx.step('a cold start, the water again (owner: "permanently broken")', async () => {
      ctx.device.stop(); await ctx.until(() => !ctx.device.pidOf(), { ms: 10_000, what: 'the stopped app gone' });
      await appUp(ctx, { ms: 60_000 });
      let r;
      try { r = await tapIn(); } catch (e) { if (!ctx.device.pidOf()) throw died(e.message); throw e; }
      await ctx.timer(4000, 'a crash on the Map view after a cold start arrives a beat late: watch before asserting the app is alive');
      if (!ctx.device.pidOf()) throw died(`on the Map view after a cold start (${r})`);
      return r;
    });
  },
};
