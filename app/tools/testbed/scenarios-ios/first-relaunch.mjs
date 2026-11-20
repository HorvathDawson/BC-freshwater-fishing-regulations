// (T756, T738, T760) THE iPHONE TWIN OF THE FIRST INSTALL'S REOPEN: a fresh install downloads the release; part way
// (the bar past `KILL_MB`) the app's process is ended (`simctl terminate`, as iOS ends a suspended app; NOT a swipe away,
// which cancels its background transfers by design) and opened again at once. Then:
//   T756  no running download is thrown away: the background session's job ids at the kill are still the ones the app
//         waits on (or has claimed), and no `[download] … queued again from zero` line is logged;
//   T738  the bar's first figure after the reopen is at least what the jobs held at the kill (`jobs.json`, written by
//         the module every 4 MB, so within a few MB), and it never goes down while the install runs;
//   T760  nothing is called stuck, the install ends on the map.
// The simulator shares the Mac's network (always Wi-Fi: "go"), so the "Wait for Wi-Fi" half of T756 and an iPhone on
// mobile data (T760's honest wait) are the owner's phone checks. Run on a FRESH install (`session-ios.sh --fresh`), by
// name only (`inAll: false`).
import fs from 'node:fs';
import path from 'node:path';

const KILL_MB = Number(process.env.FIRST_RELAUNCH_KILL_MB ?? 60);
/** "36.7 of 479.3 MB downloaded" → [36.7, 479.3] */
const barOf = (w) => {
  for (const x of w) { const m = /^([\d.]+) of ([\d.]+) MB downloaded$/.exec(x); if (m) return [Number(m[1]), Number(m[2])]; }
  return null;
};

export const firstRelaunchIos = {
  id: 'first-relaunch',
  inAll: false,
  title: 'T756/T738/T760: a first install whose process is ended part way and opened again keeps its downloads',
  async run(ctx) {
    const d = ctx.device;
    const shot = (name) => { try { d.screenshot(path.join(ctx.dir, `T738-ios-${name}.png`)); } catch { /* no shot */ } };
    let container = null;
    const jobsFile = () => path.join(container ??= d.dataDir(), 'Library', 'Application Support', 'bg-download', 'jobs.json');
    const jobs = () => { try { return JSON.parse(fs.readFileSync(jobsFile(), 'utf8')).jobs ?? []; } catch { return []; } };
    const mb = (b) => Math.round(Math.max(0, b) / 1048576 * 10) / 10;
    let atKill = [], barAtKill = null, heldAtKill = 0;

    await ctx.step(`fresh install, first open: the setup bar past ${KILL_MB} MB; the process ended`, async () => {
      d.logcatClear();
      d.launch();
      const t0 = Date.now();
      let bar = null;
      while (Date.now() - t0 < 240_000) {
        const w = d.screenText();
        if (w.some((x) => /Would Like to Send You Notifications/i.test(x))) ctx.flow('ios-alert', { BUTTON: 'Allow.*' });
        bar = barOf(w) ?? bar;
        if (bar && bar[0] >= KILL_MB) break;
        await d.sleep(400);
      }
      if (!bar || bar[0] < KILL_MB) throw new Error(`the bar did not pass ${KILL_MB} MB in 240 s (last: ${bar ? `${bar[0]} of ${bar[1]}` : 'no bar'})`);
      shot('1-before-kill');
      d.stop();
      await d.sleep(800);
      if (d.pidOf()) throw new Error('the app is still running after simctl terminate');
      barAtKill = bar[0];
      atKill = jobs();
      heldAtKill = mb(atKill.reduce((s, j) => s + Math.max(0, j.bytes), 0));
      return `ended at the bar's ${bar[0]} of ${bar[1]} MB; the background session held ${heldAtKill} MB in ${atKill.length} job(s): `
        + atKill.map((j) => `#${j.id} ${j.url.split('/').pop()} ${j.status} ${mb(j.bytes)} MB`).join(' · ');
    });

    const seen = [];
    await ctx.step('opened again at once: the first figure is at least what was held; the bar never goes down', async () => {
      if (barAtKill === null) ctx.skip('the step before did not end the app mid-download');
      d.launch();
      const t0 = Date.now();
      let first = null;
      while (Date.now() - t0 < 600_000) {
        const w = d.screenText();
        const bar = barOf(w);
        if (bar) {
          seen.push({ s: Math.round((Date.now() - t0) / 100) / 10, mb: bar[0] });
          if (!first) { first = { s: seen.at(-1).s, mb: bar[0] }; shot(`2-reopened-${first.s}s`); }
        } else if (first && !w.some((x) => /Checking the downloaded files|Downloading the map/.test(x))) break;
        await d.sleep(500);
      }
      fs.writeFileSync(path.join(ctx.dir, 'T738-ios-bar.json'), `${JSON.stringify({ barAtKill, heldAtKill, atKill, seen }, null, 1)}\n`);
      if (!first) throw new Error('no setup bar after the reopen (the install may have ended before the first look)');
      const downs = seen.filter((x, i) => i > 0 && x.mb < seen[i - 1].mb - 0.05);
      const bad = [
        ...(first.mb + 5 < heldAtKill ? [`the first figure ${first.mb} MB is under the ${heldAtKill} MB the jobs held at the kill`] : []),
        ...(downs.length ? [`the bar went down: ${downs.map((x) => `${x.mb} MB at ${x.s} s`).join(', ')}`] : []),
      ];
      if (bad.length) throw new Error(bad.join(' ; '));
      return `first figure ${first.mb} MB at +${first.s} s (held at the kill ${heldAtKill} MB, the bar then said ${barAtKill}); ${seen.length} looks, never down`;
    });

    await ctx.step('the install ends on the map; no download queued again from zero, none called stuck', async () => {
      await ctx.ios.appUp(ctx, { launch: !d.pidOf(), ms: 600_000 });
      const lines = d.appLogLines(0).filter((l) => l.includes('[download]'));
      fs.writeFileSync(path.join(ctx.dir, 'T756-ios-download-lines.txt'), `${lines.join('\n')}\n`);
      const again = lines.filter((l) => /queued again from zero|removed and queued again|the install fails/.test(l));
      // each job the session held at the kill (not yet done) is the one the reopened app claimed: its url "done (system
      // download manager)" and no second enqueue of it (a new job would show as a record with another id in jobs.json)
      const after = jobs();
      const renewed = atKill.filter((j) => j.status !== 'done' && after.some((a) => a.url === j.url && a.id !== j.id));
      shot('3-map');
      if (again.length || renewed.length) {
        throw new Error([...again.map((l) => l.slice(l.indexOf('[download]'))),
          ...renewed.map((j) => `${j.url.split('/').pop()}: job #${j.id} replaced`)].join(' ; '));
      }
      const claimed = atKill.filter((j) => lines.some((l) => l.includes(j.url) && /done \(system download manager\)/.test(l)));
      return `map up; ${claimed.length} of ${atKill.length} job(s) held at the kill claimed as they were; nothing queued again`;
    });
  },
};
