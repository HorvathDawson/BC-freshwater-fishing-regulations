// (T912) THE iOS TWIN OF scenarios/bg-update.mjs: an update keeps downloading with the app closed, on the T722 background
// URLSession (modules/bg-download/ios). The app holds the live release; the test channel names one that changes a data
// file and the rule book (`test-updates.mjs data-book`, the book padded to ~300 MB: the simulator's line cannot be slowed,
// and the download must outlast the close). Check now starts it; the moment it downloads the app is sent Home and its
// process ended (`simctl terminate`, as the system ends a background app). NOT a swipe away in the app switcher: iOS
// cancels a force-quit app's background transfers by design (the next open resumes them).
// With the app ended, the transfers must go on in iOS's own daemon: the module's job records (`Application Support/
// bg-download/jobs.json`) and files are read from the app's container. A finished background transfer is handed to the
// app by iOS relaunching it in the background (`handleEventsForBackgroundURLSession`): a record that turns "done", with
// its file, while nobody opened the app is that proof. The next open must claim them (nothing downloaded again), stage,
// and apply at the next pause. Then back to the live release. By name only (`inAll: false`).
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';

const HERE = path.dirname(new URL(import.meta.url).pathname);
const ROOT = path.resolve(HERE, '../../../..');
const PAD_MB = Number(process.env.BG_UPDATE_PAD_MB ?? 300);
const updates = (...a) => JSON.parse(execFileSync('node', [path.join(ROOT, 'deploy/staging/test-updates.mjs'), ...a],
                                                  { encoding: 'utf8', timeout: 20 * 60_000, stdio: ['ignore', 'pipe', 'pipe'] }));

export const bgUpdateIos = {
  id: 'bg-update',
  inAll: false,
  title: 'T912: an update keeps downloading with the app closed (Home + process ended); the next open claims and applies it',
  async run(ctx) {
    const d = ctx.device;
    const shot = (name) => { try { d.screenshot(path.join(ctx.dir, `T912-ios-${name}.png`)); } catch { /* no shot */ } };
    const status = () => d.screenText().find((w) => /Gauges: /.test(w) && /Release: /.test(w)) ?? '';
    let container = null;   // the app's data container: one simctl call (the close below polls the records every 150 ms)
    const jobsFile = () => path.join(container ??= d.dataDir(), 'Library', 'Application Support', 'bg-download', 'jobs.json');
    const jobs = () => { try { return JSON.parse(fs.readFileSync(jobsFile(), 'utf8')).jobs ?? []; } catch { return []; } };
    // the Updates log from the map: a card already up is often missing from the accessibility tree after a relaunch, and
    // the flow that opens it then fails on the map's chip — so back to the map first, then the flow
    const freshLog = async () => { await ctx.ios.toMap(ctx); await ctx.ios.openLog(ctx); };
    const checkNow = async (re, ms = 60_000) => { ctx.flow('updates-log-action', { ACTION: 'check' }); return ctx.waitText(re, ms); };
    let target = null, urls = null, ours = [];
    /** files the app had already claimed from the background session before it was ended (a small one lands in a second) */
    const before = new Set();
    const claimedIn = (text, u) => text.split('\n').some((l) => l.includes('[download]') && l.includes(u) && /done \(system download manager\)/.test(l));

    await ctx.step('test channel reset; app up, holding the live release', async () => {
      const live = updates('reset').release.data_release;
      await ctx.ios.coldStart(ctx);
      await ctx.ios.openLog(ctx);
      await checkNow(/Release, (unchanged|changed)/);
      const st = status();
      if (/downloading|ready|waiting/.test(st)) throw new Error(`the app does not hold the live release ${live} (${st})`);
      return `live ${live} · ${st}`;
    });

    await ctx.step(`an update that changes a data file and the rule book (data-book, the book padded ${PAD_MB} MB)`, async () => {
      const r = updates('data-book', String(Date.now() % 100000), String(PAD_MB));
      target = r.data_release;
      urls = { data: r.data.split('/').pop(), book: r.book.split('/').pop() };
      return `${target} · ${urls.data} · ${urls.book} · ${(r.bytes / 1e6).toFixed(1)} MB`;
    });

    let killedAt = 0, atKill = [];
    await ctx.step('Check now: the update downloads; sent Home and the process ended at once', async () => {
      d.logcatClear();
      await ctx.ios.openLog(ctx);
      ctx.flow('updates-log-action', { ACTION: 'check' });
      // the moment the rule book is with the background session (its record on disk, read off the container: a screen
      // read is too slow — on the Mac's line the whole update landed before one), the app goes
      const t0 = Date.now();
      while (Date.now() - t0 < 120_000 && !jobs().some((j) => j.url.endsWith(urls.book))) await d.sleep(150);
      if (!jobs().some((j) => j.url.endsWith(urls.book))) throw new Error('the rule book was not queued with the background session within 120 s of Check now');
      d.background();
      await d.sleep(500);
      d.stop();
      killedAt = Date.now();
      await d.sleep(1000);
      if (d.pidOf()) throw new Error('the app is still running after simctl terminate');
      atKill = jobs();
      const js = d.logcatJs();
      for (const u of [urls.data, urls.book]) if (claimedIn(js, u)) before.add(u);
      ours = atKill.filter((j) => j.url.endsWith(urls.data) || j.url.endsWith(urls.book));
      const missing = [urls.data, urls.book].filter((u) => !before.has(u) && !ours.some((j) => j.url.endsWith(u)));
      if (missing.length) throw new Error(`not with the background session when the app was ended (neither claimed nor queued): ${missing.join(', ')} (records: ${atKill.map((j) => `${j.url.split('/').pop()} ${j.status}`).join(', ') || 'none'})`);
      if (!ours.length) throw new Error('BENCH RACE: both files were down and claimed before the app was ended; nothing left to prove');
      return `ended; already claimed from the background session: ${[...before].join(', ') || 'none'} · still with it: ${ours.map((j) => `${j.url.split('/').pop()} ${j.status} ${Math.round(Math.max(0, j.bytes) / 1e6)} of ${Math.round(j.total / 1e6)} MB`).join(' · ')}`;
    });

    await ctx.step('with the app ended, iOS downloads what is left and hands it to the app in the background', async () => {
      if (!ours.length) throw new Error('nothing was left with the background session at the close (the step before failed)');
      const t0 = Date.now(), CAP = 12 * 60_000;
      let relaunched = false;
      for (;;) {
        if (d.pidOf()) relaunched = true;
        const js = jobs().filter((j) => ours.some((o) => o.id === j.id));
        const failed = js.filter((j) => j.status === 'failed');
        if (failed.length) throw new Error(`a background transfer failed with the app ended: ${failed.map((j) => `${j.url.split('/').pop()} reason ${j.reason}`).join(', ')}`);
        // each one: its record "done" (iOS handed the file to the relaunched app), or gone because that app's JS (it runs in
        // the background launch too) already claimed it — said in the log
        const log = d.logcatJs();
        const landed = ours.map((o) => {
          const r = js.find((j) => j.id === o.id), name = o.url.split('/').pop();
          if (r?.status === 'done') return `${name}: done, handed to the app`;
          if (!r && claimedIn(log, name)) return `${name}: claimed by the app in its background launch`;
          return null;
        });
        if (landed.every(Boolean)) {
          return `${landed.join(' · ')} — ${Math.round((Date.now() - killedAt) / 1000)} s after the process was ended, the bench never opening the app; iOS ${relaunched ? 'relaunched the app in the background (a session wake)' : 'did not show the app running while we polled'}`;
        }
        if (Date.now() - t0 > CAP) {
          throw new Error(`not done ${CAP / 60_000} min after the process was ended: ${js.map((j) => `${j.url.split('/').pop()} ${j.status} ${Math.round(j.bytes / 1e6)} MB`).join(' · ') || 'no records'} (records are written by the app's process only: still "running" means iOS has not relaunched it with the result)`);
        }
        await d.sleep(3000);
      }
    });

    await ctx.step('reopened: both files claimed (nothing downloaded again), the update staged', async () => {
      // (the background launch iOS made may have staged it already: the open then APPLIES it at once, and the map is
      // redrawn under the first try to open the log — tried again after the redraw)
      // (it comes back where it was: the Developer card up, as when it was sent Home — `toMap` closes it)
      d.launch();
      await d.sleep(5000);
      try { await freshLog(); } catch { await d.sleep(8000); await freshLog(); }
      await ctx.waitText(/Release, staged/, 10 * 60_000);
      const lines = d.logcatJs().split('\n').filter((l) => l.includes('[download]'));
      fs.writeFileSync(path.join(ctx.dir, 'T912-ios-download-lines.txt'), lines.join('\n') + '\n');
      const claimed = (u) => before.has(u) || lines.some((l) => l.includes(u) && /done \(system download manager\)/.test(l));
      const bad = [
        ...(claimed(urls.data) ? [] : [`${urls.data} not claimed from its background transfer`]),
        ...(claimed(urls.book) ? [] : [`${urls.book} not claimed from its background transfer`]),
        ...lines.filter((l) => /downloaded in the app instead|queued again from zero/.test(l)),
      ];
      shot('2-staged');
      if (bad.length) throw new Error(bad.join(' ; '));
      return `staged ${target}; ${urls.data}${before.has(urls.data) ? ' (before the close)' : ''} and ${urls.book}${before.has(urls.book) ? ' (before the close)' : ''} claimed from the background session; nothing queued again`;
    });

    await ctx.step('next pause → applied; the new release in use', async () => {
      await ctx.ios.toMap(ctx);
      d.background(); await d.sleep(2000); d.launch();
      await d.sleep(4000);
      await freshLog();
      await ctx.waitText(/Release, applied/, 60_000);
      const head = d.screenText().find((w) => /release r-/.test(w)) ?? '';
      if (!head.includes(target)) throw new Error(`in use: ${head}`);
      return head;
    });

    await ctx.step('reset: back to the live release', async () => {
      updates('reset');
      await freshLog();
      await checkNow(/Release, changed/);
      await ctx.waitText(/Release, staged/, 15 * 60_000);
      await ctx.ios.toMap(ctx);
      d.background(); await d.sleep(2000); d.launch();
      await d.sleep(4000);
      await freshLog();
      await ctx.waitText(/Release, applied/, 60_000);
      return status();
    });
  },
};
