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
    const jobsFile = () => path.join(d.dataDir(), 'Library', 'Application Support', 'bg-download', 'jobs.json');
    const jobs = () => { try { return JSON.parse(fs.readFileSync(jobsFile(), 'utf8')).jobs ?? []; } catch { return []; } };
    const checkNow = async (re, ms = 60_000) => { ctx.flow('updates-log-action', { ACTION: 'check' }); return ctx.waitText(re, ms); };
    let target = null, urls = null, ours = [];

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
      await checkNow(/Release, changed/);
      await ctx.until(() => /Release: [^\n]*downloading/.test(status()), { ms: 90_000, what: 'the release phase "downloading"' });
      shot('1-downloading');
      d.background();
      await d.sleep(500);
      d.stop();
      killedAt = Date.now();
      await d.sleep(1000);
      if (d.pidOf()) throw new Error('the app is still running after simctl terminate');
      atKill = jobs();
      ours = atKill.filter((j) => j.url.endsWith(urls.data) || j.url.endsWith(urls.book));
      if (ours.length < 2) throw new Error(`the app had queued ${ours.length} of the update's 2 files with the background session before it was ended (records: ${atKill.map((j) => `${j.url.split('/').pop()} ${j.status}`).join(', ') || 'none'})`);
      return `ended; the background session held: ${ours.map((j) => `${j.url.split('/').pop()} ${j.status} ${Math.round(j.bytes / 1e6)} of ${Math.round(j.total / 1e6)} MB`).join(' · ')}`;
    });

    await ctx.step('with the app ended, iOS downloads both files and hands them to the app in the background', async () => {
      const t0 = Date.now(), CAP = 12 * 60_000;
      let relaunched = false;
      for (;;) {
        if (d.pidOf()) relaunched = true;
        const js = jobs().filter((j) => j.url.endsWith(urls.data) || j.url.endsWith(urls.book));
        const failed = js.filter((j) => j.status === 'failed');
        if (failed.length) throw new Error(`a background transfer failed with the app ended: ${failed.map((j) => `${j.url.split('/').pop()} reason ${j.reason}`).join(', ')}`);
        if (js.length >= 2 && js.every((j) => j.status === 'done')) {
          const files = js.map((j) => { try { return fs.statSync(path.join(path.dirname(jobsFile()), j.name)).size; } catch { return 0; } });
          if (files.some((n) => n === 0)) throw new Error(`records say done but a file is missing (${files.join(', ')} bytes)`);
          return `both done ${Math.round((Date.now() - killedAt) / 1000)} s after the process was ended, never opened by the bench (${files.map((n) => `${(n / 1e6).toFixed(1)} MB`).join(', ')}); iOS ${relaunched ? 'relaunched the app in the background to hand them over' : 'did not show the app running while we polled'}`;
        }
        if (Date.now() - t0 > CAP) {
          throw new Error(`not done ${CAP / 60_000} min after the process was ended: ${js.map((j) => `${j.url.split('/').pop()} ${j.status} ${Math.round(j.bytes / 1e6)} MB`).join(' · ') || 'no records'} (records are written by the app's process only: still "running" means iOS has not relaunched it with the result)`);
        }
        await d.sleep(3000);
      }
    });

    await ctx.step('reopened: both files claimed (nothing downloaded again), the update staged', async () => {
      d.logcatClear();
      await ctx.ios.appUp(ctx);
      await ctx.ios.openLog(ctx);
      await ctx.waitText(/Release, staged/, 10 * 60_000);
      const lines = d.logcatJs().split('\n').filter((l) => l.includes('[download]'));
      fs.writeFileSync(path.join(ctx.dir, 'T912-ios-download-lines.txt'), lines.join('\n') + '\n');
      const claimed = (u) => lines.some((l) => l.includes(u) && /done \(system download manager\)/.test(l));
      const bad = [
        ...(claimed(urls.data) ? [] : [`${urls.data} not claimed from its background transfer`]),
        ...(claimed(urls.book) ? [] : [`${urls.book} not claimed from its background transfer`]),
        ...lines.filter((l) => /downloaded in the app instead|queued again from zero/.test(l)),
      ];
      shot('2-staged');
      if (bad.length) throw new Error(bad.join(' ; '));
      return `staged ${target}; ${urls.data} and ${urls.book} claimed from the background session; nothing queued again`;
    });

    await ctx.step('next pause → applied; the new release in use', async () => {
      await ctx.ios.toMap(ctx);
      d.background(); await d.sleep(2000); d.launch();
      await ctx.ios.openLog(ctx);
      await ctx.waitText(/Release, applied/, 60_000);
      const head = d.screenText().find((w) => /release r-/.test(w)) ?? '';
      if (!head.includes(target)) throw new Error(`in use: ${head}`);
      return head;
    });

    await ctx.step('reset: back to the live release', async () => {
      updates('reset');
      await ctx.ios.openLog(ctx);
      await checkNow(/Release, changed/);
      await ctx.waitText(/Release, staged/, 15 * 60_000);
      await ctx.ios.toMap(ctx);
      d.background(); await d.sleep(2000); d.launch();
      await ctx.ios.openLog(ctx);
      await ctx.waitText(/Release, applied/, 60_000);
      return status();
    });
  },
};
