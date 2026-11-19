// (T912) AN UPDATE GOES ON WITH THE APP CLOSED (owner 2026-11-19, Play 0.8.2: "if i close app it doesnt continue to
// download the regulation data updates"). The app holds the live release; the test channel names one that changes a data
// file AND the basemap (`test-updates.mjs data-big`, ~270 MB). Check now starts the update; the moment it is downloading
// the app is closed (Home, its process killed, as a swipe away does). With the app dead the system's DownloadManager must
// download every file of it (its jobs start and finish); the next open must claim them — nothing downloaded again, the
// data file from its system job too — stage the update, and apply it at the next pause. Before T912 an update fetched
// its data files in the app first and queued each big file only when the install reached it, so a close in that window
// stopped the whole update and the next open started it again.
// Leaves the app on the live release (the last step resets the channel and applies it: a second basemap download, on the
// full line).
import fs from 'node:fs';
import path from 'node:path';
import { appUp, checkNow, coldStart, logStatus, openLog, toBackground, toMap } from './common.mjs';
import { downloadJobs } from '../lib/ui-nodes.mjs';
import { startThrottleProxy } from '../lib/throttle-proxy.mjs';

// the line the phone is held to while the app is closed (as bg-install): 270 MB then take ~90 s, so the system's download
// is still running well after the kill
const SLOW_BPS = 3_000_000;

export const bgUpdate = {
  id: 'bg-update',
  title: 'An update keeps downloading with the app closed; the next open claims it and applies it',
  async run(ctx) {
    const d = ctx.device;
    const proxy = await startThrottleProxy({ rate: SLOW_BPS });
    const unset = () => { try { d.sh('settings put global http_proxy :0'); } catch { /* the device is gone */ } };
    try { await bgUpdate.steps(ctx, proxy, unset); } finally { unset(); await proxy.close(); }
  },
  async steps(ctx, proxy, unset) {
    const d = ctx.device;
    const shot = (name) => { try { d.screenshot(path.join(ctx.dir, `T912-${name}.png`)); } catch { /* no shot */ } };
    const alive = () => { try { return !!d.pidOf(); } catch { return false; } };
    const jsLines = () => d.logcatTag('ReactNativeJS').join('\n').split('\n').filter((l) => /\[download\]/.test(l))
      .map((l) => l.replace(/^.*ReactNativeJS: /, '').trim());
    const keepLogs = (name, killedAt) => { try {
      fs.writeFileSync(path.join(ctx.dir, name), [`app killed ${killedAt || '(not yet)'}`, '=== the app ([download])', ...jsLines(),
        '=== the system downloader', ...d.logcatTag('DownloadManager')].join('\n') + '\n');
    } catch { /* no log */ } };
    let target = null, urls = null, since = '', killedAt = '';

    await ctx.step('test channel reset; app up on Wi-Fi, holding the live release', async () => {
      const live = (await ctx.resetChannel()).release.data_release;
      await d.net('wifi'); d.link('full', 'none');
      await coldStart(ctx);
      openLog(ctx);
      await checkNow(ctx, /Release, (unchanged|changed)/);
      const st = logStatus(ctx);
      if (/downloading|ready|waiting/.test(st)) throw new Error(`the app does not hold the live release ${live} (${st}): run install first`);
      return `live ${live} · ${st.split('\n').find((l) => /Release/.test(l)) ?? st}`;
    }, { stop: true });

    await ctx.step('an update that changes a data file and the basemap (data-big); the line held to 3 MB/s', async () => {
      const r = ctx.updates('data-big');
      target = r.data_release;
      urls = { data: Object.values(r.data)[0].split('/').pop(), basemap: r.basemap };
      try { d.adb(['logcat', '-G', '16M']); } catch { /* kept as is */ }
      d.logcatClear();
      d.sh(`settings put global http_proxy 10.0.2.2:${proxy.port}`);
      return `${target} · data file ${urls.data} · basemap ${r.basemap}`;
    }, { stop: true });

    await ctx.step('Check now: the update starts downloading, and the app is CLOSED at once (Home, process killed)', async () => {
      since = d.deviceStamp();
      openLog(ctx);
      ctx.flow('updates-log-action', { ACTION: 'check' });
      await ctx.until(() => /Release: [^\n]*downloading/.test(logStatus(ctx)), { ms: 90_000, what: 'the release phase "downloading"' });
      const atClose = downloadJobs(d.logcatTag('DownloadManager'), since);
      shot('1-downloading');
      d.background();
      await d.sleep(300);
      d.sh(`am kill ${d.APP_ID}`);   // as the system kills a closed app (never force-stop first: it also stops receivers)
      await d.sleep(700);
      let how = 'killed';
      if (alive()) { d.stop(); how = 'force-stopped (am kill left it)'; await d.sleep(1000); }
      if (alive()) throw new Error('the app is still running after the kill');
      killedAt = d.deviceStamp();
      return `app ${how} at ${killedAt} · the system downloader had started ${atClose.started.length} job(s) by then`;
    }, { stop: true });

    await ctx.step('with the app closed, the system downloads the whole update (its jobs start and finish)', async () => {
      const t0 = Date.now(), FIRST = 60_000, CAP = 10 * 60_000;
      let noted = false;
      for (;;) {
        const j = downloadJobs(d.logcatTag('DownloadManager'), since);
        if (!noted && j.running.length) {
          d.sh('cmd statusbar expand-notifications'); await d.sleep(1200);
          shot('2-notification-closed'); d.sh('cmd statusbar collapse'); noted = true;
        }
        // the data file and the basemap: two jobs, both finished, none still running
        if (j.started.length >= 2 && !j.running.length) {
          if (proxy.bytes < 1e6) throw new Error(`BENCH: the phone did not use the bench's slow proxy (${proxy.bytes} bytes through it)`);
          return `${j.started.length} system download(s) started (first ${j.startedAt}) and finished (last ${j.finishedAt}) with the app closed at ${killedAt.slice(6)}${alive() ? ' (the app process was started meanwhile, by the system)' : ', the app not running'}`;
        }
        if (Date.now() - t0 > FIRST && !j.started.length) {
          keepLogs('T912-nothing-downloads.txt', killedAt);
          throw new Error(`nothing downloads with the app closed: no system download started in ${FIRST / 1000} s after the kill (the app had queued none of the update's files before it was closed)`);
        }
        if (Date.now() - t0 > CAP) {
          keepLogs('T912-not-finished.txt', killedAt);
          throw new Error(`the system's downloads did not finish in ${CAP / 60_000} min with the app closed (started ${j.started.length}, running ${j.running.length})`);
        }
        await d.sleep(2000);
      }
    });

    await ctx.step('reopened: the finished files are claimed (nothing downloaded again, the data file too), the update staged', async () => {
      proxy.setRate(Infinity); unset();
      const before = downloadJobs(d.logcatTag('DownloadManager'), since).started;
      d.launch();
      await appUp(ctx, { launch: false, ms: 90_000 });
      openLog(ctx);
      await ctx.waitText(/Release, staged/, 10 * 60_000);
      const lines = jsLines();
      keepLogs('T912-timeline.txt', killedAt);
      const again = downloadJobs(d.logcatTag('DownloadManager'), since).started.filter((id) => !before.includes(id));
      const claimed = (u) => lines.some((l) => l.includes(u) && /done \(system download manager\)/.test(l));
      const bad = [
        ...(claimed(urls.data) ? [] : [`the data file ${urls.data} was not claimed from its system download`]),
        ...(lines.some((l) => /basemap/.test(l) && /done \(system download manager\)/.test(l)) ? [] : ['the basemap was not claimed from its system download']),
        ...(again.length ? [`${again.length} system download(s) started again after the reopen`] : []),
        ...lines.filter((l) => /downloaded in the app instead|queued again from zero/.test(l)),
      ];
      shot('3-staged');
      if (bad.length) throw new Error(`the next open did not take what the closed app's downloads brought: ${bad.join(' ; ')}`);
      return `staged ${target}; claimed from the system downloader: ${urls.data} and the basemap; no download started again`;
    }, { stop: true });

    await ctx.step('next pause (background → foreground) → applied; the new release in use', async () => {
      await toMap(ctx);
      await toBackground(ctx);
      d.launch();
      const toast = await ctx.waitText(/^Updated to |^Data updated$/, 20_000).catch(() => null);
      openLog(ctx);
      await ctx.waitText(/Release, applied/, 60_000);
      const head = d.screenText().find((w) => /release r-/.test(w)) ?? '';
      if (!head.includes(target)) throw new Error(`in use: ${head}`);
      shot('4-applied');
      return `${toast ? `toast "${toast}"` : 'no toast seen'} · ${head}`;
    });

    await ctx.step('reset: back to the live release (its basemap id and data file differ again: downloaded on the full line)', async () => {
      ctx.updates('reset');
      openLog(ctx);
      await checkNow(ctx, /Release, changed/);
      await ctx.waitText(/Release, staged/, 15 * 60_000);
      await toMap(ctx);
      await toBackground(ctx); d.launch();
      openLog(ctx);
      await ctx.waitText(/Release, applied/, 60_000);
      return logStatus(ctx).replace(/\n/g, ' · ');
    });
  },
};
