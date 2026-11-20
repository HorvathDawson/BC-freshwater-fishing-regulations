// (T298) THE FIRST INSTALL IN THE BACKGROUND: a clean app on mobile data asks before ~500 MB ("Wait for Wi-Fi" is kept
// as the setting and said), waits, starts by itself on Wi-Fi, the fish sayings take turns; then the app is CLOSED
// mid-download (its process killed) and the system's DownloadManager goes on with its notification; a tap on the
// notification opens the app, which picks the files up WITHOUT stopping the running ones (T756), verifies them and opens
// the map; More → Settings shows the saved choice. Clears the app's data (like `install`): run it on its own or before the others.
import fs from 'node:fs';
import path from 'node:path';
import { appUp } from './common.mjs';
import { readPng } from '../lib/png.mjs';
import { stallWatch, thrownAway } from '../lib/download-watch.mjs';
import { lineProbe } from '../lib/line-probe.mjs';
import { downloadJobs, findNode } from '../lib/ui-nodes.mjs';
import { startThrottleProxy } from '../lib/throttle-proxy.mjs';

const SAYING = /…$|… of data$/;
// (2026-11-16) the line the bench holds the phone to until the notification is tapped: 479 MB then take about 160 s, so
// the system's download is still running minutes after the app is closed (on the full line it was over in 35 to 60 s)
const SLOW_BPS = 3_000_000;

export const bgInstall = {
  id: 'bg-install',
  title: 'First install: asks on mobile data, waits for Wi-Fi, keeps downloading with the app closed',
  // (2026-11-16) TRIED AND DROPPED: slowing the link with the emulator's own shaping (`d.link('hsdpa')`, `adb emu network
  // speed`). It does not reach its Wi-Fi: six runs showed no change (479 MB in 35 to 60 s). Do not try it again. What
  // works: the phone's global HTTP proxy pointed at a proxy on this Mac that holds the bytes to SLOW_BPS
  // (`lib/throttle-proxy.mjs`), from the clean install until the notification is tapped; then lifted and unset. The
  // proxy is always unset when the scenario ends, however it ends.
  async run(ctx) {
    const d = ctx.device;
    const proxy = await startThrottleProxy({ rate: SLOW_BPS });
    const unset = () => { try { d.sh('settings put global http_proxy :0'); } catch { /* the device is gone */ } };
    try { await bgInstall.steps(ctx, proxy, unset); } finally { unset(); await proxy.close(); }
  },
  async steps(ctx, proxy, unset) {
    const d = ctx.device;
    const shot = (name) => { try { d.screenshot(path.join(ctx.dir, `T298-${name}.png`)); } catch { /* no shot */ } };
    const words = () => d.screenText();
    // `pidof` exits 1 when the app is not running: that is a no, not a failure
    const alive = () => { try { return !!d.pidOf(); } catch { return false; } };
    // the app is what the reader sees (its activity resumed), not merely a live process: a notification's broadcast
    // starts the process without showing anything
    const front = () => { try { return d.sh("dumpsys activity activities | grep -E 'ResumedActivity' || true").includes(d.APP_ID); } catch { return false; } };
    await ctx.step('test channel reset to the live release', async () => `test/ = ${(await ctx.resetChannel()).release.data_release}`,
                   { shot: false, stop: true });   // (T442) a stale test/ only 404s
    await ctx.step('clean app on MOBILE DATA: asked before the download', async () => {
      d.stop(); d.cleanApp();   // (T760) not `pm clear`: the system downloader's rows go too
      d.sh(`settings put global http_proxy 10.0.2.2:${proxy.port}`);   // the slow line (SLOW_BPS), see the top
      try { d.adb(['logcat', '-G', '16M']); d.logcatClear(); } catch { /* none yet */ }
      await d.net('cellular'); d.link('full', 'none');
      d.launch();
      // (Android 13+) first the system asks to allow notifications (the download's progress after the app is closed)
      let notify = 'not asked';
      const end = Date.now() + 90_000;
      while (Date.now() < end) {
        const w = words();
        if (w.some((x) => /send you notifications/i.test(x))) {
          shot('0-notify-ask');
          notify = w.find((x) => /^Allow notifications/.test(x)) ? 'asked, with the reason on screen' : 'asked';
          ctx.flow('tap-text', { TEXT: 'Allow' });
          continue;
        }
        if (w.some((x) => /using mobile data\?/.test(x))) break;
        await d.sleep(1000);
      }
      const w = await ctx.waitText(/using mobile data\?/, 10_000);
      shot('1-mobile-prompt');
      return `${w} · notifications ${notify}`;
    });
    await ctx.step('"Wait for Wi-Fi": waiting, the choice saved and said, the sayings take turns', async () => {
      ctx.flow('tap-text', { TEXT: 'Wait for Wi-Fi' });
      await ctx.waitText(/^Waiting for Wi-Fi —/, 20_000);
      await ctx.waitText(/^Saved\. Change this any time in More → Settings\.$/, 5_000);
      const a = words().find((x) => SAYING.test(x));
      shot('2-waiting-wifi');
      await d.sleep(4500);
      const b = words().find((x) => SAYING.test(x));
      if (!a || !b || a === b) throw new Error(`the sayings did not take turns (${a} → ${b})`);
      const before = words().find((x) => /MB downloaded/.test(x));
      if (before) throw new Error(`downloading on mobile data while told to wait: ${before}`);
      return `${a} → ${b}`;
    });
    let atKill = '', killedAt = '';
    // what a failed step leaves for whoever reads it: the app's whole log since the scenario began (without the test
    // bed's heap and stall ticks) and the system downloader's, beside the screenshots
    const keepLogs = (name) => { try {
      const all = d.logcatTag('ReactNativeJS').join('\n').split('\n').filter((l) => l.trim() && !/\[heap\]|\[stall\]/.test(l));
      const dmAll = d.logcatTag('DownloadManager');
      fs.writeFileSync(path.join(ctx.dir, name), [`app killed ${killedAt || '(not yet)'}`, '=== the app (ReactNativeJS)', ...all, '=== the system downloader', ...dmAll].join('\n') + '\n');
    } catch { /* no log */ } };
    // (T760) a line probe taken at the moment of a stalled or unfinished step: Mac and emulator, raw text beside the logs,
    // and its one-line verdict in the failure text
    const probeLine = (name) => { try { const p = lineProbe(d); fs.writeFileSync(path.join(ctx.dir, name), p.raw + '\n'); return ` Line probe: ${p.verdict}.`; } catch (e) { return ` Line probe failed: ${e.message}.`; } };
    let dlSince = '';   // the device's clock when Wi-Fi came back
    await ctx.step('Wi-Fi back: the download starts by itself', async () => {
      // (2026-11-16) On the Mac's full line the whole 479 MB lands in 35 to 60 s, and the app's own byte line lags far
      // behind the system downloader, so waiting for ">2 MB" there closed the app with the downloads nearly done. The
      // step returns at the FIRST sign that the download is under way: the system downloader's "Starting" (its log)
      // or the app's byte line, whichever comes first.
      dlSince = d.deviceStamp();
      await d.net('wifi');
      // (T760, owner 2026-11-15 "keep trying by itself") a line dead for a while can end on the failure screen ("The map
      // data could not be loaded … Try again"); the app must then try again BY ITSELF (once a minute) and the download
      // start without a tap: RECOVER ms after the failure was first seen (never tapped here), or FIRST ms after Wi-Fi,
      // whichever is later
      const FIRST = 120_000, RECOVER = 120_000;
      const t0 = Date.now();
      let failedAt = 0;
      for (;;) {
        const w = words();
        const line = w.find((x) => /MB downloaded/.test(x)) ?? '';
        const got = Number((/^([\d,.]+) of/.exec(line)?.[1] ?? '0').replace(/,/g, ''));
        // early, so the app is closed with most of the download still to come
        const jobs = downloadJobs(d.logcatTag('DownloadManager'), dlSince);
        const evidence = jobs.started.length ? `the system downloader started ${jobs.started.length} job(s) at ${jobs.startedAt}` : got > 2 ? line : '';
        if (evidence) {
          shot('3-downloading'); atKill = line || evidence;
          const said = `${evidence}${jobs.started.length && line ? ` · app's line: ${line}` : ''} · line held to ${SLOW_BPS / 1e6} MB/s by the bench's proxy`;
          return failedAt ? `${said} (the app had shown the failure screen ${Math.round((failedAt - t0) / 1000)} s after Wi-Fi came back, and recovered by itself ${Math.round((Date.now() - failedAt) / 1000)} s later, no tap)` : said;
        }
        if (!failedAt && w.some((x) => /^The map data could not be loaded/.test(x))) { failedAt = Date.now(); shot('3-failed-screen'); }
        if (Date.now() > Math.max(t0 + FIRST, failedAt ? failedAt + RECOVER : 0)) {
          shot('3-no-progress'); keepLogs('T760-wifi-back-logs.txt'); const probed = probeLine('T760-wifi-back-line-probe.txt');
          const seen = line || w.slice(0, 6).join(' | ');
          throw new Error(failedAt
            ? `the app showed the failure screen ${Math.round((failedAt - t0) / 1000)} s after Wi-Fi came back and did not recover by itself in ${RECOVER / 1000} s (${seen}).${probed}`
            : `no progress on Wi-Fi in ${FIRST / 1000} s (${seen}).${probed}`);
        }
        await d.sleep(500);
      }
    });
    // (T738) THE BAR AGAINST THE BYTES: the bar's own line ("12.5 of 1,461.2 MB downloaded") read beside the bytes the system
    // downloader holds (its provider: `current_bytes` of every job), a shot each time, three moments of the line held to
    // SLOW_BPS. Pass line (the ticket): the bar is within BAR_SLACK_MB of the bytes held and never goes down between shots.
    // A provider that cannot be read from the shell is said, and the bar is then judged on never going down alone.
    // The bytes held: the Mac-side throttle proxy's count of what has gone through it to the phone (the shell cannot read the
    // system downloader's provider: ACCESS_ALL_DOWNLOADS). The bar's figure: its words cannot be read while the fish turns (the
    // accessibility tree never goes idle), so it is read off the screenshot: the violet fill's share of the grey track times
    // the release's total (BAR_TOTAL_MB, 480.8 on the live release; the bar's own line says "N of 480.8 MB downloaded").
    const held = () => proxy.bytes;
    const barMb = () => {
      const f = path.join(ctx.dir, 'T738-bar-probe.png');
      d.screenshot(f);
      const img = readPng(f), TOTAL = Number(process.env.BAR_TOTAL_MB ?? 480.8);
      for (let y = Math.round(img.h * 0.5); y < Math.round(img.h * 0.8); y++) {
        let x0 = -1, x1 = -1, track = -1;
        for (let x = 0; x < img.w; x++) {
          const i = (y * img.w + x) * img.ch, r = img.px[i], g = img.px[i + 1], b = img.px[i + 2];
          if (r < 130 && g < 80 && b > 170) { if (x0 < 0) x0 = x; x1 = x; }
          else if (x0 >= 0 && Math.abs(r - g) < 8 && Math.abs(g - b) < 10 && r > 200 && r < 245) track = x;
        }
        if (x0 >= 0 && track - x0 > 300 && x1 - x0 > 1) {
          const frac = (x1 - x0 + 1) / (track - x0 + 1);
          return { got: Math.round(frac * TOTAL * 10) / 10, of: TOTAL, frac };
        }
      }
      return null;
    };
    const BAR_SLACK_MB = Number(process.env.BAR_SLACK_MB ?? 10);
    const samples = [];
    await ctx.step('T738: the bar against the bytes held, at three moments of the download', async () => {
      const rows = [];
      for (let i = 0; i < 3; i++) {
        if (i) await d.sleep(i === 1 ? 25_000 : 40_000);   // poll interval between the three moments
        shot(`T738-bar-${i + 1}`);
        const bar = barMb(), heldB = held();
        const row = { i: i + 1, bar, heldMb: heldB == null ? null : heldB / 1e6 };
        rows.push(row); samples.push(row);
      }
      const text = rows.map((r) => `#${r.i} bar ${r.bar ? `${r.bar.got} of ${r.bar.of} MB` : 'none shown'} · held ${r.heldMb == null ? 'unreadable' : `${r.heldMb.toFixed(1)} MB`}`).join(' | ');
      fs.writeFileSync(path.join(ctx.dir, 'T738-bar-vs-bytes.txt'), `${text}\n`);
      const bad = [];
      rows.forEach((r, k) => {
        if (!r.bar) { bad.push(`#${r.i}: no bar figure on screen`); return; }
        if (k && rows[k - 1].bar && r.bar.got < rows[k - 1].bar.got) bad.push(`#${r.i}: the bar went down (${rows[k - 1].bar.got} -> ${r.bar.got} MB)`);
        if (r.heldMb != null && Math.abs(r.bar.got - r.heldMb) > BAR_SLACK_MB) bad.push(`#${r.i}: bar ${r.bar.got} MB vs held ${r.heldMb.toFixed(1)} MB (more than ${BAR_SLACK_MB} MB apart)`);
      });
      if (rows.every((r) => r.heldMb == null)) ctx.finding('T738: the bytes held could not be read from the shell (content://downloads/all_downloads); the bar was judged only on never going down');
      if (bad.length) throw new Error(`${bad.join(' ; ')} · ${text}`);
      return text;
    });
    let notes = [];
    await ctx.step('app CLOSED mid-download: the system download goes on, in the notification bar', async () => {
      d.background();
      // the process killed as the system kills a closed app (never force-stop: that also stops its receivers)
      await d.sleep(700);
      d.sh(`am kill ${d.APP_ID}`);
      await d.sleep(700);
      // the system may keep it a moment (a visible process is not killed): then stopped outright
      let how = 'killed';
      if (alive()) { d.stop(); how = 'force-stopped (am kill left it)'; await d.sleep(1000); }
      killedAt = d.deviceStamp();
      if (alive()) throw new Error('the app is still running after the kill');
      if (proxy.bytes < 1e6) throw new Error(`BENCH: the phone did not use the bench's slow proxy (${proxy.bytes} bytes through it), so the download is not held back`);
      d.sh('cmd statusbar expand-notifications');
      await d.sleep(1200);
      notes = words().filter((x) => /Downloading BC fishing rules|files? downloading|·|\d+ ?%/i.test(x));
      shot('4-notification');
      // ours, or the system's group of them ("2 files downloading")
      if (!notes.some((x) => /Downloading BC fishing rules|files? downloading/i.test(x))) throw new Error(`no download notification with the app closed (saw ${notes.join(' | ')})`);
      return `app ${how} at "${atKill}" (${killedAt}) · notification: ${notes.join(' | ')}`;
    });
    // (T756) the tap is its own step: before, a tap that opened nothing fell back to a launch and the step still passed
    await ctx.step('a tap on the download notification opens the app', async () => {
      const OURS = /^Downloading BC fishing rules/, GROUP = /files? downloading/i;
      const jobs = () => downloadJobs(d.logcatTag('DownloadManager'), dlSince);
      // (2026-11-16) a bench race, not an app fault: the system's downloads were all done before this step began
      const j0 = jobs();
      if (j0.started.length && !j0.running.length) {
        d.sh('cmd statusbar collapse');
        throw new Error(`BENCH RACE, not the app: the system's ${j0.started.length} download(s) had all finished (last at ${j0.finishedAt}) before this step began, so no download notification can exist (downloads started ${j0.startedAt}, app closed ${killedAt.slice(6)}).`);
      }
      // the shade is still open from the step before; one dump, no Maestro flow. `expand-notifications` on an open shade
      // turns it into the quick settings (seen 2026-11-15), so only when no notification is found is it closed and opened again
      let nodes = d.screenNodes();
      if (!findNode(nodes, OURS) && !findNode(nodes, GROUP)) {
        d.sh('cmd statusbar collapse'); await d.sleep(500);
        d.sh('cmd statusbar expand-notifications'); await d.sleep(1000);
        nodes = d.screenNodes();
      }
      let how = 'the notification';
      // several downloads are folded into the system's group ("2 files downloading"): a tap on the group only unfolds it
      const group = findNode(nodes, GROUP);
      if (!findNode(nodes, OURS) && group) {
        d.tapAt(group.cx, group.cy);
        await d.sleep(900);
        nodes = d.screenNodes();
        how = `the group "${group.text}" unfolded, then its first notification`;
      }
      const ours = findNode(nodes, OURS);
      if (!ours) {
        d.sh('cmd statusbar collapse');
        const j = jobs();
        const late = j.started.length && !j.running.length ? ` The system's downloads had finished by ${j.finishedAt} (downloads started ${j.startedAt}, app closed ${killedAt.slice(6)}): a bench race.` : '';
        throw new Error(`no "Downloading BC fishing rules" notification to tap (the shade shows: ${nodes.map((n) => n.text).slice(0, 12).join(' | ').slice(0, 300)}).${late}`);
      }
      d.tapAt(ours.cx, ours.cy);
      const end = Date.now() + 10_000;
      while (Date.now() < end && !front()) await d.sleep(500);   // poll interval
      if (!front()) {
        d.sh('cmd statusbar collapse');
        throw new Error(`a tap on ${how} did not bring the app to the front in 10 s (its process is ${alive() ? 'running' : 'not running'})`);
      }
      return `opened by a tap on ${how}`;
    });
    await ctx.step('reopened mid-download: no running download is thrown away; setup finishes and the map opens', async () => {
      proxy.setRate(Infinity); unset();   // the tap is done: the rest at full speed (open tunnels too)
      d.sh('cmd statusbar collapse'); await d.sleep(1500);   // the shade must be shut before the reading
      shot('T738-after-relaunch');   // (T738) the first look after the app came back, the bar beside what was held
      const relaunchBar = barMb(), relaunchHeld = held();
      ctx.finding(`T738 after the relaunch: bar ${relaunchBar ? `${relaunchBar.got} MB` : 'none shown'} · held ${relaunchHeld == null ? 'unreadable' : `${(relaunchHeld / 1e6).toFixed(1)} MB`} (last bar before the kill: ${samples.at(-1)?.bar?.got ?? '?'} MB)`);
      let opened = 'the notification tap';
      if (!front()) { opened = 'a launch (the tap did not open it)'; d.sh('cmd statusbar collapse'); d.launch(); }
      const t0 = Date.now();
      // (T756) the wait follows the bytes, not the clock: as long as the app's own line or the downloader's files move it
      // goes on (a slow line is not a failure); no movement for STALL is, and it is said in the app's words
      const STALL = 4 * 60_000, CAP = 45 * 60_000;
      const watch = stallWatch(STALL);
      const jsLines = new Set();
      // read again and again: on a long run the first lines roll out of the log before the end. The APP's own lines
      // (`[download] …: queued again from zero (was …, N of M bytes)`), never the system downloader's (T760)
      const readJs = () => { try { for (const l of d.logcatTag('ReactNativeJS').join('\n').split('\n')) if (/\[download\]|\[setup\]/.test(l)) jsLines.add(l.trim()); } catch { /* no log this time */ } };
      const lost = () => thrownAway([...jsLines]);
      const saidLost = () => (lost().length ? ` The app also queued ${lost().length} download(s) again from zero: ${lost().map((x) => `${x.url.split('/').pop()} (${x.was}, ${Math.round(x.bytes / 1e6)} of ${Math.round(x.total / 1e6)} MB: ${x.why})`).join(' ; ')}` : '');
      let setupShot = false, n = 0, line = '';
      for (;;) {
        const w = words();
        if (w.includes('More') || w.includes('MORE') || w.some((x) => /^(Before you fish|I understand|Skip)$/.test(x))) break;
        const dl = w.find((x) => /MB downloaded/.test(x));
        line = dl ?? w.find((x) => /^Setting up|^Waiting for Wi-Fi|^Downloading the map/.test(x)) ?? line;
        if (!setupShot && w.some((x) => /^Setting up|MB downloaded/.test(x))) { shot('5-reopened'); setupShot = true; }
        if (n++ % 10 === 0) readJs();
        // the measure is the APP's byte line. The downloader's files are no measure: it reserves each at its full size
        // from the first second (`ls -l` and `du` both read 424 MB twenty seconds in). A line that reads full, or the
        // "Setting up" stage, is the app checking and unpacking (CAP is its limit); otherwise a line that does not
        // change, or no byte line at all, for STALL is a download that is not moving
        const full = (() => { const m = /^([\d,.]+) of ([\d,.]+) MB/.exec(dl ?? ''); return !!m && Number(m[1].replace(/,/g, '')) >= Number(m[2].replace(/,/g, '')); })();
        const moving = !full && !w.some((x) => /^Setting up/.test(x));
        const still = watch.see(moving ? (dl ?? 'no byte line') : `setup ${Date.now()}`);
        if (moving && watch.stalled()) {
          readJs(); shot('5-stalled'); keepLogs('T756-stalled-logs.txt'); const probed = probeLine('T760-line-probe.txt');
          const last = [...jsLines].filter((l) => /\[download\]/.test(l)).slice(-2).map((l) => l.replace(/^.*ReactNativeJS: /, '')).join(' ; ');
          throw new Error(`the download stopped moving: for ${Math.round(still / 1000)} s the setup screen has ${dl ? `read "${dl}"` : `shown no byte line at all (it reads: ${w.slice(0, 4).join(' | ')})`} (${Math.round((Date.now() - t0) / 1000)} s after the app was reopened). The app's last download lines: ${last || 'none'}.${saidLost()}${probed}`);
        }
        if (Date.now() - t0 > CAP) {
          readJs(); shot('5-not-finished'); keepLogs('T756-not-finished-logs.txt'); const probed = probeLine('T760-line-probe.txt');
          throw new Error(`setup not finished ${CAP / 60_000} min after the app was reopened, though it kept moving: the setup screen reads "${line}"; the app's log has ${[...jsLines].filter((l) => /\[download\]/.test(l)).length} download lines.${saidLost()}${probed}`);
        }
        if (!alive()) d.launch();
        await d.sleep(2000);   // poll interval
      }
      await appUp(ctx, { launch: false, ms: 60_000 });
      ctx.flow('dismiss-first-run');
      shot('6-map');
      d.link('full', 'none');
      readJs();
      let after = '';
      try {
        // the system's downloader's own lines: which jobs started and finished, and when (against the kill)
        const dm = d.logcatTag('DownloadManager').filter((l) => /Finished|Starting/.test(l));
        fs.writeFileSync(path.join(ctx.dir, 'T298-install-timeline.txt'), [`app killed ${killedAt}`, ...dm, ...jsLines].join('\n') + '\n');
        after = ` · system downloads finished: ${dm.filter((l) => /Finished/.test(l)).map((l) => l.slice(6, 14)).join(', ')} (app killed ${killedAt.slice(6)})`;
      } catch { /* no log */ }
      // (T756) "Wait for Wi-Fi", Wi-Fi back, the app reopened: both choices permit the jobs under way, so no bytes may go
      if (lost().length) throw new Error(`reopening the app threw away bytes already down.${saidLost()}`);
      return `opened by ${opened}; map up ${Math.round((Date.now() - t0) / 1000)} s later; no download queued again from zero${after}`;
    }, { stop: true });
    await ctx.step('More → Settings: "Download over mobile data" shows the saved Wi-Fi only', async () => {
      ctx.flow('tab', { TAB: 'More' });
      ctx.flow('tap-text', { TEXT: 'Settings' });   // (T703) the controls live in the Settings screen
      try { ctx.flow('scroll-to', { TEXT: 'Wi-Fi only' }); } catch { /* already in view */ }
      await ctx.waitText(/^Download over mobile data$/, 10_000);
      shot('7-more-setting');
      return 'shown';
    });
  },
};
