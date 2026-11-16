// (T298) THE FIRST INSTALL IN THE BACKGROUND: a clean app on mobile data asks before ~500 MB ("Wait for Wi-Fi" is kept
// as the setting and said), waits, starts by itself on Wi-Fi, the fish sayings take turns; then the app is CLOSED
// mid-download (its process killed) and the system's DownloadManager goes on with its notification; a tap on the
// notification opens the app, which picks the files up WITHOUT stopping the running ones (T756), verifies them and opens
// the map; More → Settings shows the saved choice. Clears the app's data (like `install`): run it on its own or before the others.
import fs from 'node:fs';
import path from 'node:path';
import { appUp } from './common.mjs';
import { stallWatch, thrownAway } from '../lib/download-watch.mjs';
import { lineProbe } from '../lib/line-probe.mjs';

const SAYING = /…$|… of data$/;

export const bgInstall = {
  id: 'bg-install',
  title: 'First install: asks on mobile data, waits for Wi-Fi, keeps downloading with the app closed',
  // the link is slowed for part of the run (see "Wi-Fi back"): whatever happens, the next scenario gets a full one
  async run(ctx) {
    try { await bgInstall.body(ctx); } finally { try { ctx.device.link('full'); } catch { /* the emulator is gone */ } }
  },
  async body(ctx) {
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
    await ctx.step('Wi-Fi back: the download starts by itself', async () => {
      // (2026-11-16) THE LINK IS SLOWED FROM HERE UNTIL THE NOTIFICATION HAS BEEN TAPPED (about 1.8 MB/s, `hsdpa`). On the
      // Mac's full line the whole 479 MB landed in about a minute: the system's downloads finished 4 to 6 s after the
      // app was closed, so three runs in a row had no notification left to tap, and the step failed on the bench's
      // timing, not on the app. Slowed, the download outlasts the close and the tap; it goes back to full for the rest.
      d.link('hsdpa');
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
        if (got > 2) {
          shot('3-downloading'); atKill = line;
          return failedAt ? `${line} (the app had shown the failure screen ${Math.round((failedAt - t0) / 1000)} s after Wi-Fi came back, and recovered by itself ${Math.round((Date.now() - failedAt) / 1000)} s later, no tap)` : line;
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
    let notes = [];
    await ctx.step('app CLOSED mid-download: the system download goes on, in the notification bar', async () => {
      d.background();
      // the process killed as the system kills a closed app (never force-stop: that also stops its receivers)
      await d.sleep(2000);
      d.sh(`am kill ${d.APP_ID}`);
      await d.sleep(1000);
      // the system may keep it a moment (a visible process is not killed): then stopped outright
      let how = 'killed';
      if (alive()) { d.stop(); how = 'force-stopped (am kill left it)'; await d.sleep(1000); }
      killedAt = d.sh('date +%m-%d\\ %H:%M:%S').trim();
      if (alive()) throw new Error('the app is still running after the kill');
      d.sh('cmd statusbar expand-notifications');
      await d.sleep(1500);
      notes = words().filter((x) => /Downloading BC fishing rules|files? downloading|·|\d+ ?%/i.test(x));
      shot('4-notification');
      // ours, or the system's group of them ("2 files downloading")
      if (!notes.some((x) => /Downloading BC fishing rules|files? downloading/i.test(x))) throw new Error(`no download notification with the app closed (saw ${notes.join(' | ')})`);
      return `app ${how} at "${atKill}" (${killedAt}) · notification: ${notes.join(' | ')}`;
    });
    // (T756) the tap is its own step: before, a tap that opened nothing fell back to a launch and the step still passed
    await ctx.step('a tap on the download notification opens the app', async () => {
      // closed first: `expand-notifications` on a shade that is already open turns it into the quick settings (seen
      // 2026-11-15: "Internet | Bluetooth | Flashlight …" and no notification to tap)
      d.sh('cmd statusbar collapse');
      await d.sleep(800);
      d.sh('cmd statusbar expand-notifications');
      await d.sleep(1500);
      let w = words();
      const OURS = /^Downloading BC fishing rules/;
      let how = 'the notification';
      // several downloads are folded into the system's group ("2 files downloading"): a tap on the group only unfolds it
      const group = w.find((x) => /files? downloading/i.test(x));
      if (!w.some((x) => OURS.test(x)) && group) {
        ctx.flow('tap-text', { TEXT: group });
        await d.sleep(1500);
        w = words();
        how = `the group "${group}" unfolded, then its first notification`;
      }
      if (!w.some((x) => OURS.test(x))) {
        d.sh('cmd statusbar collapse');
        throw new Error(`no "Downloading BC fishing rules" notification to tap (the shade shows: ${w.slice(0, 12).join(' | ').slice(0, 300)})`);
      }
      ctx.flow('tap-text', { TEXT: 'Downloading BC fishing rules.*' });
      const end = Date.now() + 10_000;
      while (Date.now() < end && !front()) await d.sleep(500);   // poll interval
      if (!front()) {
        d.sh('cmd statusbar collapse');
        throw new Error(`a tap on ${how} did not bring the app to the front in 10 s (its process is ${alive() ? 'running' : 'not running'})`);
      }
      return `opened by a tap on ${how}`;
    }).finally(() => d.link('full'));   // tapped or not: the rest of the download runs on the full line
    await ctx.step('reopened mid-download: no running download is thrown away; setup finishes and the map opens', async () => {
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
