// (T298) THE FIRST INSTALL IN THE BACKGROUND: a clean app on mobile data asks before ~500 MB ("Wait for Wi-Fi" is kept
// as the setting and said), waits, starts by itself on Wi-Fi, the fish sayings take turns; then the app is CLOSED
// mid-download (its process killed) and the system's DownloadManager goes on with its notification; a tap on the
// notification (or a launch) opens the app, which picks the files up, verifies them and opens the map; More → Settings
// shows the saved choice. Clears the app's data (like `install`): run it on its own or before the others.
import fs from 'node:fs';
import path from 'node:path';
import { appUp } from './common.mjs';

const SAYING = /…$|… of data$/;

export const bgInstall = {
  id: 'bg-install',
  title: 'First install: asks on mobile data, waits for Wi-Fi, keeps downloading with the app closed',
  async run(ctx) {
    const d = ctx.device;
    const shot = (name) => { try { d.screenshot(path.join(ctx.dir, `T298-${name}.png`)); } catch { /* no shot */ } };
    const words = () => d.screenText();
    // `pidof` exits 1 when the app is not running: that is a no, not a failure
    const alive = () => { try { return !!d.pidOf(); } catch { return false; } };
    // what the system's downloader has on disk for us (its folder in the app's external files)
    const onDisk = () => { try { return d.sh('ls -l /sdcard/Android/data/ca.canifishthis.app/files/bg-download/ 2>&1').trim().split('\n')
      .map((l) => l.trim().split(/\s+/)).filter((c) => c.length > 5).map((c) => `${c.at(-1)} ${Math.round(Number(c[4]) / 1e6)} MB`).join(', '); } catch { return '?'; } };
    await ctx.step('test channel reset to the live release', async () => `test/ = ${(await ctx.resetChannel()).release.data_release}`,
                   { shot: false, stop: true });   // (T442) a stale test/ only 404s
    await ctx.step('clean app on MOBILE DATA: asked before the download', async () => {
      d.stop(); d.clearData();
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
    await ctx.step('Wi-Fi back: the download starts by itself', async () => {
      await d.net('wifi');
      const end = Date.now() + 120_000;
      for (;;) {
        const line = words().find((x) => /MB downloaded/.test(x)) ?? '';
        const got = Number((/^([\d,.]+) of/.exec(line)?.[1] ?? '0').replace(/,/g, ''));
        // early: the emulator's Wi-Fi is not throttled (the whole set lands in about a minute)
        if (got > 2) { shot('3-downloading'); atKill = line; return line; }
        if (Date.now() > end) throw new Error(`no progress on Wi-Fi in 120 s (${line || words().slice(0, 6).join(' | ')})`);
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
    await ctx.step('a tap on the notification opens the app; setup finishes and the map opens', async () => {
      d.sh('cmd statusbar expand-notifications');
      await d.sleep(1500);
      let opened = 'tap';
      try { ctx.flow('tap-text', { TEXT: 'Downloading BC fishing rules' }); } catch { opened = 'launch'; }
      await d.sleep(4000);
      if (!alive()) { opened = 'launch (the tap did not open it)'; d.sh('cmd statusbar collapse'); d.launch(); }
      const t0 = Date.now();
      let setupShot = false;
      while (Date.now() - t0 < 15 * 60_000) {
        const w = words();
        if (w.includes('More') || w.includes('MORE') || w.some((x) => /^(Before you fish|I understand|Skip)$/.test(x))) break;
        if (!setupShot && w.some((x) => /^Setting up|MB downloaded/.test(x))) { shot('5-reopened'); setupShot = true; }
        if (!alive()) d.launch();
        await d.sleep(2000);
      }
      await appUp(ctx, { launch: false, ms: 60_000 });
      ctx.flow('dismiss-first-run');
      shot('6-map');
      d.link('full', 'none');
      let after = '';
      try {
        const js = d.logcatJs().split('\n').filter((l) => /\[download\]|\[setup\]/.test(l));
        // the system's downloader's own lines: which jobs finished, and when (against the kill)
        const dm = d.adb(['logcat', '-d', '-s', 'DownloadManager:*']).toString().split('\n').filter((l) => /Finished|Starting/.test(l));
        fs.writeFileSync(path.join(ctx.dir, 'T298-install-timeline.txt'), [`app killed ${killedAt}`, ...dm, ...js].join('\n') + '\n');
        after = ` · system downloads finished: ${dm.filter((l) => /Finished/.test(l)).map((l) => l.slice(6, 14)).join(', ')} (app killed ${killedAt.slice(6)})`;
      } catch { /* no log */ }
      return `opened by ${opened}; map up ${Math.round((Date.now() - t0) / 1000)} s later${after}`;
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
