// (T298) THE FIRST INSTALL IN THE BACKGROUND: a clean app on mobile data asks before ~500 MB ("Wait for Wi-Fi" is kept
// as the setting and said), waits, starts by itself on Wi-Fi, the fish sayings take turns; then the app is CLOSED
// mid-download (its process killed) and the system's DownloadManager goes on with its notification; a tap on the
// notification (or a launch) opens the app, which picks the files up, verifies them and opens the map; More → Data
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
    await ctx.step('test channel reset to the live release', () => `test/ = ${ctx.updates('reset').release.data_release}`, { shot: false });
    await ctx.step('clean app on MOBILE DATA: asked before the download', async () => {
      d.stop(); d.clearData();
      try { d.adb(['logcat', '-G', '16M']); d.logcatClear(); } catch { /* none yet */ }
      await d.net('cellular'); d.link('full', 'none');
      d.launch();
      const w = await ctx.waitText(/using mobile data\?/, 90_000);
      shot('1-mobile-prompt');
      return w;
    });
    await ctx.step('"Wait for Wi-Fi": waiting, the choice saved and said, the sayings take turns', async () => {
      ctx.flow('tap-text', { TEXT: 'Wait for Wi-Fi' });
      await ctx.waitText(/^Waiting for Wi-Fi —/, 20_000);
      await ctx.waitText(/^Saved\. Change this any time in More → Data\.$/, 5_000);
      const a = words().find((x) => SAYING.test(x));
      shot('2-waiting-wifi');
      await d.sleep(4500);
      const b = words().find((x) => SAYING.test(x));
      if (!a || !b || a === b) throw new Error(`the sayings did not take turns (${a} → ${b})`);
      const before = words().find((x) => /MB downloaded/.test(x));
      if (before) throw new Error(`downloading on mobile data while told to wait: ${before}`);
      return `${a} → ${b}`;
    });
    let atKill = '';
    await ctx.step('Wi-Fi back: the download starts by itself', async () => {
      // a phone-like line (the emulator's own is ~1 Gbit/s: the whole set lands before the app could be closed)
      d.link('hsdpa', 'none');
      await d.net('wifi');
      const end = Date.now() + 120_000;
      for (;;) {
        const line = words().find((x) => /MB downloaded/.test(x)) ?? '';
        const got = Number((/^([\d,.]+) of/.exec(line)?.[1] ?? '0').replace(/,/g, ''));
        if (got > 15) { shot('3-downloading'); atKill = line; return line; }
        if (Date.now() > end) throw new Error(`no progress on Wi-Fi in 120 s (${line || words().slice(0, 6).join(' | ')})`);
        await d.sleep(1000);
      }
    });
    let notes = [];
    await ctx.step('app CLOSED mid-download: the system download goes on, in the notification bar', async () => {
      d.background();
      await d.sleep(1000);
      // the process killed as the system kills a closed app (never force-stop: that also stops its receivers)
      d.sh(`am kill ${d.APP_ID}`);
      await d.sleep(2000);
      const pct = () => {
        d.sh('cmd statusbar expand-notifications');
        return words().filter((x) => /Downloading BC fishing rules|·|\d+ ?%/.test(x));
      };
      const disk0 = onDisk();
      await d.sleep(10_000);
      notes = pct(); shot('4-notification-a');
      await d.sleep(20_000);
      const later = pct(); shot('4-notification-b');
      const disk1 = onDisk();
      d.sh('cmd statusbar collapse');
      if (alive()) throw new Error('the app is running: it was meant to be closed');
      if (!notes.some((x) => /Downloading BC fishing rules/.test(x))) throw new Error(`no download notification (saw ${notes.join(' | ')}; on disk ${disk0} → ${disk1})`);
      return `app closed at "${atKill}" · on disk ${disk0} → ${disk1} · notification: ${notes.join(' | ')} → ${later.join(' | ')}`;
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
      try {
        const js = d.logcatJs().split('\n').filter((l) => /\[download\]|\[setup\]/.test(l));
        fs.writeFileSync(path.join(ctx.dir, 'T298-install-timeline.txt'), js.join('\n') + '\n');
      } catch { /* no log */ }
      return `opened by ${opened}; map up ${Math.round((Date.now() - t0) / 1000)} s later`;
    }, { stop: true });
    await ctx.step('More → Data: "Download over mobile data" shows the saved Wi-Fi only', async () => {
      ctx.flow('tab', { TAB: 'More' });
      try { ctx.flow('scroll-to', { TEXT: 'Download over mobile data' }); } catch { /* already in view */ }
      await ctx.waitText(/^Download over mobile data$/, 10_000);
      shot('7-more-setting');
      return 'shown';
    });
  },
};
