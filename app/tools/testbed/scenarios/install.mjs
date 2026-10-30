// (T255) 0. FIRST RUN: a clean app on Wi-Fi installs the whole staging release (the test channel's manifest, a copy of
// the live one) for offline use, then opens the map. Everything after builds on this install.
import fs from 'node:fs';
import path from 'node:path';
import { appUp, logStatus, openLog } from './common.mjs';

export const install = {
  id: 'install',
  title: 'First run downloads the release and opens the map',
  async run(ctx) {
    await ctx.step('test channel reset to the live release', () => {
      const s = ctx.updates('reset');
      return `test/ = ${s.release.data_release}`;
    }, { shot: false });
    await ctx.step('clean app on Wi-Fi', async () => {
      ctx.device.stop(); ctx.device.clearData();
      try { ctx.device.adb(['logcat', '-G', '16M']); ctx.device.logcatClear(); } catch { /* none yet */ }
      await ctx.device.net('wifi'); ctx.device.link('full', 'none');
      ctx.device.launch();
      await ctx.device.sleep(4000);
    });
    await ctx.step('first-run download, then the map (no crash)', async () => {
      const t0 = Date.now();
      let relaunched = 0;
      // (T262) the peak heap over the download, sampled every 5 s
      const peak = { java: 0, native: 0, total: 0 };
      // (T262) TESTBED_KILL_MB=<n>: the app is killed once its download line passes n MB, to prove the resume
      const killMb = Number(process.env.TESTBED_KILL_MB || 0);
      let killed = false;
      // (T272) the post-download wait, as a reader sees it: when the download line first says 100 %, when the second
      // stage ("Setting up") is first seen, when the map is up; a screenshot of each stage
      const seen = { full: 0, setup: 0, shotDl: false, shotSetup: false };
      const shot = (name) => { try { ctx.device.screenshot(path.join(ctx.dir, `T272-${name}.png`)); } catch { /* no shot */ } };
      // the download is watched; a dead app (T262: OutOfMemoryError in expo fetch) is relaunched as a reader would,
      // and the step fails on the crash (the runner reads the crash buffer) while the rest of the run goes on
      while (Date.now() - t0 < 20 * 60_000) {
        // the map's tab bar, or the first-run notice over it (a cleared app): the download is done
        const w = ctx.device.screenText();
        // (T298) a first install asks to notify (Android 13+): allowed, as a reader would
        if (w.some((x) => /send you notifications/i.test(x))) { ctx.flow('tap-text', { TEXT: 'Allow' }); continue; }
        if (w.includes('More') || w.includes('MORE') || w.some((x) => /^(Before you fish|I understand|Skip|Got it)$/.test(x))) break;
        const line = /([\d,.]+) of ([\d,.]+) MB downloaded/.exec(w.join(' '));
        if (line && !seen.shotDl && Number(line[1].replace(/,/g, '')) > 20) { shot('stage-download'); seen.shotDl = true; }
        if (line && line[1] === line[2] && !seen.full) seen.full = Date.now();
        if (w.some((x) => /^Setting up/.test(x))) {
          if (!seen.setup) seen.setup = Date.now();
          if (!seen.shotSetup) { shot('stage-setup'); seen.shotSetup = true; }
        }
        if (killMb && !killed) {
          const got = Number((/([\d,.]+) of [\d,.]+ MB downloaded/.exec(w.join(' '))?.[1] ?? '0').replace(/,/g, ''));
          if (got >= killMb) {
            killed = `${got} MB`; ctx.device.stop(); await ctx.device.sleep(2000); ctx.device.launch(); continue;
          }
          await ctx.device.sleep(500);
          continue;
        }
        if (!ctx.device.pidOf()) { relaunched++; ctx.device.launch(); }
        const h = ctx.device.heap();
        if (h) for (const k of Object.keys(peak)) peak[k] = Math.max(peak[k], h[k] || 0);
        await ctx.device.sleep(1000);
      }
      const upAt = Date.now();
      const took = Math.round((upAt - t0) / 1000);
      // (T272) the app's own timings (logcat): the download, verify and set-up lines, kept beside the report
      let timeline = '';
      try {
        const js = ctx.device.logcatJs().split('\n').filter((l) => /\[download\]|\[setup\]/.test(l));
        fs.writeFileSync(path.join(ctx.dir, 'T272-install-timeline.txt'), js.join('\n') + '\n');
        const done = js.filter((l) => /: done\b.*\d+ bytes/.test(l)).pop();
        if (done) timeline = ` · last download done ${done.slice(0, 18)}`;
      } catch { /* no log */ }
      const after = (t) => (t ? `${Math.round((upAt - t) / 1000)} s` : 'not seen');
      const wait = ` · 100 % → map ${after(seen.full)}, "Setting up" → map ${after(seen.setup)}${timeline}`;
      await appUp(ctx, { launch: false, ms: 60_000 });
      // (T287) THE FIRST-INSTALL FINISH: the JS stalls (test-bed build: `[stall]`) from the set installed to the map up,
      // with the timed decodes (`[work]`) and set-up lines between them — kept beside the report
      let finish = '';
      try {
        const js = ctx.device.logcatJs().split('\n');
        const from = js.findIndex((l) => /\[setup\] installed/.test(l));
        if (from >= 0) {
          const tail = js.slice(from).filter((l) => /\[(stall|work|setup|swap)\]/.test(l));
          fs.writeFileSync(path.join(ctx.dir, 'T287-first-install.txt'), tail.join('\n') + '\n');
          const ms = tail.map((l) => /\[stall\] (\{.*\})/.exec(l)).filter(Boolean).map((m) => JSON.parse(m[1]).ms);
          finish = ` · install finish → map: ${ms.length} stalls >50 ms${ms.length ? ` (worst ${Math.max(...ms)} ms: ${ms.sort((a, b) => b - a).slice(0, 5).join(', ')})` : ''}`;
        }
      } catch { /* no log */ }
      ctx.flow('dismiss-first-run');
      const mb = (kb) => `${Math.round(kb / 1024)} MB`;
      return `downloaded and up in ${took} s after launch${relaunched ? ` — the app died ${relaunched}× and was relaunched (resumed)` : ''}${killed ? ` — killed at ${killed} and resumed` : ''}` +
        ` · peak heap: Java ${mb(peak.java)}, native ${mb(peak.native)}, total PSS ${mb(peak.total)}` + wait + finish;
    }, { stop: true });
    await ctx.step('Updates log: every source checked on start', async () => {
      openLog(ctx);
      await ctx.device.sleep(3000);
      const st = logStatus(ctx);
      if (!/Gauges: ok/.test(st) || !/Release: ok/.test(st)) throw new Error(`status: ${st || 'not read'}`);
      return st.replace(/\n/g, ' · ');
    });
  },
};
