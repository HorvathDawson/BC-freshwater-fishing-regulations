#!/usr/bin/env node
// (T313) THE TEST BED'S iOS TARGET — the basic scenarios on the iOS simulator (session-ios.sh boots it and installs
// app/dist/ios-sim/canifishthis.app, a Release simulator build reading STAGING). Same Maestro flows and report as
// run.mjs; the device is lib/device-ios.mjs (simctl). The simulator shares the Mac's network, so "offline" is the
// app's own "Simulate offline" (Updates log, staging builds), said as such in the report.
//
//   node app/tools/testbed/run-ios.mjs [all | <scenario id>…] [--out <dir>]
//   scenarios: first-open map tap-water offline satellite location walkthrough pdf perf (perf: a --probes build)
import { execFileSync, spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import * as device from './lib/device-ios.mjs';
import { flow } from './lib/maestro.mjs';
import { writeReport } from './lib/report.mjs';
import { count, readPng } from './lib/png.mjs';

process.env.TESTBED_PLATFORM = 'ios';
const HERE = path.dirname(new URL(import.meta.url).pathname);
const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args.splice(i, 2)[1] : d; };
const OUTDIR = opt('--out', null);
const [LON, LAT] = (process.env.LOCATE_AT ?? '-121.95,49.1').split(',').map(Number);
const sleep = device.sleep;

// ---------- shared steps ----------
const hasTabs = (w) => w.some((x) => /^MORE$|^More$/.test(x)) && w.some((x) => /^MAP$|^Map$/.test(x));
/** the walkthrough card's count line ("FIND A WATER · 1 OF 25") */
const GUIDE = /\d+ of \d+$/i;

/** The app in front with the map drawn: the first-run notice, the tour and system prompts closed as a reader would. */
async function appUp(ctx, { launch = true, ms = 180_000, alerts = 'Allow' } = {}) {
  if (launch) device.launch();
  const end = Date.now() + ms;
  let w = [];
  while (Date.now() < end) {
    w = device.screenText();
    if (w.some((x) => /Would Like to Send You Notifications|Allow .* to use your location/i.test(x))) {
      ctx.flow('ios-alert', { BUTTON: alerts === 'Allow' ? 'Allow.*' : "Don.t Allow" });
    } else if (w.some((x) => /^(I understand|Skip|Before you fish|Welcome|Got it)$/.test(x) || GUIDE.test(x))) {
      ctx.flow('dismiss-first-run');                     // the notice, the walkthrough (it reopens itself until skipped)
    } else if (!hasTabs(w) && w.includes('Layers') && w.includes('Done')) {
      // (T313 finding, T332) a walk left at its Layers step reopens the sheet, and the card over it is not in the
      // accessibility tree: the sheet's Done first, then the card can be skipped
      ctx.flow('tap-text', { TEXT: 'Done' });
    } else if (hasTabs(w)) {
      // the walkthrough opens once the map has drawn: a beat, then a last look before calling the app up
      await sleep(4000);
      const again = device.screenText();
      if (!again.some((x) => GUIDE.test(x) || /^(I understand|Got it)$/.test(x))) { w = again; break; }
      continue;
    } else if (!device.pidOf()) device.launch();
    await sleep(3000);
  }
  if (!hasTabs(w)) throw new Error(`the app is not up in ${ms / 1000} s (saw: ${w.slice(0, 8).join(' | ').slice(0, 300)})`);
  const bad = w.find((x) => /could not be loaded|Something went wrong|Unable to load/i.test(x));
  if (bad) throw new Error(`the app says: ${bad}`);
}
async function coldStart(ctx, opts) { device.stop(); await sleep(1000); return appUp(ctx, opts); }
/** Back to the Map tab: the edge-back (iOS has no system Back) until the tab bar shows, then the tab. */
async function toMap(ctx) {
  for (let i = 0; i < 5; i++) {
    const w = device.screenText();
    if (hasTabs(w)) break;
    // a menu page (a Modal from below: no edge swipe) has its own "‹ Back"; a water's page the edge-back
    if (w.some((x) => /^(Back|Glossary)$/.test(x))) ctx.flow('tap-text', { TEXT: '(Back|Glossary)' });
    else ctx.flow('edge-back');
    await sleep(1000);
  }
  if (!hasTabs(device.screenText())) await coldStart(ctx);
  ctx.flow('tab', { TAB: 'Map' });
  await sleep(1000);
}
/** The share of the map area (below the top bar, above the tab bar) that is drawn: not the blank/grey ground. */
function painted(file) {
  const img = readPng(file);
  const r = { x: 0, y: Math.round(img.h * 0.18), w: img.w, h: Math.round(img.h * 0.6) };
  const grey = count(img, r, (R, G, B) => Math.abs(R - G) < 6 && Math.abs(G - B) < 6 && R > 200);
  const blueish = count(img, r, (R, G, B) => B > R + 20);                  // water, open-status colours
  const total = r.w * r.h;
  return { grey: grey / total, colour: blueish / total, w: img.w, h: img.h };
}
const shot = (ctx, name) => device.screenshot(path.join(ctx.dir, `T313-${name}.png`));

// ---------- the scenarios ----------
const SCENARIOS = [
  { id: 'first-open', title: 'Install / first open: the app starts against staging and reaches the map',
    async run(ctx) {
      await ctx.step('launch; the first-run notice, the tour and the prompts closed; the map tab up', async () => {
        const t0 = Date.now();
        await appUp(ctx, { ms: 600_000 });
        return `${Math.round((Date.now() - t0) / 1000)} s to the map (a first open downloads the release)`;
      });
      await ctx.step('the data is staging (More → About names the environment)', async () => {
        ctx.flow('tab', { TAB: 'More' });
        await sleep(1000);
        ctx.flow('scroll-to', { TEXT: 'About' });
        ctx.flow('tap-text', { TEXT: 'About' });
        await sleep(1500);
        const w = device.screenText();
        shot(ctx, 'about');
        const env = w.find((x) => /staging/i.test(x));
        await toMap(ctx);
        if (!env) throw new Error(`About does not say staging (saw: ${w.slice(0, 12).join(' | ').slice(0, 300)})`);
        return env;
      });
    } },
  { id: 'map', title: 'The map paints (waters coloured, no grey ground)',
    async run(ctx) {
      await ctx.step('cold start → the map drawn', async () => {
        await coldStart(ctx);
        await sleep(6000);
        const p = painted(shot(ctx, 'map'));
        if (p.colour < 0.01) throw new Error(`the map looks unpainted (${(p.colour * 100).toFixed(1)}% coloured, ${(p.grey * 100).toFixed(1)}% grey)`);
        return `${(p.colour * 100).toFixed(1)}% coloured, ${(p.grey * 100).toFixed(1)}% light grey (${p.w}×${p.h})`;
      });
    } },
  { id: 'tap-water', title: 'A water to its regulations: from search, and by a tap on the map',
    async run(ctx) {
      await ctx.step('search "Cultus" → open → its regulations', async () => {
        await toMap(ctx);
        ctx.flow('search', { Q: 'Cultus' });
        ctx.flow('open-result', { LINE: '.*Lindell Beach.*' });
        await ctx.waitText(/Regulations/, 30_000);
        const w = device.screenText();
        shot(ctx, 'regs-search');
        if (!w.some((x) => /Cultus Lake/.test(x))) throw new Error('the page does not name Cultus Lake');
        return w.find((x) => /open|closed/i.test(x)) ?? 'regulations drawn';
      });
      await ctx.step('edge-back to the map (EdgeBack, iOS only)', async () => {
        // a few drags (height, speed): which ones go back is the note; none going back fails
        const tried = [];
        for (const [Y, MS] of [['50%', '400'], ['30%', '800'], ['80%', '250']]) {
          ctx.flow('edge-back', { Y, MS });
          await sleep(1500);
          const w = device.screenText();
          const back = !(w.some((x) => /^Regulations$/.test(x)) && !hasTabs(w));
          tried.push(`y ${Y} ${MS} ms: ${back ? 'back' : 'stayed'}`);
          if (back) return tried.join(' · ');
        }
        throw new Error(`the edge drag did not go back (${tried.join(' · ')})`);
      });
      await ctx.step('framed on Cultus Lake, a tap on the map\'s middle → that water\'s page', async () => {
        await toMap(ctx);
        ctx.flow('search', { Q: 'Cultus' });
        ctx.flow('tap-text', { TEXT: '.*Lindell Beach.*' });           // the row previews: the map frames the lake
        await sleep(3500);
        ctx.flow('edge-back');                                               // the search closed, the framing kept
        await sleep(2500);
        shot(ctx, 'framed');
        ctx.flow('tap-point', { POINT: '50%,50%' });
        await sleep(4000);
        const w = device.screenText();
        shot(ctx, 'regs-tap');
        const named = w.find((x) => /Cultus/.test(x));
        if (!named) throw new Error(`the tap opened no Cultus page (saw: ${w.slice(0, 10).join(' | ').slice(0, 300)})`);
        return named;
      });
    } },
  { id: 'offline', title: 'Offline (the app\'s "Simulate offline": the simulator shares the Mac\'s network)',
    async run(ctx) {
      await ctx.step('Updates log → Simulate offline: on', async () => {
        await toMap(ctx);
        ctx.flow('open-updates-log');
        ctx.flow('updates-log-action', { ACTION: 'offline' });
        await sleep(1500);
        const w = device.screenText();
        const on = w.find((x) => /Simulate offline: on/i.test(x));
        if (!on) throw new Error(`no "Simulate offline: on" (saw: ${w.filter((x) => /offline/i.test(x)).join(' | ')})`);
        ctx.flow('updates-log-action', { ACTION: 'check' });
        await sleep(2500);
        shot(ctx, 'offline-log');
        const row = device.screenText().find((x) => /offline/i.test(x) && !/Simulate/.test(x));
        return `${on}${row ? ` · Check now: ${row}` : ''}`;
      });
      await ctx.step('offline: search + a water\'s regulations from the installed set', async () => {
        await toMap(ctx);
        ctx.flow('search', { Q: 'Cultus' });
        ctx.flow('open-result', { LINE: '.*Lindell Beach.*' });
        await ctx.waitText(/Regulations/, 30_000);
        const w = device.screenText();
        shot(ctx, 'offline-regs');
        const stuck = w.find((x) => /not available offline|Loading the regulations|this will load when/.test(x));
        if (stuck) throw new Error(`the installed set should answer offline: "${stuck}"`);
        return 'regulations drawn';
      });
      await ctx.step('Simulate offline put back off', async () => {
        await toMap(ctx);
        ctx.flow('open-updates-log');
        // only when still on (a cold start in between puts it back off by itself: it is not kept)
        if (device.screenText().some((x) => /Simulate offline: on/i.test(x))) ctx.flow('updates-log-action', { ACTION: 'offline' });
        await sleep(1000);
        const off = device.screenText().find((x) => /Simulate offline: off/i.test(x));
        await toMap(ctx);
        if (!off) throw new Error('Simulate offline did not go off');
        return `${off} · (real airplane mode: not on the simulator; a phone check)`;
      });
    } },
  { id: 'satellite', title: 'Satellite imagery drawn (online)',
    async run(ctx) {
      await ctx.step('Layers → Satellite → imagery drawn', async () => {
        await toMap(ctx);
        const before = painted(shot(ctx, 'sat-before'));
        ctx.flow('layers-satellite');
        await sleep(800);
        const w = device.screenText();
        shot(ctx, 'sat-layers');
        ctx.flow('tap-text', { TEXT: 'Done' });
        await sleep(8000);
        const after = painted(shot(ctx, 'satellite'));
        const needs = w.find((x) => /needs a connection/i.test(x));
        if (needs) throw new Error(`online, the sheet says: ${needs}`);
        // imagery: far fewer flat light-grey pixels than the vector base map
        if (after.grey > before.grey * 0.8 && after.grey > 0.05) throw new Error(`no imagery seen (light grey ${(before.grey * 100).toFixed(1)}% → ${(after.grey * 100).toFixed(1)}%)`);
        return `light grey ${(before.grey * 100).toFixed(1)}% → ${(after.grey * 100).toFixed(1)}%`;
      });
      await ctx.step('the Map base put back', async () => {
        ctx.flow('tap-text', { TEXT: 'Choose layers.*' });
        await sleep(1200);
        ctx.flow('tap-id', { ID: 'option-ground\\.map' });
        await sleep(800);
        ctx.flow('tap-text', { TEXT: 'Done' });
        await sleep(1500);
      });
    } },
  { id: 'location', title: `My location (simctl location ${LAT},${LON}): denied line; allowed → the dot, centred`,
    async run(ctx) {
      const rail = (w) => w.find((x) => /^(My location|Follow my location|Stop following my location)$/.test(x)) ?? null;
      await ctx.step('denied (simctl privacy revoke) → the line saying how to turn it on', async () => {
        device.privacy('revoke');
        device.geo(LON, LAT);
        await coldStart(ctx);
        await sleep(2500);
        ctx.flow('tap-text', { TEXT: 'My location' });
        await sleep(2500);
        const w = device.screenText();
        shot(ctx, 'location-denied');
        const line = w.find((x) => /^Location is off for this app/.test(x));
        if (!line) throw new Error(`no line (saw: ${w.slice(0, 12).join(' | ').slice(0, 300)})`);
        ctx.flow('tap-text', { TEXT: 'Location is off for this app.*' });
        return line;
      });
      await ctx.step('allowed (simctl privacy grant) → the blue dot, centred; the button says Follow', async () => {
        device.privacy('grant');
        await coldStart(ctx);
        await sleep(2000);
        ctx.flow('tap-text', { TEXT: 'My location' });
        await sleep(5000);
        const f = shot(ctx, 'location-dot');
        const img = readPng(f);
        const mid = { x: Math.round(img.w / 2) - 90, y: Math.round(img.h / 2) - 90, w: 180, h: 180 };
        const blue = count(img, mid, (r, g, b) => b > 180 && r < 110 && b - r > 110 && b - g > 40);
        const l = rail(device.screenText());
        if (blue < 20) throw new Error(`no blue dot in the middle (${blue} px); button: ${l}`);
        return `${blue} blue px in the middle · button: ${l}`;
      });
      await ctx.step('the location sheet / pick-the-water at the fix ("At my location" from search)', async () => {
        await toMap(ctx);
        try { ctx.flow('tap-text', { TEXT: 'Search waters and towns' }); } catch { /* the bar's own label varies */ }
        await sleep(1000);
        const w = device.screenText();
        const at = w.find((x) => /^At my location/.test(x));
        if (!at) ctx.skip('no "At my location" row in search');
        ctx.flow('tap-text', { TEXT: 'At my location.*' });
        await sleep(4000);
        shot(ctx, 'at-my-location');
        return device.screenText().slice(0, 6).join(' | ').slice(0, 200);
      });
    } },
  { id: 'walkthrough', title: 'The guided walkthrough replayed from Help, walked to its end',
    async run(ctx) {
      const COUNT = /(\d+) of (\d+)$/i;
      await ctx.step('More → Help → Replay the walkthrough', async () => {
        await coldStart(ctx);
        ctx.flow('tab', { TAB: 'More' });
        await sleep(1500);
        ctx.flow('scroll-to', { TEXT: 'Help' });
        ctx.flow('tap-text', { TEXT: 'Help' });
        await sleep(1500);
        ctx.flow('scroll-to', { TEXT: 'Replay the walkthrough' });
        ctx.flow('tap-text', { TEXT: 'Replay the walkthrough' });
        await sleep(3000);
        const c = device.screenText().find((x) => COUNT.test(x));
        if (!c) throw new Error('no walkthrough card');
        return c;
      });
      await ctx.step('every step: its primary button, until the card closes', async () => {
        const seen = []; let last = null, same = 0, total = 0, reached = 0, closed = false;
        for (let i = 0; i < 70; i++) {
          const w = device.screenText();
          let c = w.find((x) => COUNT.test(x));
          if (!c) {                                          // a read mid-animation can come back empty: look twice
            await sleep(2500);
            const w2 = device.screenText();
            c = w2.find((x) => COUNT.test(x));
            if (!c && w2.includes('Layers') && w2.includes('Done')) {
              // (T332) the card over the Layers sheet is not in the accessibility tree: the walk cannot be driven on
              shot(ctx, 'walk-hidden');
              ctx.finding(`T332: after ${reached} of ${total} the walkthrough card over the Layers sheet is missing from the accessibility tree`);
              await coldStart(ctx);                         // appUp closes the sheet and skips the walk
              ctx.skip(`walked 1–${reached} of ${total}; step ${reached + 1} (over the Layers sheet) is unreachable by accessibility (T332)`);
            }
            if (!c) { closed = true; break; }
          }
          const [, at, n] = COUNT.exec(c).map(Number);
          total = n; reached = Math.max(reached, at);
          if (c !== last) { seen.push(c); shot(ctx, `walk-${String(at).padStart(2, '0')}`); last = c; same = 0; }
          else if (++same > 4) throw new Error(`stuck at ${c} (${seen.slice(-4).join(' / ')})`);
          ctx.flow('tap-id', { ID: 'guide-primary' });
          await sleep(3500);
        }
        if (!closed) throw new Error(`the card never closed (${seen.slice(-3).join(' / ')})`);
        if (reached < total) throw new Error(`closed at ${reached} of ${total}`);
        return `${reached} of ${total}`;
      });
    } },
  { id: 'pdf', title: 'The synopsis PDF opens from a "Rule book p.N ↗" link',
    async run(ctx) {
      await ctx.step('Cultus Lake page → Rule book link → a PDF is shown', async () => {
        await toMap(ctx);
        ctx.flow('search', { Q: 'Cultus' });
        ctx.flow('open-result', { LINE: '.*Lindell Beach.*' });
        await ctx.waitText(/Regulations/, 30_000);
        ctx.flow('scroll-to', { TEXT: 'Rule book p\\.[0-9]+ ↗.*' });
        ctx.flow('tap-text', { TEXT: 'Rule book p\\.[0-9]+ ↗.*' });
        let w = [];
        for (let i = 0; i < 20; i++) {                                       // a first open downloads the book (~25 MB)
          await sleep(3000);
          w = device.screenText();
          if (!w.some((x) => /Loading the synopsis|^\d+%$/.test(x))) break;
        }
        shot(ctx, 'pdf');
        if (w.some((x) => /Couldn.t load the synopsis/.test(x))) throw new Error('"Couldn’t load the synopsis."');
        const noViewer = w.find((x) => /No app on this device opens PDFs/.test(x));
        if (noViewer) throw new Error(noViewer);
        const pdf = w.find((x) => /synopsis[^ ]*\.pdf|\.pdf$|PDF Document|Rule book|Fishing Synopsis/i.test(x) && !/↗/.test(x));
        const share = w.find((x) => /^(Copy|Save to Files|AirDrop|Messages|Mail)$/.test(x));
        if (share) return `a share sheet, not the PDF (${share}): see shot`;
        return pdf ? `shown: ${pdf}` : `see shot (words: ${w.slice(0, 6).join(' | ').slice(0, 200)})`;
      });
    } },
  { id: 'perf', title: 'Performance, the Android measures (T245 / T286 / T273): cold start → map drawn, a map tap → its page, pan; memory',
    // The simulator runs on the Mac's CPU/GPU: these numbers flag regressions and iOS-only stalls, not a phone's speed.
    // The app's own probes ([open], [pan], [stall], [target]) need a `build-ios-sim.sh --probes` build; they reach the
    // unified log (com.facebook.react.log), streamed to app-os.log beside the report. Native map frame times are not
    // read on iOS (no gfxinfo; Instruments is the follow-up): the JS thread's lag and stalls stand in.
    async run(ctx) {
      const COLD = Number(process.env.PERF_COLD ?? 3), TAPS = Number(process.env.OPEN_TAPS ?? 3), SWIPES = Number(process.env.PERF_SWIPES ?? 8);
      const out = { device: run.device, note: 'iOS Simulator on the Mac: regressions / iOS-only stalls, not phone speed', cold: [], open: [], pan: null, rssPeakMb: 0 };
      const keep = () => fs.writeFileSync(path.join(ctx.dir, 'T313-perf.json'), `${JSON.stringify(out, null, 1)}\n`);
      const rss = () => {
        const pid = device.pidOf();
        if (!pid) return null;
        try { const kb = Number(execFileSync('ps', ['-o', 'rss=', '-p', pid], { encoding: 'utf8' }).trim()); const mb = Math.round(kb / 1024); out.rssPeakMb = Math.max(out.rssPeakMb, mb); return mb; } catch { return null; }
      };
      const tmp = path.join(ctx.dir, 'perf-tmp.png');
      for (let i = 1; i <= COLD; i++) {
        await ctx.step(`cold start ${i}/${COLD}: launch → the map drawn`, async () => {
          device.stop(); await sleep(1500);
          const t0 = Date.now();
          device.launch();
          let drawn = null;
          while (Date.now() - t0 < 60_000) {
            device.screenshot(tmp);
            if (painted(tmp).colour > 0.03) { drawn = Date.now() - t0; break; }
            await sleep(150);
          }
          await sleep(3000);
          const m = rss();
          out.cold.push({ mapDrawnMs: drawn, rssMb: m }); keep();
          if (drawn === null) throw new Error('the map was not drawn in 60 s');
          return `map drawn ${drawn} ms (a screenshot every ~0.3 s: ±0.3 s) · RSS ${m} MB`;
        });
      }
      try { fs.unlinkSync(tmp); } catch { /* none */ }
      await appUp(ctx, { launch: false });
      // a tap on the map at the point the app names (`[target]`), timed by the app's `[open]` probe (T286)
      let at = null;
      await ctx.step('search "Cultus" → framed; the app names the tap point ([target])', async () => {
        await toMap(ctx);
        const mark = logMark();
        ctx.flow('search', { Q: 'Cultus' });
        ctx.flow('tap-text', { TEXT: '.*Lindell Beach.*' });
        await sleep(3500);
        ctx.flow('edge-back');
        await sleep(6000);
        const t = logSince(mark, '[target] ').at(-1);
        if (!t) ctx.skip('no [target] line (a build without --probes): the map tap is not timed');
        const img = readPng(shot(ctx, 'perf-framed'));
        const k = img.w / 402;                                              // px per point (iPhone 17: 3)
        at = { x: Math.round(((t.win?.x ?? 0) + t.x) * k / img.w * 1000) / 10, y: Math.round(((t.win?.y ?? 0) + t.y) * k / img.h * 1000) / 10 };
        return `target ${JSON.stringify(t).slice(0, 120)} → ${at.x}%, ${at.y}%`;
      });
      for (let i = 1; at && i <= TAPS; i++) {
        await ctx.step(`map tap ${i}/${TAPS} → its regulations ([open])`, async () => {
          const mark = logMark();
          ctx.flow('tap-point', { POINT: `${at.x}%,${at.y}%` });
          let o = null;
          for (let n = 0; n < 30 && !o; n++) { await sleep(700); o = logSince(mark, '[open] ').at(-1) ?? null; }
          if (!o) throw new Error('no [open] line in 20 s');
          out.open.push(o); keep();
          ctx.flow('edge-back'); await sleep(2000);
          const frame = o.marks?.frame?.[0] ?? o.marks?.timeout?.[0];
          if (o.marks?.spinner) throw new Error(`a spinner at ${o.marks.spinner[0]} ms · first frame ${frame} ms`);
          return `first frame ${frame} ms · sql ${o.sql} (wait ${o.sqlWait}, run ${o.sqlRun}) · ${Object.entries(o.marks).map(([k, v]) => `${k} ${v[0]}`).join(', ')}`;
        });
      }
      await ctx.step(`pan: ${SWIPES} travelling swipes ([pan] JS lag, [stall] > 50 ms)`, async () => {
        await toMap(ctx);
        const mark = logMark(), t0 = Date.now();
        const moves = [['80%, 45%', '25%, 45%'], ['80%, 45%', '25%, 45%'], ['50%, 35%', '50%, 55%'], ['50%, 35%', '50%, 55%'],
                       ['25%, 45%', '80%, 45%'], ['25%, 45%', '80%, 45%'], ['50%, 55%', '50%, 35%'], ['50%, 55%', '50%, 35%']];
        for (let i = 0; i < SWIPES; i++) { const [FROM, TO] = moves[i % moves.length]; ctx.flow('swipe', { FROM, TO }); rss(); }
        await sleep(2500);
        const ps = logSince(mark, '[pan] '), st = logSince(mark, '[stall] ');
        const max = (k) => ps.reduce((n, p) => Math.max(n, p[k] || 0), 0), sum = (k) => ps.reduce((n, p) => n + (p[k] || 0), 0);
        const stalls = st.map((s) => s.ms).sort((a, b) => a - b);
        out.pan = { seconds: Math.round((Date.now() - t0) / 1000), lagMax: max('lagMax'), late: sum('late'), tiles: sum('tiles'), labels: sum('labels'),
                    sends: sum('sends'), ackMax: max('ackMax'), renders: sum('renders'), stalls: stalls.length, stallMax: stalls.at(-1) ?? 0,
                    stallP95: stalls[Math.floor(stalls.length * 0.95)] ?? 0 };
        keep();
        if (!ps.length) ctx.skip('no [pan] lines (a build without --probes)');
        return `JS lag max ${out.pan.lagMax} ms, ${out.pan.late} ticks >100 ms late · stalls >50 ms: ${out.pan.stalls} (max ${out.pan.stallMax}, p95 ${out.pan.stallP95} ms) · tiles ${out.pan.tiles}, labels ${out.pan.labels}, sends ${out.pan.sends} (ack max ${out.pan.ackMax} ms), renders ${out.pan.renders} · RSS peak ${out.rssPeakMb} MB`;
      });
    } },
];

// ---------- the runner (as run.mjs, without the Android-only crash buffer and test channel) ----------
const want = args.length && !args.includes('all') ? args : SCENARIOS.map((s) => s.id);
for (const w of want) if (!SCENARIOS.some((s) => s.id === w)) { console.error(`no scenario ${w}: ${SCENARIOS.map((s) => s.id).join(', ')}`); process.exit(2); }
const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
const dir = OUTDIR ?? path.join(HERE, 'out', 'runs', `ios-${stamp}`);
fs.mkdirSync(dir, { recursive: true });
let simName = device.udid();
try {
  const j = JSON.parse(device.simctl(['list', 'devices', '-j']).toString()).devices;
  for (const [rt, ds] of Object.entries(j)) for (const d of ds) if (d.udid === simName) simName = `${d.name} · ${rt.split('.').pop()}`;
} catch { /* the udid alone */ }
const run = { id: stamp, started: new Date().toISOString(), device: `${device.udid()} · ${simName}`,
  apk: 'app/dist/ios-sim/canifishthis.app (Release, iOS Simulator, staging, env switch on)', channel: 'staging', sync: 'local', scenarios: [] };
try {
  const prev = JSON.parse(fs.readFileSync(path.join(dir, 'report.json'), 'utf8'));
  run.id = prev.id; run.started = prev.started; run.scenarios.push(...prev.scenarios.filter((x) => !want.includes(x.id)));
} catch { /* a new report */ }
console.log(`[testbed-ios] report → ${dir}`);
let shotN = fs.readdirSync(dir).filter((f) => /^\d{3}-/.test(f)).length;

function makeCtx(sc) {
  const rec = { id: sc.id, title: sc.title, result: 'pass', steps: [], findings: [], ms: 0 };
  run.scenarios.push(rec);
  const ctx = {
    device, rec, dir,
    flow(name, env) {
      let r = flow(name, env, { logDir: path.join(dir, 'maestro', sc.id) });
      if (!r.ok) { execSleep(2); r = flow(name, env, { logDir: path.join(dir, 'maestro', sc.id) }); }
      ctx.lastSaid = r.said;
      if (!r.ok) throw new Error(`flow ${name} failed: ${r.said.split('\n').slice(-3).join(' / ') || 'see maestro log'}`);
      return r;
    },
    async waitText(re, ms = 30_000) {
      const end = Date.now() + ms; let words = [];
      while (Date.now() < end) { words = device.screenText(); const hit = words.find((w) => re.test(w)); if (hit) return hit; await sleep(1500); }
      throw new Error(`no text ${re} on screen in ${ms / 1000} s (saw: ${words.slice(0, 12).join(' | ').slice(0, 300)})`);
    },
    async step(title, fn) {
      const t0 = Date.now(); const s = { title, result: 'pass', ms: 0 }; ctx.lastSaid = null;
      try { const note = await fn(); if (typeof note === 'string') s.note = note; }
      catch (e) { s.result = e.skip ? 'skip' : 'fail'; s.error = e.message; if (!e.skip) rec.result = 'fail'; }
      s.ms = Date.now() - t0;
      // a crash: a new crash report for the app (the simulator's are the Mac's DiagnosticReports) — said, never hidden
      for (const c of newCrashes()) { s.result = 'fail'; rec.result = 'fail'; ctx.finding(`crash during "${title}": ${c}`); }
      if (ctx.lastSaid && s.result === 'fail') s.said = ctx.lastSaid;
      try {
        const name = `${String(++shotN).padStart(3, '0')}-${sc.id}-${title.toLowerCase().replace(/[^a-z0-9]+/g, '-').slice(0, 40)}.png`;
        device.screenshot(path.join(dir, name)); s.shot = name;
      } catch { /* never fails a step */ }
      rec.steps.push(s);
      console.log(`  ${s.result.toUpperCase()} ${title}${s.note ? ` — ${s.note}` : ''}${s.error ? ` — ${s.error}` : ''}`);
      writeReport(dir, run);
      return s;
    },
    finding(text) { rec.findings.push(text); console.log(`  FINDING ${text}`); },
    skip(msg) { throw Object.assign(new Error(msg), { skip: true }); },
  };
  return ctx;
}
// the simulator's crash reports land in the Mac's DiagnosticReports (CanIFish-<date>.ips)
const DIAG = path.join(process.env.HOME, 'Library/Logs/DiagnosticReports');
const crashList = () => { try { return fs.readdirSync(DIAG).filter((f) => /^CanIFish.*\.ips$/.test(f)); } catch { return []; } };
const crashSeen = new Set(crashList());
function newCrashes() {
  const out = [];
  for (const f of crashList()) if (!crashSeen.has(f)) {
    crashSeen.add(f);
    fs.copyFileSync(path.join(DIAG, f), path.join(dir, f));
    let why = '';
    try { const t = fs.readFileSync(path.join(DIAG, f), 'utf8'); why = (/"exception"\s*:\s*\{[^}]*\}/.exec(t)?.[0] ?? '').slice(0, 200); } catch { /* unreadable */ }
    out.push(`${f} ${why}`);
  }
  return out;
}
// (T313) the app's unified log (CFNetwork, RN errors, MapLibre) for the whole run, beside the report
const osLog = spawn(device.SIMCTL, ['spawn', device.udid(), 'log', 'stream', '--style', 'compact', '--level', 'debug',
  '--predicate', 'process == "CanIFish"'], { stdio: ['ignore', fs.openSync(path.join(dir, 'app-os.log'), 'a'), 'ignore'] });
process.on('exit', () => { try { osLog.kill(); } catch { /* gone */ } });

/** The app-os.log's length now (a mark), or -1 when the log is not being written. */
function logMark() { try { return fs.statSync(path.join(dir, 'app-os.log')).size; } catch { return -1; } }
/** The probe lines (`tag` "[open] ", "[pan] " …) written to app-os.log since `mark`, parsed. */
function logSince(mark, tag) {
  try {
    const b = fs.readFileSync(path.join(dir, 'app-os.log'));
    return b.subarray(Math.max(0, mark)).toString('utf8').split('\n').filter((l) => l.includes(tag))
      .map((l) => { try { return JSON.parse(l.slice(l.indexOf(tag) + tag.length)); } catch { return null; } }).filter(Boolean);
  } catch { return []; }
}

function execSleep(s) { Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, s * 1000); }

for (const sc of SCENARIOS.filter((s) => want.includes(s.id))) {
  console.log(`[testbed-ios] ${sc.id} — ${sc.title}`);
  const ctx = makeCtx(sc);
  const t0 = Date.now();
  try { await sc.run(ctx); }
  catch (e) { ctx.rec.result = 'fail'; ctx.rec.findings.push(`runner: ${e.message}`); console.log(`  ERROR ${e.stack}`); }
  ctx.rec.ms = Date.now() - t0;
  writeReport(dir, run);
}
run.finished = new Date().toISOString();
writeReport(dir, run);
const bad = run.scenarios.filter((s) => s.result === 'fail');
console.log(`[testbed-ios] ${run.scenarios.length - bad.length}/${run.scenarios.length} scenarios passed → ${path.join(dir, 'index.html')}`);
process.exit(bad.length ? 1 : 0);
