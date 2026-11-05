#!/usr/bin/env node
// (T313) THE TEST BED'S iOS TARGET — the basic scenarios on the iOS simulator (session-ios.sh boots it and installs
// app/dist/ios-sim/canifishthis.app, a Release simulator build reading STAGING). Same Maestro flows and report as
// run.mjs; the device is lib/device-ios.mjs (simctl). The simulator shares the Mac's network, so "offline" is the
// app's own "Simulate offline" (Updates log, staging builds), said as such in the report.
//
//   node app/tools/testbed/run-ios.mjs [all | <scenario id>…] [--out <dir>]
//   scenarios: first-open map tap-water offline satellite location walkthrough pdf crash perf memory
//   (perf, memory: a --probes build — the app's [open] [pan] [heap] lines)
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
// (iPhone 17 layout) the search bar's "‹" and a water page's "‹ Map": neither has words Maestro can match on iOS
const SEARCH_BACK = '9%,11%', PAGE_BACK = '10%,10%';
/** the walkthrough card's count line ("FIND A WATER · 1 OF 25") */
const GUIDE = /\d+ of \d+$/i;
/** a water's page is up: its back link by id (iOS reads "‹ Map" as the label "Back"; the tab bar can stay in the tree) */
const onPage = (w) => (w.ids ?? []).includes('water-back') || (w.includes('Regulations') && w.includes('Conditions') && !hasTabs(w));

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
    } else if (onPage(w)) {
      ctx.flow('tap-point', { POINT: PAGE_BACK });          // a cold start reopened the water's page: back to the map
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
  for (let i = 0; i < 6; i++) {
    const w = device.screenText();
    // a water's page ("‹ Map" over Regulations · Conditions): its own back link, by id (the tab bar may still be in the tree)
    if (onPage(w)) {
      try { ctx.flow('tap-id', { ID: 'water-back' }); } catch { ctx.flow('tap-point', { POINT: PAGE_BACK }); }
      await sleep(1500); continue;
    }
    if (hasTabs(w)) break;
    // a menu page (a Modal from below: no edge swipe) has its own "‹ Back"; a water's page the edge-back
    if (w.some((x) => /^(Back|Glossary)$/.test(x))) ctx.flow('tap-text', { TEXT: '(Back|Glossary)' });
    else if (w.some((x) => /NAMED WATERS/i.test(x))) ctx.flow('tap-point', { POINT: SEARCH_BACK });   // the search open
    else ctx.flow('edge-back');
    await sleep(1000);
  }
  const w = device.screenText();
  if (!hasTabs(w) || onPage(w)) await coldStart(ctx);
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
        for (const [f, Y, MS] of [['edge-back', '50%', 400], ['edge-back-high', '30%', 900], ['edge-back-low', '80%', 400]]) {
          ctx.flow(f);
          await sleep(1500);
          // an empty read is not "back", nor is one taken mid-slide: back only when two reads 1.5 s apart both show no page
          const w = device.screenText();
          let back = w.length > 0 && !onPage(w);
          if (back) { await sleep(1500); const w2 = device.screenText(); back = w2.length > 0 && !onPage(w2); }
          tried.push(`y ${Y} ${MS} ms: ${back ? 'back' : 'stayed'}`);
          if (back) return tried.join(' · ');
        }
        throw new Error(`the edge drag did not go back (${tried.join(' · ')})`);
      });
      await ctx.step('framed on Cultus Lake, a tap on the lake on the map → its page', async () => {
        await toMap(ctx);
        ctx.flow('search', { Q: 'Cultus' });
        ctx.flow('tap-text', { TEXT: '.*Lindell Beach.*' });           // the row previews: the map frames the lake
        await sleep(3500);
        // the map above the results is framed on the lake: a tap on the lake there (closing the search with its ‹ puts
        // the camera back where it was on iOS, so the tap is made with the results up)
        shot(ctx, 'framed');
        ctx.flow('tap-point', { POINT: '60%,40%' });
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
              // (T336, fixed) the card over the Layers sheet missing from the accessibility tree: a failure again
              shot(ctx, 'walk-hidden');
              ctx.finding(`T336: after ${reached} of ${total} the walkthrough card over the Layers sheet is missing from the accessibility tree`);
              await coldStart(ctx);                         // appUp closes the sheet and skips the walk
              throw new Error(`walked 1–${reached} of ${total}; step ${reached + 1} (over the Layers sheet) is unreachable by accessibility (T336)`);
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
        // (T354) the app's own viewer (PDFKit): "Page N of M" in its header, or its failure face
        if (w.some((x) => /Couldn.t load the synopsis/.test(x))) throw new Error('"Couldn’t load the synopsis."');
        const failed = w.find((x) => /Couldn.t show the rule book here/.test(x));
        if (failed) throw new Error(failed);
        const page = w.find((x) => /^Page \d+ of \d+$/.test(x));
        if (!page) throw new Error(`the in-app viewer did not open (words: ${w.slice(0, 6).join(' | ').slice(0, 200)})`);
        ctx.flow('tap-id', { ID: 'pdf-viewer-close' });
        return `shown in the app: ${page}`;
      });
    } },
  { id: 'crash', title: 'A native crash is reported on the next start (T362: the app\'s own record, iOS keeps none), once',
    // The app is crashed the way a native fault does it: SIGSEGV to its process (a simulator app is a Mac process). The
    // next start reads modules/exit-info's record and sends it (POST /crash, tagged `test` on a --probes build); the app
    // logs "[crash-reports] sent N exit record(s)", streamed to app-os.log. A start after a normal stop sends none.
    async run(ctx) {
      const sent = (mark) => { const l = logLines(mark, '[crash-reports] sent').at(-1); const m = l && /sent (\d+) exit record[^\n]*/.exec(l); return m ? { n: Number(m[1]), line: m[0] } : null; };
      const waitSent = async (mark, ms) => { const end = Date.now() + ms; let r = null; while (Date.now() < end && !(r = sent(mark))) await sleep(2000); return r; };
      await ctx.step('the app up, then crashed (SIGSEGV to its process)', async () => {
        await toMap(ctx);
        const pid = device.pidOf();
        if (!pid) throw new Error('the app is not running');
        process.kill(Number(pid), 'SIGSEGV');
        const end = Date.now() + 15_000;
        while (Date.now() < end && device.pidOf() === pid) await sleep(500);
        if (device.pidOf() === pid) throw new Error('the app did not die');
        await sleep(4000); ctx.expectCrash();                    // this crash is the test, not a finding
        return `pid ${pid} gone`;
      });
      await ctx.step('the next start sends 1 exit record', async () => {
        const mark = logMark();
        await appUp(ctx);
        const r = await waitSent(mark, 90_000);
        if (!r) throw new Error('no "[crash-reports] sent" line in 90 s');
        if (r.n < 1) throw new Error(`the record was not sent: ${r.line}`);
        return r.line;
      });
      await ctx.step('a start after a normal stop sends none', async () => {
        const mark = logMark();
        await coldStart(ctx);
        const r = await waitSent(mark, 60_000);
        if (!r) throw new Error('no "[crash-reports] sent" line in 60 s');
        if (r.n !== 0) throw new Error(`sent again: ${r.line}`);
        return r.line;
      });
    } },
  { id: 'memory', title: 'Memory per screen (T363, T360\'s method on iOS): footprint, MALLOC, graphics, JS heap + external',
    // `footprint` of the simulator process (the Mac's view): the footprint is what iOS's memory limit reads; graphics is
    // the Mac's GPU here, so the phone's is not known (T364). JS: the build's [heap] probe (--probes), from app-os.log.
    async run(ctx) {
      const rows = [];
      const keep = () => fs.writeFileSync(path.join(ctx.dir, 'T363-memory.json'), `${JSON.stringify(rows, null, 1)}\n`);
      const measure = async (what, settle = 3000) => {
        await sleep(settle);
        const m = device.memory();
        const h = logLines(-1, '[heap] ').at(-1);
        let js = null, ext = null;
        try { const j = JSON.parse(h.slice(h.indexOf('[heap] ') + 7)); js = Math.round(j.size / 104857.6) / 10; ext = j.ext == null ? null : Math.round(j.ext / 104857.6) / 10; } catch { /* no probe line */ }
        rows.push({ step: what, ...m, js, ext }); keep();
        return m ? `footprint ${m.footprint} · malloc ${m.malloc} · graphics ${m.graphics} · mapped ${m.mapped} · RSS ${m.rss} · js heap ${js ?? '?'} · js external ${ext ?? '?'}` : 'not running';
      };
      const layer = (id) => {
        ctx.flow('tap-text', { TEXT: 'Choose layers.*' }); await_(1200);
        ctx.flow('tap-id', { ID: id.startsWith('ground.') ? `option-${id.replace('.', '\\.')}` : `layer-${id.replace('.', '\\.')}` });
        ctx.flow('tap-text', { TEXT: 'Done' }); await_(1200);
      };
      await ctx.step('boot: cold start → the map drawn', async () => { await coldStart(ctx); return measure('boot', 8000); });
      await ctx.step('8 pans', async () => {
        for (const f of ['pan-left', 'pan-left', 'pan-down', 'pan-down', 'pan-right', 'pan-right', 'pan-up', 'pan-up']) ctx.flow(f);
        return measure('pan x8');
      });
      await ctx.step('satellite on, 4 pans (then the Map base)', async () => {
        await toMap(ctx);
        layer('ground.satellite');
        for (const f of ['pan-left', 'pan-right', 'pan-left', 'pan-right']) ctx.flow(f);
        const r = await measure('satellite on', 5000);
        layer('ground.map');
        return r;
      });
      await ctx.step("a water's page (Cultus Lake)", async () => {
        await toMap(ctx);
        ctx.flow('search', { Q: 'Cultus' });
        ctx.flow('open-result', { LINE: '.*Lindell Beach.*' });
        await ctx.waitText(/Regulations/, 30_000);
        return measure('water page');
      });
      const LINK = process.env.BOOK_LINK ?? 'Rule book p\\.[2-9][0-9] ↗.*';
      const book = async (label) => {
        ctx.flow('scroll-to', { TEXT: LINK });
        const t0 = Date.now();
        ctx.flow('tap-text', { TEXT: LINK });
        let page = null, failed = null;
        while (Date.now() - t0 < 60_000) {
          const w = device.screenText();
          failed = w.find((x) => /Couldn.t (show|load) the (rule book|synopsis)/.test(x)) ?? null;
          page = w.find((x) => /^Page \d+ of \d+$/.test(x)) ?? null;
          if (failed || page) break;
          await sleep(500);
        }
        const ms = Date.now() - t0;
        if (failed) throw new Error(`the viewer said: ${failed}`);
        if (!page) throw new Error(`the viewer never said its page in ${ms} ms`);
        const open = await measure(`${label} open`, 2000);
        ctx.flow('tap-id', { ID: 'pdf-viewer-close' });
        await sleep(1500);
        if (device.screenText().some((x) => /^Page \d+ of \d+$/.test(x))) throw new Error('the viewer did not close');
        return `${page} in ${ms} ms · open: ${open} · closed: ${await measure(`${label} closed`, 5000)}`;
      };
      await ctx.step('the rule book: open in the app, closed', async () => book('rule book'));
      await ctx.step('the rule book again: open, closed (a second close that keeps more is a leak)', async () => book('rule book (2nd)'));
      await ctx.step('Conditions', async () => {
        await toMap(ctx); ctx.flow('tab', { TAB: 'Conditions' });
        await sleep(2000);
        if (device.screenText().includes('Skip')) ctx.flow('tap-text', { TEXT: 'Skip' });   // its first-use tour
        return measure('conditions', 6000);
      });
      await ctx.step('the Map tab after Conditions', async () => { ctx.flow('tab', { TAB: 'Map' }); return measure('map after conditions', 6000); });
      await ctx.step('Salmon (the map coloured by salmon)', async () => {
        await toMap(ctx);
        ctx.flow('tap-text', { TEXT: 'Salmon' });
        const r = await measure('salmon', 6000);
        let back = 'Trout & more put back';
        try { ctx.flow('tap-text', { TEXT: 'Trout & more' }); } catch { back = 'left on Salmon (Trout & more not found)'; }
        return `${r} · ${back}`;
      });
      await ctx.step('iOS memory warning (the Simulator\'s "Simulate Memory Warning")', async () => {
        device.memoryWarning();
        return measure('after memory warning', 5000);
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
      // a tap on the map, timed by the app's `[open]` probe (T286): Hope Slough's label at the start view a cold start
      // opens on (iPhone 17). A search's framing is not kept once the search is closed with its ‹ on iOS, so the
      // app's own `[target]` (Android's way) cannot be used here.
      let at = null;
      await ctx.step('the start view (cold start, the Map base)', async () => {
        await coldStart(ctx);
        if (device.screenText().some((x) => /Powered by Esri/.test(x))) {
          ctx.flow('tap-text', { TEXT: 'Choose layers.*' }); await sleep(1000);
          ctx.flow('tap-id', { ID: 'option-ground\\.map' }); ctx.flow('tap-text', { TEXT: 'Done' });
        }
        await coldStart(ctx); await sleep(4000);
        shot(ctx, 'perf-start-view');
        at = { x: 53, y: 42 };
        return 'Hope Slough at 53%, 42%';
      });
      for (let i = 1; at && i <= TAPS; i++) {
        await ctx.step(`map tap ${i}/${TAPS} → its regulations ([open])`, async () => {
          const mark = logMark();
          ctx.flow('tap-point', { POINT: `${at.x}%,${at.y}%` });
          let o = null;
          for (let n = 0; n < 30 && !o; n++) { await sleep(700); o = logSince(mark, '[open] ').at(-1) ?? null; }
          if (!o) throw new Error('no [open] line in 20 s');
          const named = device.screenText().find((x) => /Slough|River|Creek|Lake/.test(x));
          out.open.push({ ...o, named }); keep();
          ctx.flow('tap-point', { POINT: PAGE_BACK }); await sleep(2000);
          const frame = o.marks?.frame?.[0] ?? o.marks?.timeout?.[0];
          if (o.marks?.spinner) throw new Error(`a spinner at ${o.marks.spinner[0]} ms · first frame ${frame} ms`);
          return `${named ?? '?'} · first frame ${frame} ms · sql ${o.sql} (wait ${o.sqlWait}, run ${o.sqlRun}) · ${Object.entries(o.marks).map(([k, v]) => `${k} ${v[0]}`).join(', ')}`;
        });
      }
      await ctx.step(`pan: ${SWIPES} travelling swipes ([pan] JS lag, [stall] > 50 ms)`, async () => {
        await toMap(ctx);
        const mark = logMark(), t0 = Date.now();
        const moves = ['pan-left', 'pan-left', 'pan-down', 'pan-down', 'pan-right', 'pan-right', 'pan-up', 'pan-up'];
        for (let i = 0; i < SWIPES; i++) { ctx.flow(moves[i % moves.length]); rss(); }
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
    /** a crash the scenario caused on purpose: its report is not a finding */
    expectCrash() { for (const f of crashList()) crashSeen.add(f); },
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
function await_(ms) { execSleep(ms / 1000); }
/** The raw app-os.log lines holding `tag` since `mark` (-1: the whole log; only its last 8 MB are read). */
function logLines(mark, tag) {
  try {
    const f = path.join(dir, 'app-os.log'), size = fs.statSync(f).size;
    const from = mark >= 0 ? mark : Math.max(0, size - (8 << 20));
    const fd = fs.openSync(f, 'r'), buf = Buffer.alloc(size - from);
    fs.readSync(fd, buf, 0, buf.length, from); fs.closeSync(fd);
    return buf.toString('utf8').split('\n').filter((l) => l.includes(tag));
  } catch { return []; }
}

for (const sc of SCENARIOS.filter((s) => want.includes(s.id))) {
  console.log(`[testbed-ios] ${sc.id} — ${sc.title}`);
  const ctx = makeCtx(sc);
  const t0 = Date.now();
  // (T313) every scenario after the first starts from the map: one left on a page must not cascade into the next
  if (sc.id !== 'first-open' && device.pidOf()) { try { await toMap(ctx); } catch (e) { console.log(`  (reset) ${e.message}`); try { await coldStart(ctx); } catch { /* the scenario says */ } } }
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
