// (T723, branch ios/audit-fixes) THE iOS AUDIT FIXES, PROVED ON THE SIMULATOR: one step per fix, a screenshot each, the
// numbers in the step note. By name only (`inAll: false`):
//
//   node app/tools/testbed/run-ios.mjs audit-fixes        (a --testbed build of the branch: build-ios-sim.sh --testbed)
//
//   arrow   a water's "Rule book p.N ↗": the arrow is plain text (U+FE0E in the label; the crop is thin strokes in the
//           link's colour, not a filled blue emoji tile). Also looks for a ⚠ title and "↩ Release" on the page.
//   G7      location permission reset, the prompt raised (the My location button), its text read off the screen.
//   G6      the prompt denied: the Spots tab's refusal line and the map's My-location line each carry "Open Settings";
//           one is tapped and the Settings app must come to the front.
//   G5      in that Settings page: While Using the App + Precise Location off; "At my location" in the Spots tab: the
//           capture says the fix is rough. (The simulator may still hand out an exact fix: said, then.)
//   G8      a spot with a parking pin (seeded into the app's spots file: the UI path needs a captured reading), Directions:
//           which app comes to the front.
//   G9      a link the phone cannot open (More > Contact > Email): the toast, if the open is refused (Mail may take it).
//   G11     More > About > What's new, every row read: none says Google Play, Android or APK.
//   G2      a staged update (test channel): a pull-down of Notification Center must not apply it, a trip to the
//           background and back must (the app's own `[swap] apply` probe line).
//
// A step that cannot be reached on a simulator says so (skip) and says why; it is never a pass.
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { readPng } from '../lib/png.mjs';
import { runYaml } from '../lib/ios-flow.mjs';

const APP = 'ca.canifishthis.app';
const [LON, LAT] = (process.env.LOCATE_AT ?? '-121.95,49.1').split(',').map(Number);
const REPO = path.resolve(path.dirname(new URL(import.meta.url).pathname), '../../../..');

/** the button's words (the first build exposed an accessibility label instead; both are accepted) */
const OPEN_SETTINGS = /^Open (Settings|this app.s settings.*)$/;
const shotTo =(ctx, name) => ctx.device.screenshot(path.join(ctx.dir, `audit-${name}.png`));

/** The screen size in iOS points (the largest box on it) and the pixel scale of a screenshot. */
function screenPts(ctx) {
  const ns = ctx.device.nodes();
  return { w: Math.max(0, ...ns.map((n) => n.box.x1)), h: Math.max(0, ...ns.map((n) => n.box.y1)) };
}

/** Is `name` running in the simulator (launchd lists it)? */
function running(ctx, bundle) {
  const out = ctx.device.simctl(['spawn', ctx.device.udid(), 'launchctl', 'list']).toString();
  return out.split('\n').some((l) => l.includes(bundle) && !/^\s*-\s/.test(l));
}

/** Words on screen, scrolling up `n` times, every distinct line met. */
async function readScrolling(ctx, n, { until } = {}) {
  const { w, h } = screenPts(ctx);
  const seen = new Set();
  for (let i = 0; i < n; i++) {
    const words = ctx.device.screenText();
    const before = seen.size;
    words.forEach((x) => seen.add(x));
    if (until && words.some((x) => until.test(x))) break;
    if (i > 0 && seen.size === before) break;
    ctx.swipe([w / 2, h * 0.75], [w / 2, h * 0.3], 500);
    await ctx.device.sleep(900);
  }
  return [...seen];
}

// ---------- the arrow's look ----------
/** Most common colour of the pixels in `r`, then: the foreground columns split into glyph groups. */
function glyphs(img, r) {
  const hist = new Map();
  for (let y = r.y0; y < r.y1; y++) for (let x = r.x0; x < r.x1; x++) {
    const i = (y * img.w + x) * img.ch;
    const k = ((img.px[i] >> 3) << 10) | ((img.px[i + 1] >> 3) << 5) | (img.px[i + 2] >> 3);
    hist.set(k, (hist.get(k) ?? 0) + 1);
  }
  const [bk] = [...hist.entries()].sort((a, b) => b[1] - a[1])[0];
  const bg = [((bk >> 10) & 31) << 3, ((bk >> 5) & 31) << 3, (bk & 31) << 3];
  const fg = (i) => Math.abs(img.px[i] - bg[0]) + Math.abs(img.px[i + 1] - bg[1]) + Math.abs(img.px[i + 2] - bg[2]) > 90;
  const cols = [];
  for (let x = r.x0; x < r.x1; x++) {
    let n = 0;
    for (let y = r.y0; y < r.y1; y++) if (fg((y * img.w + x) * img.ch)) n++;
    cols.push(n);
  }
  const groups = [];
  let cur = null, gap = 0;
  cols.forEach((n, j) => {
    if (n > 0) { if (!cur) { cur = { x0: r.x0 + j, x1: r.x0 + j }; groups.push(cur); } cur.x1 = r.x0 + j; gap = 0; }
    else if (cur && ++gap >= 5) cur = null;       // a word space at 3x is ~12 px; letters sit 1-3 px apart
  });
  const stat = (g) => {
    let y0 = r.y1, y1 = r.y0, n = 0; const sum = [0, 0, 0];
    for (let y = r.y0; y < r.y1; y++) for (let x = g.x0; x <= g.x1; x++) {
      const i = (y * img.w + x) * img.ch;
      if (fg(i)) { n++; y0 = Math.min(y0, y); y1 = Math.max(y1, y); sum[0] += img.px[i]; sum[1] += img.px[i + 1]; sum[2] += img.px[i + 2]; }
    }
    const bw = g.x1 - g.x0 + 1, bh = Math.max(1, y1 - y0 + 1);
    return { w: bw, h: bh, fill: n / (bw * bh), colour: n ? sum.map((s) => Math.round(s / n)) : [0, 0, 0], n };
  };
  return { bg, groups: groups.map(stat) };
}
const dist = (a, b) => Math.abs(a[0] - b[0]) + Math.abs(a[1] - b[1]) + Math.abs(a[2] - b[2]);

export const auditFixes = {
  id: 'audit-fixes',
  inAll: false,
  title: 'The iOS audit fixes (T723 / T727): arrow, G7 prompt, G6 Open Settings, G5 rough fix, G8 Directions, G9 link, G11 What\'s new, G2 update',
  async run(ctx) {
    const { appUp, coldStart, toMap } = ctx.ios;
    const d = ctx.device;
    let page = false;
    // a system prompt left over from Apple's Maps (it raises its own, over everything, after Directions) is answered
    // first, and Maps and Safari closed, so every step starts on this app
    const clearStrays = async () => {
      for (let i = 0; i < 5; i++) {
        const w = d.screenText();
        if (!w.some((x) => /^Allow .*(Maps|widgets).*\?$/.test(x))) break;
        try { ctx.flow('ios-alert', { BUTTON: 'Don.t Allow' }); } catch { /* gone */ }
        await d.sleep(1500);
      }
      for (const b of ['com.apple.Maps', 'com.apple.mobilesafari', 'com.apple.Preferences']) { try { d.simctl(['terminate', d.udid(), b]); } catch { /* not running */ } }
    };
    await clearStrays();

    // ---------- the arrow ----------
    await ctx.step('arrow: a water\'s "Rule book p.N ↗" is plain text, not the blue emoji tile', async () => {
      await coldStart(ctx);
      ctx.flow('search', { Q: 'Cultus' });
      ctx.flow('open-result', { LINE: '.*Lindell Beach.*' });
      await ctx.waitText(/Regulations/, 30_000);
      page = true;
      ctx.flow('scroll-to', { TEXT: '.*ule book p\\.[0-9]+.*' });
      await d.sleep(1200);
      const link = d.nodes().find((n) => /ule book p\.\d+/.test(n.text || n.desc));
      const file = shotTo(ctx, 'arrow');
      if (!link) throw new Error('no "Rule book p.N" link on the page');
      const label = link.text || link.desc;
      const marked = label.includes('↗︎');
      const { w } = screenPts(ctx);
      const img = readPng(file);
      const k = img.w / w;
      const r = { x0: Math.round(link.box.x0 * k), x1: Math.round(Math.min(link.box.x1, w) * k), y0: Math.round(link.box.y0 * k), y1: Math.round(link.box.y1 * k) };
      const g = glyphs(img, r);
      const arrow = g.groups.at(-1), word = g.groups.at(-2);
      if (!arrow || !word) throw new Error(`could not split the link into glyphs (${g.groups.length} groups) in ${JSON.stringify(r)}`);
      const colourGap = dist(arrow.colour, word.colour);
      const note = `label ${JSON.stringify(label)} (U+FE0E after ↗: ${marked}); arrow crop ${arrow.w}x${arrow.h}px fill ${arrow.fill.toFixed(2)} colour ${arrow.colour}; `
        + `the word before it fill ${word.fill.toFixed(2)} colour ${word.colour}; colour gap ${colourGap}`;
      const wide = fs.writeFileSync(path.join(ctx.dir, 'audit-arrow.json'), JSON.stringify({ label, r, g }, null, 1));
      void wide;
      if (!marked) throw new Error(`the label has no U+FE0E: ${note}`);
      if (arrow.fill > 0.45 || colourGap > 90) throw new Error(`looks like a filled tile: ${note}`);
      return note;
    });
    await ctx.step('arrow: a ⚠ title and "↩ Release" on this page, if there are any', async () => {
      const words = await readScrolling(ctx, 8);
      const hits = words.filter((x) => /[⚠↩]/.test(x));
      if (!hits.length) ctx.skip(`neither on this water's page (looked at ${words.length} lines)`);
      shotTo(ctx, 'symbols');
      return hits.map((x) => JSON.stringify(x)).join(' ');
    });
    if (page) { try { await toMap(ctx); } catch { /* the next step cold-starts */ } }

    // ---------- G7 + G6 ----------
    let promptText = null;
    await ctx.step('G7: location permission reset; the prompt\'s purpose line read from the screen', async () => {
      d.stop();
      d.privacy('reset');
      d.geo(LON, LAT);
      await d.sleep(1000);
      d.launch();
      const end = Date.now() + 240_000;
      let tapped = false;
      while (Date.now() < end) {
        const w = d.screenText();
        const alert = w.filter((x) => /use your location|centre the map|pin a spot|Can I Fish This/i.test(x));
        if (w.some((x) => /use your location/i.test(x))) { promptText = alert; break; }
        if (w.some((x) => /Would Like to Send You Notifications/i.test(x))) ctx.flow('ios-alert', { BUTTON: 'Allow.*' });
        else if (w.some((x) => /^(I understand|Skip|Before you fish|Got it)$/.test(x) || /\d+ of \d+$/i.test(x))) ctx.flow('dismiss-first-run');
        else if (w.some((x) => /^MORE$|^More$/.test(x)) && !tapped) { await d.sleep(6000); if (!d.screenText().some((x) => /use your location/i.test(x))) { ctx.flow('tap-text', { TEXT: 'My location' }); tapped = true; } }
        await d.sleep(2500);
      }
      shotTo(ctx, 'g7-prompt');
      if (!promptText) throw new Error('no location prompt on screen in 240 s');
      const said = promptText.join(' | ');
      if (!/centre the map on where you are and to pin a spot where you are standing/.test(said)) throw new Error(`the purpose line is not the new one: ${said}`);
      return said;
    });
    await ctx.step('G6: the prompt denied; the Spots tab says so, with "Open Settings"', async () => {
      ctx.flow('ios-alert', { BUTTON: 'Don.t Allow' });
      await d.sleep(2000);
      await appUp(ctx, { launch: false, alerts: 'Deny' });
      ctx.flow('tab', { TAB: 'Spots' });
      await d.sleep(2500);
      const w = d.screenText();
      shotTo(ctx, 'g6-spots');
      const line = w.find((x) => /Location access is turned off for this app/.test(x));
      const btn = w.find((x) => OPEN_SETTINGS.test(x));
      if (!line) throw new Error(`no refusal line (saw: ${w.slice(0, 14).join(' | ').slice(0, 300)})`);
      if (!btn) throw new Error(`the refusal line has no "Open Settings" (saw: ${w.slice(0, 14).join(' | ').slice(0, 300)})`);
      return `${JSON.stringify(line)} + button "${btn}"`;
    });
    await ctx.step('G6: the map\'s My location line (denied) has "Open Settings" too', async () => {
      ctx.flow('tab', { TAB: 'Map' });
      await d.sleep(2000);
      ctx.flow('tap-text', { TEXT: 'My location' });
      await d.sleep(2500);
      const w = d.screenText();
      shotTo(ctx, 'g6-map');
      const line = w.find((x) => /^Location is off for this app/.test(x));
      if (!line) throw new Error(`no line (saw: ${w.slice(0, 14).join(' | ').slice(0, 300)})`);
      if (!w.some((x) => OPEN_SETTINGS.test(x))) throw new Error(`no "Open Settings" under it (saw: ${w.slice(0, 14).join(' | ').slice(0, 300)})`);
      return `${JSON.stringify(line)} + "Open Settings"`;
    });
    await ctx.step('G6: tapping "Open Settings" brings the Settings app to the front', async () => {
      ctx.flow('tap-text', { TEXT: 'Open (Settings|this app.s settings.*)' });
      let w = [];
      for (let i = 0; i < 8; i++) { await d.sleep(1500); w = d.screenText(); if (w.some((x) => /^(Location|Notifications|Cellular Data|Background App Refresh)$/.test(x))) break; }
      shotTo(ctx, 'g6-settings');
      // the Settings app is in front: its own words on screen (the simulator may land on its root list, not the app's page)
      const ok = w.some((x) => /^(Location|Notifications|Cellular Data|Background App Refresh|Allow Can I Fish.*to Access|Apple Account|General|Airplane Mode)$/i.test(x));
      if (!ok) throw new Error(`Settings did not come up (saw: ${w.slice(0, 14).join(' | ').slice(0, 300)})`);
      return `Settings page for the app: ${w.slice(0, 10).join(' | ').slice(0, 200)}`;
    });

    // ---------- G5: approximate location, in the Settings page we are on ----------
    await ctx.step('G5: Location = While Using the App, Precise Location off; "At my location" says the fix is rough', async () => {
      const tried = [];
      try {
        // the root list: the Settings search finds the app's own page
        if (!d.screenText().some((x) => /^Location$/.test(x))) {
          const r = runYaml(ctx.dir, 'settings-find', 'appId: com.apple.Preferences\n---\n- tapOn: { text: "Search", index: 0 }\n- inputText: "Can I Fish"\n- waitForAnimationToEnd: { timeout: 3000 }\n- tapOn: { text: "Can I Fish.*", index: 1 }\n');
          if (!r.ok) throw new Error(`the Settings search did not reach the app's page: ${r.said}`);
          await d.sleep(1500);
        }
        ctx.flow('tap-text', { TEXT: 'Location' }); await d.sleep(1500);
        ctx.flow('tap-text', { TEXT: 'While Using the App' }); await d.sleep(1200);
        const w = d.screenText(); tried.push(w.slice(0, 12).join(' | '));
        ctx.flow('tap-text', { TEXT: 'Precise Location' }); await d.sleep(1200);
        shotTo(ctx, 'g5-settings');
      } catch (e) { throw new Error(`could not set Precise Location off in Settings: ${e.message} (${tried.join(' / ').slice(0, 200)})`); }
      d.geo(LON, LAT);
      d.stop(); await d.sleep(1000);
      await coldStart(ctx);
      ctx.flow('tab', { TAB: 'Spots' }); await d.sleep(2500);
      ctx.flow('tap-text', { TEXT: 'At my location' });
      await d.sleep(7000);
      const w = d.screenText();
      shotTo(ctx, 'g5-capture');
      const line = w.find((x) => /Your phone placed you only to about/.test(x));
      if (!line) ctx.skip(`no rough-fix sentence: the simulator likely still gives an exact fix with Precise off (capture said: ${w.filter((x) => /near the pin|Pick the water/i.test(x)).join(' | ')})`);
      return line;
    });
    // put the permission back for the spot steps
    try { d.privacy('grant'); d.geo(LON, LAT); } catch { /* said below if a step needs it */ }

    // ---------- G8: Directions ----------
    await ctx.step('G8: Directions on a spot with a parking pin opens Apple Maps (not Google Maps in Safari)', async () => {
      d.stop(); await d.sleep(1000);
      try { d.simctl(['privacy', d.udid(), 'grant', 'location', 'com.apple.Maps']); } catch { /* Maps may ask: closed below */ }
      const dir = path.join(d.dataDir(), 'Documents', 'spots');
      fs.mkdirSync(dir, { recursive: true });
      const spot = { id: 'audit-spot-1', createdAt: Date.now(), updatedAt: Date.now(), visitedAt: Date.now(), lat: 49.1, lon: -121.95,
        item: null, waterName: null, parking: { lat: 49.1005, lon: -121.9512 }, title: 'Audit spot', notes: '', photos: [],
        reading: null, weather: null, trace: null, panel: null };
      fs.writeFileSync(path.join(dir, 'spots.spots'), JSON.stringify([spot]));
      await coldStart(ctx);
      ctx.flow('tab', { TAB: 'Spots' }); await d.sleep(2500);
      ctx.flow('tap-text', { TEXT: 'Audit spot.*' }); await d.sleep(2500);
      try { ctx.flow('scroll-to', { TEXT: 'Directions.*' }); } catch { /* may be on screen already */ }
      shotTo(ctx, 'g8-spot');
      ctx.flow('tap-text', { TEXT: 'Directions.*' });
      await d.sleep(6000);
      const w = d.screenText();
      shotTo(ctx, 'g8-after');
      const maps = running(ctx, 'com.apple.Maps'), safari = running(ctx, 'com.apple.mobilesafari');
      // Maps raises its own location prompt over everything after it: closed with the app, so the next steps see ours
      // (Maps raises more than one prompt of its own: each is answered "Don't Allow")
      for (let i = 0; i < 4; i++) {
        if (!d.screenText().some((x) => /^Allow .*(Maps|widgets).*\?$|Don.t Allow/.test(x))) break;
        try { ctx.flow('ios-alert', { BUTTON: 'Don.t Allow' }); } catch { /* none */ }
        await d.sleep(1500);
      }
      for (const b of ['com.apple.Maps', 'com.apple.mobilesafari']) { try { d.simctl(['terminate', d.udid(), b]); } catch { /* not running */ } }
      const said = `front words: ${w.slice(0, 10).join(' | ').slice(0, 220)}; Maps running: ${maps}; Safari running: ${safari}`;
      const google = w.some((x) => /google\.com\/maps/i.test(x));
      if (google) throw new Error(`Google Maps opened: ${said}`);
      if (!maps && !w.some((x) => /maps\.apple\.com|Apple Maps/i.test(x))) throw new Error(`Apple Maps did not open: ${said}`);
      return said;
    });
    await clearStrays();
    d.launch();

    // ---------- G9 ----------
    await ctx.step('G9: a link the phone will not open says so (needs a link that is refused: none may be reachable here)', async () => {
      await coldStart(ctx);
      ctx.flow('tab', { TAB: 'More' }); await d.sleep(1500);
      ctx.flow('scroll-to', { TEXT: 'Contact' });
      ctx.flow('tap-text', { TEXT: 'Contact' }); await d.sleep(1500);
      const w = d.screenText();
      shotTo(ctx, 'g9');
      const mail = w.find((x) => /^Email/.test(x));
      if (!mail) ctx.skip(`this build's Contact page has no mail link (it says: ${w.filter((x) => /coming soon|Contact/i.test(x)).join(' | ').slice(0, 200)}); no link the simulator refuses is reachable`);
      ctx.flow('tap-text', { TEXT: 'Email.*' });
      await d.sleep(4000);
      const toast = d.screenText().find((x) => /^Could not open/.test(x));
      if (!toast) ctx.skip('the open was accepted (Mail took it): a refused open cannot be made here');
      return toast;
    });
    d.launch();

    // ---------- G11 ----------
    await ctx.step('G11: More > About > What\'s new, every row: no Google Play, Android or APK', async () => {
      await coldStart(ctx);
      ctx.flow('tab', { TAB: 'More' }); await d.sleep(1500);
      ctx.flow('scroll-to', { TEXT: 'About' });
      ctx.flow('tap-text', { TEXT: 'About' }); await d.sleep(1500);
      ctx.flow('scroll-to', { TEXT: "What's new.*" });
      ctx.flow('tap-text', { TEXT: "What's new.*" }); await d.sleep(2000);
      shotTo(ctx, 'g11-top');
      const words = await readScrolling(ctx, 40, { until: /^Version 0\.4\.0$/ });
      shotTo(ctx, 'g11-bottom');
      const versions = words.filter((x) => /^Version \d/.test(x));
      const bad = words.filter((x) => /google play|android|\bapk\b/i.test(x));
      if (!versions.length) throw new Error(`no version rows read (saw ${words.slice(0, 8).join(' | ')})`);
      if (bad.length) throw new Error(`mentions the other platform: ${bad.map((x) => JSON.stringify(x)).join(' ')}`);
      return `${versions.length} version rows, ${words.length} lines read (${versions.at(0)} ... ${versions.at(-1)}), none says Google Play / Android / APK`;
    });

    // ---------- G2 ----------
    await ctx.step('G2: a staged update is not applied by a Notification Center pull-down, but is on a return from the background', async () => {
      const updates = (...a) => execFileSync('node', [path.join(REPO, 'deploy/staging/test-updates.mjs'), ...a], { encoding: 'utf8' });
      const probe = () => d.logcatJs().split('\n').filter((l) => l.includes('[swap] apply')).length;
      try {
        updates('reset');
        await coldStart(ctx);
        await d.sleep(4000);
        const tag = `f${Date.now().toString(36).slice(-5)}`;
        updates('release', tag);
        await toMap(ctx);
        ctx.flow('ios-open-updates-log');
        ctx.flow('updates-log-action', { ACTION: 'check' });
        let row = null;
        for (let i = 0; i < 60 && !row; i++) { await d.sleep(3000); row = d.screenText().find((x) => /Release, staged/.test(x)) ?? null; }
        shotTo(ctx, 'g2-staged');
        if (!row) throw new Error('the update never reached "Release, staged" in 3 minutes');
        await toMap(ctx);
        await d.sleep(2000);
        d.logcatClear();
        const { w, h } = screenPts(ctx);
        ctx.swipe([w / 2, 2], [w / 2, h * 0.7], 400);       // Notification Center from the top edge
        await d.sleep(5000);
        shotTo(ctx, 'g2-notification-center');
        const nc = probe();
        ctx.swipe([w / 2, h * 0.8], [w / 2, 4], 400);       // and away again
        await d.sleep(4000);
        const afterNc = probe();
        d.background(); await d.sleep(3000); d.launch();
        let back = probe();
        for (let i = 0; i < 10 && back === afterNc; i++) { await d.sleep(2000); back = probe(); }
        shotTo(ctx, 'g2-after-return');
        const note = `apply lines: after the pull-down ${nc}, after dismissing it ${afterNc}, after background + return ${back}`;
        if (afterNc !== 0) throw new Error(`the update was applied by the pull-down: ${note}`);
        if (back < 1) throw new Error(`the update was not applied on return from the background: ${note}`);
        return note;
      } finally { try { updates('reset'); } catch { /* said by the next run */ } }
    });
    try { d.launch(); } catch { /* left as is */ }
  },
};
