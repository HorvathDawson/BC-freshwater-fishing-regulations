#!/usr/bin/env node
// (T319) STORE SHOTS FROM THE iOS SIMULATOR: the scenes the web app cannot show. Spots are phone-only (the web's Spots tab is
// a "get the app" notice) and the Help page's "Offline and updates" lines are the app's (the web says it needs a signal).
// Runs inside one iOS test-bed session (it boots the simulator and installs app/dist/ios-sim/canifishthis.app):
//
//   TESTBED_SIM="iPhone 17 Pro Max" scripts/heavy.sh store-ios 30 -- bash app/tools/testbed/session-ios.sh -- \
//     node app/tools/store-shots/device-ios.mjs <raw dir>
//
// Writes <raw dir>/device-<scene>.png (the simulator's own pixels; 1320x2868 on the 17 Pro Max) and device-report.json.
// store-shots.mjs frames a shot marked `"device": true` from that file, cropped to each target's shape (store-shots.json).
// PINNED: light appearance, the status bar (9:41, full battery; cleared at the end), one sample spot (the same as the web
// set's seed in play-shots.mjs: the Vedder's "Bridge pool", 4 days ago) written to the app's own spots file. NOT pinned: the
// release and the feeds (live staging). Each scene is CHECKED on the screen's text before its capture: no check, no shot.
import fs from 'node:fs'; import path from 'node:path';
import * as device from '../testbed/lib/device-ios.mjs';
import { flow } from '../testbed/lib/maestro.mjs';

const OUT = path.resolve(process.argv[2] ?? '');
if (!process.argv[2]) throw new Error('device-ios: name the raw dir (store-shots.mjs reads <dir>/device-<scene>.png)');
fs.mkdirSync(OUT, { recursive: true });
const sleep = device.sleep;
const run = (name, env = {}) => { const r = flow(name, env); if (!r.ok) throw new Error(`flow ${name} ${JSON.stringify(env)}: ${r.said.split('\n').slice(-3).join(' / ')}`); };
const hasTabs = (w) => w.some((x) => /^MORE$|^More$/.test(x)) && w.some((x) => /^MAP$|^Map$/.test(x));
const installing = (w) => w.some((x) => / MB downloaded$|^Downloading the map and regulations/.test(x)) || (w.ids ?? []).includes('boot-close-ok');

/** the app in front with its tabs: the first-run notice, the tour and system prompts closed as a reader would (run-ios.mjs appUp, lean) */
async function appUp(ms = 600_000) {
  device.launch();
  const end = Date.now() + ms;
  let w = [];
  while (Date.now() < end) {
    w = device.screenText();
    if (w.some((x) => /^Allow .* to use|Would Like to Send You Notifications/i.test(x))) run('ios-alert', { BUTTON: "Don.t Allow" });
    else if (w.some((x) => /^(I understand|Skip|Before you fish|Welcome|Got it)$/.test(x) || /\d+ of \d+$/i.test(x))) run('dismiss-first-run');
    else if ((w.ids ?? []).includes('inseason-notice') && w.includes('Done')) run('tap-text', { TEXT: 'Done' });
    else if (hasTabs(w) && !installing(w)) { await sleep(4000); const again = device.screenText(); if (hasTabs(again) && !installing(again) && !again.some((x) => /^(I understand|Got it|Skip)$/.test(x))) return again; }
    else if (!device.pidOf()) device.launch();
    await sleep(3000);
  }
  throw new Error(`the app is not up in ${ms / 1000} s (saw: ${w.slice(0, 8).join(' | ').slice(0, 300)})`);
}
async function waitText(re, ms = 60_000) {
  const end = Date.now() + ms;
  while (Date.now() < end) { const w = device.screenText(); if (w.some((x) => re.test(x))) return w; await sleep(1500); }
  throw new Error(`not on screen in ${ms / 1000} s: ${re}`);
}

// the sample spot, as play-shots.mjs seeds the web set (its words match by hand), here visited at 8:30 four days ago (B.C. is on
// permanent UTC-7) and rated 4 of 5
const bcDay = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Vancouver', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date(Date.now() - 4 * 86400000));
const visit = Date.parse(`${bcDay}T08:30:00-07:00`), iso = (t) => new Date(t).toISOString();
const sample = (t, c) => ({ at: iso(t), tempC: c, cloudPct: 40, pressureHpa: 1012, humidityPct: 63 });
const SPOT = { id: 'play-spot', createdAt: visit, updatedAt: visit, visitedAt: visit, lat: 49.0985, lon: -121.9585, item: 'gnis:3062', waterName: 'Vedder River',
  title: 'Bridge pool', rating: 4, notes: 'Fished the tail-out below the bridge from 7 to 10. Three rainbow on a small pheasant tail, one bull trout on a streamer. Water a little tea-coloured, still dropping.',
  photos: [], reading: { station: 'Vedder River at Vedder Crossing', discharge: 41.2, level: null, parameter: null, percentile: 0.42, at: iso(visit) },
  weather: { tempC: 14, windKph: 9, windDir: 250, rain3h: 0, pressureHpa: 1012, code: 2, at: iso(visit), window: { before: sample(visit - 3600e3, 13), at: sample(visit, 14), after: sample(visit + 3600e3, 15) } }, trace: null, panel: null };

const report = {};
const shot = (scene) => { const f = path.join(OUT, `device-${scene}.png`); device.screenshot(f); report[scene] = 'ok'; console.log(f); };
device.simctl(['status_bar', device.udid(), 'override', '--time', '9:41', '--batteryState', 'charged', '--batteryLevel', '100', '--cellularMode', 'active', '--cellularBars', '4', '--wifiBars', '3']);
device.appearance('light');
try {
  if (!device.installed()) throw new Error('the app is not installed (session-ios.sh installs app/dist/ios-sim/canifishthis.app)');
  // a first open downloads the release; then the spot goes into the app's own file (Documents/spots/spots.spots) while it is stopped
  await appUp();
  device.stop(); await sleep(1500);
  const dir = path.join(String(device.dataDir()).trim(), 'Documents', 'spots');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'spots.spots'), JSON.stringify([SPOT]));

  // 07-saved-spot: the Spots tab, the spot opened
  await appUp();
  run('tab', { TAB: 'Spots' });
  await waitText(/Bridge pool/);
  run('tap-text', { TEXT: 'Bridge pool.*' });
  const w = await waitText(/tail-out below the bridge/);
  if (!w.some((x) => /Vedder/.test(x))) throw new Error('07-saved-spot: the spot does not name the Vedder River');
  await sleep(8000);   // the day's weather and gauge fill in
  shot('07-saved-spot');   // the top of the page: its name, the notes, the rating, the flow that day (picked over scrolled frames)

  // 20-help: More -> Help, the map key first and the offline lines
  device.stop(); await sleep(1500); await appUp();
  run('tab', { TAB: 'More' });
  run('scroll-to', { TEXT: 'Help' });
  run('tap-text', { TEXT: 'Help' });
  await waitText(/map colours/i).catch((e) => { shot('20-help-failed'); throw e; });
  // one drag up (a fifth of the screen): the map key's lower half and the "Offline and updates" lines in one frame
  await sleep(1500); run('pan-up'); await sleep(2000);
  if (!device.screenText().some((x) => /offline and updates/i.test(x))) throw new Error('20-help: "Offline and updates" is not on screen');
  shot('20-help');
} catch (e) { report.error = String(e.message ?? e); throw e; }
finally {
  try { device.simctl(['status_bar', device.udid(), 'clear']); } catch { /* the session shuts the simulator down anyway */ }
  fs.writeFileSync(path.join(OUT, 'device-report.json'), JSON.stringify({ sim: process.env.TESTBED_SIM ?? null, scenes: report }, null, 2) + '\n');
}
