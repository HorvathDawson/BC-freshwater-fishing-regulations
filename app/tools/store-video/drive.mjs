// STORE VIDEO, THE DEVICE HALF (T709): run INSIDE a test-bed session (app/tools/testbed/session.sh, under scripts/heavy.sh),
// which `run.mjs` starts. Pins the phone (clock, status bar, touch dots, GPS), checks the app holds the release, then drives
// each scene of checks.mjs SCENES with real taps at real speed while `adb screenrecord` records it, one clip per scene.
// Before a scene: its starting screen is checked (nothing recorded blank or stale); after it: its end state is checked
// (the water, the rule line, the colour) or the run fails. Writes <work>/raw/<scene>.mp4 and <work>/capture.json.
//
//   node app/tools/store-video/drive.mjs --work DIR --at ISO --release r-…   (TESTBED_SERIAL set by session.sh)
import { spawn, spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import * as d from '../testbed/lib/device.mjs';
import { flow as tbFlow, maestroCmd } from '../testbed/lib/maestro.mjs';
import { makeWaits } from '../testbed/lib/waits.mjs';
import { nodes, look, shown } from '../testbed/lib/screen.mjs';
import { readPng, count } from '../testbed/lib/png.mjs';
import { coldStart, closeWaters, toMap } from '../testbed/scenarios/common.mjs';
import { settledTarget } from '../testbed/scenarios/regs-open.mjs';
import { SCENES, mapProblems, regsProblems, conditionsProblems, spotNotesProblems, badWords } from './checks.mjs';

const HERE = path.dirname(new URL(import.meta.url).pathname);
const args = process.argv.slice(2), val = (k) => (args.includes(k) ? args[args.indexOf(k) + 1] : null);
const WORK = val('--work'), AT = val('--at'), RELEASE = val('--release');
if (!WORK || !AT || !RELEASE) throw new Error('drive.mjs: --work DIR --at ISO --release r-… are required (run.mjs passes them)');
const RAW = path.join(WORK, 'raw'), SHOTS = path.join(WORK, 'device');
fs.mkdirSync(RAW, { recursive: true }); fs.mkdirSync(SHOTS, { recursive: true });
const ADB = path.join(process.env.ANDROID_HOME ?? path.join(os.homedir(), 'Library/Android/sdk'), 'platform-tools/adb');

// THE PINNED STORY: one water (the Vedder: a coloured river with salmon, a gauge and a June closure), its search row, the
// spot saved on it, the day the picker goes to. The GPS sits on the Vedder at Vedder Crossing (the spot's water is picked there).
const WATER = { name: 'Vedder River', q: 'Vedder River', row: 'Stream · Region 2 · MU 2-4 · near Vedder Crossing.*' };
const SPOT = { title: 'Vedder Crossing pool', note: 'Park by the bridge. Tailout on river left, water clear.', lon: -121.9666, lat: 49.0975 };
const PICK = { month: 6, day: 10 };                       // June 10: the Vedder is closed to trout then (its own rows)

const log = (s) => console.log(`[store-video] ${s}`);
const findings = [];
const ctx = {
  device: d, ...makeWaits(d),
  finding: (t) => { findings.push(t); log(`finding: ${t}`); },
  // as the test bed's runner: one more try after a beat (Maestro's own driver sometimes does not start in time, 2026-11-13);
  // the second failure stops the run with Maestro's words
  flow(name, env) {
    let r = tbFlow(name, env, { logDir: path.join(WORK, 'maestro') });
    if (!r.ok) { log(`flow ${name} failed once (${r.said.split('\n').pop() || 'see the maestro log'}): trying again`); spawnSync('sleep', ['3']); r = tbFlow(name, env, { logDir: path.join(WORK, 'maestro') }); }
    if (!r.ok) throw new Error(`flow ${name} failed: ${r.said.split('\n').slice(-3).join(' / ') || /Exception[^\n]*/.exec(r.log)?.[0] || 'see the maestro log'}`);
    return r;
  },
};
/** One of this tool's own flows (store-video/flows), run like the test bed's. */
function ownFlow(name, env) {
  const file = path.join(HERE, 'flows', `${name}.yaml`);
  const argv = ['--host', '127.0.0.1', '--port', String(Number(/^emulator-(\d+)$/.exec(d.serial())[1]) + 1), 'test', file, ...Object.entries(env).flatMap(([k, v]) => ['-e', `${k}=${v}`])];
  const [cmd, a] = maestroCmd(process.env.MAESTRO_BIN ?? path.join(os.homedir(), '.maestro/maestro/bin/maestro'), argv);
  const r = spawnSync(cmd, a, { encoding: 'utf8', timeout: 180_000, env: { ...process.env, MAESTRO_CLI_NO_ANALYTICS: '1', JAVA_HOME: process.env.JAVA_HOME ?? '/Applications/Android Studio.app/Contents/jbr/Contents/Home' } });
  if (r.status !== 0) throw new Error(`flow ${name} failed: ${`${r.stdout}${r.stderr}`.split('\n').filter((l) => /FAILED|not found/.test(l)).slice(-3).join(' / ')}`);
}
const fail = (scene, problems) => { if (problems.length) throw new Error(`${scene}: ${problems.join('; ')}`); };
const node = (ns, pred, what) => { const n = ns.find((x) => shown(x) && pred(x)); if (!n) throw new Error(`not on screen: ${what}`); return n; };
const mid = (n) => [Math.round((n.box.x0 + n.box.x1) / 2), Math.round((n.box.y0 + n.box.y1) / 2)];
const tapAt = ([x, y]) => d.sh(`input tap ${x} ${y}`);
const H = () => Number(/(\d+)x(\d+)/.exec(d.sh('wm size'))[2]);

// ---------- the phone, pinned ----------
const at = new Date(AT);
const bcHM = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Vancouver', hour: '2-digit', minute: '2-digit', hourCycle: 'h23' }).format(at).replace(':', '');
function pinPhone() {
  d.adb(['root']); d.adb(['wait-for-device']);
  d.sh('settings put global auto_time 0');
  const u = at.toISOString();   // toybox date, UTC: MMDDhhmmYYYY.ss
  d.sh(`date -u ${u.slice(5, 7)}${u.slice(8, 10)}${u.slice(11, 13)}${u.slice(14, 16)}${u.slice(0, 4)}.${u.slice(17, 19)}`);
  const now = Number(d.sh('date +%s').trim());
  if (Math.abs(now - at.getTime() / 1000) > 120) throw new Error(`the phone's clock did not take --at ${AT} (it says ${new Date(now * 1000).toISOString()})`);
  const demo = (cmd, extra = '') => d.sh(`am broadcast -a com.android.systemui.demo -e command ${cmd} ${extra} >/dev/null`);
  d.sh('settings put global sysui_demo_allowed 1');
  demo('enter'); demo('clock', `-e hhmm ${bcHM}`); demo('battery', '-e level 100 -e plugged false');
  demo('network', '-e wifi show -e level 4 -e fully true'); demo('network', '-e mobile hide'); demo('notifications', '-e visible false');
  d.sh('settings put system show_touches 1');
  for (const p of ['ACCESS_FINE_LOCATION', 'ACCESS_COARSE_LOCATION']) d.sh(`pm grant ${d.APP_ID} android.permission.${p}`);
  d.geo(SPOT.lon, SPOT.lat);
}
function unpinPhone() {
  try { d.sh('am broadcast -a com.android.systemui.demo -e command exit >/dev/null'); d.sh('settings put system show_touches 0'); d.sh('settings put global auto_time 1'); }
  catch (e) { log(`could not put the phone back (demo mode / touches / auto time): ${e.message}`); }
}

// ---------- recording ----------
/** Record one scene: screenrecord runs while `fn(mark)` drives it; `mark(name)` stamps host seconds since the recorder started. */
async function record(id, fn) {
  const remote = `/sdcard/store-video-${id}.mp4`;
  d.sh(`rm -f ${remote}`);
  const p = spawn(ADB, ['-s', d.serial(), 'shell', 'screenrecord', '--bit-rate', '20000000', '--time-limit', '60', remote], { stdio: 'ignore' });
  const t0 = Date.now();
  const marks = {};
  const mark = (k) => { marks[k] = (Date.now() - t0) / 1000; return marks[k]; };
  await ctx.timer(1600, 'the recorder starts writing; the screen stays still so the first touch is found on the clip');
  let err = null;
  try { await fn(mark); } catch (e) { err = e; }
  mark('stop');
  for (const l of d.sh('ps -A -o PID,ARGS').split('\n').filter((x) => x.includes(`screenrecord`) && x.includes(remote))) d.sh(`kill -2 ${l.trim().split(/\s+/)[0]}`);
  await new Promise((r) => { const t = setTimeout(() => { try { p.kill(); } catch { /* gone */ } r(); }, 8000); p.on('exit', () => { clearTimeout(t); r(); }); });
  if (err) throw err;
  const local = path.join(RAW, `${id}.mp4`);
  d.adb(['pull', remote, local], { timeout: 60_000 });
  if (!fs.existsSync(local) || fs.statSync(local).size < 10_000) throw new Error(`${id}: the recording ${remote} did not come back`);
  return { clip: path.relative(WORK, local), marks };
}
/** Poll the screen until `ok(nodes)` holds; returns the nodes. */
async function waitScreen(ok, what, ms = 20_000) {
  return ctx.until(async () => { let ns; try { ns = nodes(d); } catch { return null; } return ok(ns) ? ns : null; }, { ms, every: 250, what });
}
const shot = (name) => d.screenshot(path.join(SHOTS, `${name}.png`));
/** Dark red of a closed line ("Closed", the map's closed colour) within `r` px of a point, on a screenshot. */
const closedRed = (file, [x, y], r = 60) => count(readPng(file), { x: x - r, y: y - r, w: 2 * r, h: 2 * r }, (R, G, B) => R > 110 && G < 60 && B < 70);

// ---------- the run ----------
const capture = { at: AT, release: RELEASE, water: WATER.name, spot: SPOT.title, pick: PICK, scenes: {}, findings };
const save = () => fs.writeFileSync(path.join(WORK, 'capture.json'), JSON.stringify(capture, null, 2) + '\n');
try {
  log(`phone pinned to ${AT} (status bar ${bcHM}), touches shown, GPS on the Vedder`);
  pinPhone();
  await coldStart(ctx, { ms: 120_000 });
  await closeWaters(ctx); await toMap(ctx);

  // the spot the last scene opens: made once through the UI (At my location → the Vedder → today → named → a note)
  ctx.flow('tab', { TAB: 'Spots' });
  let ns = await look(d);
  if (!ns.some((n) => n.text === SPOT.title)) {
    log('saving the spot through the UI (first run on this phone)');
    ctx.flow('tap-text', { TEXT: 'At my location' });
    ns = await waitScreen((x) => x.some((n) => n.text === 'Pick the water'), 'the spot\'s "Pick the water" map');
    await ctx.settle({ ms: 6000, frame: true });
    const m = node(ns, (n) => /^Showing a Map/.test(n.desc), 'the pick map');
    tapAt(mid(m));                                          // the GPS pin sits on the Vedder, the map's middle
    await ctx.timer(1500, 'the picked water shows');
    ctx.flow('tap-text', { TEXT: 'Next' }); ctx.flow('tap-text', { TEXT: 'Next' });
    ns = await waitScreen((x) => x.some((n) => n.text === `on ${WATER.name}`), `the spot draft on ${WATER.name}`);
    ctx.flow('spot-name-save', { TITLE: SPOT.title });
    await ctx.until(() => ctx.hasId('spot-visits'), { ms: 20_000, what: 'the saved spot\'s page (spot-visits)' });
    ctx.flow('tap-text', { TEXT: 'Edit' });
    ownFlow('spot-note', { NOTES: SPOT.note });
    await ctx.until(() => d.screenText().some((w) => w.startsWith(SPOT.note.slice(0, 20))), { ms: 15_000, what: 'the note saved on the spot' });
    d.sh('input keyevent 4');
  }
  await toMap(ctx);

  // trout mode and today, the Vedder framed: its search row opened, then Back (the map stays framed on it, T340)
  ns = await look(d);
  if (/Keep salmon/.test(ns.map((n) => n.text).join('\n'))) { tapAt(mid(node(ns, (n) => /^Colour by trout/.test(n.desc), 'the TROUT mode button'))); await ctx.timer(1500, 'the mode switch'); }
  d.logcatClear();
  ctx.flow('search', { Q: WATER.q });
  ctx.flow('open-result', { LINE: WATER.row });
  await ctx.until(() => ctx.hasId('water-view-regs'), { ms: 20_000, what: `${WATER.name}'s page from search` });
  d.sh('input keyevent 4');
  const target = await settledTarget(ctx, 30_000, 3000);
  if (!target) throw new Error(`the app named no tap target for ${WATER.name} (no [target] line)`);
  capture.target = target;
  await ctx.settle({ ms: 8000, frame: true });

  // 1. MAP → TAP → ITS RULES
  ns = await look(d);
  fail('map-tap (before)', [...mapProblems(ns), ...badWords(ns)]);
  shot('01-before');
  capture.scenes['map-tap'] = await record('map-tap', async (mark) => {
    mark('tap'); tapAt([target.x, target.y]); mark('last');
    const after = await waitScreen((x) => regsProblems(x, WATER.name).length === 0, `${WATER.name}'s rules after the tap`);
    mark('checked');
    fail('map-tap (after)', badWords(after));
    await ctx.timer((SCENES[0].hold + 1.2) * 1000, 'the rules stay on screen for the scene\'s hold');
    shot('01-after');
  });
  save();

  // 2. THE DAY PICKER: a day in June, the river turns closed. The sheet's layout changes month by month (rows, the year
  //    note), so it is read OFF the record first, month by month, and then played back at a person's pace.
  d.sh('input keyevent 4');
  ns = await waitScreen((x) => mapProblems(x).length === 0, 'the map back, framed on the Vedder, coloured for today');
  const pickBtn = mid(node(ns, (n) => n.id === 'map-day-pick', 'the map\'s day picker'));
  const route = [];
  tapAt(pickBtn); ns = await waitScreen((x) => x.some((n) => n.id === 'day-grid'), 'the day sheet');
  const monthOf = (x) => /^(\w+) (\d{4})\./.exec(x.find((n) => n.id === 'day-grid')?.desc ?? '')?.[1];
  const MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
  for (let i = 0; i < 12 && MONTHS.indexOf(monthOf(ns)) + 1 !== PICK.month; i++) {
    const back = MONTHS.indexOf(monthOf(ns)) + 1 > PICK.month;
    const b = mid(node(ns, (n) => n.desc === (back ? 'Previous month' : 'Next month'), 'the month button'));
    route.push(b); tapAt(b);
    const was = monthOf(ns);
    ns = await waitScreen((x) => monthOf(x) && monthOf(x) !== was, 'the next month in the sheet');
  }
  const dayRe = new RegExp(`^\\w+, ${MONTHS[PICK.month - 1]} ${PICK.day}(,|$)`);
  const dayCell = mid(node(ns, (n) => dayRe.test(n.desc), `${MONTHS[PICK.month - 1]} ${PICK.day} in the sheet`));
  const done = mid(node(ns, (n) => n.text === 'Done', 'the sheet\'s Done'));
  // back to today, the sheet closed: the same start the record then makes
  tapAt(mid(node(ns, (n) => n.text === 'Today' && n.box.y0 > ns.find((x) => x.id === 'day-grid').box.y1, 'the sheet\'s Today')));
  await ctx.timer(500, 'the sheet takes today');
  tapAt(mid(node(await look(d), (n) => n.text === 'Done', 'Done')));
  ns = await waitScreen((x) => mapProblems(x).length === 0 && !x.some((n) => n.id === 'day-grid'), 'the map on today, the sheet closed');
  await ctx.settle({ ms: 6000, frame: true });
  shot('02-before');
  const dayLabel = `\\w+, ${MONTHS[PICK.month - 1]} ${PICK.day}`;
  capture.scenes.day = await record('day', async (mark) => {
    mark('tap'); tapAt(pickBtn);
    await ctx.timer(900, 'a person reads the sheet');
    for (const b of route) { tapAt(b); await ctx.timer(420, 'a person tapping through months'); }
    await ctx.timer(300, 'a person finds the day'); tapAt(dayCell);
    await ctx.timer(550, 'a person reaches for Done'); tapAt(done); mark('last');
    const after = await waitScreen((x) => mapProblems(x, { day: dayLabel }).length === 0 && !x.some((n) => n.id === 'day-grid'), `the map coloured for ${MONTHS[PICK.month - 1]} ${PICK.day}`);
    mark('checked');
    fail('day (after)', badWords(after));
    await ctx.timer((SCENES[1].hold + 1.2) * 1000, 'the recoloured map stays for the scene\'s hold');
    const f = shot('02-after');
    const red = closedRed(f, [target.x, target.y]);
    if (red < 150) throw new Error(`day (after): the Vedder at the tap point is not drawn closed on ${MONTHS[PICK.month - 1]} ${PICK.day} (${red} closed-red pixels near ${target.x},${target.y})`);
    capture.closedRedPixels = red;
  });
  save();

  // 3. SALMON: back to today (off the record), then the SALMON mode button
  ns = await look(d);
  tapAt(pickBtn); ns = await waitScreen((x) => x.some((n) => n.id === 'day-grid'), 'the day sheet');
  tapAt(mid(node(ns, (n) => n.text === 'Today' && n.box.y0 > ns.find((x) => x.id === 'day-grid').box.y1, 'the sheet\'s Today')));
  await ctx.timer(500, 'the sheet takes today');
  tapAt(mid(node(await look(d), (n) => n.text === 'Done', 'Done')));
  ns = await waitScreen((x) => mapProblems(x).length === 0 && !x.some((n) => n.id === 'day-grid'), 'the map on today again');
  await ctx.settle({ ms: 6000, frame: true });
  const salmonBtn = mid(node(ns, (n) => /^Colour by salmon/.test(n.desc), 'the SALMON mode button'));
  shot('03-before');
  capture.scenes.salmon = await record('salmon', async (mark) => {
    mark('tap'); tapAt(salmonBtn); mark('last');
    const after = await waitScreen((x) => mapProblems(x, { mode: 'salmon' }).length === 0, 'the map in salmon mode');
    mark('checked');
    fail('salmon (after)', badWords(after));
    await ctx.timer((SCENES[2].hold + 1.2) * 1000, 'the salmon map stays for the scene\'s hold');
    shot('03-after');
  });
  save();

  // 4. CONDITIONS: trout mode back, the Vedder's page opened by the same tap, then its Conditions tab and a scroll to the chart
  ns = await look(d);
  tapAt(mid(node(ns, (n) => /^Colour by trout/.test(n.desc), 'the TROUT mode button')));
  await waitScreen((x) => mapProblems(x).length === 0, 'trout mode back');
  tapAt([target.x, target.y]);
  ns = await waitScreen((x) => regsProblems(x, WATER.name).length === 0, `${WATER.name}'s rules`);
  await ctx.settle({ ms: 5000 });
  const condTab = mid(node(ns, (n) => n.id === 'water-view-conditions', 'the Conditions tab'));
  shot('04-before');
  const screenH = H();
  capture.scenes.conditions = await record('conditions', async (mark) => {
    mark('tap'); tapAt(condTab);
    await ctx.timer(1900, 'a person reads the day\'s estimate (the check after the scene proves the view opened)');
    // one slow drag in the page's right gutter (a drag on the route map pans the map): 1500 px brings the gauge card's chart
    // under the header (measured 2026-11-13; the check below fails the run if it does not)
    d.sh('input swipe 1062 2150 1062 650 900'); mark('last');
    const x = await waitScreen((y) => conditionsProblems(y, { water: WATER.name, release: RELEASE, screenH }).length === 0, 'the gauge chart whole on screen', 8000)
      .catch((e) => { throw new Error(`${e.message}: ${conditionsProblems(nodes(d), { water: WATER.name, release: RELEASE, screenH }).join('; ')}`); });
    mark('checked');
    fail('conditions (after)', badWords(x));
    await ctx.timer((SCENES[3].hold + 1.2) * 1000, 'the chart stays for the scene\'s hold');
    shot('04-after');
  });
  save();

  // 5. A SAVED SPOT: the water closed, the Spots tab, the spot, one fling down to its notes
  await closeWaters(ctx); await toMap(ctx);
  ns = await waitScreen((x) => mapProblems(x).length === 0, 'the map');
  await ctx.settle({ ms: 6000, frame: true });
  const spotsTab = mid(node(ns, (n) => n.desc === 'Spots' || n.text === 'SPOTS', 'the Spots tab'));
  // where the spot's row sits in the list, read off the record (then back to the map, the scene's start)
  tapAt(spotsTab);
  const spotRow = mid(node(await waitScreen((y) => y.some((n) => n.text === SPOT.title), 'the Spots list with the spot'), (n) => n.text === SPOT.title, 'the spot\'s row'));
  ctx.flow('tab', { TAB: 'Map' });
  ns = await waitScreen((x) => mapProblems(x).length === 0, 'the map');
  await ctx.settle({ ms: 6000, frame: true });
  shot('05-before');
  capture.scenes.spot = await record('spot', async (mark) => {
    mark('tap'); tapAt(spotsTab);
    await ctx.timer(1000, 'a person finds the spot in the list');
    tapAt(spotRow);
    await ctx.timer(1400, 'a person reads the spot\'s top (the check after the scene proves the page opened)');
    // one fling down the page's gutter: measured 2026-11-13 to land the notes block near the top (checked below)
    d.sh('input swipe 1062 2050 1062 650 150'); mark('last');
    const x = await waitScreen((y) => spotNotesProblems(y, { title: SPOT.title, note: SPOT.note, screenH }).length === 0, 'the spot\'s notes on screen', 8000)
      .catch((e) => { throw new Error(`${e.message}: ${spotNotesProblems(nodes(d), { title: SPOT.title, note: SPOT.note, screenH }).join('; ')}`); });
    mark('checked');
    fail('spot (after)', badWords(x));
    await ctx.timer((SCENES[4].hold + 1.2) * 1000, 'the notes stay for the scene\'s hold');
    shot('05-after');
  });
  save();
  capture.ok = true;
} catch (e) {
  capture.ok = false; capture.error = e.message;
  try { shot('FAILED'); } catch { /* the screenshot is a courtesy; the error above is the report */ }
  log(`FAILED: ${e.message}`);
} finally {
  unpinPhone();
  save();
}
process.exit(capture.ok ? 0 : 1);
