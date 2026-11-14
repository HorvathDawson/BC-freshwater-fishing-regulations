// (T313) THE iOS SIMULATOR: simctl for one booted simulator (its UDID from session-ios.sh, `TESTBED_UDID`, or
// out/UDID). The app's lifecycle, screenshots, the screen's text (Maestro's hierarchy), location and privacy. What the
// simulator cannot do is said, not faked: it shares the Mac's network (no airplane mode, no link shaping from simctl),
// so `net('offline')` throws a skip and the scenarios use the app's own "Simulate offline" (Updates log) instead.
// Nothing here kills a process it did not start; the simulator's lifetime is session-ios.sh's.
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { maestroCmd } from './maestro.mjs';

export const APP_ID = 'ca.canifishthis.app';
const OUT = path.join(path.dirname(new URL(import.meta.url).pathname), '..', 'out');
// `xcrun simctl` is a wrapper that first runs `xcodebuild -runFirstLaunch` when CoreSimulator is older than Xcode wants
// (it then waits for an admin password and hangs); the framework's own simctl answers either way
const DIRECT = '/Library/Developer/PrivateFrameworks/CoreSimulator.framework/Versions/A/Resources/bin/simctl';
export const SIMCTL = process.env.SIMCTL ?? (fs.existsSync(DIRECT) ? DIRECT : 'simctl');

export function udid() {
  const s = process.env.TESTBED_UDID ?? (fs.existsSync(path.join(OUT, 'UDID')) ? fs.readFileSync(path.join(OUT, 'UDID'), 'utf8').trim() : null);
  if (!s) throw new Error('no simulator: run through session-ios.sh (TESTBED_UDID / out/UDID)');
  return s;
}
export function serial() { return udid(); }   // the Android device module's name for the same thing

const base = SIMCTL === 'simctl' ? ['xcrun', ['simctl']] : [SIMCTL, []];
export const simctl = (args, opts = {}) =>
  execFileSync(base[0], [...base[1], ...args], { maxBuffer: 64 << 20, timeout: opts.timeout ?? 120_000, ...opts });
export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------- network ----------
/** The simulator shares the Mac's network: only `wifi` is real. Offline is the app's own switch (scenarios say so). */
export async function net(state) {
  if (state === 'wifi') return 'WIFI';
  throw Object.assign(new Error(`the iOS simulator cannot go ${state} (it shares the Mac's network)`), { skip: true });
}
export function link() { /* no link shaping on the simulator */ }

// ---------- the app ----------
export function launch() { simctl(['launch', udid(), APP_ID]); }
export function stop() { try { simctl(['terminate', udid(), APP_ID]); } catch { /* not running */ } }
export function install(app) { simctl(['install', udid(), app], { timeout: 300_000 }); }
export function uninstall() { try { simctl(['uninstall', udid(), APP_ID]); } catch { /* not installed */ } }
export function installed() {
  try { simctl(['get_app_container', udid(), APP_ID], { stdio: ['ignore', 'pipe', 'ignore'] }); return true; } catch { return false; }
}
/** The app's data container (Documents, Library: the release lives under it). */
export const dataDir = () => simctl(['get_app_container', udid(), APP_ID, 'data']).toString().trim();
/** The app's pid in the simulator, or null (launchd's list names it UIKitApplication:<id>[…]). */
export function pidOf() {
  const l = simctl(['spawn', udid(), 'launchctl', 'list']).toString().split('\n').find((x) => x.includes(`UIKitApplication:${APP_ID}`));
  const pid = l?.trim().split(/\s+/)[0];
  return pid && pid !== '-' ? pid : null;
}
export const background = () => simctl(['launch', udid(), 'com.apple.springboard']);
export const openUrl = (url) => simctl(['openurl', udid(), url]);
/** Location permission: grant | revoke | reset (simctl privacy). */
export function privacy(action, service = 'location') { simctl(['privacy', udid(), action, service, APP_ID]); }
/** The simulator's GPS (lon, lat: the Android geo order; simctl wants lat,lon). */
export const geo = (lon, lat) => simctl(['location', udid(), 'set', `${lat},${lon}`]);

// ---------- the screen ----------
export function screenshot(file) {
  simctl(['io', udid(), 'screenshot', '--type=png', file]);
  return file;
}

/** Every text / accessibility label on screen (Maestro's view of it: XCUITest's accessibility tree). */
export function screenText() {
  try {
    const bin = process.env.MAESTRO_BIN ?? path.join(os.homedir(), '.maestro/maestro/bin/maestro');
    const [cmd, argv] = maestroCmd(bin, ['--device', udid(), 'hierarchy']);   // off the emulator ports (lib/maestro.mjs)
    const json = execFileSync(cmd, argv, {
      encoding: 'utf8', timeout: 90_000, maxBuffer: 64 << 20, stdio: ['ignore', 'pipe', 'ignore'],
      env: { ...process.env, MAESTRO_CLI_NO_ANALYTICS: '1', JAVA_HOME: process.env.JAVA_HOME ?? '/Applications/Android Studio.app/Contents/jbr/Contents/Home' } });
    const out = [], ids = [];
    const walk = (n) => {
      const a = n.attributes ?? {};
      for (const k of ['text', 'accessibilityText', 'hintText', 'value']) if (a[k] && !out.includes(a[k])) out.push(a[k]);
      if (a['resource-id']) ids.push(a['resource-id']);
      (n.children ?? []).forEach(walk);
    };
    walk(JSON.parse(json.slice(json.indexOf('{'))));
    // (T313) the RN testIDs on screen ride along (not words: a scenario's text match never sees them)
    Object.defineProperty(out, 'ids', { value: ids, enumerable: false });
    return out;
  } catch { return Object.defineProperty([], 'ids', { value: [], enumerable: false }); }
}

/** (T363) The app's memory as iOS counts it (`footprint`, the host's view of the simulator process; MB): the footprint
 *  (what iOS's memory limit reads), MALLOC (the native heap: Hermes' external bytes, MapLibre, SQLite), graphics
 *  (IOSurface / IOAccelerator / Metal: on the simulator the Mac's GPU, so a lower bound), mapped files, RSS. */
export function memory() {
  const pid = pidOf();
  if (!pid) return null;
  try {
    const t = execFileSync('/usr/bin/footprint', [pid], { encoding: 'utf8', timeout: 60_000, maxBuffer: 16 << 20 });
    const mb = (v, u) => Math.round(Number(v) * ({ B: 1 / 1048576, KB: 1 / 1024, MB: 1, GB: 1024 }[u] ?? 0) * 10) / 10;
    const head = /Footprint:\s+([\d.]+)\s+(B|KB|MB|GB)/.exec(t);
    const cat = { malloc: 0, graphics: 0, mapped: 0 };
    for (const m of t.matchAll(/^\s*([\d.]+)\s+(B|KB|MB|GB)\s+[\d.]+\s+(?:B|KB|MB|GB)\s+[\d.]+\s+(?:B|KB|MB|GB)\s+\d+\s+(.+)$/gm)) {
      const v = mb(m[1], m[2]), c = m[3].trim();
      if (/^MALLOC/.test(c)) cat.malloc += v;
      else if (/IOSurface|IOAccelerator|Metal|IOKit/.test(c)) cat.graphics += v;
      else if (/^mapped file/.test(c)) cat.mapped += v;
    }
    let rss = null;
    try { rss = Math.round(Number(execFileSync('ps', ['-o', 'rss=', '-p', pid], { encoding: 'utf8' }).trim()) / 102.4) / 10; } catch { /* gone */ }
    const r1 = (x) => Math.round(x * 10) / 10;
    return { footprint: head ? mb(head[1], head[2]) : null, malloc: r1(cat.malloc), graphics: r1(cat.graphics), mapped: r1(cat.mapped), rss };
  } catch { return null; }
}
/** (T363) iOS's memory warning, as the Simulator's menu sends it */
export const memoryWarning = () => simctl(['spawn', udid(), 'notifyutil', '-p', 'UISimulatedMemoryWarningNotification']);

// ---------- the app's log, as the Android scenarios read logcat (T719/T720) ----------
// run-ios.mjs streams the app's unified log (`log stream --style compact --level debug`, process CanIFish) to app-os.log
// beside the report and names it here (`useAppLog`). `logcatClear()` is a MARK (the file's length now), never a delete;
// `logcatJs()` is the app's JS lines (RN console.log → `[com.facebook.react.log:javascript]`) written since it, so a
// scenario that reads `[open]` / `[stall]` / `[target]` lines with `l.indexOf('[tag] ')` reads them the same on both.
// The stream is written a beat after the app logs: a reader waits (the scenarios poll), never assumes "nothing yet".
let appLog = null, appLogMark = 0;
export function useAppLog(file) { appLog = file; appLogMark = 0; }
const appLogSize = () => { try { return fs.statSync(appLog).size; } catch { return 0; } };
export function logcatClear() {
  if (!appLog) throw new Error('no app log: run-ios.mjs names it (device.useAppLog)');
  appLogMark = appLogSize();
}
/** Every app-log line (all subsystems: UIKit, MapLibre, RN) since the mark, or since `from` (a byte offset). */
export function appLogLines(from = appLogMark) {
  if (!appLog) throw new Error('no app log: run-ios.mjs names it (device.useAppLog)');
  try {
    const size = appLogSize(), start = Math.max(0, Math.min(from, size));
    const fd = fs.openSync(appLog, 'r'), buf = Buffer.alloc(size - start);
    fs.readSync(fd, buf, 0, buf.length, start); fs.closeSync(fd);
    return buf.toString('utf8').split('\n');
  } catch { return []; }
}
export const logcatJs = () => appLogLines().filter((l) => l.includes('[com.facebook.react.log:javascript]')).join('\n');
/** The Mac's UTC offset as `log` prints times (ms), from macOS's own zone data (`date +%z`). NOT Node's: Node 2026a's
 *  ICU still turns America/Vancouver back to UTC−8 on 1 Nov 2026 while macOS keeps B.C.'s permanent UTC−7, so a Node
 *  local-time parse of a log line was an hour out (checked 2026-11-14 on ios-t313g's log). */
let zoneMs = null;
function macZoneMs() {
  if (zoneMs === null) {
    const z = /^([+-])(\d\d)(\d\d)$/.exec(execFileSync('date', ['+%z'], { encoding: 'utf8' }).trim());
    if (!z) throw new Error('date +%z: no UTC offset');
    zoneMs = (z[1] === '-' ? -1 : 1) * (Number(z[2]) * 60 + Number(z[3])) * 60_000;
  }
  return zoneMs;
}
/** A compact-style log line's own time (`2026-11-14 10:23:45.123 …`, the Mac's local clock) as epoch ms; null if none.
 *  The simulator app's `Date.now()` is the Mac's clock, so this is directly comparable with a probe's `wall`/`at`
 *  (checked on ios-t313g: `[stall] {"at":1793868519188}` was logged at 01:48:39.188, UTC−7). */
export function logTime(line) {
  const m = /^(\d{4})-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)\.(\d{3})/.exec(line ?? '');
  if (!m) return null;
  const [, y, mo, d, h, mi, s, ms] = m.map(Number);
  return Date.UTC(y, mo - 1, d, h, mi, s, ms) - macZoneMs();
}
/** (T719) The app's clock minus the log's, ms: each `[heap]` / `[stall]` line since `from` carries the app's `Date.now()`
 *  (`at`) and is stamped by the log a moment later, so this is ≤ 0 and small (a few ms) when the two agree; null with
 *  no such line. A bench that compares `wall` with a log time checks it first. */
export function clockSkew(from = appLogMark) {
  const d = [];
  for (const l of appLogLines(from)) {
    const k = l.includes('[heap] {') ? '[heap] ' : l.includes('[stall] {') ? '[stall] ' : null;
    if (!k) continue;
    try { const at = JSON.parse(l.slice(l.indexOf(k) + k.length)).at, t = logTime(l); if (Number.isFinite(at) && t) d.push(at - t); } catch { /* a cut line */ }
  }
  d.sort((a, b) => a - b);
  return d.length ? d[Math.floor(d.length / 2)] : null;
}
/** (T719) UIKit's own word on each touch the app received: `[com.apple.UIKit:EventDispatch] Evaluating dispatch of
 *  UIEvent …; type: 0;` is one touches event (a finger down, moved or up), and `(UIKitCore) send gesture actions` a
 *  gesture recogniser firing its action. Both in the app process, on the Mac's clock (ms). Since the mark / `from`. */
export function touchLog(from = appLogMark) {
  const touches = [], actions = [];
  for (const l of appLogLines(from)) {
    if (l.includes('Evaluating dispatch of UIEvent') && /type: 0;/.test(l)) { const t = logTime(l); if (t) touches.push(t); }
    else if (l.includes('send gesture actions')) { const t = logTime(l); if (t) actions.push(t); }
  }
  return { touches, actions };
}

/** The app's log lines (os_log, the simulator's unified log) since `since` (a Date), matching `re`. */
export function logSince(since, re) {
  const t = new Date(since).toISOString().replace('T', ' ').slice(0, 19);
  try {
    const out = simctl(['spawn', udid(), 'log', 'show', '--style', 'compact', '--start', t,
      '--predicate', `process == "CanIFish"`], { timeout: 120_000 }).toString();
    return out.split('\n').filter((l) => !re || re.test(l));
  } catch { return []; }
}
