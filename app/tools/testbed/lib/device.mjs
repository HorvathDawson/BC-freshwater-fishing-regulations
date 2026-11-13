// (T255) THE DEVICE: adb and the emulator console, for one emulator (its serial from session.sh, `TESTBED_SERIAL`, or
// out/SERIAL). Network states, the app's lifecycle, screenshots and the screen's text. Nothing here kills a process it
// did not start; the emulator's lifetime is session.sh's.
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

export const APP_ID = 'ca.canifishthis.app';
const SDK = process.env.ANDROID_HOME ?? path.join(os.homedir(), 'Library/Android/sdk');
const ADB = path.join(SDK, 'platform-tools/adb');
const OUT = path.join(path.dirname(new URL(import.meta.url).pathname), '..', 'out');

export function serial() {
  const s = process.env.TESTBED_SERIAL ?? (fs.existsSync(path.join(OUT, 'SERIAL')) ? fs.readFileSync(path.join(OUT, 'SERIAL'), 'utf8').trim() : null);
  if (!s) throw new Error('no emulator: run through session.sh (TESTBED_SERIAL / out/SERIAL)');
  return s;
}

/** (T688 lane) Maestro's device selector (global options): an emulator by its OWN adb port, never through the adb server's
 *  device list — one phantom offline entry there (`emulator-5554` from a foreign listener on 5555) fails every flow. */
export function maestroDeviceArgs() {
  const m = /^emulator-(\d+)$/.exec(serial());
  return m ? ['--host', '127.0.0.1', '--port', String(Number(m[1]) + 1)] : ['--device', serial()];
}

export const adb = (args, opts = {}) =>
  execFileSync(ADB, ['-s', serial(), ...args], { maxBuffer: 64 << 20, timeout: opts.timeout ?? 120_000, ...opts });
export const sh = (cmd) => adb(['shell', cmd]).toString().replace(/\r/g, '');
export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------- network ----------
/**
 * The network states a scenario asks for. `wifi`: Wi-Fi on, mobile data on (Wi-Fi wins: unmetered). `cellular`: Wi-Fi
 * off, mobile data on (NetInfo says "cellular": METERED). `offline`: airplane mode (no Wi-Fi, no data). The emulator's
 * own link (`slow`/`fast`) shapes speed and latency on top (`adb emu network speed|delay`).
 */
export async function net(state) {
  if (state === 'offline') {
    // airplane mode AND both radios off: Android remembers "Wi-Fi on in airplane mode" once Wi-Fi was turned on
    // during one, so airplane mode alone can leave Wi-Fi up (seen in the T255 runs)
    sh('cmd connectivity airplane-mode enable');
    sh('svc wifi disable');
    sh('svc data disable');
  } else {
    sh('cmd connectivity airplane-mode disable');
    await sleep(1500);
    sh('svc data enable');
    sh(state === 'cellular' ? 'svc wifi disable' : 'svc wifi enable');
  }
  if (state !== 'offline') await waitOnline(state === 'cellular' ? 'MOBILE' : 'WIFI');
  else {
    const end = Date.now() + 20_000;
    while (netState() !== 'none' && Date.now() < end) await sleep(1000);
    if (netState() !== 'none') throw new Error(`still online after airplane mode (${netState()})`);
    await sleep(1500);
  }
  return netState();
}

/** The emulator's link speed/latency: `full` (none) | `slow` (EDGE-ish) | any `adb emu network speed` name. */
export function link(speed = 'full', delay = 'none') {
  adb(['emu', 'network', 'speed', speed]);
  adb(['emu', 'network', 'delay', delay]);
}

/** What the device's connectivity service says is active: "WIFI" | "MOBILE" | "none". */
export function netState() {
  let out = '';
  try { out = sh('dumpsys connectivity | grep -m1 "Active default network"'); } catch { return 'none'; }
  const id = out.match(/network:\s*(\d+)/)?.[1];
  if (!id) return 'none';
  let nai = '';
  try { nai = sh(`dumpsys connectivity | grep -m1 "NetworkAgentInfo{network{${id}}"`); }
  catch { return 'none'; }      // the network went away between the two reads (grep found nothing: exit 1)
  return /WIFI/.test(nai) ? 'WIFI' : /MOBILE|CELLULAR/.test(nai) ? 'MOBILE' : 'other';
}

async function waitOnline(want, ms = 45_000) {
  const end = Date.now() + ms;
  let kicked = false;
  while (Date.now() < end) {
    const s = netState();
    if (s === want && probe()) return;
    // (T314/T315 reruns) the emulator's radios sometimes stick after airplane mode (Wi-Fi never back, or up with no
    // route): once, halfway, turn the wanted radio off and on again
    if (!kicked && end - Date.now() < ms / 2) {
      kicked = true;
      const radio = want === 'WIFI' ? 'wifi' : 'data';
      sh(`svc ${radio} disable`); await sleep(2000); sh(`svc ${radio} enable`);
    }
    await sleep(1500);
  }
  throw new Error(`network did not reach ${want} (now ${netState()})`);
}

/** One ping from the device to the data origin (no HTTP client in the image): "online" means it is reached. */
export function probe() {
  try { return /bytes from/.test(sh('ping -c1 -W3 data-staging.canifishthis.ca 2>&1 | head -2')); }
  catch { return false; }
}

// ---------- the app ----------
export const launch = () => sh(`am start -W -n ${APP_ID}/.MainActivity`);
export const stop = () => sh(`am force-stop ${APP_ID}`);
export const clearData = () => sh(`pm clear ${APP_ID}`);
export const background = () => sh('input keyevent KEYCODE_HOME');
export const installed = () => /package:/.test(sh(`pm path ${APP_ID}`));
export function install(apk) { adb(['install', '-r', apk], { timeout: 300_000 }); }
// `pidof` exits 1 when no process has the name: that is the answer "not running" (null), not a failed command
// (the T542 wait for a stopped app threw exactly when it should have passed)
export const pidOf = () => sh(`pidof ${APP_ID} || true`).trim() || null;
/** (T262) the app's heap now, KB (`dumpsys meminfo`): Java, native and total PSS; null when it is not running. */
export function heap() {
  try {
    const t = sh(`dumpsys meminfo ${APP_ID}`);
    const kb = (re) => Number(re.exec(t)?.[1] ?? NaN);
    const java = kb(/Java Heap:\s+(\d+)/), native = kb(/Native Heap:\s+(\d+)/), total = kb(/TOTAL PSS:\s+(\d+)/);
    return Number.isFinite(java) ? { java, native, total } : null;
  } catch { return null; }
}

/** Crash-buffer lines naming the app (a crash in a step is said in the report). */
export function crashed(sinceIso) {
  const log = adb(['logcat', '-d', '-b', 'crash', ...(sinceIso ? ['-T', sinceIso] : [])]).toString();
  return log.split('\n').filter((l) => l.includes(APP_ID) || /FATAL EXCEPTION/.test(l));
}
export const logcatClear = () => adb(['logcat', '-c']);
export const logcatJs = () => adb(['logcat', '-d', '-s', 'ReactNativeJS:*']).toString();

// ---------- the screen ----------
export function screenshot(file) {
  fs.writeFileSync(file, adb(['exec-out', 'screencap', '-p']));
  return file;
}

/**
 * Every text / content-description on screen, for host-side asserts. uiautomator first (fast); it refuses a screen that
 * never goes idle (a spinner turning: "could not get idle state"), so then Maestro's own view of it (`maestro hierarchy`).
 */
export function screenText() {
  try {
    const r = sh('uiautomator dump /sdcard/tb-ui.xml 2>&1');
    if (!/dumped to/.test(r)) throw new Error(r);
    const x = sh('cat /sdcard/tb-ui.xml');
    const out = [];
    for (const m of x.matchAll(/(?:text|content-desc)="([^"]+)"/g)) out.push(unxml(m[1]));
    return out;
  } catch { return hierarchyText(); }
}
const unxml = (s) => s.replace(/&amp;/g, '&').replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>')
  .replace(/&#10;/g, '\n');

function hierarchyText() {
  try {
    const bin = process.env.MAESTRO_BIN ?? path.join(os.homedir(), '.maestro/maestro/bin/maestro');
    const json = execFileSync(bin, [...maestroDeviceArgs(), 'hierarchy'], {
      encoding: 'utf8', timeout: 60_000, maxBuffer: 64 << 20, stdio: ['ignore', 'pipe', 'ignore'],
      env: { ...process.env, MAESTRO_CLI_NO_ANALYTICS: '1', JAVA_HOME: process.env.JAVA_HOME ?? '/Applications/Android Studio.app/Contents/jbr/Contents/Home' } });
    const out = [];
    const walk = (n) => {
      const a = n.attributes ?? {};
      for (const k of ['text', 'accessibilityText', 'hintText']) if (a[k]) out.push(a[k]);
      (n.children ?? []).forEach(walk);
    };
    walk(JSON.parse(json.slice(json.indexOf('{'))));
    return out;
  } catch { return []; }
}

/** Push a file into the device's gallery (the photo picker lists MediaStore). */
export function addPhoto(local, name = path.basename(local)) {
  const dst = `/sdcard/Pictures/${name}`;
  adb(['push', local, dst]);
  sh(`content call --method scan_volume --uri content://media --arg external_primary >/dev/null 2>&1 || am broadcast -a android.intent.action.MEDIA_SCANNER_SCAN_FILE -d file://${dst} >/dev/null`);
  return dst;
}

/** The emulator's GPS (lon, lat). */
export const geo = (lon, lat) => adb(['emu', 'geo', 'fix', String(lon), String(lat)]);
