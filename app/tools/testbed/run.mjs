#!/usr/bin/env node
// (T255) THE APP TEST BED'S RUNNER — scenarios on the Android emulator: downloads, sync and offline. Each scenario is a
// list of steps; a step is host work (the network, the staging test channel) and/or a Maestro flow (the UI), then a
// screenshot and the screen's words, recorded pass/fail. The report lands in out/runs/<stamp>/ (index.html, report.md,
// report.json, shots). See README.md.
//
//   node app/tools/testbed/run.mjs [all | <scenario id>…] [--sync local|account] [--keep-going] [--out <dir>]
//
// Needs the emulator up (session.sh, inside scripts/heavy.sh) with the test-bed APK installed, rclone `r2:` for the
// test channel (deploy/staging/test-updates.mjs: STAGING `test/` only), and Maestro (~/.maestro).
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import * as device from './lib/device.mjs';
import { checkChannel } from './lib/channel.mjs';
import { flow } from './lib/maestro.mjs';
import { writeReport } from './lib/report.mjs';
import { SCENARIOS } from './scenarios/index.mjs';
import { adapterFor } from './sync/adapter.mjs';

const HERE = path.dirname(new URL(import.meta.url).pathname);
const ROOT = path.resolve(HERE, '../../..');
const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args.splice(i, 2)[1] : d; };
const flag = (k) => { const i = args.indexOf(k); if (i >= 0) args.splice(i, 1); return i >= 0; };
const SYNC = opt('--sync', 'local');
const OUTDIR = opt('--out', null);
const KEEP_GOING = flag('--keep-going');
/** --no-r2: nothing is written to the staging bucket (no test-channel reset at the end; a scenario asking for the channel
 *  fails; `ctx.resetChannel` then only checks that test/ is already the live release, T442) */
const NO_R2 = flag('--no-r2');
const want = args.length && !args.includes('all') ? args : SCENARIOS.map((s) => s.id);
for (const w of want) if (!SCENARIOS.some((s) => s.id === w)) { console.error(`no scenario ${w}: ${SCENARIOS.map((s) => s.id).join(', ')}`); process.exit(2); }

const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
const dir = OUTDIR ?? path.join(HERE, 'out', 'runs', stamp);
fs.mkdirSync(dir, { recursive: true });
const run = {
  id: stamp, started: new Date().toISOString(), device: `${device.serial()} · ${device.sh('getprop ro.product.model').trim()} · Android ${device.sh('getprop ro.build.version.release').trim()}`,
  apk: process.env.TESTBED_ENV === 'local' ? 'app/tools/testbed/out/app-testbed-local.apk (release, LOCAL data :39217, env switch on)' : 'app/tools/testbed/out/app-testbed.apk (release, staging, channel test, env switch on)', channel: 'test (staging test/)',
  sync: SYNC, scenarios: [],
};
// (batches) a run into a folder that already holds a report adds its scenarios to it: one report for several heavy jobs
try {
  const prev = JSON.parse(fs.readFileSync(path.join(dir, 'report.json'), 'utf8'));
  run.id = prev.id; run.started = prev.started;
  run.scenarios.push(...prev.scenarios.filter((x) => !want.includes(x.id)));
} catch { /* a new report */ }
console.log(`[testbed] report → ${dir}`);

/** The staging test channel (deploy/staging/test-updates.mjs); an R2/rclone hiccup is tried again (3 tries). */
function updates(cmd, arg) {
  if (NO_R2) throw new Error(`test-updates ${cmd}: refused (--no-r2)`);
  let last;
  for (let i = 0; i < 3; i++) {
    try {
      const out = execFileSync('node', [path.join(ROOT, 'deploy/staging/test-updates.mjs'), cmd, ...(arg ? [String(arg)] : [])],
                               { encoding: 'utf8', timeout: 180_000, stdio: ['ignore', 'pipe', 'pipe'] });
      return JSON.parse(out);
    } catch (e) { last = e; execFileSync('sleep', ['5']); }
  }
  throw new Error(`test-updates ${cmd}: ${String(last?.stderr ?? last?.message ?? last).trim().split('\n').pop()}`);
}

/** The app's FATAL lines in the crash buffer since the last call (then cleared): the first line of each. */
function crashes() {
  try {
    const log = device.adb(['logcat', '-d', '-b', 'crash']).toString();
    device.adb(['logcat', '-b', 'crash', '-c']);
    const lines = log.split('\n');
    const at = lines.findIndex((l) => /FATAL EXCEPTION/.test(l));
    if (at < 0 || !log.includes(device.APP_ID)) return null;
    return lines.slice(at, at + 4).map((l) => l.replace(/^.*?E AndroidRuntime:\s*/, '')).join(' / ').slice(0, 400);
  } catch { return null; }
}
try { device.adb(['logcat', '-b', 'crash', '-c']); } catch { /* none yet */ }

let shotN = fs.readdirSync(dir).filter((f) => f.endsWith('.png')).length;
function makeCtx(sc) {
  const rec = { id: sc.id, title: sc.title, result: 'pass', steps: [], findings: [], ms: 0 };
  run.scenarios.push(rec);
  const ctx = {
    device, updates, sync: null, rec, dir,
    /** (T442) test/ = the live release before a fresh install: reset, or with --no-r2 checked read only (throws if not) */
    resetChannel: () => (NO_R2 ? checkChannel() : updates('reset')),
    /** A UI flow; throws its Maestro words when it fails (the step then fails). */
    flow(name, env) {
      let r = flow(name, env, { logDir: path.join(dir, 'maestro', sc.id) });
      // one more try after a beat: a sheet still settling or a toast over a tab is not the app's failure
      if (!r.ok && !/^(updates-log-action|spot-name-save|report-send|spot-add-photo)$/.test(name)) {
        execFileSync('sleep', ['2']);
        r = flow(name, env, { logDir: path.join(dir, 'maestro', sc.id) });
      }
      ctx.lastSaid = r.said;
      if (!r.ok) throw new Error(`flow ${name} failed: ${r.said.split('\n').slice(-3).join(' / ') || 'see maestro log'}`);
      return r;
    },
    /** Wait until the screen's words match `re` (uiautomator; host side, no flow). */
    async waitText(re, ms = 30_000) {
      const end = Date.now() + ms;
      let words = [];
      while (Date.now() < end) {
        words = device.screenText();
        const hit = words.find((w) => re.test(w));
        if (hit) return hit;
        await device.sleep(1500);
      }
      throw new Error(`no text ${re} on screen in ${ms / 1000} s (saw: ${words.slice(0, 12).join(' | ').slice(0, 300)})`);
    },
    /** One step: run, screenshot, record. A failed step fails the scenario; the rest still run unless `stop`. */
    async step(title, fn, { stop = false, shot = true } = {}) {
      const t0 = Date.now();
      const s = { title, result: 'pass', ms: 0 };
      ctx.lastSaid = null;
      try { const note = await fn(); if (typeof note === 'string') s.note = note; }
      catch (e) {
        s.result = e.skip ? 'skip' : 'fail'; s.error = e.message;
        if (!e.skip) rec.result = 'fail';
      }
      s.ms = Date.now() - t0;
      // a crash during the step fails it, whatever the step saw (the app may have restarted and looked fine)
      const crash = crashes();
      if (crash) { s.result = 'fail'; s.error = `${s.error ? `${s.error}; ` : ''}APP CRASHED: ${crash}`; rec.result = 'fail';
                   rec.findings.push(`crash in "${title}": ${crash}`); }
      if (ctx.lastSaid && s.result === 'fail') s.said = ctx.lastSaid;
      if (shot) {
        try {
          const name = `${String(++shotN).padStart(3, '0')}-${sc.id}-${title.toLowerCase().replace(/[^a-z0-9]+/g, '-').slice(0, 40)}.png`;
          device.screenshot(path.join(dir, name)); s.shot = name;
        } catch { /* a screenshot never fails a step */ }
      }
      rec.steps.push(s);
      console.log(`  ${s.result.toUpperCase()} ${title}${s.note ? ` — ${s.note}` : ''}${s.error ? ` — ${s.error}` : ''}`);
      writeReport(dir, run);
      if (s.result === 'fail' && stop) throw Object.assign(new Error(`stopped: ${title}`), { stopped: true });
      return s;
    },
    /** A finding (an app bug or a gap) for the report, said once per scenario. */
    finding(text) { rec.findings.push(text); console.log(`  FINDING ${text}`); },
    skip(msg) { throw Object.assign(new Error(msg), { skip: true }); },
  };
  return ctx;
}

for (const sc of SCENARIOS.filter((s) => want.includes(s.id))) {
  console.log(`[testbed] ${sc.id} — ${sc.title}`);
  const ctx = makeCtx(sc);
  ctx.sync = adapterFor(SYNC, ctx);
  const t0 = Date.now();
  try { await sc.run(ctx); }
  catch (e) { if (!e.stopped) { ctx.rec.result = 'fail'; ctx.rec.findings.push(`runner: ${e.message}`); console.log(`  ERROR ${e.stack}`); } }
  finally {
    try { await device.net('wifi'); device.link('full', 'none'); } catch { /* best effort */ }
    try { await ctx.sync.teardown?.(); } catch { /* best effort */ }
  }
  ctx.rec.ms = Date.now() - t0;
  writeReport(dir, run);
  if (ctx.rec.result === 'fail' && !KEEP_GOING && want.length > 1) console.log('  (continuing with the next scenario; each starts from a clean app)');
}
// a `session.sh --local` APK reads this machine's release: staging's test channel is not touched
if (process.env.TESTBED_ENV !== 'local' && !NO_R2) try { updates('reset'); } catch (e) { console.log(`[testbed] reset of test/ failed: ${e.message}`); }
run.finished = new Date().toISOString();
writeReport(dir, run);
const bad = run.scenarios.filter((s) => s.result === 'fail');
console.log(`[testbed] ${run.scenarios.length - bad.length}/${run.scenarios.length} scenarios passed → ${path.join(dir, 'index.html')}`);
process.exit(bad.length ? 1 : 0);
