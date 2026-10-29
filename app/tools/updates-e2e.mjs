// (T251) THE UPDATE SCENARIOS, END TO END: the staging test channel (deploy/staging/test-updates.mjs) publishes each
// synthetic update, and a headless Chrome (regs-web/cdp.mjs — the repo's browser driver, no npm dependency) drives the
// web app on that channel and reads the Updates log: every check lands as a row, the right result each time.
//
//   cd app/apps/mobile && EXPO_PUBLIC_APP_ENV=staging EXPO_PUBLIC_UPDATE_CHANNEL=test EXPO_PUBLIC_RELEASE_GATE=1 \
//     npx expo start --web --port 39251
//   node app/tools/updates-e2e.mjs [--url http://localhost:39251] [--shots handoff/PLAN/app-integration/T251-shots]
//   → exit 1 on any FAIL
//
// Scenarios, in order (each builds on what the app then holds): boot (every source read) · unchanged (304 ×4) · feed
// bumped (gauges changed) · force re-download · a new release (changed → staged → applied) · a corrupt file
// (sha-mismatch) · a missing file (http-404) · a feed removed (dfo http-404) · a release for a newer app (app too old,
// and More says "Update the app to get new regulations") · simulate offline (offline ×4) · the export copied. Ends
// with `reset`, so the test channel is the live copy again. `EXPO_PUBLIC_RELEASE_GATE=1` turns on, for the browser, the
// minAppVersion gate a phone always has (the web's code ships with its data and is otherwise never gated).
import fs from 'node:fs';
import path from 'node:path';
import { open } from './regs-web/cdp.mjs';
import { sleep, tid, until } from './regs-web/drive.mjs';
import * as H from '../../deploy/staging/test-updates.mjs';

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; };
const URL0 = opt('--url', 'http://localhost:39251');
const SHOTS = opt('--shots', null);
let fails = 0;
const ok = (cond, what) => { console.log(`${cond ? 'ok  ' : 'FAIL'} ${what}`); if (!cond) fails++; };

const ROWS = `[...document.querySelectorAll('[data-testid="updates-log-row"]')].map((e) => e.getAttribute('aria-label'))`;
const rows = (b) => b.evaluate(ROWS);

async function click(b, id) {
  // the element's own click (react-native-web's Pressable takes it): a page sliding in moves coordinates under a tap
  const done = await b.evaluate(`(() => { const e = ${tid(id)}; if (!e) return false; e.scrollIntoView({ block: 'center' });
    e.click(); return true; })()`);
  if (!done) throw new Error(`nothing to tap: ${id}`);
  await sleep(300);
}
const clickLabel = (b, label) => b.evaluate(`(() => { const e = [...document.querySelectorAll('[aria-label]')]
  .find((x) => x.getAttribute('aria-label') === ${JSON.stringify(label)} && x.getBoundingClientRect().width > 0);
  if (!e) return false; e.click(); return true; })()`);

/** More → About → Updates log (again, when an apply re-rendered the shell away from it). */
async function openLog(b) {
  if (await b.evaluate(`!!${tid('updates-log-head')}`)) return;
  if (!(await b.evaluate(`!!${tid('menu-item-about')}`))) {
    await until(b, `[...document.querySelectorAll('[aria-label="More"]')].some((e) => e.getBoundingClientRect().width > 0)`,
                120000, 'the More tab');
    await clickLabel(b, 'More');
    await until(b, `!!${tid('menu-item-about')}`, 10000, 'the More list');
  }
  await click(b, 'menu-item-about');
  await until(b, `!!${tid('about-updates-log')}`, 10000, 'About\'s Updates log link');
  await click(b, 'about-updates-log');
  await until(b, `!!${tid('updates-log-head')}`, 10000, 'the Updates log page');
}

/** Tap a lever, then wait for `n` new rows and the check to settle; the new rows, newest first. */
async function act(b, id, n, wait = 30000) {
  const before = (await rows(b)).length;
  await click(b, id);
  await until(b, `${ROWS}.length >= ${before + n}`, wait, `${n} new log rows after ${id}`);
  await sleep(600);
  const all = await rows(b);
  return all.slice(0, all.length - before);
}
const has = (rs, re) => rs.some((r) => re.test(r ?? ''));

/** Close any sheet the app opened over the page (the in-season notice, the failed-update bell): its "Done". */
const dismiss = (b) => b.evaluate(`(() => { let n = 0; for (const e of [...document.querySelectorAll('[role="button"], button')])
  if (e.innerText?.trim() === 'Done' && e.getBoundingClientRect().width > 0) { e.click(); n++; } return n; })()`);

async function shot(b, name, { width, height, dpr }) {
  if (!SHOTS) return;
  for (let i = 0; i < 3 && (await dismiss(b)) > 0; i++) await sleep(500);
  await b.send('Emulation.setDeviceMetricsOverride', { width, height, deviceScaleFactor: dpr, mobile: width < 600 });
  await sleep(700);
  const { data } = await b.send('Page.captureScreenshot', { format: 'png' });
  fs.mkdirSync(SHOTS, { recursive: true });
  fs.writeFileSync(path.join(SHOTS, `${name}.png`), Buffer.from(data, 'base64'));
  console.log(`shot ${name}.png`);
}
const PHONE = { width: 412, height: 915, dpr: 2 }, DESK = { width: 1280, height: 860, dpr: 1 };

async function main() {
  console.log('harness reset:', JSON.stringify(await H.reset()));
  // the first-run notice accepted beforehand (`FIRST_RUN_VERSION`), so it does not sit over the shots
  const b = await open(URL0, { ...PHONE, mobile: true,
                               init: "try { localStorage.setItem('canifishthis.firstRun.v1', 'disclaimer-1'); } catch {}" });
  try {
    await b.send('Emulation.setFocusEmulationEnabled', { enabled: true }).catch(() => {});   // the clipboard wants focus
    await b.send('Browser.grantPermissions', { permissions: ['clipboardReadWrite', 'clipboardSanitizedWrite'] }).catch(() => {});
    await openLog(b);
    // boot: the service's first check read every source (a fresh profile holds no validator: each one 200)
    // (the release is not asked at start: the boot read the manifest an instant before — `seedRelease`)
    await until(b, `${ROWS}.length >= 3`, 60000, 'the start check');
    const boot = await rows(b);
    ok(has(boot, /Gauges, changed, 200/) && has(boot, /DFO salmon, changed, 200/) && has(boot, /In-season, changed, 200/),
       'boot: the three feeds read whole (200)');

    let r = await act(b, 'updates-log-check', 4);
    ok(r.filter((x) => /unchanged, 304/.test(x)).length === 3 && has(r, /Release, unchanged, 200/),
       `check now: three feed 304s, the manifest read once whole (${r.length} rows)`);
    r = await act(b, 'updates-log-check', 4);
    ok(r.filter((x) => /unchanged, 304/.test(x)).length === 4, `check again: four 304s`);

    await H.feedBump('gauges');
    r = await act(b, 'updates-log-check', 4);
    ok(has(r, /Gauges, changed, 200 · 3\d\d\.\d kB/) && has(r, /DFO salmon, unchanged, 304/), 'feed bumped: gauges changed, the rest 304');

    r = await act(b, 'updates-log-force', 5);
    ok(has(r, /App, action/) && r.filter((x) => /, changed, 200/.test(x)).length === 3, 'force re-download: validators forgotten, each feed whole');

    const rel = await H.release('1');
    r = await act(b, 'updates-log-check', 4);
    ok(has(r, new RegExp(`Release, changed, 200 .*→ ${rel.data_release}`)), `new release ${rel.data_release}: seen`);
    await until(b, `${ROWS}.some((x) => /Release, staged/.test(x))`, 60000, 'staged');
    await openLog(b);
    if (await b.evaluate(`!!${tid('updates-log-apply')} && ${tid('updates-log-apply')}.getAttribute('aria-disabled') !== 'true'`))
      await click(b, 'updates-log-apply');
    await until(b, `${ROWS}.some((x) => /Release, applied/.test(x))`, 60000, 'applied');
    await openLog(b);
    ok(has(await rows(b), new RegExp(`Release, applied.*→ ${rel.data_release}`)), 'new release: staged and applied');
    await shot(b, 'updates-log-phone', PHONE);
    await b.evaluate(`${tid('updates-log-rows')}.scrollIntoView({ block: 'start' })`);
    await shot(b, 'updates-log-phone-rows', PHONE);
    await shot(b, 'updates-log-desktop', DESK);
    await b.send('Emulation.setDeviceMetricsOverride', { ...PHONE, deviceScaleFactor: PHONE.dpr, mobile: true });

    await H.corrupt();
    await act(b, 'updates-log-check', 4);
    await until(b, `${ROWS}.some((x) => /Release, failed.*sha-mismatch/.test(x))`, 60000, 'the sha mismatch');
    ok(true, 'corrupt file: failed, sha-mismatch; the set in use kept');

    await H.missing();
    await act(b, 'updates-log-check', 4);
    await until(b, `${ROWS}.some((x) => /Release, failed.*http-404/.test(x))`, 60000, 'the 404');
    ok(true, 'missing file: failed, http-404');

    await H.feed404('dfo');
    r = await act(b, 'updates-log-check', 4);
    ok(has(r, /DFO salmon, failed, 404.*http-404/), 'feed removed: dfo failed, http-404');

    await H.minApp('99.0.0');
    r = await act(b, 'updates-log-check', 4);
    ok(has(r, /Release, app too old.*needs app ≥ 99\.0\.0/), 'minAppVersion 99.0.0: app too old, not applied');
    ok(await b.evaluate(`!!${tid('updates-log-app-too-old')}`), 'the page says "Update the app to get new regulations"');

    await click(b, 'updates-log-offline');
    r = await act(b, 'updates-log-check', 4);
    ok(r.filter((x) => /, offline/.test(x)).length === 4, 'simulate offline: four offline rows, no request');
    await click(b, 'updates-log-offline');

    await click(b, 'updates-log-copy');
    await until(b, `!!${tid('updates-log-copied')}`, 5000, 'the copy');
    // headless Chrome's clipboard may refuse a read: the page's "Copied" is the app's own word that the write landed
    const copied = await b.evaluate(`${tid('updates-log-copied')}.innerText`);
    const text = await b.evaluate('navigator.clipboard.readText().catch((e) => "read refused: " + e.message)');
    console.log(`  copied: ${copied}; clipboard: ${text.split('\n').slice(0, 3).join(' / ').slice(0, 200)}`);
    ok(/^Can I Fish This\? updates log/.test(text) && /\napp \d+\.\d+\.\d+/.test(text) && /channel test/.test(text),
       'export copied: the build, the source and channel, then each event');

    // More's Data block says the gate too; then About's version, for the shots
    await b.evaluate(`${tid('menu-page-updates')}?.querySelector('[aria-label="Back"]')?.click()`);
    await sleep(400);
    await b.evaluate(`${tid('menu-page-about')}?.querySelector('[aria-label="Back"]')?.click()`);
    await sleep(600);
    const line = await b.evaluate(`${tid('data-updates-line')}?.innerText ?? ''`);
    ok(/Update the app to get new regulations/.test(line), `More's Updates line: "${line}"`);
    await click(b, 'menu-item-about');
    await until(b, `!!${tid('menu-page-about')}`, 10000, 'About');
    const ver = await b.evaluate(`[...document.querySelectorAll('[data-testid^="about-version-"]')][0]?.innerText ?? ''`);
    ok(/^\d+\.\d+\.\d+ \(build \d+, [0-9a-f]{7}, \d{4}-\d{2}-\d{2}\)/.test(ver), `About's version: "${ver}"`);
    await shot(b, 'about-version-phone', PHONE);
    await shot(b, 'about-version-desktop', DESK);
    if (b.errors.length) console.log(`console errors (${b.errors.length}):\n  ${b.errors.slice(0, 5).join('\n  ')}`);
  } finally {
    await b.close();
    console.log('harness reset:', JSON.stringify(await H.reset()));
  }
  console.log(fails ? `${fails} FAIL` : 'all scenarios passed');
  process.exit(fails ? 1 : 0);
}

main().catch(async (e) => { console.error(e); try { await H.reset(); } catch {} process.exit(1); });
