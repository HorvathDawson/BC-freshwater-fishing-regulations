// T253 — THE BACKGROUND PREFETCH, MEASURED: the time from a tap on an unnamed stream to its Regulations on screen, on a
// cold visit (the background tier off, as the request walk runs) and with the background tier on (the tap made once
// it has gone quiet), both on a throttled "Fast 4G" (DevTools' preset: 9 Mbit/s × 0.9 down, 1.5 Mbit/s × 0.9 up,
// 60 ms × 2.75 latency); and the release bytes the background tier fetched for the default view (Chilliwack, Region 2)
// — until Region 2 was ready, and in all.
//
//   node app/tools/prefetch-measure.mjs [--url http://localhost:39218] [--reps 3] [--json out.json]
//
// The app on a dev server reading the release served locally (as the request walk). One headless Chrome at a time.
import fs from 'node:fs';
import { open } from './regs-web/cdp.mjs';

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; };
const URL0 = opt('--url', 'http://localhost:39218').replace(/\/$/, '');
const REPS = Number(opt('--reps', '3'));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const FAST_4G = { offline: false, latency: 60 * 2.75, downloadThroughput: (9e6 / 8) * 0.9, uploadThroughput: (1.5e6 / 8) * 0.9 };
// the request walk's unnamed tap (390 × 844, the default camera)
const TAP = { x: 319, y: 602, name: 'Unnamed stream · Region 2' };
const isRelease = (u) => /\/release\/r-[0-9a-f]+\/.+\.gz$/.test(new URL(u).pathname);
const fileOf = (u) => /\/release\/r-[0-9a-f]+\/(.+)-[0-9a-f]{12}\.[a-z]+\.gz$/.exec(new URL(u).pathname)?.[1] ?? null;

async function run(background) {
  // the first-run notice (T236) accepted, so the tap reaches the map
  const init = `try { localStorage.setItem('canifishthis.firstRun.v1', 'disclaimer-1'); } catch {}`
    + (background ? '' : 'globalThis.__releasePrefetchBackground = false;');
  const b = await open(`${URL0}/`, { width: 390, height: 844, network: true, init });
  try {
    await b.send('Network.emulateNetworkConditions', FAST_4G);
    const until = async (expr, ms = 180000) => {
      for (let t = 0; t < ms; t += 100) { if (await b.evaluate(expr).catch(() => false)) return; await sleep(100); }
      throw new Error(`timed out: ${expr}`);
    };
    const quiet = async (q = 4000, max = 240000) => {
      let sig = '', last = Date.now(); const t0 = Date.now();
      while (Date.now() - t0 < max) {
        const all = b.requests.filter((r) => /^https?:/.test(r.url)), done = all.filter((r) => r.bytes !== null || r.failed).length;
        const s = `${all.length}/${done}`;
        if (s !== sig) { sig = s; last = Date.now(); } else if (done === all.length && Date.now() - last > q) return;
        await sleep(200);
      }
    };
    await until(`!!document.querySelector('[data-testid=map-day]')`);
    // a sheet that opens by itself (the in-season notice, T230) closed, as a reader would before tapping
    for (let i = 0; i < 20; i++) {
      await sleep(500);
      const closed = await b.evaluate(`(() => { const e = [...document.querySelectorAll('[aria-label],[role=button]')]
        .find((e) => /^(Done|I understand)$/.test((e.getAttribute('aria-label') || e.innerText || '').trim())); if (!e) return false; e.click(); return true; })()`);
      if (!closed && i >= 6) break;
    }
    // the idle tier, then (background on) the background tier — wait until the network is quiet a while
    await quiet(background ? 8000 : 5000);
    const before = b.requests.splice(0);
    const bgAt = await b.evaluate(`(() => { const e = performance.getEntriesByType('mark').filter((m) => m.name.includes(':bg:'));
      return e.length ? performance.timeOrigin + Math.min(...e.map((m) => m.startTime)) : null; })()`);
    const bgReqs = before.filter((r) => bgAt !== null && r.wall !== null && r.wall * 1000 >= bgAt - 1 && isRelease(r.url));
    const log = await b.evaluate(`(() => { try { return JSON.parse(localStorage.getItem('canifishthis.updateLog.v1') || '{}').events || []; } catch { return []; } })()`);
    const pre = log.filter((e) => e.source === 'prefetch');
    // the bytes up to Region 2 ready: the prefetched files logged before that line
    const readyIdx = pre.findIndex((e) => e.note === 'Region 2 ready');
    const toReady = readyIdx < 0 ? null : pre.slice(0, readyIdx).filter((e) => e.kind === 'prefetched' && e.bytes).reduce((a, e) => a + e.bytes, 0);
    // the tap
    await b.evaluate(`window.__tapDone = null; new MutationObserver((m, o) => { if (document.querySelector('[data-testid=regs-header]')) {
      window.__tapDone = performance.now(); o.disconnect(); } }).observe(document.body, { subtree: true, childList: true });
      window.__tap0 = performance.now(); true`);
    for (const type of ['mouseMoved', 'mousePressed', 'mouseReleased'])
      await b.send('Input.dispatchMouseEvent', { type, x: TAP.x, y: TAP.y, button: 'left', clickCount: 1 });
    await until(`window.__tapDone !== null`);
    const head = await b.evaluate(`document.querySelector('[data-testid=water-title]')?.innerText ?? ''`);
    if (!head.startsWith(TAP.name)) throw new Error(`the tap opened "${head.split('\n')[0]}", not ${TAP.name}`);
    const ms = await b.evaluate(`window.__tapDone - window.__tap0`);
    await quiet(2000);
    const tapReqs = b.requests.splice(0).filter((r) => isRelease(r.url));
    return {
      background, tapMs: Math.round(ms),
      tapFiles: tapReqs.map((r) => fileOf(r.url)), tapBytes: tapReqs.reduce((a, r) => a + (r.bytes ?? 0), 0),
      bgFiles: bgReqs.map((r) => fileOf(r.url)), bgBytes: bgReqs.reduce((a, r) => a + (r.bytes ?? 0), 0),
      bytesToRegion2Ready: toReady, holds: pre.filter((e) => e.kind === 'paused').map((e) => e.note),
      ready: pre.filter((e) => /ready$/.test(e.note ?? '')).map((e) => e.note),
    };
  } finally { await b.close(); }
}

const out = [];
for (let i = 0; i < REPS; i++) for (const bg of [false, true]) {
  const r = await run(bg);
  out.push(r);
  console.log(`${bg ? 'prefetched' : 'cold      '} tap ${String(r.tapMs).padStart(6)} ms · tap fetched ${(r.tapBytes / 1e6).toFixed(2)} MB [${r.tapFiles.join(' ')}]`
    + (bg ? ` · background ${(r.bgBytes / 1e6).toFixed(2)} MB (${r.bgFiles.length} files), to Region 2 ready ${r.bytesToRegion2Ready === null ? '—' : (r.bytesToRegion2Ready / 1e6).toFixed(2) + ' MB (gz)'} · holds [${r.holds.join(' ')}] · ${r.ready.length} ready` : ''));
}
const med = (xs) => { const s = [...xs].sort((a, b) => a - b); return s[Math.floor(s.length / 2)]; };
for (const bg of [false, true])
  console.log(`median ${bg ? 'prefetched' : 'cold'}: ${med(out.filter((r) => r.background === bg).map((r) => r.tapMs))} ms`);
if (opt('--json', null)) fs.writeFileSync(opt('--json'), JSON.stringify(out, null, 1));
