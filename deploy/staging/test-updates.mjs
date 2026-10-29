#!/usr/bin/env node
// (T251) THE STAGING UPDATE TEST CHANNEL — publish synthetic updates under `test/` in the STAGING bucket, where a dev or
// staging build on the "test" update channel looks (apps/mobile/src/env.ts `endpointsFor(base, "test")`: the manifest
// and the three feed indexes move under test/, nothing else). The real staging feeds the crons write (feeds/**,
// runtiming/**) and the real release (release/**) are only READ, never written. Needs the rclone remote `r2:`.
//
//   node deploy/staging/test-updates.mjs reset                 test/ = copies of the live manifest + the three feed indexes
//   node deploy/staging/test-updates.mjs feed-bump [gauges|dfo|inseason]   that index's bytes change (a `_testBump` key)
//   node deploy/staging/test-updates.mjs release [tag]         a "new" release: data_release <live>-t<tag>, same files
//   node deploy/staging/test-updates.mjs corrupt               a release whose paint file's sha256 is wrong (sha mismatch)
//   node deploy/staging/test-updates.mjs missing               a release whose paint file is not there (HTTP 404)
//   node deploy/staging/test-updates.mjs feed-404 [gauges|dfo|inseason]    that index removed (HTTP 404)
//   node deploy/staging/test-updates.mjs min-app <semver>      a release that needs app >= <semver> (minAppVersion)
//   node deploy/staging/test-updates.mjs status                what test/ holds now
//   node deploy/staging/test-updates.mjs clean                 remove test/
//
// Each prints the new state. Scenarios build on what the app already holds (a release it applied stays its set), so
// `reset` between runs. deploy/staging/upload.sh's clear also removes test/ (its manifest would name deleted files).
import { execFileSync } from 'node:child_process';

const BUCKET = 'bc-fishing-regulations-staging';      // hardcoded: this script must never name a production bucket
const ORIGIN = 'https://data-staging.canifishthis.ca';
const D = `r2:${BUCKET}`;
const FEEDS = { gauges: 'feeds/live', dfo: 'feeds/dfo', inseason: 'feeds/inseason' };

const rclone = (args, input) => execFileSync('rclone', [...args, '--s3-no-check-bucket'],
                                             { input, stdio: [input === undefined ? 'ignore' : 'pipe', 'pipe', 'pipe'] }).toString();
const put = (key, text) => {
  if (!key.startsWith('test/')) throw new Error(`refusing to write ${key}: the harness writes test/ only`);
  rclone(['rcat', `${D}/${key}`, '--header-upload', 'Content-Type: application/json',
          '--header-upload', 'Cache-Control: no-cache'], text);
};
const del = (key) => {
  if (!key.startsWith('test/')) throw new Error(`refusing to delete ${key}`);
  try { rclone(['deletefile', `${D}/${key}`]); } catch { /* already gone */ }
};
async function get(key) {
  const r = await fetch(`${ORIGIN}/${key}`, { cache: 'no-store' });
  if (!r.ok) throw new Error(`${key}: HTTP ${r.status}`);
  return r.text();
}
const liveManifest = async () => JSON.parse(await get('release/manifest.json'));
const testManifest = async () => JSON.parse(await get('test/release/manifest.json'));
const putManifest = (m) => put('test/release/manifest.json', JSON.stringify(m, null, 1) + '\n');

export async function reset() {
  putManifest(await liveManifest());
  for (const dir of Object.values(FEEDS)) put(`test/${dir}/index.json`, await get(`${dir}/index.json`));
  return status();
}

export async function feedBump(feed = 'gauges') {
  if (!FEEDS[feed]) throw new Error(`feed ${feed}: one of ${Object.keys(FEEDS).join(', ')}`);
  const key = `test/${FEEDS[feed]}/index.json`;
  let j;
  try { j = JSON.parse(await get(key)); } catch { j = JSON.parse(await get(`${FEEDS[feed]}/index.json`)); }
  j._testBump = new Date().toISOString();
  put(key, JSON.stringify(j));
  return { feed, bumped: j._testBump };
}

export async function feed404(feed = 'dfo') {
  if (!FEEDS[feed]) throw new Error(`feed ${feed}: one of ${Object.keys(FEEDS).join(', ')}`);
  del(`test/${FEEDS[feed]}/index.json`);
  return { feed, removed: true };
}

/** A synthetic release on the live one: its id moved, its files as named (`edit` changes what it needs to). */
async function synthetic(suffix, edit = (m) => m) {
  const live = await liveManifest();
  const m = edit({ ...structuredClone(live), data_release: `${live.data_release}-${suffix}` });
  putManifest(m);
  return { data_release: m.data_release, minAppVersion: m.minAppVersion ?? null };
}

export const release = (tag = String(Date.now() % 100000)) => synthetic(`t${tag}`);
// the paint file: the first file every session opens (so the web stages it too); a wrong sha → "sha-mismatch"
export const corrupt = () => synthetic('corrupt', (m) => {
  m.files.paint = { ...m.files.paint, sha256: '0'.repeat(64) };
  return m;
});
export const missing = () => synthetic('missing', (m) => {
  m.files.paint = { ...m.files.paint, url: 'r-test-missing/paint-000000000000.bin.gz', sha256: '1'.repeat(64) };
  return m;
});
export const minApp = (v) => {
  if (!/^\d+\.\d+\.\d+/.test(v ?? '')) throw new Error('min-app <semver>, e.g. min-app 99.0.0');
  return synthetic(`needs-${v}`, (m) => ({ ...m, minAppVersion: v }));
};

export async function status() {
  const out = {};
  try { const m = await testManifest(); out.release = { data_release: m.data_release, minAppVersion: m.minAppVersion ?? null }; }
  catch (e) { out.release = String(e.message); }
  for (const [k, dir] of Object.entries(FEEDS)) {
    try { const j = JSON.parse(await get(`test/${dir}/index.json`)); out[k] = j._testBump ? `bumped ${j._testBump}` : 'live copy'; }
    catch (e) { out[k] = String(e.message); }
  }
  return out;
}

export async function clean() {
  rclone(['purge', `${D}/test`]);
  return { cleaned: 'test/' };
}

const CMDS = { reset, 'feed-bump': feedBump, 'feed-404': feed404, release, corrupt, missing, 'min-app': minApp, status, clean };

if (import.meta.url === `file://${process.argv[1]}`) {
  const [cmd, arg] = process.argv.slice(2);
  if (!CMDS[cmd]) {
    console.error(`usage: test-updates.mjs ${Object.keys(CMDS).join('|')} [arg]`);
    process.exit(2);
  }
  CMDS[cmd](arg).then((r) => console.log(JSON.stringify(r, null, 1)), (e) => { console.error(e.message); process.exit(1); });
}
