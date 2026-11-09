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
//   node deploy/staging/test-updates.mjs tiles [tag]           (T418) the tile folder moves (same files): the in-session
//                                                              update that broke the map's labels on 0.5.0
//   node deploy/staging/test-updates.mjs min-app <semver>      a release that needs app >= <semver> (minAppVersion)
//   node deploy/staging/test-updates.mjs big [tag]             (T255) a LARGE release: the basemap's id moves (same file,
//                                                              same sha), so a phone re-downloads it whole (~270 MB) —
//                                                              over the 20 MB line a metered connection holds for Wi-Fi
//   node deploy/staging/test-updates.mjs gauge <station> <flow|level> <value>   (T234) the gauge alerts' feed (the
//                                                              background check reads test/feeds/live/obs.json on this
//                                                              channel): that station's newest reading set to <value>, now
//   node deploy/staging/test-updates.mjs app-version <semver> [build] [notes…]   (T234 v1.1) the new-app-version notice:
//                                                              test/app/android/latest.json and play.json name that version
//                                                              (a build on the test channel reads them in its background check)
//   node deploy/staging/test-updates.mjs status                what test/ holds now
//   node deploy/staging/test-updates.mjs clean                 remove test/
//
// Each prints the new state. Scenarios build on what the app already holds (a release it applied stays its set), so
// `reset` between runs. deploy/staging/upload.sh resets test/ after its clear (T442: a test/ manifest naming a cleared
// release 404s a fresh install).
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
  put('test/feeds/live/obs.json', await get('feeds/live/obs.json'));
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
// (T255) the basemap is the biggest object a phone installs; a new id with the same url and sha makes the update service
// count it (`releaseNeed`) and the phone stream it again (install's ranged download onto the same path) — the honest big
// path (Wi-Fi hold, progress, resume, apply) without writing anything outside test/
export const big = (tag = String(Date.now() % 100000)) => synthetic(`big${tag}`, (m) => {
  if (!m.basemap) throw new Error('the live manifest names no basemap');
  m.basemap = { ...m.basemap, id: `${m.basemap.id}-t${tag}` };
  return m;
});
// (T418) the map's TILE FOLDER moves: a new tiles id (same handles, so the data still pairs; same urls and sha) makes the
// phone install the same tiles into a new folder and drop the old one — the in-session update that left the 0.5.0 map
// without its sprite, glyphs and `outside` source. Apply it while the map is open: the labels and the shading must stay.
export const tiles = (tag = String(Date.now() % 100000)) => synthetic(`tiles${tag}`, (m) => {
  const [h] = m.tiles.current.id.split('.');
  const build = ('7e57' + Number(tag).toString(16)).padEnd(12, '0').slice(0, 12);
  m.tiles = { ...m.tiles, current: { ...m.tiles.current, id: `${h}.${build}` } };
  return m;
});
export const minApp = (v) => {
  if (!/^\d+\.\d+\.\d+/.test(v ?? '')) throw new Error('min-app <semver>, e.g. min-app 99.0.0');
  return synthetic(`needs-${v}`, (m) => ({ ...m, minAppVersion: v }));
};

/** (T234) A gauge's newest reading, as the background check will read it: `[now, level, discharge]` on the test channel. */
export async function gauge(station, quantity, value) {
  const v = Number(value);
  if (!station || !['flow', 'level'].includes(quantity) || !Number.isFinite(v))
    throw new Error('gauge <station> <flow|level> <value>, e.g. gauge 08MH001 flow 60');
  let j;
  try { j = JSON.parse(await get('test/feeds/live/obs.json')); } catch { j = JSON.parse(await get('feeds/live/obs.json')); }
  // stamped as the feed stamps ECCC's rows (Pacific standard time with its offset): a percentile alert's line is the
  // reading's own date (T527), so a UTC stamp would read tomorrow's line every evening
  const at = `${new Date(Date.now() - 8 * 3_600_000).toISOString().slice(0, 19)}-08:00`;
  const last = j.stations[station]?.at(-1) ?? [at, null, null];
  j.stations[station] = [[at, quantity === 'level' ? v : last[1], quantity === 'flow' ? v : last[2]]];
  put('test/feeds/live/obs.json', JSON.stringify(j));
  return { station, row: j.stations[station][0] };
}

/** (T234 v1.1) A "newer" app version on the test channel (both the sideload and the Play file names). */
export async function appVersion(version, build = '999', ...notes) {
  if (!/^\d+\.\d+\.\d+/.test(version ?? '')) throw new Error('app-version <semver> [build] [notes…], e.g. app-version 0.9.0 99 "Alerts"');
  const j = { version, build: Number(build), sha: 'test', date: new Date().toISOString().slice(0, 10),
              notes: notes.length ? notes : ['A test version from the staging test channel'] };
  for (const f of ['latest.json', 'play.json']) put(`test/app/android/${f}`, JSON.stringify(j));
  return j;
}

export async function status() {
  const out = {};
  try { const m = await testManifest(); out.release = { data_release: m.data_release, minAppVersion: m.minAppVersion ?? null, basemap: m.basemap?.id ?? null, tiles: m.tiles?.current?.id ?? null }; }
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

const CMDS = { reset, gauge, 'app-version': appVersion, 'feed-bump': feedBump, 'feed-404': feed404, release, big, tiles, corrupt, missing, 'min-app': minApp, status, clean };

if (import.meta.url === `file://${process.argv[1]}`) {
  const [cmd, ...args] = process.argv.slice(2);
  if (!CMDS[cmd]) {
    console.error(`usage: test-updates.mjs ${Object.keys(CMDS).join('|')} [arg]`);
    process.exit(2);
  }
  CMDS[cmd](...args).then((r) => console.log(JSON.stringify(r, null, 1)), (e) => { console.error(e.message); process.exit(1); });
}
