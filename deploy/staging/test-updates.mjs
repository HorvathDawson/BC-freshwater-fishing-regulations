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
//   node deploy/staging/test-updates.mjs data-big [tag]        (T912) `big` AND a changed data file: the live `climate`
//                                                              file with its SQLite change counter moved (same tables and
//                                                              rows, other bytes) under test/release/<id>/, so an update
//                                                              downloads a data file as well as the ~270 MB basemap
//   node deploy/staging/test-updates.mjs gauge <station> <flow|level> <value>   (T234) the gauge alerts' feed (the
//                                                              background check reads test/feeds/live/obs.json on this
//                                                              channel): that station's newest reading set to <value>, now
//   node deploy/staging/test-updates.mjs app-version <semver> [build] [notes…]   (T234 v1.1) the new-app-version notice:
//                                                              test/app/android/latest.json and play.json name that version
//                                                              (a build on the test channel reads them in its background check)
//   node deploy/staging/test-updates.mjs side <release dir>    (T810 V-py, trout contract 2.8 / Q-D11) a SIDE release (one
//                                                              holding the trout section, e.g. by scripts/trout-refresh.sh):
//                                                              its data files go to test/release/<id>/ and the test
//                                                              manifest names them (see `sideManifest` for the one way a
//                                                              test manifest can reach them, and its limit)
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
/**
 * (T912) THE SAME SQLITE FILE WITH OTHER BYTES: its file change counter (header offset 24) moved on, and the
 * "version-valid-for" number (offset 92) with it, so SQLite still trusts the header's page count. Nothing a reader queries
 * changes; the sha256 does, so a phone downloads it (an update's data file) instead of copying the one it holds.
 */
export function bumpSqlite(raw) {
  if (new TextDecoder().decode(raw.subarray(0, 15)) !== 'SQLite format 3') throw new Error('not a SQLite file');
  const out = new Uint8Array(raw), v = new DataView(out.buffer, out.byteOffset, out.byteLength);
  const c = (v.getUint32(24) + 1) >>> 0;
  v.setUint32(24, c); v.setUint32(92, c);
  return out;
}
/** (T912) The `data-big` manifest: `live` with a new id, the basemap's id moved (`big`), and `name` replaced by `f`. */
export function dataBigManifest(live, id, tag, name, f) {
  if (!live.basemap) throw new Error('the live manifest names no basemap');
  if (!live.files[name]) throw new Error(`the live manifest names no ${name}`);
  const m = { ...structuredClone(live), data_release: id };
  m.basemap = { ...m.basemap, id: `${m.basemap.id}-t${tag}` };
  m.files[name] = { ...m.files[name], ...f };
  return m;
}
export async function dataBig(tag = String(Date.now() % 100000)) {
  const { createHash } = await import('node:crypto');
  const { gzipSync, gunzipSync } = await import('node:zlib');
  const fs = await import('node:fs'), os = await import('node:os'), path = await import('node:path');
  const live = await liveManifest();
  const name = 'climate', f = live.files[name];
  const id = `${live.data_release}-data${tag}`;
  const got = new Uint8Array(await (await fetch(`${ORIGIN}/release/${f.url}`, { cache: 'no-store' })).arrayBuffer());
  const raw = bumpSqlite(got[0] === 0x1f && got[1] === 0x8b ? new Uint8Array(gunzipSync(got)) : got);
  const sha256 = createHash('sha256').update(raw).digest('hex');
  const gz = gzipSync(raw);
  const file = `${id}/${name}-${sha256.slice(0, 12)}.sqlite.gz`, key = `${SIDE_PREFIX}${file}`;
  if (!key.startsWith('test/')) throw new Error(`refusing to write ${key}`);
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'test-updates-'));
  try {
    fs.writeFileSync(path.join(tmp, 'f.gz'), gz);
    // as upload.sh ships a data file: stored gzip, served with Content-Encoding (see `side`)
    rclone(['copyto', path.join(tmp, 'f.gz'), `${D}/${key}`, '--header-upload', 'Cache-Control: no-cache',
            '--header-upload', 'Content-Encoding: gzip', '--header-upload', 'Content-Type: application/octet-stream']);
  } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
  const m = dataBigManifest(live, id, tag, name, { url: `../${key}`, bytes: raw.length, gz: gz.length, sha256 });
  if (keyOf(m.files[name].url) !== key) throw new Error(`${name}: ${m.files[name].url} resolves to ${keyOf(m.files[name].url)}`);
  putManifest(m);
  return { data_release: id, minAppVersion: m.minAppVersion ?? null, data: { [name]: m.files[name].url }, basemap: m.basemap.id };
}

export const minApp = (v) => {
  if (!/^\d+\.\d+\.\d+/.test(v ?? '')) throw new Error('min-app <semver>, e.g. min-app 99.0.0');
  return synthetic(`needs-${v}`, (m) => ({ ...m, minAppVersion: v }));
};

// (T810 V-py, Q-D11) A SIDE RELEASE ON THE TEST CHANNEL. CHECKED FIRST: can a test/ manifest point at
// test/release/<id>/? The app reads every release file's url RELATIVE TO `${origin}/release/` on BOTH channels
// (apps/mobile/src/env.ts `endpointsFor`: only the manifest and the feed indexes move under test/; install.ts fetches
// `${release}${url}` and stores the file under its last segment). So a plain "<id>/<file>" url names release/<id>/, which
// this harness never writes. The one url that reaches test/release/<id>/ is "../test/release/<id>/<file>": a client that
// normalises it (browsers, the web build; OkHttp on Android) asks for test/release/…, and one that sends it as written
// still gets that key, because the staging data worker resolves the path with its own URL parse (data-worker.js).
// Proved in test-updates.test.mjs; not yet walked on a device. The cleaner fix is
// the app's: on the test channel, resolve file urls against the manifest's own folder (`${origin}/test/release/`). Tiles,
// basemap, map assets and the synopsis keep their urls: a side release built from the live inputs names the live ones,
// which staging already holds under release/ (read only here).
export const SIDE_PREFIX = 'test/release/';
/** The test manifest for side release `m`: its data files' urls reach test/release/<id>/ from `${origin}/release/`. */
export function sideManifest(m) {
  if (!(m.regulations?.versions ?? {}).trout)
    throw new Error(`${m.data_release}: no trout section — a release cut before the trout cutover (T810 L)`);
  const id = m.data_release;
  const files = {};
  for (const [name, f] of Object.entries(m.files)) {
    if (!f.url.startsWith(`${id}/`)) throw new Error(`${name}: ${f.url} is not in the release folder ${id}/`);
    files[name] = { ...f, url: `../${SIDE_PREFIX}${f.url}` };
  }
  const out = { ...structuredClone(m), files };
  // (T810 bench 2026-11-18) a side release cuts its OWN synopsis into its folder (r-…/synopsis-….pdf): staging's release/
  // never holds it, so a first install's synopsis download was a 404 and the start failed at 468.6 of 480.4 MB. It goes
  // to test/ as the data files do; a synopsis named outside the folder (the live one's) keeps its url.
  if (m.synopsis?.url?.startsWith(`${id}/`)) out.synopsis = { ...out.synopsis, url: `../${SIDE_PREFIX}${m.synopsis.url}` };
  return out;
}
/** Where each data file goes: [local path under the release root, bucket key] — test/ keys only (the synopsis too, when
 *  it is the side release's own). */
export function sideUploads(m) {
  const own = m.synopsis?.url?.startsWith(`${m.data_release}/`) ? [m.synopsis] : [];
  return [...Object.values(m.files), ...own].map((f) => [f.url, `${SIDE_PREFIX}${f.url}`]);
}
/** The bucket key a manifest url names, as a normalising client resolves it against `${origin}/release/`. */
export function keyOf(url) {
  return new URL(url, `${ORIGIN}/release/`).pathname.slice(1);
}

export async function side(dir) {
  if (!dir) throw new Error('side <release dir>: a side release folder (r-…/ holding manifest.json)');
  const fs = await import('node:fs'), path = await import('node:path');
  const m = JSON.parse(fs.readFileSync(path.join(dir, 'manifest.json'), 'utf8'));
  const root = path.dirname(path.resolve(dir));
  const t = sideManifest(m);
  for (const [name, f] of Object.entries(t.files)) {
    const key = keyOf(f.url);
    if (key !== `${SIDE_PREFIX}${m.files[name].url}`) throw new Error(`${name}: ${f.url} resolves to ${key}`);
  }
  if (t.synopsis && t.synopsis.url !== m.synopsis.url && keyOf(t.synopsis.url) !== `${SIDE_PREFIX}${m.synopsis.url}`)
    throw new Error(`synopsis: ${t.synopsis.url} resolves to ${keyOf(t.synopsis.url)}`);
  const ups = sideUploads(m);
  for (const [local] of ups) if (!fs.existsSync(path.join(root, local))) throw new Error(`missing ${local} under ${root}`);
  for (const [local, key] of ups) {
    if (!key.startsWith('test/')) throw new Error(`refusing to write ${key}: the harness writes test/ only`);
    // as upload.sh ships a release's data files: stored gzip, served with Content-Encoding (data-worker passes it through)
    rclone(['copyto', path.join(root, local), `${D}/${key}`, '--header-upload', 'Cache-Control: no-cache',
            ...(local.endsWith('.gz') ? ['--header-upload', 'Content-Encoding: gzip', '--header-upload',
                                         'Content-Type: application/octet-stream'] : [])]);
  }
  putManifest(t);
  return { data_release: t.data_release, files: ups.length, folder: `${SIDE_PREFIX}${m.data_release}/`,
           note: 'file urls are "../test/release/<id>/…" from the app\'s release base; clean removes the folder with test/' };
}

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

const CMDS = { reset, gauge, 'app-version': appVersion, 'feed-bump': feedBump, 'feed-404': feed404, release, big, 'data-big': dataBig, tiles, corrupt, missing, 'min-app': minApp, side, status, clean };

if (import.meta.url === `file://${process.argv[1]}`) {
  const [cmd, ...args] = process.argv.slice(2);
  if (!CMDS[cmd]) {
    console.error(`usage: test-updates.mjs ${Object.keys(CMDS).join('|')} [arg]`);
    process.exit(2);
  }
  CMDS[cmd](...args).then((r) => console.log(JSON.stringify(r, null, 1)), (e) => { console.error(e.message); process.exit(1); });
}
