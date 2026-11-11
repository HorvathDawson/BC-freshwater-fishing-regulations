// (T534) THE TEST BED NAMES THE BUILD IT TESTS. session.sh writes a sidecar `<apk>.stamp.json` beside every APK it builds
// ({ commit, dirty, dirty_files, maplibre, maplibre_build_id, built_at }); before the heavy lock it refuses a kept APK whose
// commit is not HEAD, whose tree was dirty, or that has no stamp at all — unless a reason is given (`--allow-stale "<why>"`,
// printed). run.mjs prints the same stamp at the top of every report. Pure functions here (tested by
// app/tools/testbed-stamp.test.ts); the CLI is what session.sh calls:
//
//   node build-stamp.mjs write  <apk> <maplibre: patched|stock> <build-id> [<stock reason>]   (after a build)
//   node build-stamp.mjs check  <apk> [--allow-stale "<reason>"]                              (pre-lock; exit 1 = refused)
//   node build-stamp.mjs line   <apk>                                                         (one line, for headers)
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '../../../..');
/** The paths whose uncommitted edits reach a test-bed APK (the app and the build scripts it is made with). */
export const BUILD_PATHS = ['app', 'package.json', 'pnpm-lock.yaml'];

export const stampPath = (apk) => `${apk}.stamp.json`;

/** { commit, dirty_files: [paths] } for the checkout the build reads. `git` is injectable for tests. */
export function readTree(git = (args) => execFileSync('git', args, { cwd: ROOT, encoding: 'utf8' })) {
  const commit = git(['rev-parse', 'HEAD']).trim();
  const dirty_files = git(['status', '--porcelain', '--', ...BUILD_PATHS]).split('\n').filter(Boolean).map((l) => l.slice(3));
  return { commit, dirty_files };
}

export function makeStamp({ tree, maplibre, buildId, stockReason, now = new Date() }) {
  if (maplibre !== 'patched' && maplibre !== 'stock') throw new Error(`stamp: maplibre must be patched or stock, not ${maplibre}`);
  if (maplibre === 'stock' && !String(stockReason ?? '').trim()) throw new Error('stamp: a stock MapLibre build needs its reason');
  if (!buildId) throw new Error('stamp: no libmaplibre build id');
  return { commit: tree.commit, dirty: tree.dirty_files.length > 0, dirty_files: tree.dirty_files.slice(0, 40),
           maplibre, maplibre_build_id: buildId, ...(maplibre === 'stock' ? { stock_reason: stockReason } : {}),
           built_at: now.toISOString() };
}

/** null when the kept APK may be used, else the sentence that refuses it. `allowStale` is the human's reason. */
export function staleProblem(stamp, head, allowStale = '') {
  const why = [];
  if (!stamp) why.push('no stamp beside it (built before T534, or by hand)');
  else {
    if (stamp.commit !== head) why.push(`built from ${stamp.commit.slice(0, 9)}, HEAD is ${head.slice(0, 9)}`);
    if (stamp.dirty) why.push(`its tree was dirty (${stamp.dirty_files.length} path(s): ${stamp.dirty_files.slice(0, 3).join(', ')})`);
  }
  if (!why.length) return null;
  if (String(allowStale).trim()) return null;
  return `kept APK refused: ${why.join('; ')}. Rebuild with --build, or say why it is fine: --allow-stale "<reason>"`;
}

/** The one line a report header and a STATUS entry quote. */
export function stampLine(stamp) {
  if (!stamp) return 'UNSTAMPED build (no .stamp.json beside the APK): the commit it tests is unknown';
  return `commit ${stamp.commit.slice(0, 9)}${stamp.dirty ? ` DIRTY (${stamp.dirty_files.length} path(s))` : ' clean'} · `
       + `libmaplibre build id ${stamp.maplibre_build_id} (${stamp.maplibre === 'patched' ? 'patched, T420 lean' : `STOCK: ${stamp.stock_reason}`}) · built ${stamp.built_at}`;
}

export function readStamp(apk) {
  try { return JSON.parse(fs.readFileSync(stampPath(apk), 'utf8')); } catch { return null; }
}

if (process.argv[1] && path.resolve(process.argv[1]) === new URL(import.meta.url).pathname) {
  const [cmd, apk, ...rest] = process.argv.slice(2);
  if (cmd === 'write') {
    const [maplibre, buildId, stockReason] = rest;
    fs.writeFileSync(stampPath(apk), JSON.stringify(makeStamp({ tree: readTree(), maplibre, buildId, stockReason }), null, 1));
    console.log(stampLine(readStamp(apk)));
  } else if (cmd === 'check') {
    const i = rest.indexOf('--allow-stale');
    const allow = i >= 0 ? rest[i + 1] ?? '' : '';
    if (i >= 0 && !allow.trim()) { console.error('--allow-stale needs a reason'); process.exit(2); }
    const stamp = fs.existsSync(apk) ? readStamp(apk) : undefined;
    if (stamp === undefined) { console.log('no kept APK: it will be built'); process.exit(0); }   // build path stamps it
    const problem = staleProblem(stamp, readTree().commit, allow);
    if (problem) { console.error(`testbed: ${problem}`); process.exit(1); }
    if (allow.trim() && staleProblem(stamp, readTree().commit, '')) console.log(`testbed: STALE APK ALLOWED — ${allow.trim()}`);
    console.log(`testbed: ${stampLine(stamp)}`);
  } else if (cmd === 'line') {
    console.log(stampLine(readStamp(apk)));
  } else { console.error('usage: build-stamp.mjs write|check|line <apk> …'); process.exit(2); }
}
