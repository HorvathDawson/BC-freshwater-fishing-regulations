#!/usr/bin/env node
// (T918) A TESTFLIGHT BUILD REACHES THE EXTERNAL GROUP BY ITSELF. Run by deploy/staging/build-ios-testflight.sh after a
// successful upload (opt out: --no-distribute), or alone for a build already uploaded:
//
//   node deploy/staging/testflight-distribute.mjs [--version 0.8.3] [--build 39] [--group "<name>"] [--dry-run]
//            [--max-wait-min 45] [--poll-s 30]
//
//   --version   CFBundleShortVersionString; default app/apps/mobile/app.json expo.version
//   --build     CFBundleVersion; default ~/.canifishthis/testflight-last-build (what the build script wrote last)
//   --group     the EXTERNAL TestFlight group's name; default ~/.canifishthis/testflight-external-group (one line), else
//               the one external group the app has (several: refused, naming them). An internal group is never touched.
//   --dry-run   reads App Store Connect (GET only, it still waits for processing) and prints every change it would make
//
// Steps (App Store Connect API v1, an ES256 JWT signed here with node:crypto, no dependency):
//   1. the app by bundle id ca.canifishthis.app; the build by app + version + build number;
//   2. wait for processing: every --poll-s (30 s) up to --max-wait-min (45 min), printed; a build not yet listed (the
//      first minutes after an upload) counts as waiting; FAILED / INVALID stop it;
//   3. export compliance: when the build has no `usesNonExemptEncryption` answer, app.json's
//      ios.config.usesNonExemptEncryption (= Info.plist ITSAppUsesNonExemptEncryption) is set on the build; app.json
//      without it is refused (a human answers it in App Store Connect);
//   4. "What to Test" (the build's beta build localization, required for external testers): when empty, this version's
//      iPhone changelog bullets (core `notesOn(e, "ios")`, as build-ios-testflight.sh's appstore.json), else
//      "Version X (build N)"; a text already there is kept;
//   5. the build is added to the external group (already in it: nothing done);
//   6. Beta App Review: when the build's external state is READY_FOR_BETA_SUBMISSION (Apple wants a review: the first
//      build of a version; later builds of a version are usually approved at once) it is submitted; a submission that
//      exists is only reported. A state still settling (PROCESSING, IN_EXPORT_COMPLIANCE_REVIEW, MISSING_EXPORT_COMPLIANCE
//      just after the answer) is re-read every 30 s up to 5 min; a state it does not know stops it, named. The last line says where it stands: waiting for review / in review / approved / testing.
//
// The key: ~/.canifishthis/asc-api-key.json {"key_id", "issuer_id", "key_path"} (build-ios-testflight.sh's header), read at
// run time only. Any failure prints Apple's own message and exits 1 (no silent fallback); rerun the same command: every
// step is a no-op when already done. App-level beta review details (contact, "Beta App Information") are set once in
// App Store Connect → TestFlight; a submission refused for lack of them says so in Apple's message.
// Tests: node --test deploy/staging/testflight-distribute.test.mjs (HTTP mocked; pipeline/tests/test_deploy_node_scripts.py).
import { createPrivateKey, sign } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

export const BUNDLE_ID = 'ca.canifishthis.app';
export const API = 'https://api.appstoreconnect.apple.com';
const REPO = join(dirname(fileURLToPath(import.meta.url)), '..', '..');
const SECRETS = join(homedir(), '.canifishthis');

const b64url = (b) => Buffer.from(b).toString('base64url');

/** An App Store Connect token: ES256 (raw r||s signature), kid = the key id, aud appstoreconnect-v1, 20 min at most. */
export function makeJwt({ keyId, issuerId, privateKeyPem, now = Date.now(), ttlS = 15 * 60 }) {
  if (ttlS > 20 * 60) throw new Error('an App Store Connect token lives 20 minutes at most');
  const iat = Math.floor(now / 1000);
  const head = b64url(JSON.stringify({ alg: 'ES256', kid: keyId, typ: 'JWT' }));
  const body = b64url(JSON.stringify({ iss: issuerId, iat, exp: iat + ttlS, aud: 'appstoreconnect-v1' }));
  const sig = sign('sha256', Buffer.from(`${head}.${body}`), { key: createPrivateKey(privateKeyPem), dsaEncoding: 'ieee-p1363' });
  return `${head}.${body}.${b64url(sig)}`;
}

export class AscError extends Error {}

/** Apple's own words from an error response: "STATUS code: title — detail" per error. */
function appleMessage(status, json, text) {
  const errs = json?.errors;
  if (Array.isArray(errs) && errs.length)
    return errs.map((e) => `${e.status ?? status} ${e.code ?? ''}: ${e.title ?? ''}${e.detail ? ` — ${e.detail}` : ''}`).join('; ');
  return `${status} ${String(text).slice(0, 300)}`;
}

/** A small client: GET / POST / PATCH with a fresh token, Apple's message on failure, a bounded retry on 429 / 5xx. */
export function ascClient({ token, fetchImpl = globalThis.fetch, sleep = (ms) => new Promise((r) => setTimeout(r, ms)), log = () => {} }) {
  async function call(method, path, body) {
    const url = path.startsWith('http') ? path : API + path;
    for (let attempt = 1; ; attempt++) {
      const res = await fetchImpl(url, {
        method,
        headers: { Authorization: `Bearer ${token()}`, ...(body ? { 'Content-Type': 'application/json' } : {}) },
        ...(body ? { body: JSON.stringify(body) } : {}),
      });
      const text = res.status === 204 ? '' : await res.text();
      let json = null;
      try { json = text ? JSON.parse(text) : null; } catch { /* Apple's message is the text */ }
      if (res.ok) return json;
      if ((res.status === 429 || res.status >= 500) && attempt < 4) {
        log(`  App Store Connect said ${res.status} to ${method} ${path}; retry ${attempt} of 3 in ${10 * attempt} s`);
        await sleep(10_000 * attempt);
        continue;
      }
      throw new AscError(`${method} ${path} failed: ${appleMessage(res.status, json, text)}`);
    }
  }
  return {
    get: (p) => call('GET', p),
    post: (p, b) => call('POST', p, b),
    patch: (p, b) => call('PATCH', p, b),
    /** every page of a list (links.next) */
    async all(p) {
      const out = [];
      for (let next = p; next; ) {
        const page = await call('GET', next);
        out.push(...(page?.data ?? []));
        next = page?.links?.next ?? null;
      }
      return out;
    },
  };
}

/** The external group to use: by name (an internal one of that name is refused), else the only external one. */
export function pickGroup(groups, name) {
  const ext = groups.filter((g) => g.attributes?.isInternalGroup === false);
  if (name) {
    const hit = ext.find((g) => g.attributes.name === name);
    if (hit) return hit;
    const internal = groups.find((g) => g.attributes?.name === name);
    if (internal) throw new AscError(`TestFlight group "${name}" is an INTERNAL group; this script only adds builds to external groups`);
    throw new AscError(`no external TestFlight group named "${name}"; the external groups are: ${ext.map((g) => `"${g.attributes.name}"`).join(', ') || 'none'}`);
  }
  if (ext.length === 1) return ext[0];
  if (!ext.length) throw new AscError('the app has no external TestFlight group: make one in App Store Connect → TestFlight, then rerun');
  throw new AscError(`several external TestFlight groups (${ext.map((g) => `"${g.attributes.name}"`).join(', ')}): name one with --group "<name>" or one line in ~/.canifishthis/testflight-external-group`);
}

/**
 * This version's iPhone notes as "What to Test" (core notesOn), else a plain line. Returns {text, fallback}: `fallback`
 * names why the plain line was used (no changelog file / no entry for the version / no iPhone notes), null otherwise.
 */
export async function whatToTestFor(version, build, changelogPath = join(REPO, 'app/packages/core/src/changelog.ts')) {
  let notes = [], fallback = null;
  if (!existsSync(changelogPath)) fallback = `no changelog file ${changelogPath}`;
  else {
    const m = await import(pathToFileURL(changelogPath).href);
    const e = m.CHANGELOG.find((x) => x.version === version);
    if (!e) fallback = `the changelog has no entry for ${version}`;
    else notes = [...m.notesOn(e, 'ios')];
    if (e && !notes.length) fallback = `the changelog entry for ${version} has no iPhone notes`;
  }
  const text = notes.length ? `Version ${version} (build ${build}):\n${notes.map((n) => `• ${n}`).join('\n')}` : `Version ${version} (build ${build})`;
  return { text: text.slice(0, 4000), fallback };   // 4000: Apple's limit for whatsNew (the API's name for TestFlight's "What to Test")
}

// buildBetaDetail.externalBuildState: what each one means for this step (anything not listed stops it, named)
const EXT_DONE = new Set(['WAITING_FOR_BETA_REVIEW', 'IN_BETA_REVIEW', 'BETA_APPROVED', 'IN_BETA_TESTING', 'READY_FOR_BETA_TESTING']);
const EXT_SETTLING = new Set(['PROCESSING', 'IN_EXPORT_COMPLIANCE_REVIEW', 'MISSING_EXPORT_COMPLIANCE']);

const REVIEW_SAYS = {
  WAITING_FOR_REVIEW: 'submitted: waiting for Beta App Review (testers get it once Apple approves; often within a day)',
  IN_REVIEW: 'in Beta App Review',
  APPROVED: 'Beta App Review APPROVED',
  REJECTED: 'Beta App Review REJECTED — see App Store Connect → TestFlight for Apple\'s reason',
};

/**
 * The whole step. Injected: the client (`asc`), the clock and sleep (tests), `log`. Returns a summary
 * {build, group, added, submitted, reviewState, externalState}; throws AscError on any failure.
 */
export async function distribute({ asc, version, build, groupName = null, dryRun = false, usesNonExemptEncryption,
  whatToTest, maxWaitMs = 45 * 60_000, pollMs = 30_000, settleMs = 5 * 60_000, now = () => Date.now(),
  sleep = (ms) => new Promise((r) => setTimeout(r, ms)), log = console.log }) {
  const would = (s) => log(`  [dry-run] would ${s}`);
  const apps = await asc.get(`/v1/apps?filter[bundleId]=${encodeURIComponent(BUNDLE_ID)}&limit=2`);
  const app = apps?.data?.find((a) => a.attributes?.bundleId === BUNDLE_ID);
  if (!app) throw new AscError(`App Store Connect has no app with bundle id ${BUNDLE_ID} (or the key cannot see it)`);
  const appId = app.id;

  // the group first: a wrong name fails before the long wait
  const group = pickGroup(await asc.all(`/v1/betaGroups?filter[app]=${appId}&limit=200`), groupName);
  log(`external group "${group.attributes.name}" (${group.id})`);

  // 2. processing
  const q = `/v1/builds?filter[app]=${appId}&filter[version]=${encodeURIComponent(build)}`
    + `&filter[preReleaseVersion.version]=${encodeURIComponent(version)}&limit=2`;
  const t0 = now();
  let b;
  for (;;) {
    const list = (await asc.get(q))?.data ?? [];
    b = list[0];
    const state = b?.attributes?.processingState;
    const mins = ((now() - t0) / 60_000).toFixed(1);
    if (state === 'VALID') { log(`build ${version} (${build}) processed (${b.id})`); break; }
    if (state === 'FAILED' || state === 'INVALID')
      throw new AscError(`build ${version} (${build}) processing ${state} in App Store Connect (Apple's e-mail to the account says why); it cannot be tested — upload a new build`);
    if (now() - t0 + pollMs > maxWaitMs)
      throw new AscError(`build ${version} (${build}) still ${b ? state : 'not listed'} after ${mins} min (limit ${Math.round(maxWaitMs / 60_000)} min): `
        + `Apple's processing is slow today; rerun later: node deploy/staging/testflight-distribute.mjs --version ${version} --build ${build}`
        + `${groupName ? ` --group "${groupName}"` : ''}`);
    log(`  ${mins} min: build ${version} (${build}) ${b ? `processing (${state})` : 'not in App Store Connect yet'}; next look in ${Math.round(pollMs / 1000)} s`);
    await sleep(pollMs);
  }
  const buildId = b.id;
  if (b.attributes.expired) throw new AscError(`build ${version} (${build}) has EXPIRED in TestFlight; upload a new build`);

  // 3. export compliance
  if (b.attributes.usesNonExemptEncryption == null) {
    if (typeof usesNonExemptEncryption !== 'boolean')
      throw new AscError(`build ${version} (${build}) needs its export compliance answered and app.json has no ios.config.usesNonExemptEncryption: `
        + 'answer it in App Store Connect → TestFlight → the build ("Manage"), then rerun');
    if (dryRun) would(`set usesNonExemptEncryption=${usesNonExemptEncryption} on the build (app.json)`);
    else {
      await asc.patch(`/v1/builds/${buildId}`, { data: { type: 'builds', id: buildId, attributes: { usesNonExemptEncryption } } });
      log(`export compliance answered from app.json: usesNonExemptEncryption=${usesNonExemptEncryption}`);
    }
  }

  // 4. What to Test
  const locs = (await asc.get(`/v1/builds/${buildId}/betaBuildLocalizations?limit=50`))?.data ?? [];
  if (!locs.some((l) => (l.attributes?.whatsNew ?? '').trim())) {
    const { text, fallback } = await whatToTest();
    if (fallback) log(`  WARNING: "What to Test" falls back to the plain line (${fallback})`);
    log(`  "What to Test": ${JSON.stringify(text)}`);
    if (locs.length) {
      if (dryRun) would(`set "What to Test" (${locs[0].attributes.locale}): ${JSON.stringify(text)}`);
      else await asc.patch(`/v1/betaBuildLocalizations/${locs[0].id}`, { data: { type: 'betaBuildLocalizations', id: locs[0].id, attributes: { whatsNew: text } } });
    } else {
      const locale = app.attributes?.primaryLocale || 'en-US';
      if (dryRun) would(`add "What to Test" (${locale}): ${JSON.stringify(text)}`);
      else await asc.post('/v1/betaBuildLocalizations', { data: { type: 'betaBuildLocalizations', attributes: { locale, whatsNew: text },
        relationships: { build: { data: { type: 'builds', id: buildId } } } } });
    }
    if (!dryRun) log(`"What to Test" set${fallback ? ' (plain line)' : ' from the changelog'}`);
  }

  // 5. the group
  const inGroup = (await asc.all(`/v1/betaGroups/${group.id}/relationships/builds?limit=200`)).some((x) => x.id === buildId);
  let added = false;
  if (inGroup) log(`already in "${group.attributes.name}": nothing to add`);
  else if (dryRun) would(`add the build to "${group.attributes.name}"`);
  else {
    await asc.post(`/v1/betaGroups/${group.id}/relationships/builds`, { data: [{ type: 'builds', id: buildId }] });
    added = true;
    log(`added to "${group.attributes.name}"`);
  }

  // 6. Beta App Review, decided by the external state (allow-listed). A state still settling (processing, export
  // compliance just answered) is re-read every pollMs up to settleMs; the dry run reads it once and reports.
  const detail = async () => (await asc.get(`/v1/builds/${buildId}/buildBetaDetail`))?.data?.attributes ?? {};
  const t1 = now();
  let ext;
  for (;;) {
    ext = (await detail()).externalBuildState;
    if (!EXT_SETTLING.has(ext) || dryRun) break;
    const mins = ((now() - t1) / 60_000).toFixed(1);
    if (now() - t1 + pollMs > settleMs)
      throw new AscError(`build ${version} (${build}) still ${ext} after ${mins} min`
        + (ext === 'PROCESSING' ? '' : ': answer export compliance in App Store Connect → TestFlight → the build')
        + `; then rerun: node deploy/staging/testflight-distribute.mjs --version ${version} --build ${build}${groupName ? ` --group "${groupName}"` : ''}`);
    log(`  ${mins} min: external state ${ext}; next look in ${Math.round(pollMs / 1000)} s`);
    await sleep(pollMs);
  }
  const subs = (await asc.get(`/v1/betaAppReviewSubmissions?filter[build]=${buildId}&limit=1`))?.data ?? [];
  let reviewState = subs[0]?.attributes?.betaReviewState ?? null;
  let submitted = false;
  if (reviewState) log(`Beta App Review already submitted: ${reviewState}`);
  else if (ext === 'READY_FOR_BETA_SUBMISSION') {
    if (dryRun) would('submit the build for Beta App Review (Apple asks for one: the first build of this version)');
    else {
      const r = await asc.post('/v1/betaAppReviewSubmissions', { data: { type: 'betaAppReviewSubmissions',
        relationships: { build: { data: { type: 'builds', id: buildId } } } } });
      reviewState = r?.data?.attributes?.betaReviewState ?? 'WAITING_FOR_REVIEW';
      submitted = true;
      log(`submitted for Beta App Review: ${reviewState}`);
      ext = (await detail()).externalBuildState ?? ext;
    }
  } else if (EXT_DONE.has(ext)) { /* reported below */ }
  else if (dryRun && EXT_SETTLING.has(ext)) log(`  [dry-run] external state ${ext}: a real run waits up to ${Math.round(settleMs / 60_000)} min for it to settle, then decides`);
  else if (ext !== 'BETA_REJECTED')   // rejected: thrown below, after the report line
    throw new AscError(`build ${version} (${build}) has external TestFlight state ${ext ?? '(none given)'}, which this script does not handle; look in App Store Connect → TestFlight`);
  const where = reviewState ? REVIEW_SAYS[reviewState] ?? reviewState
    : dryRun && ext === 'READY_FOR_BETA_SUBMISSION' ? 'would submit for Beta App Review' : 'no Beta App Review needed';
  log(`${dryRun ? '[dry-run] ' : ''}${version} (${build}) → "${group.attributes.name}": ${where}; external state ${ext ?? 'unknown'}`
    + (ext === 'IN_BETA_TESTING' || ext === 'BETA_APPROVED' ? ' — testers can install it (Automatic Updates installs it on their phones)' : ''));
  if (reviewState === 'REJECTED' || ext === 'BETA_REJECTED') throw new AscError(`Beta App Review rejected ${version} (${build})`);
  return { build: buildId, group: group.attributes.name, added, submitted, reviewState, externalState: ext ?? null };
}

/** --flag value pairs; --dry-run alone */
export function parseArgs(argv) {
  const o = { dryRun: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--dry-run') o.dryRun = true;
    else if (['--version', '--build', '--group', '--max-wait-min', '--poll-s'].includes(a)) {
      if (argv[i + 1] == null) throw new AscError(`${a} needs a value`);
      const v = (o[a.slice(2)] = argv[++i]);
      if ((a === '--max-wait-min' || a === '--poll-s') && !(Number.isFinite(Number(v)) && Number(v) > 0))
        throw new AscError(`${a} needs a positive number (got "${v}")`);
    } else if (a === '-h' || a === '--help') o.help = true;
    else throw new AscError(`unknown argument ${a} (see the header of deploy/staging/testflight-distribute.mjs)`);
  }
  return o;
}

const readTrim = (f) => (existsSync(f) ? readFileSync(f, 'utf8').trim() : '');

async function main() {
  const o = parseArgs(process.argv.slice(2));
  if (o.help) { console.log(readFileSync(fileURLToPath(import.meta.url), 'utf8').split('\nimport ')[0]); return; }
  const appJson = JSON.parse(readFileSync(join(REPO, 'app/apps/mobile/app.json'), 'utf8')).expo;
  const version = o.version ?? appJson.version;
  const build = o.build ?? readTrim(join(SECRETS, 'testflight-last-build'));
  if (!/^\d+$/.test(build)) throw new AscError(`no build number: pass --build N (${join(SECRETS, 'testflight-last-build')} is missing or not a number)`);
  const groupName = o.group ?? (readTrim(join(SECRETS, 'testflight-external-group')) || null);
  const keyFile = join(SECRETS, 'asc-api-key.json');
  if (!existsSync(keyFile)) throw new AscError(`missing ${keyFile} (build-ios-testflight.sh's header)`);
  const k = JSON.parse(readFileSync(keyFile, 'utf8'));
  if (!k.key_id || !k.issuer_id || !k.key_path || !existsSync(k.key_path))
    throw new AscError(`${keyFile} needs key_id, issuer_id and key_path (an existing .p8)`);
  const pem = readFileSync(k.key_path, 'utf8');
  let tok = null, tokAt = 0;
  const token = () => {   // one token per 10 minutes (each lives 15)
    if (!tok || Date.now() - tokAt > 10 * 60_000) { tok = makeJwt({ keyId: k.key_id, issuerId: k.issuer_id, privateKeyPem: pem }); tokAt = Date.now(); }
    return tok;
  };
  console.log(`TestFlight external distribution of ${version} (${build})${o.dryRun ? ' — DRY RUN (reads only)' : ''}`);
  await distribute({
    asc: ascClient({ token, log: console.log }), version, build, groupName, dryRun: o.dryRun,
    usesNonExemptEncryption: appJson.ios?.config?.usesNonExemptEncryption,
    whatToTest: () => whatToTestFor(version, build),
    maxWaitMs: Number(o['max-wait-min'] ?? 45) * 60_000, pollMs: Number(o['poll-s'] ?? 30) * 1000,
  });
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main().catch((e) => {
    console.error(`testflight-distribute: FAILED — ${e.message}`);
    process.exit(1);
  });
}
