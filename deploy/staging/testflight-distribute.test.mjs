// (T918) testflight-distribute.mjs with the HTTP layer mocked: no network, no real key (a throwaway one is generated).
// Run: node --test deploy/staging/testflight-distribute.test.mjs (pipeline/tests/test_deploy_node_scripts.py runs it).
import assert from 'node:assert/strict';
import { generateKeyPairSync, verify } from 'node:crypto';
import { test } from 'node:test';
import { ascClient, AscError, distribute, makeJwt, parseArgs, pickGroup } from './testflight-distribute.mjs';

const dec = (s) => JSON.parse(Buffer.from(s, 'base64url').toString());

test('the token: ES256 with kid, aud appstoreconnect-v1, at most 20 minutes, a raw r||s signature that verifies', () => {
  const { privateKey, publicKey } = generateKeyPairSync('ec', { namedCurve: 'P-256' });
  const pem = privateKey.export({ type: 'pkcs8', format: 'pem' });
  const now = 1_763_500_000_000;
  const jwt = makeJwt({ keyId: 'ABC123DEFG', issuerId: 'iss-uuid', privateKeyPem: pem, now });
  const [h, p, s] = jwt.split('.');
  assert.deepEqual(dec(h), { alg: 'ES256', kid: 'ABC123DEFG', typ: 'JWT' });
  const body = dec(p);
  assert.equal(body.aud, 'appstoreconnect-v1');
  assert.equal(body.iss, 'iss-uuid');
  assert.equal(body.iat, Math.floor(now / 1000));
  assert.ok(body.exp > body.iat && body.exp - body.iat <= 20 * 60);
  const sig = Buffer.from(s, 'base64url');
  assert.equal(sig.length, 64);   // ieee-p1363, not DER
  assert.ok(verify('sha256', Buffer.from(`${h}.${p}`), { key: publicKey, dsaEncoding: 'ieee-p1363' }, sig));
  assert.throws(() => makeJwt({ keyId: 'k', issuerId: 'i', privateKeyPem: pem, ttlS: 21 * 60 }), /20 minutes/);
});

const grp = (id, name, internal) => ({ type: 'betaGroups', id, attributes: { name, isInternalGroup: internal } });

test('group picking: by name, the only external one, several refused by name, internal never', () => {
  const one = [grp('i1', 'Me', true), grp('e1', 'Testers', false)];
  assert.equal(pickGroup(one, null).id, 'e1');
  assert.equal(pickGroup(one, 'Testers').id, 'e1');
  assert.throws(() => pickGroup(one, 'Me'), /INTERNAL/);
  assert.throws(() => pickGroup(one, 'Nope'), /no external TestFlight group named "Nope".*"Testers"/);
  const two = [...one, grp('e2', 'Friends', false)];
  assert.throws(() => pickGroup(two, null), /several external TestFlight groups \("Testers", "Friends"\)/);
  assert.equal(pickGroup(two, 'Friends').id, 'e2');
  assert.throws(() => pickGroup([grp('i1', 'Me', true)], null), /no external TestFlight group/);
});

test('arguments', () => {
  assert.deepEqual(parseArgs(['--version', '0.8.3', '--build', '39', '--group', 'Test ers', '--dry-run']),
    { dryRun: true, version: '0.8.3', build: '39', group: 'Test ers' });
  assert.throws(() => parseArgs(['--build']), /needs a value/);
  assert.throws(() => parseArgs(['--bild', '3']), /unknown argument/);
});

/** A fake App Store Connect: routes by method + path; records every call. */
function fakeAsc({ processing = ['VALID'], inGroup = false, review = null, ext = 'READY_FOR_BETA_SUBMISSION', locs = [],
  encryption = false, groups = [grp('i1', 'Me', true), grp('e1', 'Testers', false)] } = {}) {
  const calls = [];
  let poll = 0;
  const fetchImpl = async (url, init) => {
    const path = url.replace('https://api.appstoreconnect.apple.com', '');
    const method = init.method;
    calls.push({ method, path, body: init.body ? JSON.parse(init.body) : undefined, auth: init.headers.Authorization });
    const ok = (json, status = 200) => ({ ok: true, status, text: async () => JSON.stringify(json) });
    if (method === 'GET' && path.startsWith('/v1/apps?'))
      return ok({ data: [{ type: 'apps', id: 'app1', attributes: { bundleId: 'ca.canifishthis.app', primaryLocale: 'en-CA' } }] });
    if (method === 'GET' && path.startsWith('/v1/betaGroups?')) return ok({ data: groups, links: {} });
    if (method === 'GET' && path.startsWith('/v1/builds?')) {
      const s = processing[Math.min(poll++, processing.length - 1)];
      return ok({ data: s ? [{ type: 'builds', id: 'b39', attributes: { version: '39', processingState: s, expired: false, usesNonExemptEncryption: encryption } }] : [] });
    }
    if (method === 'GET' && path.includes('/betaBuildLocalizations')) return ok({ data: locs });
    if (method === 'GET' && path.includes('/relationships/builds')) return ok({ data: inGroup ? [{ type: 'builds', id: 'b39' }] : [], links: {} });
    if (method === 'GET' && path.endsWith('/buildBetaDetail')) return ok({ data: { attributes: { externalBuildState: ext } } });
    if (method === 'GET' && path.startsWith('/v1/betaAppReviewSubmissions?'))
      return ok({ data: review ? [{ id: 's1', attributes: { betaReviewState: review } }] : [] });
    if (method === 'POST' && path === '/v1/betaAppReviewSubmissions') {
      ext = 'WAITING_FOR_BETA_REVIEW';
      return ok({ data: { id: 's1', attributes: { betaReviewState: 'WAITING_FOR_REVIEW' } } }, 201);
    }
    if (method === 'POST' || method === 'PATCH') return { ok: true, status: method === 'PATCH' ? 200 : 204, text: async () => '' };
    return { ok: false, status: 404, text: async () => JSON.stringify({ errors: [{ status: '404', code: 'NOT_FOUND', title: 'nope', detail: path }] }) };
  };
  return { calls, asc: ascClient({ token: () => 'TOKEN', fetchImpl, sleep: async () => {} }) };
}

const run = (asc, extra = {}) => {
  const lines = [];
  return distribute({ asc, version: '0.8.3', build: '39', usesNonExemptEncryption: false, whatToTest: async () => 'Version 0.8.3 (build 39)',
    pollMs: 30_000, sleep: async () => {}, log: (l) => lines.push(l), ...extra }).then((r) => ({ r, lines }));
};
const writes = (calls) => calls.filter((c) => c.method !== 'GET').map((c) => `${c.method} ${c.path}`);

test('a fresh build: waits for processing, adds What to Test, joins the external group, is submitted for review', async () => {
  const f = fakeAsc({ processing: [null, 'PROCESSING', 'VALID'] });
  const { r, lines } = await run(f.asc);
  assert.deepEqual(writes(f.calls), ['POST /v1/betaBuildLocalizations', 'POST /v1/betaGroups/e1/relationships/builds', 'POST /v1/betaAppReviewSubmissions']);
  assert.ok(f.calls.every((c) => c.auth === 'Bearer TOKEN'));
  const loc = f.calls.find((c) => c.path === '/v1/betaBuildLocalizations').body.data;
  assert.equal(loc.attributes.locale, 'en-CA');
  assert.equal(loc.relationships.build.data.id, 'b39');
  assert.deepEqual(f.calls.find((c) => c.path.endsWith('/relationships/builds') && c.method === 'POST').body, { data: [{ type: 'builds', id: 'b39' }] });
  assert.ok(f.calls.every((c) => !c.path.includes('/i1')));   // the internal group is never touched
  assert.equal(r.submitted, true);
  assert.equal(r.reviewState, 'WAITING_FOR_REVIEW');
  assert.ok(lines.some((l) => /not in App Store Connect yet/.test(l)));
  assert.ok(lines.some((l) => /processing \(PROCESSING\)/.test(l)));
  assert.match(lines.at(-1), /waiting for Beta App Review/);
});

test('already in the group, review already submitted, What to Test already there: nothing written', async () => {
  const f = fakeAsc({ inGroup: true, review: 'APPROVED', ext: 'IN_BETA_TESTING', locs: [{ id: 'l1', attributes: { locale: 'en-CA', whatToTest: 'x' } }] });
  const { r, lines } = await run(f.asc);
  assert.deepEqual(writes(f.calls), []);
  assert.equal(r.added, false);
  assert.equal(r.submitted, false);
  assert.ok(lines.some((l) => /already in "Testers"/.test(l)));
  assert.ok(lines.some((l) => /already submitted: APPROVED/.test(l)));
  assert.match(lines.at(-1), /testers can install it/);
});

test('a later build of the version Apple needs no review for: added, not submitted', async () => {
  const f = fakeAsc({ ext: 'IN_BETA_TESTING', locs: [{ id: 'l1', attributes: { locale: 'en-CA', whatToTest: '' } }] });
  const { r } = await run(f.asc);
  assert.deepEqual(writes(f.calls), ['PATCH /v1/betaBuildLocalizations/l1', 'POST /v1/betaGroups/e1/relationships/builds']);
  assert.equal(r.submitted, false);
});

test('export compliance unanswered: set from app.json; app.json without it is refused', async () => {
  const f = fakeAsc({ encryption: null, inGroup: true, review: 'APPROVED', locs: [{ id: 'l1', attributes: { whatToTest: 'x' } }] });
  await run(f.asc);
  const p = f.calls.find((c) => c.method === 'PATCH' && c.path === '/v1/builds/b39');
  assert.deepEqual(p.body.data.attributes, { usesNonExemptEncryption: false });
  const g = fakeAsc({ encryption: null });
  await assert.rejects(run(g.asc, { usesNonExemptEncryption: undefined }), /export compliance answered.*App Store Connect/);
});

test('dry run: reads only, prints what it would do', async () => {
  const f = fakeAsc({ encryption: null });
  const { lines } = await run(f.asc, { dryRun: true });
  assert.deepEqual(writes(f.calls), []);
  for (const re of [/would set usesNonExemptEncryption=false/, /would add "What to Test"/, /would add the build to "Testers"/, /would submit the build for Beta App Review/])
    assert.ok(lines.some((l) => re.test(l)), re);
});

test('processing that never ends: a bounded wait and the rerun command', async () => {
  const f = fakeAsc({ processing: ['PROCESSING'] });
  let t = 0;
  await assert.rejects(run(f.asc, { maxWaitMs: 45 * 60_000, now: () => t, sleep: async (ms) => { t += ms; }, groupName: 'Testers' }),
    (e) => e instanceof AscError && /still PROCESSING after 4[45]\.\d min \(limit 45 min\)/.test(e.message)
      && e.message.includes('node deploy/staging/testflight-distribute.mjs --version 0.8.3 --build 39 --group "Testers"'));
  const polls = f.calls.filter((c) => c.path.startsWith('/v1/builds?')).length;
  assert.ok(polls >= 89 && polls <= 91, `${polls} polls`);   // every 30 s for 45 min
});

test('a failed build stops at once; Apple\'s error text is shown', async () => {
  await assert.rejects(run(fakeAsc({ processing: ['INVALID'] }).asc), /processing INVALID/);
  const asc = ascClient({ token: () => 'T', sleep: async () => {}, fetchImpl: async () => ({ ok: false, status: 409, text: async () =>
    JSON.stringify({ errors: [{ status: '409', code: 'STATE_ERROR', title: 'Missing info', detail: 'Beta App Review contact is required' }] }) }) });
  await assert.rejects(asc.post('/v1/betaAppReviewSubmissions', {}), /409 STATE_ERROR: Missing info — Beta App Review contact is required/);
});
