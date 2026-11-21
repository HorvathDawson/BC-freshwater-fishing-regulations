#!/usr/bin/env node
// TestFlight tester feedback (owner 2026-11-21: "is there a way to have an agent get it?"). Reads the App Store Connect
// API's beta feedback for the app: screenshot submissions (the tester's comment + screenshots) and crash submissions.
// Read-only. Same key as testflight-distribute.mjs (SECRETS/asc-api-key.json); the key is never printed.
//   node deploy/staging/testflight-feedback.mjs [--days 30] [--json] [--shots <dir>]
// --shots saves each screenshot (Apple's URLs expire) as <dir>/<submission id>-<n>.png.
import { readFileSync, existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { homedir } from 'node:os';
import { API, BUNDLE_ID, makeJwt, ascClient } from './testflight-distribute.mjs';

const SECRETS = process.env.CANIFISHTHIS_SECRETS || join(homedir(), '.canifishthis');
const args = process.argv.slice(2);
const opt = (n, d) => { const i = args.indexOf(n); return i >= 0 ? args[i + 1] : d; };
const days = Number(opt('--days', '30'));
const asJson = args.includes('--json');
const shotsDir = opt('--shots', null);

const keyFile = join(SECRETS, 'asc-api-key.json');
if (!existsSync(keyFile)) { console.error(`testflight-feedback: missing ${keyFile}`); process.exit(1); }
const k = JSON.parse(readFileSync(keyFile, 'utf8'));
const pem = readFileSync(k.key_path, 'utf8');
let tok = null, tokAt = 0;
const token = () => {
  if (!tok || Date.now() - tokAt > 10 * 60_000) { tok = makeJwt({ keyId: k.key_id, issuerId: k.issuer_id, privateKeyPem: pem }); tokAt = Date.now(); }
  return tok;
};
const asc = ascClient({ token });
const since = Date.now() - days * 86_400_000;

const apps = await asc.all(`/v1/apps?filter[bundleId]=${BUNDLE_ID}`);
if (!apps.length) { console.error(`testflight-feedback: no app ${BUNDLE_ID}`); process.exit(1); }
const appId = apps[0].id;

async function list(kind) {
  try {
    return (await asc.all(`/v1/apps/${appId}/${kind}?limit=200`))
      .filter((s) => Date.parse(s.attributes?.createdDate ?? 0) >= since);
  } catch (e) {
    console.error(`testflight-feedback: ${kind}: ${e.message}`);
    return [];
  }
}
const shots = await list('betaFeedbackScreenshotSubmissions');
const crashes = await list('betaFeedbackCrashSubmissions');

if (shotsDir) {
  mkdirSync(shotsDir, { recursive: true });
  for (const s of shots) {
    for (const [n, sc] of (s.attributes?.screenshots ?? []).entries()) {
      const res = await fetch(sc.url);
      if (res.ok) writeFileSync(join(shotsDir, `${s.id}-${n + 1}.png`), Buffer.from(await res.arrayBuffer()));
    }
  }
}
if (asJson) {
  console.log(JSON.stringify({ app: appId, days, screenshots: shots, crashes }, null, 2));
} else {
  const line = (s, kind) => {
    const a = s.attributes ?? {};
    return `${a.createdDate} ${kind} ${s.id} | ${a.deviceModel ?? '?'} iOS ${a.osVersion ?? '?'} | build ${a.buildBundleId ?? ''}`
      + `\n  ${(a.comment ?? '(no comment)').replace(/\n/g, '\n  ')}`
      + ((a.screenshots ?? []).length ? `\n  screenshots: ${(a.screenshots).length}` : '');
  };
  console.log(`TestFlight feedback, last ${days} days: ${shots.length} with screenshots, ${crashes.length} crash(es)`);
  for (const s of shots) console.log(line(s, 'feedback'));
  for (const s of crashes) console.log(line(s, 'crash'));
}
void API;
