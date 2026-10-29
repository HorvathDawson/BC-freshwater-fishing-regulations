// T249 — NOTHING PRIVATE IS PUBLIC (STAGING ONLY: bucket bc-fishing-regulations-staging, data-staging.canifishthis.ca).
//
//   node deploy/staging/check-public.mjs [--site-dir app/dist/staging-site]
//
// 1. Lists the bucket (rclone remote `r2:` on the Mac; in CI the AWS CLI with R2_ENDPOINT + AWS_* credentials).
// 2. Every key under private/ is asked of the PUBLIC data worker (GET) → must be 404/403; three of them also in other
//    spellings (%2F, //, ./, PRIVATE/, ..).
// 3. Every public text object (json/jsonl/txt/html/csv/xml/geojson under 5 MB; not the content-named .gz release files,
//    which the build writes from the repo) is fetched through the data worker and scanned for secrets:
//      by VALUE — RUN_TOKEN (app/workers/feeds/.run-token or $RUN_TOKEN), NOTIFY_EMAIL_TO / REPORT_SALT ($env), the R2
//                 key pair ($AWS_* or the rclone remote's), and every reporter email in private/reports/ (read locally,
//                 never printed);
//      by SHAPE — a Bearer token, a PEM private key, a "token|secret|password|api_key": "<long>" field.
//    A hit prints the object key and the secret's NAME, never its value.
// 4. --site-dir: the same by-value scan over the built site folder (what staging.canifishthis.ca serves).
// Exit 1 on any finding. Runs by hand, after upload.sh, and in .github/workflows/staging-security.yml.
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

const BUCKET = 'bc-fishing-regulations-staging';   // hardcoded: never a production bucket
const DATA = 'https://data-staging.canifishthis.ca';
const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; };
const SITE_DIR = opt('--site-dir', null);
const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '../..');
let fails = 0;
const out = (ok, what) => { console.log(`${ok ? 'PASS' : 'FAIL'} ${what}`); if (!ok) fails++; };

function hasRclone() {
  try { return execFileSync('rclone', ['listremotes'], { encoding: 'utf8' }).split('\n').includes('r2:'); } catch { return false; }
}
const RCLONE = hasRclone();
function list() {
  if (RCLONE) return JSON.parse(execFileSync('rclone', ['lsjson', '-R', '--files-only', '--no-mimetype', '--no-modtime', `r2:${BUCKET}`,
    '--s3-no-check-bucket'], { encoding: 'utf8', maxBuffer: 1 << 28 })).map((o) => ({ key: o.Path, size: o.Size }));
  if (!process.env.R2_ENDPOINT) throw new Error('no rclone remote r2: and no R2_ENDPOINT for the AWS CLI');
  const j = JSON.parse(execFileSync('aws', ['s3api', 'list-objects-v2', '--bucket', BUCKET, '--endpoint-url', process.env.R2_ENDPOINT, '--output', 'json'], { encoding: 'utf8', maxBuffer: 1 << 28 }));
  return (j.Contents ?? []).map((o) => ({ key: o.Key, size: o.Size }));
}
function readPrivate(key) {
  try {
    if (RCLONE) return execFileSync('rclone', ['cat', `r2:${BUCKET}/${key}`, '--s3-no-check-bucket'], { encoding: 'utf8', maxBuffer: 1 << 26 });
    return execFileSync('aws', ['s3', 'cp', `s3://${BUCKET}/${key}`, '-', '--endpoint-url', process.env.R2_ENDPOINT], { encoding: 'utf8', maxBuffer: 1 << 26 });
  } catch { return ''; }
}
/** the secret VALUES to look for, by name (never printed) */
function secrets(privateKeys) {
  const s = [];
  const add = (name, v) => { if (v && String(v).trim().length >= 6) s.push({ name, value: String(v).trim() }); };
  try { add('RUN_TOKEN', fs.readFileSync(path.join(ROOT, 'app/workers/feeds/.run-token'), 'utf8')); } catch {}
  add('RUN_TOKEN (env)', process.env.RUN_TOKEN);
  add('NOTIFY_EMAIL_TO', process.env.NOTIFY_EMAIL_TO);
  add('REPORT_SALT', process.env.REPORT_SALT);
  add('R2 access key id', process.env.AWS_ACCESS_KEY_ID);
  add('R2 secret access key', process.env.AWS_SECRET_ACCESS_KEY);
  if (RCLONE) try {
    const c = JSON.parse(execFileSync('rclone', ['config', 'dump'], { encoding: 'utf8' })).r2 ?? {};
    add('R2 access key id (rclone)', c.access_key_id); add('R2 secret access key (rclone)', c.secret_access_key);
  } catch {}
  // reporter emails (T237): a report's email must never reach a public object
  const reports = privateKeys.filter((k) => /^private\/reports\/\d{4}-\d{2}-\d{2}\/[^/]+\.json$/.test(k)).slice(0, 500);
  for (const k of reports) { try { add('a reporter email', JSON.parse(readPrivate(k)).email); } catch {} }
  return s;
}
const SHAPES = [
  ['a Bearer token', /Bearer\s+[A-Za-z0-9._~+/-]{16,}/],
  ['a PEM private key', /-----BEGIN [A-Z ]*PRIVATE KEY-----/],
  ['a secret-named field', /"(?:[a-z_]*token|secret|password|passwd|api[_-]?key|access[_-]?key[_-]?id|secret[_-]?access[_-]?key)"\s*:\s*"[^"]{12,}"/i],
];
async function pool(items, n, fn) { const it = items[Symbol.iterator](); await Promise.all(Array.from({ length: n }, async () => { for (const x of it) await fn(x); })); }
const get = (k, init) => fetch(`${DATA}/${k}`, { redirect: 'manual', headers: { 'User-Agent': 'canifishthis-check-public/1 (staging)' }, ...init });

const objs = list();
const priv = objs.filter((o) => o.key.startsWith('private/'));
const pub = objs.filter((o) => !o.key.startsWith('private/'));
console.log(`bucket ${BUCKET}: ${objs.length} objects (${priv.length} private) via ${RCLONE ? 'rclone' : 'aws cli'}`);

// 2. private/ is never served
const served = [];
await pool(priv.map((o) => o.key), 8, async (k) => { const r = await get(k.split('/').map(encodeURIComponent).join('/')); if (r.status !== 404 && r.status !== 403) served.push(`${k} → ${r.status}`); await r.body?.cancel(); });
const sample = priv.slice(0, 3).map((o) => o.key);
for (const k of sample) for (const v of [k.replaceAll('/', '%2F'), `/${k}`, `./${k}`, k.replace(/^private/, 'PRIVATE'), `feeds/%2e%2e/${k}`]) {
  const r = await get(v); if (r.status !== 404 && r.status !== 403) served.push(`${v} → ${r.status}`); await r.body?.cancel();
}
out(!served.length, `private/ not served: ${priv.length} keys + ${sample.length * 5} spellings → 404${served.length ? ` — SERVED: ${served.slice(0, 5).join(', ')}` : ''}`);

// 3. no secret in a public object
const SECRETS = secrets(priv.map((o) => o.key));
const text = pub.filter((o) => /\.(json|jsonl|txt|html?|csv|xml|geojsonl?)$/i.test(o.key) && o.size < 5e6);
const hits = [];
await pool(text, 8, async (o) => {
  const r = await get(o.key.split('/').map(encodeURIComponent).join('/'));
  if (r.status !== 200) { hits.push(`${o.key}: HTTP ${r.status} (public object not served)`); await r.body?.cancel(); return; }
  const t = await r.text();
  for (const s of SECRETS) if (t.includes(s.value)) hits.push(`${o.key}: contains ${s.name}`);
  for (const [name, re] of SHAPES) if (re.test(t)) hits.push(`${o.key}: looks like ${name}`);
});
out(!hits.length, `no secret in ${text.length} public text objects (${SECRETS.length} secret values by name: ${[...new Set(SECRETS.map((s) => s.name))].join(', ') || 'none found locally'}; ${SHAPES.length} shapes)${hits.length ? ` — ${hits.slice(0, 8).join('; ')}` : ''}`);

// 4. the built site
if (SITE_DIR) {
  const vals = SECRETS.map((s) => s.value);
  let found = [];
  if (vals.length) {
    const pat = path.join(ROOT, 'app/dist/.secret-patterns'); fs.writeFileSync(pat, vals.join('\n') + '\n', { mode: 0o600 });
    try { found = execFileSync('grep', ['-rlF', '-f', pat, SITE_DIR], { encoding: 'utf8' }).trim().split('\n').filter(Boolean); } catch { found = []; }
    finally { fs.rmSync(pat, { force: true }); }
  }
  out(!found.length, `no secret value in ${SITE_DIR}${found.length ? ` — in ${found.slice(0, 5).join(', ')}` : ''}`);
}
process.exit(fails ? 1 : 0);
