// T249 — A REQUEST FLOOD GETS 429 (STAGING ONLY; hosts hardcoded). Cheap requests only: the feeds worker gets GETs on an
// unknown path (404, no R2 read) past RL_PUBLIC's 60/min; the data worker gets OPTIONS (204, no R2 read) past RL's
// 300 per 10 s. The limits are approximate (counted per machine, synced within a Cloudflare location): a short burst can
// pass whole, so each flood keeps going until it has seen 20 × 429 or hits its cap; "some 429" passes.
// Afterwards THIS address is limited for up to a minute (the smoke test waits it out: run them a minute apart).
//   node deploy/staging/flood-test.mjs [--only feeds,data]
const FEEDS = 'https://bc-fishing-feeds-staging.horvath-dawson.workers.dev';
const DATA = 'https://data-staging.canifishthis.ca';
const args = process.argv.slice(2);
const ONLY = (args.includes('--only') ? args[args.indexOf('--only') + 1] : 'feeds,data').split(',');
let fails = 0;

async function flood(name, url, method, total, conc) {
  const codes = {};
  let first429 = null, sent = 0;
  const t0 = Date.now();
  await Promise.all(Array.from({ length: conc }, async () => {
    while (sent < total && (codes[429] ?? 0) < 20) {
      const n = ++sent;
      const r = await fetch(url, { method, headers: { 'User-Agent': 'canifishthis-flood-test/1 (staging)' } }).catch(() => null);
      const c = r ? r.status : 'err';
      await r?.body?.cancel();
      codes[c] = (codes[c] ?? 0) + 1;
      if (c === 429 && first429 === null) first429 = n;
    }
  }));
  const ok = (codes[429] ?? 0) > 0;
  console.log(`${ok ? "PASS" : "FAIL"} ${name}: ${sent} (cap ${total}) × ${method} in ${((Date.now() - t0) / 1000).toFixed(1)} s → ${JSON.stringify(codes)}` +
    (first429 ? `, first 429 at request #${first429}` : ', never 429'));
  if (!ok) fails++;
}
if (ONLY.includes('feeds')) await flood('feeds worker (RL_PUBLIC 60/min)', `${FEEDS}/flood-test-404`, 'GET', 1000, 8);
if (ONLY.includes('data')) await flood('data worker (RL 300/10 s)', `${DATA}/flood-test`, 'OPTIONS', 15000, 32);
process.exit(fails ? 1 : 0);
