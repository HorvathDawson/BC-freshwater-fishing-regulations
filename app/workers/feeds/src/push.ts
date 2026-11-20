// (T739) PUSH "WAKE-UPS": a content-free "check now" push to every registered phone when a feed changes or a new release
// is published. The phone then runs the SAME background check it runs on its own schedule (apps/mobile notify.native.ts,
// packages/data notify/check.ts) and raises its own local notification if one of its rules fires. WHAT THIS SERVER
// LEARNS: a device push token, its platform and its APNs environment — never the alert rules, the watched waters or a
// location (owner 2026-11-14). A token the push service calls dead is deleted; unregister deletes it at once.
// IPHONE ONLY (owner 2026-11-16 12:08, on Firebase for Android: "we dont want this right now"): Android registers no
// token, `platform: "android"` is refused (400) and there is no FCM sender.
//
//   POST /push/register    {platform: "ios", token, env: "sandbox"|"production"}  (public, rate-limited per IP
//   POST /push/unregister  {platform, token, env}                                 like POST /crash)
//   POST /run/push         (Bearer RUN_TOKEN) send one wake now, `?reason=` for the record
//
//   iPhone   APNs HTTP/2 with a provider token: an ES256 JWT made with WebCrypto from the secrets APNS_KEY (the .p8 text),
//            APNS_KEY_ID, APNS_TEAM_ID; topic = the bundle id; `apns-push-type: background`, `apns-priority: 5`, payload
//            {"aps":{"content-available":1},"k":"check"}; the sandbox host for a token registered as "sandbox".
//   The sender sends NOTHING while a secret is missing, and says so (`missing`) in GET /status(.json) — never a silent
//   skip. Every send's counts (sent / dead / failed, the first errors) are kept at PUSH_STATUS_KEY and shown there; a
//   send that throws is kept there too (`writePushFailure`: {at, reason, ok: false, error}), and a status that cannot
//   be written fails the run (review 2026-11-20, M2).
//
// Stored at private/push/tokens/<sha256(token)>.json — never backed up (backup.ts `personalKind`: "never"): a lost store
// is rebuilt by the phones: one with notifications on registers its token again when it changed, or when its last
// register is over PUSH_REFRESH_MS (7 days) old (packages/data notify/push.ts).
import type { R2BucketLike } from "./io";
import { LIMIT_PREFIXES, listKeys, rateLimit, readCapped, sha256hex, noSalt, type ReportEnv } from "./reports";

export const PUSH_PREFIX = "private/push/";
export const TOKENS_PREFIX = `${PUSH_PREFIX}tokens/`;
/** the per-address counters (a salted hash of the address); `purgeLimits` deletes them once their day is over */
export const PUSH_LIMITS_PREFIX = LIMIT_PREFIXES[2];
/** the last send: when, why, per platform configured / missing / tokens / sent / dead / failed (GET /status shows it) */
export const PUSH_STATUS_KEY = `${PUSH_PREFIX}last.json`;
/** what the sender last saw of the release (the manifest's data_release), so a new release wakes the phones once */
export const PUSH_STATE_KEY = `${PUSH_PREFIX}state.json`;
/** the release manifest the app's update check reads (app/apps/mobile env.ts `endpointsFor(...).manifest`, live channel) */
export const MANIFEST_KEY = "release/manifest.json";

export const APNS_TOPIC = "ca.canifishthis.app";
export const APNS_HOSTS = { production: "https://api.push.apple.com", sandbox: "https://api.sandbox.push.apple.com" } as const;
/** the whole silent payload: no alert, no sound, no words — "check now" and nothing else */
export const APNS_PAYLOAD = { aps: { "content-available": 1 }, k: "check" } as const;
/** a wake older than this is not worth delivering (the next feed change sends another) */
export const PUSH_TTL_S = 3600;

export const PUSH_PLATFORMS = ["ios"] as const;
export type PushPlatform = (typeof PUSH_PLATFORMS)[number];
export const PUSH_ENVS = ["sandbox", "production"] as const;
export type PushEnvName = (typeof PUSH_ENVS)[number];

export interface PushRecord { format: "push-token/1"; platform: PushPlatform; token: string; env: PushEnvName; at: string }

export interface PushEnv extends ReportEnv {
  APNS_KEY?: string;
  APNS_KEY_ID?: string;
  APNS_TEAM_ID?: string;
}

/**
 * Per address (an IPv4 address or an IPv6 /64): one call every 2 s, 200 a day. A phone registers when its token, its
 * environment or our address changed, or once its last register is 7 days old (review 2026-11-20, L1: it was every
 * app start, and phones behind one address relaunching together on a wake met the 2 s gap), plus a switch turned on or
 * off. Over the limit the app is answered 429, waits once, then says "pending" and registers again at its next start.
 * Unregister is counted the same way.
 */
export const PUSH_GAP_MS = 2_000;
export const PUSH_PER_DAY = 200;
const MAX_BODY = 1024;
/** in flight at once while sending */
const PARALLEL = 20;
/** first errors kept per platform in the status */
const KEEP_ERRORS = 5;

const json = (body: unknown, status: number) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json", "Cache-Control": "no-store" } });

/** The payload, checked — or why it is refused (a 400). APNs tokens are hex (32 bytes today). Android is refused (no push). */
export function validatePush(raw: unknown): { ok: true; platform: PushPlatform; token: string; env: PushEnvName } | { ok: false; error: string } {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return { ok: false, error: "body must be a JSON object" };
  const o = raw as Record<string, unknown>;
  const extra = Object.keys(o).filter((k) => !["platform", "token", "env"].includes(k));
  if (extra.length) return { ok: false, error: `unknown field(s): ${extra.join(", ")}` };
  if (o.platform === "android") return { ok: false, error: "no push on Android" };
  if (!(PUSH_PLATFORMS as readonly unknown[]).includes(o.platform)) return { ok: false, error: "platform must be ios" };
  const platform = o.platform as PushPlatform;
  if (typeof o.token !== "string") return { ok: false, error: "token must be a string" };
  const token = o.token;
  if (!/^[0-9a-f]{64,200}$/i.test(token)) return { ok: false, error: "an iOS token is 64 to 200 hex characters" };
  const env = o.env;
  if (!(PUSH_ENVS as readonly unknown[]).includes(env)) return { ok: false, error: "env must be sandbox or production" };
  return { ok: true, platform, token: token.toLowerCase(), env: env as PushEnvName };
}

export const tokenKey = async (token: string) => `${TOKENS_PREFIX}${(await sha256hex(token)).slice(0, 40)}.json`;

/** POST /push/register and /push/unregister */
export async function handlePush(req: Request, env: PushEnv, action: "register" | "unregister", now: Date = new Date()): Promise<Response> {
  const unset = noSalt(env);
  if (unset) return unset;
  let raw: string | null;
  try { raw = await readCapped(req, MAX_BODY); } catch { return json({ ok: false, error: "unreadable body" }, 400); }
  if (raw === null) return json({ ok: false, error: "too large" }, 413);
  let body: unknown;
  try { body = JSON.parse(raw); } catch { return json({ ok: false, error: "not JSON" }, 400); }
  const v = validatePush(body);
  if (!v.ok) return json({ ok: false, error: v.error }, 400);
  const ip = req.headers.get("CF-Connecting-IP") ?? "unknown";
  let refused: string | null;
  try {
    refused = await rateLimit(env, ip, now, { prefix: PUSH_LIMITS_PREFIX, gapMs: PUSH_GAP_MS, perDay: PUSH_PER_DAY, edge: false });
  } catch (e) {
    console.error(JSON.stringify({ job: "push", limitError: String(e) }));
    return json({ ok: false, error: "could not count" }, 503);
  }
  if (refused) return json({ ok: false, error: refused.replace("reports", "changes") }, 429);
  const key = await tokenKey(v.token);
  try {
    if (action === "unregister") { await env.BUCKET.delete(key); return json({ ok: true }, 200); }
    const rec: PushRecord = { format: "push-token/1", platform: v.platform, token: v.token, env: v.env, at: now.toISOString() };
    await env.BUCKET.put(key, JSON.stringify(rec), { httpMetadata: { contentType: "application/json" } });
  } catch (e) {
    console.error(JSON.stringify({ job: "push", storeError: String(e) }));
    return json({ ok: false, error: "could not store" }, 500);
  }
  return json({ ok: true }, 201);
}

// ── JWTs with WebCrypto ─────────────────────────────────────────────────────────────────────────────────────────────

const b64url = (b: Uint8Array | string) => {
  const bytes = typeof b === "string" ? new TextEncoder().encode(b) : b;
  let s = "";
  for (const x of bytes) s += String.fromCharCode(x);
  return btoa(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
};
/** the DER bytes of a PEM private key (PKCS#8: "-----BEGIN PRIVATE KEY-----", as Apple's .p8 holds it) */
export function pemBytes(pem: string): Uint8Array<ArrayBuffer> {
  const body = pem.replace(/\\n/g, "\n").replace(/-----(BEGIN|END) [A-Z ]+-----/g, "").replace(/\s+/g, "");
  if (!body) throw new Error("the key is empty");
  const s = atob(body);
  const out = new Uint8Array(new ArrayBuffer(s.length));
  for (let i = 0; i < s.length; i++) out[i] = s.charCodeAt(i);
  return out;
}

async function signJwt(header: Record<string, unknown>, claims: Record<string, unknown>, key: CryptoKey, alg: AlgorithmIdentifier | EcdsaParams): Promise<string> {
  const input = `${b64url(JSON.stringify(header))}.${b64url(JSON.stringify(claims))}`;
  // WebCrypto's ECDSA signature is IEEE P1363 (r || s, 64 bytes): exactly what JWS ES256 wants
  const sig = new Uint8Array(await crypto.subtle.sign(alg, key, new TextEncoder().encode(input)));
  return `${input}.${b64url(sig)}`;
}

/** APNs' provider token: ES256, `kid` = the key id, `iss` = the team id, `iat` = now (APNs takes it for up to an hour) */
export async function apnsJwt(e: { APNS_KEY: string; APNS_KEY_ID: string; APNS_TEAM_ID: string }, now: Date): Promise<string> {
  const key = await crypto.subtle.importKey("pkcs8", pemBytes(e.APNS_KEY), { name: "ECDSA", namedCurve: "P-256" }, false, ["sign"]);
  return signJwt({ alg: "ES256", kid: e.APNS_KEY_ID }, { iss: e.APNS_TEAM_ID, iat: Math.floor(now.getTime() / 1000) }, key,
                 { name: "ECDSA", hash: "SHA-256" });
}

// ── the requests, whole ─────────────────────────────────────────────────────────────────────────────────────────────

export function apnsRequest(rec: Pick<PushRecord, "token" | "env">, jwt: string, now: Date): { url: string; init: RequestInit } {
  return {
    url: `${APNS_HOSTS[rec.env]}/3/device/${rec.token}`,
    init: {
      method: "POST",
      headers: {
        authorization: `bearer ${jwt}`, "apns-topic": APNS_TOPIC, "apns-push-type": "background", "apns-priority": "5",
        "apns-expiration": String(Math.floor(now.getTime() / 1000) + PUSH_TTL_S), "content-type": "application/json",
      },
      body: JSON.stringify(APNS_PAYLOAD),
    },
  };
}

type Outcome = "sent" | "dead" | { failed: string };
type Fetch = (u: string, i?: RequestInit) => Promise<Response>;

/** APNs: 200 sent; 410 (Unregistered) or 400 BadDeviceToken / DeviceTokenNotForTopic → dead; anything else failed */
export async function apnsOutcome(res: Response): Promise<Outcome> {
  if (res.status === 200) return "sent";
  let reason = "";
  try { reason = String(((await res.json()) as { reason?: string }).reason ?? ""); } catch { /* no body */ }
  if (res.status === 410 || (res.status === 400 && (reason === "BadDeviceToken" || reason === "DeviceTokenNotForTopic"))) return "dead";
  return { failed: `APNs ${res.status}${reason ? ` ${reason}` : ""}` };
}

/** which secrets a platform still lacks ([] = it can send) */
export function pushMissing(env: PushEnv): Record<PushPlatform, string[]> {
  return {
    ios: (["APNS_KEY", "APNS_KEY_ID", "APNS_TEAM_ID"] as const).filter((k) => !env[k]?.trim()),
  };
}

export interface PlatformSend { configured: boolean; missing: string[]; tokens: number; sent: number; dead: number; failed: number; errors: string[] }
export interface PushSend {
  at: string; reason: string; ok: boolean; ios: PlatformSend; error?: string;
  /** token records that were stored but did not parse, or are not an iPhone's: a fault (counted, logged, shown on
   *  /status), each deleted */
  unreadable: number;
}

const blankSend = (missing: string[]): PlatformSend => ({ configured: !missing.length, missing, tokens: 0, sent: 0, dead: 0, failed: 0, errors: [] });

/**
 * Send one wake to every registered token. A platform with a secret missing sends nothing and says `missing`; a token
 * the service calls dead is deleted; a failure is counted with its first errors. The result is kept at PUSH_STATUS_KEY.
 */
export async function sendWake(env: PushEnv, fetch_: Fetch, now: Date, reason: string): Promise<PushSend> {
  const missing = pushMissing(env);
  const out: PushSend = { at: now.toISOString(), reason, ok: true, ios: blankSend(missing.ios), unreadable: 0 };
  const keys = await listKeys(env.BUCKET, TOKENS_PREFIX, 100_000);
  const recs: { key: string; rec: PushRecord }[] = [];
  for (const key of keys) {
    const o = await env.BUCKET.get(key);
    if (!o) continue;
    try {
      const rec = JSON.parse(await o.text()) as PushRecord;
      if (rec?.platform !== "ios") throw new Error(`not an iPhone's token (platform ${JSON.stringify(rec?.platform)})`);
      recs.push({ key, rec });
    } catch (e) {
      // a record we wrote that does not parse (or names a platform with no push) is a fault: counted and said; deleted
      // (an iPhone registers again at its next start)
      out.unreadable++;
      console.error(JSON.stringify({ job: "push", unreadableToken: key, error: String((e as Error)?.message ?? e) }));
      await env.BUCKET.delete(key);
    }
  }
  out.ios.tokens = recs.length;
  // one credential per send (made only when there is someone to send to)
  let apns: string | null = null;
  const fail = (p: PlatformSend, why: string) => { p.failed++; if (p.errors.length < KEEP_ERRORS) p.errors.push(why.slice(0, 200)); };
  try {
    if (out.ios.configured && out.ios.tokens) apns = await apnsJwt(env as Required<Pick<PushEnv, "APNS_KEY" | "APNS_KEY_ID" | "APNS_TEAM_ID">>, now);
  } catch (e) {
    out.ios.errors.push(`APNs key: ${String((e as Error)?.message ?? e).slice(0, 200)}`); out.ios.failed = out.ios.tokens;
    console.error(JSON.stringify({ job: "push", apnsKey: String((e as Error)?.message ?? e) }));
  }

  const one = async ({ key, rec }: { key: string; rec: PushRecord }) => {
    const p = out.ios;
    if (!apns) return;   // missing secret (said in `missing`) or a key that failed (counted above)
    let r: Outcome;
    try {
      const q = apnsRequest(rec, apns, now);
      r = await apnsOutcome(await fetch_(q.url, q.init));
    } catch (e) { r = { failed: `network: ${String((e as Error)?.message ?? e)}` }; }
    if (r === "sent") p.sent++;
    else if (r === "dead") { p.dead++; await env.BUCKET.delete(key); }
    else fail(p, r.failed);
  };
  for (let i = 0; i < recs.length; i += PARALLEL) await Promise.all(recs.slice(i, i + PARALLEL).map(one));
  out.ok = !out.ios.failed && !out.unreadable;
  // (review 2026-11-20, M2) a send whose record cannot be kept THROWS (after the sends): the caller tries the failure
  // record and fails the run, so a lost status is never silent
  try {
    await env.BUCKET.put(PUSH_STATUS_KEY, JSON.stringify(out), { httpMetadata: { contentType: "application/json", cacheControl: "no-store" } });
  } catch (e) {
    throw Object.assign(new Error(`push: sent (${out.ios.sent} of ${out.ios.tokens}) but the status was not written (${String((e as Error)?.message ?? e)})`), { send: out });
  }
  const log = { job: "push", reason, ios: { ...out.ios, errors: out.ios.errors.length } };
  if (out.ok) console.log(JSON.stringify(log)); else console.error(JSON.stringify({ ...log, errors: out.ios.errors }));
  for (const p of PUSH_PLATFORMS) if (out[p].tokens && out[p].missing.length) console.error(JSON.stringify({ job: "push", platform: p, notSent: out[p].tokens, missing: out[p].missing }));
  return out;
}

/**
 * (review 2026-11-20, M2) A wake that threw before its result was kept: `{at, reason, ok: false, error}` at PUSH_STATUS_KEY,
 * so /status says the last wake failed and why. Throws when that cannot be written (the caller fails the run).
 */
export async function writePushFailure(bucket: R2BucketLike, at: Date, reason: string, error: string): Promise<void> {
  await bucket.put(PUSH_STATUS_KEY, JSON.stringify({ at: at.toISOString(), reason, ok: false, error: error.slice(0, 500) }),
                   { httpMetadata: { contentType: "application/json", cacheControl: "no-store" } });
}

/**
 * (T739, owner 2026-11-14 "if new data is there we could send a notification") Has a new release been published since the
 * sender last looked? Reads the live manifest from the bucket; the first look only records it (no wake for the release
 * every phone already has). Throws when the manifest cannot be read or parsed (the caller says so in the run).
 * (review 2026-11-20, M2) A NEW release is NOT recorded here: the caller records it (`markReleaseSeen`) only once the
 * wake for it was sent ok, so a send that throws or fails is tried again at the next gauge run.
 */
export async function newRelease(bucket: R2BucketLike): Promise<string | null> {
  const o = await bucket.get(MANIFEST_KEY);
  if (!o) throw new Error(`${MANIFEST_KEY} is not in the bucket`);
  let rel: unknown;
  try { rel = (JSON.parse(await o.text()) as { data_release?: unknown }).data_release; } catch (e) { throw new Error(`${MANIFEST_KEY} does not parse: ${String(e)}`); }
  if (typeof rel !== "string" || !rel) throw new Error(`${MANIFEST_KEY} names no data_release`);
  const s = await bucket.get(PUSH_STATE_KEY);
  let seen: string | null = null;
  // a missing record is a first look; one that exists but does not parse is a fault: it is replaced with the release
  // now published and the run is told (a step error on /status) — no wake for this release, said, never silent
  if (s) {
    const text = await s.text();
    try { seen = (JSON.parse(text) as { release?: string }).release ?? null; } catch (e) {
      await bucket.put(PUSH_STATE_KEY, JSON.stringify({ release: rel }), { httpMetadata: { contentType: "application/json" } });
      throw new Error(`${PUSH_STATE_KEY} did not parse (${String((e as Error)?.message ?? e)}); replaced with ${rel}, so no wake was sent for it`);
    }
  }
  if (seen === rel) return null;
  if (seen === null) { await markReleaseSeen(bucket, rel); return null; }   // the first look: recorded, no wake
  return rel;
}

/** (M2) the release whose wake was sent: the next look does not wake for it again */
export async function markReleaseSeen(bucket: R2BucketLike, rel: string): Promise<void> {
  await bucket.put(PUSH_STATE_KEY, JSON.stringify({ release: rel }), { httpMetadata: { contentType: "application/json" } });
}

/**
 * (review 2026-11-20, M1) Did a DFO run change what a phone reads? The published answers compared with the last feed's:
 * every place's answers (`locs`), the verdict and gear tables they point into, the issues, and each region's dated
 * state (`asof`, `stale`, `held`). Not the run's own bookkeeping (`at`, `checked`, `failures`, the page hash, which regions
 * it asked: `issues.unread`) and not its notices: an unchanged region's notices are re-offered every run (dfo/live.ts,
 * the outbox tells a key once) and a one-region retry counts nothing as changed. No last feed: changed. Compared with
 * keys sorted: the last feed is read back from its published bytes, the new one is the run's object (other key order).
 */
const stable = (v: unknown): string => JSON.stringify(v, (_k, x: unknown) =>
  x && typeof x === "object" && !Array.isArray(x)
    ? Object.fromEntries(Object.keys(x as object).sort().map((k) => [k, (x as Record<string, unknown>)[k]])) : x);
export function dfoAnswersChanged(prev: unknown, next: unknown): boolean {
  if (!prev || typeof prev !== "object") return true;
  const nx = (next ?? {}) as Record<string, unknown>;
  // the issues the app shows are those of the regions READ in a feed (`salmonIssues.ts` hides an `unread` region's); a
  // run lists only the regions it read, so the waters compared are those of the regions this run read. `no_water` is
  // left out: no stretch can show it.
  const unreadNext = ((nx.issues as { unread?: Record<string, string> } | undefined)?.unread) ?? {};
  const read = new Set(Object.keys((nx.regions ?? {}) as object).filter((s) => !(s in unreadNext)));
  const view = (ix: Record<string, unknown>) => {
    const regions: Record<string, unknown> = {};
    for (const [slug, r] of Object.entries((ix.regions ?? {}) as Record<string, Record<string, unknown>>))
      regions[slug] = { asof: r?.asof ?? null, stale: r?.stale ?? null, held: r?.held ?? null };
    const waters = Object.entries(((ix.issues as { waters?: Record<string, { r?: string }> } | undefined)?.waters) ?? {})
      .filter(([, w]) => read.has(String(w?.r))).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    return stable({ locs: ix.locs ?? null, verdicts: ix.verdicts ?? null, gears: ix.gears ?? null, waters, regions });
  };
  return view(prev as Record<string, unknown>) !== view(nx);
}

/** What a feed run changed that a phone's check reads, in a word for the record — or null (no wake). */
export function wakeReason(job: "gauges" | "dfo" | "inseason", ok: boolean, counts: Record<string, number>): string | null {
  if (!ok) return null;
  if (job === "gauges") return (counts.stations ?? 0) > 0 && (counts.unchanged ?? 0) < (counts.asked ?? 0) ? "gauges" : null;
  // (M1) only answers that changed this run (`dfoAnswersChanged`); notices and region counts never wake
  if (job === "dfo") return (counts.answersChanged ?? 0) > 0 ? "dfo" : null;
  return (counts.new ?? 0) + (counts.changed ?? 0) + (counts.removed ?? 0) > 0 ? "inseason" : null;
}
