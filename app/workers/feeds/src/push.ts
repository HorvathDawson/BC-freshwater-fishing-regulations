// (T739) PUSH "WAKE-UPS": a content-free "check now" push to every registered phone when a feed changes or a new release
// is published. The phone then runs the SAME background check it runs on its own schedule (apps/mobile notify.native.ts,
// packages/data notify/check.ts) and raises its own local notification if one of its rules fires. WHAT THIS SERVER
// LEARNS: a device push token, its platform and its APNs environment — never the alert rules, the watched waters or a
// location (owner 2026-11-14). A token the push service calls dead is deleted; unregister deletes it at once.
//
//   POST /push/register    {platform: "ios"|"android", token, env: "sandbox"|"production"}  (public, rate-limited per IP
//   POST /push/unregister  {platform, token, env}                                            like POST /crash)
//   POST /run/push         (Bearer RUN_TOKEN) send one wake now, `?reason=` for the record
//
//   iPhone   APNs HTTP/2 with a provider token: an ES256 JWT made with WebCrypto from the secrets APNS_KEY (the .p8 text),
//            APNS_KEY_ID, APNS_TEAM_ID; topic = the bundle id; `apns-push-type: background`, `apns-priority: 5`, payload
//            {"aps":{"content-available":1},"k":"check"}; the sandbox host for a token registered as "sandbox".
//   Android  FCM HTTP v1: the secret FCM_SERVICE_ACCOUNT (the service account's JSON) → an OAuth token (RS256 JWT,
//            WebCrypto) → a data-only, high-priority message {k: "check"}.
//   A platform whose secrets are missing sends NOTHING and says so (`missing`) in GET /status(.json) — never a silent
//   skip. Every send's counts (sent / dead / failed, the first errors) are kept at PUSH_STATUS_KEY and shown there.
//
// Stored at private/push/tokens/<sha256(token)>.json — never backed up (backup.ts `personalKind`: "never"): a lost store
// is rebuilt by the phones, which register again at every start.
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
export const FCM_SCOPE = "https://www.googleapis.com/auth/firebase.messaging";

export const PUSH_PLATFORMS = ["ios", "android"] as const;
export type PushPlatform = (typeof PUSH_PLATFORMS)[number];
export const PUSH_ENVS = ["sandbox", "production"] as const;
export type PushEnvName = (typeof PUSH_ENVS)[number];

export interface PushRecord { format: "push-token/1"; platform: PushPlatform; token: string; env: PushEnvName; at: string }

export interface PushEnv extends ReportEnv {
  APNS_KEY?: string;
  APNS_KEY_ID?: string;
  APNS_TEAM_ID?: string;
  FCM_SERVICE_ACCOUNT?: string;
}

/** per address: one change every 2 s, 20 a day (a reader toggling notifications, a token turning over) */
export const PUSH_GAP_MS = 2_000;
export const PUSH_PER_DAY = 20;
const MAX_BODY = 1024;
/** in flight at once while sending */
const PARALLEL = 20;
/** first errors kept per platform in the status */
const KEEP_ERRORS = 5;

const json = (body: unknown, status: number) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json", "Cache-Control": "no-store" } });

/** The payload, checked — or why it is refused (a 400). APNs tokens are hex (32 bytes today); FCM tokens are url-safe text. */
export function validatePush(raw: unknown): { ok: true; platform: PushPlatform; token: string; env: PushEnvName } | { ok: false; error: string } {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return { ok: false, error: "body must be a JSON object" };
  const o = raw as Record<string, unknown>;
  const extra = Object.keys(o).filter((k) => !["platform", "token", "env"].includes(k));
  if (extra.length) return { ok: false, error: `unknown field(s): ${extra.join(", ")}` };
  if (!(PUSH_PLATFORMS as readonly unknown[]).includes(o.platform)) return { ok: false, error: "platform must be ios or android" };
  const platform = o.platform as PushPlatform;
  if (typeof o.token !== "string") return { ok: false, error: "token must be a string" };
  const token = o.token;
  if (platform === "ios" && !/^[0-9a-f]{64,200}$/i.test(token)) return { ok: false, error: "an iOS token is 64 to 200 hex characters" };
  if (platform === "android" && !/^[A-Za-z0-9_:\-.]{32,4096}$/.test(token)) return { ok: false, error: "an Android token is 32 to 4096 url-safe characters" };
  const env = o.env ?? (platform === "android" ? "production" : undefined);
  if (!(PUSH_ENVS as readonly unknown[]).includes(env)) return { ok: false, error: "env must be sandbox or production" };
  if (platform === "android" && env !== "production") return { ok: false, error: "an Android token's env is production" };
  return { ok: true, platform, token: platform === "ios" ? token.toLowerCase() : token, env: env as PushEnvName };
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
/** the DER bytes of a PEM private key (PKCS#8: "-----BEGIN PRIVATE KEY-----", as Apple's .p8 and Google's JSON hold it) */
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

export interface ServiceAccount { client_email: string; private_key: string; project_id: string; token_uri?: string }
export function parseServiceAccount(text: string): ServiceAccount {
  let j: Record<string, unknown>;
  try { j = JSON.parse(text); } catch { throw new Error("FCM_SERVICE_ACCOUNT is not JSON"); }
  for (const k of ["client_email", "private_key", "project_id"]) if (typeof j[k] !== "string" || !j[k]) throw new Error(`FCM_SERVICE_ACCOUNT has no ${k}`);
  return j as unknown as ServiceAccount;
}

/** Google's OAuth assertion for FCM: RS256, iss = the service account, scope firebase.messaging, one hour */
export async function fcmAssertion(sa: ServiceAccount, now: Date): Promise<string> {
  const key = await crypto.subtle.importKey("pkcs8", pemBytes(sa.private_key), { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" }, false, ["sign"]);
  const iat = Math.floor(now.getTime() / 1000);
  return signJwt({ alg: "RS256", typ: "JWT" }, { iss: sa.client_email, scope: FCM_SCOPE, aud: sa.token_uri ?? "https://oauth2.googleapis.com/token", iat, exp: iat + 3600 },
                 key, { name: "RSASSA-PKCS1-v1_5" });
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

export function fcmMessage(token: string) {
  return { message: { token, data: { k: "check" }, android: { priority: "HIGH", ttl: `${PUSH_TTL_S}s`, collapse_key: "check" } } };
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

/** FCM v1: 200 sent; an UNREGISTERED error (404 NOT_FOUND) → dead; anything else failed */
export async function fcmOutcome(res: Response): Promise<Outcome> {
  if (res.ok) return "sent";
  let j: { error?: { status?: string; details?: { errorCode?: string }[] } } = {};
  try { j = await res.json(); } catch { /* no body */ }
  const code = j.error?.details?.find((d) => d.errorCode)?.errorCode ?? j.error?.status ?? "";
  if (code === "UNREGISTERED" || (res.status === 404 && code === "NOT_FOUND")) return "dead";
  return { failed: `FCM ${res.status}${code ? ` ${code}` : ""}` };
}

/** which secrets a platform still lacks ([] = it can send) */
export function pushMissing(env: PushEnv): Record<PushPlatform, string[]> {
  return {
    ios: (["APNS_KEY", "APNS_KEY_ID", "APNS_TEAM_ID"] as const).filter((k) => !env[k]?.trim()),
    android: env.FCM_SERVICE_ACCOUNT?.trim() ? [] : ["FCM_SERVICE_ACCOUNT"],
  };
}

export interface PlatformSend { configured: boolean; missing: string[]; tokens: number; sent: number; dead: number; failed: number; errors: string[] }
export interface PushSend { at: string; reason: string; ok: boolean; ios: PlatformSend; android: PlatformSend; error?: string }

const blankSend = (missing: string[]): PlatformSend => ({ configured: !missing.length, missing, tokens: 0, sent: 0, dead: 0, failed: 0, errors: [] });

/**
 * Send one wake to every registered token. A platform with a secret missing sends nothing and says `missing`; a token
 * the service calls dead is deleted; a failure is counted with its first errors. The result is kept at PUSH_STATUS_KEY.
 */
export async function sendWake(env: PushEnv, fetch_: Fetch, now: Date, reason: string): Promise<PushSend> {
  const missing = pushMissing(env);
  const out: PushSend = { at: now.toISOString(), reason, ok: true, ios: blankSend(missing.ios), android: blankSend(missing.android) };
  const keys = await listKeys(env.BUCKET, TOKENS_PREFIX, 100_000);
  const recs: { key: string; rec: PushRecord }[] = [];
  for (const key of keys) {
    const o = await env.BUCKET.get(key);
    if (!o) continue;
    try { recs.push({ key, rec: JSON.parse(await o.text()) as PushRecord }); } catch { await env.BUCKET.delete(key); }   // our own junk: gone
  }
  for (const { rec } of recs) if (out[rec.platform]) out[rec.platform].tokens++;
  // one credential per platform per send (made only when that platform has someone to send to)
  let apns: string | null = null;
  let fcm: { url: string; bearer: string } | null = null;
  const fail = (p: PlatformSend, why: string) => { p.failed++; if (p.errors.length < KEEP_ERRORS) p.errors.push(why.slice(0, 200)); };
  try {
    if (out.ios.configured && out.ios.tokens) apns = await apnsJwt(env as Required<Pick<PushEnv, "APNS_KEY" | "APNS_KEY_ID" | "APNS_TEAM_ID">>, now);
  } catch (e) { out.ios.errors.push(`APNs key: ${String((e as Error)?.message ?? e).slice(0, 200)}`); out.ios.failed = out.ios.tokens; }
  try {
    if (out.android.configured && out.android.tokens) {
      const sa = parseServiceAccount(env.FCM_SERVICE_ACCOUNT!);
      const res = await fetch_(sa.token_uri ?? "https://oauth2.googleapis.com/token", {
        method: "POST", headers: { "content-type": "application/x-www-form-urlencoded" },
        body: `grant_type=${encodeURIComponent("urn:ietf:params:oauth:grant-type:jwt-bearer")}&assertion=${await fcmAssertion(sa, now)}`,
      });
      if (!res.ok) throw new Error(`OAuth token: HTTP ${res.status}`);
      const tok = ((await res.json()) as { access_token?: string }).access_token;
      if (!tok) throw new Error("OAuth token: no access_token in the answer");
      fcm = { url: `https://fcm.googleapis.com/v1/projects/${sa.project_id}/messages:send`, bearer: tok };
    }
  } catch (e) { out.android.errors.push(`FCM credential: ${String((e as Error)?.message ?? e).slice(0, 200)}`); out.android.failed = out.android.tokens; }

  const one = async ({ key, rec }: { key: string; rec: PushRecord }) => {
    const p = out[rec.platform];
    if (!p) return;
    const via = rec.platform === "ios" ? apns : fcm;
    if (!via) return;   // missing secret (said in `missing`) or a credential that failed (counted above)
    let r: Outcome;
    try {
      if (rec.platform === "ios") {
        const q = apnsRequest(rec, apns!, now);
        r = await apnsOutcome(await fetch_(q.url, q.init));
      } else {
        r = await fcmOutcome(await fetch_(fcm!.url, { method: "POST", headers: { authorization: `Bearer ${fcm!.bearer}`, "content-type": "application/json" },
                                                      body: JSON.stringify(fcmMessage(rec.token)) }));
      }
    } catch (e) { r = { failed: `network: ${String((e as Error)?.message ?? e)}` }; }
    if (r === "sent") p.sent++;
    else if (r === "dead") { p.dead++; await env.BUCKET.delete(key); }
    else fail(p, r.failed);
  };
  for (let i = 0; i < recs.length; i += PARALLEL) await Promise.all(recs.slice(i, i + PARALLEL).map(one));
  out.ok = !out.ios.failed && !out.android.failed;
  try {
    await env.BUCKET.put(PUSH_STATUS_KEY, JSON.stringify(out), { httpMetadata: { contentType: "application/json", cacheControl: "no-store" } });
  } catch (e) { console.error(JSON.stringify({ job: "push", statusError: String(e) })); }
  const log = { job: "push", reason, ios: { ...out.ios, errors: out.ios.errors.length }, android: { ...out.android, errors: out.android.errors.length } };
  if (out.ok) console.log(JSON.stringify(log)); else console.error(JSON.stringify({ ...log, errors: [...out.ios.errors, ...out.android.errors] }));
  for (const p of PUSH_PLATFORMS) if (out[p].tokens && out[p].missing.length) console.error(JSON.stringify({ job: "push", platform: p, notSent: out[p].tokens, missing: out[p].missing }));
  return out;
}

/**
 * (T739, owner 2026-11-14 "if new data is there we could send a notification") Has a new release been published since the
 * sender last looked? Reads the live manifest from the bucket; the first look only records it (no wake for the release
 * every phone already has). Throws when the manifest cannot be read or parsed (the caller says so in the run).
 */
export async function newRelease(bucket: R2BucketLike): Promise<string | null> {
  const o = await bucket.get(MANIFEST_KEY);
  if (!o) throw new Error(`${MANIFEST_KEY} is not in the bucket`);
  let rel: unknown;
  try { rel = (JSON.parse(await o.text()) as { data_release?: unknown }).data_release; } catch (e) { throw new Error(`${MANIFEST_KEY} does not parse: ${String(e)}`); }
  if (typeof rel !== "string" || !rel) throw new Error(`${MANIFEST_KEY} names no data_release`);
  const s = await bucket.get(PUSH_STATE_KEY);
  let seen: string | null = null;
  if (s) { try { seen = (JSON.parse(await s.text()) as { release?: string }).release ?? null; } catch { seen = null; } }
  if (seen === rel) return null;
  await bucket.put(PUSH_STATE_KEY, JSON.stringify({ release: rel }), { httpMetadata: { contentType: "application/json" } });
  return seen === null ? null : rel;
}

/** What a feed run changed that a phone's check reads, in a word for the record — or null (no wake). */
export function wakeReason(job: "gauges" | "dfo" | "inseason", ok: boolean, counts: Record<string, number>): string | null {
  if (!ok) return null;
  if (job === "gauges") return (counts.stations ?? 0) > 0 && (counts.unchanged ?? 0) < (counts.asked ?? 0) ? "gauges" : null;
  if (job === "dfo") return (counts.regions ?? 0) - (counts.unchangedRegions ?? 0) > 0 || (counts.notices ?? 0) > 0 ? "dfo" : null;
  return (counts.new ?? 0) + (counts.changed ?? 0) + (counts.removed ?? 0) > 0 ? "inseason" : null;
}
