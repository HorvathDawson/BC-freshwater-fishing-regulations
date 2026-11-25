// (T739) PUSH "WAKE-UPS": a content-free "check now" push to every registered phone when a feed changes or a new release
// is published. The phone then runs the SAME background check it runs on its own schedule (apps/mobile notify.native.ts,
// packages/data notify/check.ts) and raises its own local notification if one of its rules fires. WHAT THIS SERVER
// LEARNS: a device push token, its platform and its APNs environment — never the alert rules, the watched waters or a
// location (owner 2026-11-14). A token the push service calls dead is deleted; unregister deletes it at once; (T1116) one
// not registered again for PUSH_TOKEN_MAX_AGE_DAYS (30) is deleted by the daily sweep and never woken. The app sends a
// token only while a notification is on and the phone's notification permission is granted (packages/ui notify.ts).
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
import { STATE_KEYS } from "./dfo/state";
import { jsonResponse, readJsonBody, type R2BucketLike } from "./io";
import { dailyTotal, LIMIT_PREFIXES, listKeys, PUSH_NEW_TOKENS_PER_DAY_TOTAL, rateLimit, sha256hex, noSalt, type ReportEnv } from "./reports";

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
 * Unregister is counted the same way. (T994 #25) A NEW token also counts against PUSH_NEW_TOKENS_PER_DAY_TOTAL from
 * everyone; reaching it tells the owner once (reports.ts `capEvents`).
 */
export const PUSH_GAP_MS = 2_000;
export const PUSH_PER_DAY = 200;
const MAX_BODY = 1024;
/** in flight at once while sending */
const PARALLEL = 20;
/** first errors kept per platform in the status */
const KEEP_ERRORS = 5;

const json = jsonResponse;

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

/**
 * (T1116, owner 2026-11-25) A TOKEN NOT REGISTERED AGAIN FOR THIS MANY DAYS IS DELETED (privacy page). The app registers
 * again at a start once its last register is 7 days old (packages/data notify/push.ts PUSH_REFRESH_MS), and only while a
 * notification is on AND the phone's permission is granted (packages/ui notify.ts `pushWanted`); so a token whose phone
 * turned notifications off, lost the permission without opening the app, or stopped opening it is gone 30 days after its
 * last register. Checked by the daily sweep (`purgePushTokens`, the "25 9 * * *" purge) and by every send (`sendWake`:
 * a stale token is never woken).
 */
export const PUSH_TOKEN_MAX_AGE_DAYS = 30;
const PUSH_TOKEN_MAX_AGE_MS = PUSH_TOKEN_MAX_AGE_DAYS * 86_400_000;

/** (T1116) when a record was last registered (ms), or null when its `at` does not read (the sweep dates it, below) */
export function registeredAt(rec: { at?: unknown }): number | null {
  const t = typeof rec.at === "string" ? Date.parse(rec.at) : NaN;
  return Number.isFinite(t) ? t : null;
}

/** (T1116) older than PUSH_TOKEN_MAX_AGE_DAYS since its last register (exactly 30 days is still kept) */
export const tokenStale = (rec: { at?: unknown }, now: Date): boolean => {
  const t = registeredAt(rec);
  return t !== null && now.getTime() - t > PUSH_TOKEN_MAX_AGE_MS;
};

/** (T996, T1116) a dropped token is named by its record id (its key's first 12 hex), never by the token */
const recordId = (key: string) => key.slice(TOKENS_PREFIX.length, TOKENS_PREFIX.length + 12);
const staleWhy = (rec: { at?: unknown }) => `not registered again for ${PUSH_TOKEN_MAX_AGE_DAYS} days (last ${String(rec.at)})`;

export interface TokenPurge {
  ok: boolean; at: string; deleted: number; kept: number; failed: number;
  /** (T1116 migration) records with no `at` that reads: dated now (kept; they age from this sweep) */
  dated: number;
  /** records that do not parse: left for the send, which counts them as a fault and deletes them (`unreadable`) */
  unreadable: number;
  error?: string;
}

/**
 * (T1116) THE DAILY TOKEN SWEEP: every stored token older than PUSH_TOKEN_MAX_AGE_DAYS since its last register is
 * deleted, each drop logged by record id (as a dead token is, T996). MIGRATION: a record with no `at` that reads (none
 * is expected: every register since T739 writes one) is never dropped on sight: it is DATED NOW (written back with
 * `at` = this sweep), so it ages from the first sweep after the deploy and nobody's token goes at once. A delete or a
 * write that fails is counted and the result is not ok (the purge tells the owner).
 */
export async function purgePushTokens(bucket: R2BucketLike, now: Date = new Date()): Promise<TokenPurge> {
  const at = now.toISOString();
  const res: TokenPurge = { ok: true, at, deleted: 0, kept: 0, failed: 0, dated: 0, unreadable: 0 };
  if (!bucket.list) return { ...res, ok: false, error: "bucket cannot list" };
  try {
    for (const key of await listKeys(bucket, TOKENS_PREFIX, Infinity)) {
      const o = await bucket.get(key);
      if (!o) continue;
      let rec: Partial<PushRecord>;
      try { rec = JSON.parse(await o.text()) as Partial<PushRecord>; if (!rec || typeof rec !== "object") throw new Error("not an object"); }
      catch (e) {
        res.unreadable++;
        console.error(JSON.stringify({ job: "push-tokens", unreadableToken: recordId(key), error: String((e as Error)?.message ?? e) }));
        continue;
      }
      try {
        if (registeredAt(rec) === null) {
          await bucket.put(key, JSON.stringify({ ...rec, at }), { httpMetadata: { contentType: "application/json" } });
          res.dated++;
          console.log(JSON.stringify({ job: "push-tokens", dated: recordId(key), at }));
        } else if (tokenStale(rec, now)) {
          await bucket.delete(key);
          res.deleted++;
          console.log(JSON.stringify({ job: "push-tokens", dropped: recordId(key), env: rec.env ?? null, why: staleWhy(rec) }));
        } else res.kept++;
      } catch (e) {
        res.failed++;
        console.error(JSON.stringify({ job: "push-tokens", record: recordId(key), error: String((e as Error)?.message ?? e) }));
      }
    }
    if (res.failed) { res.ok = false; res.error = `${res.failed} push token record(s) could not be deleted or dated`; }
  } catch (e) {
    res.ok = false; res.error = e instanceof Error ? e.message : String(e);
  }
  return res;
}

/** POST /push/register and /push/unregister */
export async function handlePush(req: Request, env: PushEnv, action: "register" | "unregister", now: Date = new Date()): Promise<Response> {
  const unset = noSalt(env);
  if (unset) return unset;
  const b = await readJsonBody(req, MAX_BODY, "too large");
  if (!b.ok) return json({ ok: false, error: b.error }, b.status);
  const v = validatePush(b.body);
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
  if (action === "register") {
    // (T994 #25) a NEW token counts against the day's total from everyone (the per-address limit alone let rotating
    // addresses fill the store with junk that takes send slots ahead of real phones); a known one re-registering does not.
    // A total or a token that cannot be read is a 503, never "not counted"
    let full: string | null = null;
    try {
      if ((await env.BUCKET.head(key)) === null) full = await dailyTotal(env.BUCKET, PUSH_LIMITS_PREFIX, now, PUSH_NEW_TOKENS_PER_DAY_TOTAL);
    } catch (e) {
      console.error(JSON.stringify({ job: "push", totalError: String(e) }));
      return json({ ok: false, error: "could not count" }, 503);
    }
    if (full) return json({ ok: false, error: "too many new phones today; try again tomorrow" }, 429);
  }
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

/**
 * (T994 #23) THE PROVIDER TOKEN IS KEPT BETWEEN WAKES. Apple: refresh it "no more than once every 20 minutes and no less
 * than once every 60" — a token made more often is answered 429 TooManyProviderTokenUpdates. Wakes come from runs 10 min
 * apart (the :05 / :15 / :35 / :45 crons, hand runs), so one token a wake broke that rule and each refusal counted against
 * RELEASE_WAKE_TRIES. It is kept at APNS_JWT_KEY (never backed up: backup.ts `personalKind`) and reused while it is under
 * APNS_JWT_REUSE_S old and was made with the same key id and team; APNs calling it expired or invalid drops it.
 */
export const APNS_JWT_KEY = `${PUSH_PREFIX}apns-jwt.json`;
/** reuse for 50 minutes: past the 20-minute floor, inside APNs' one-hour life */
export const APNS_JWT_REUSE_S = 50 * 60;
interface KeptJwt { format: "apns-jwt/1"; kid: string; iss: string; iat: number; jwt: string }
type ApnsSecrets = { APNS_KEY: string; APNS_KEY_ID: string; APNS_TEAM_ID: string };

/**
 * (T996) WHICH PROVIDER TOKEN A SEND USED, for the log and /status: `id` = the first 12 hex of the token's SHA-256 (the
 * token itself is a credential for an hour and is never logged), when it was made (`iat`), its key id, and whether this
 * send made it (`minted`) or reused the kept one. Over an hour of runs the log then shows one id per ~50 minutes.
 */
export interface ApnsTokenUsed { id: string; iat: string; kid: string; minted: boolean }
export const apnsTokenUsed = async (t: { jwt: string; iat: number; kid: string; minted: boolean }): Promise<ApnsTokenUsed> =>
  ({ id: (await sha256hex(t.jwt)).slice(0, 12), iat: new Date(t.iat * 1000).toISOString(), kid: t.kid, minted: t.minted });

/** The provider token to send with: the kept one while it may be reused, else a new one (then kept). A kept record that
 *  does not parse is our own cache: said in the log, replaced by a new token. A failed read or write THROWS. */
export async function apnsProviderToken(bucket: R2BucketLike, e: ApnsSecrets, now: Date): Promise<{ jwt: string; minted: boolean; iat: number; kid: string }> {
  const t = Math.floor(now.getTime() / 1000);
  const o = await bucket.get(APNS_JWT_KEY);
  if (o) {
    let k: Partial<KeptJwt> | null = null;
    try { k = JSON.parse(await o.text()); } catch (err) {
      console.error(JSON.stringify({ job: "push", apnsJwtUnreadable: String((err as Error)?.message ?? err) }));
    }
    if (k?.format === "apns-jwt/1" && k.kid === e.APNS_KEY_ID && k.iss === e.APNS_TEAM_ID && typeof k.jwt === "string"
        && typeof k.iat === "number" && t >= k.iat && t - k.iat < APNS_JWT_REUSE_S) return { jwt: k.jwt, minted: false, iat: k.iat, kid: k.kid };
  }
  const jwt = await apnsJwt(e, now);
  const kept: KeptJwt = { format: "apns-jwt/1", kid: e.APNS_KEY_ID, iss: e.APNS_TEAM_ID, iat: t, jwt };
  await bucket.put(APNS_JWT_KEY, JSON.stringify(kept), { httpMetadata: { contentType: "application/json" } });
  return { jwt, minted: true, iat: t, kid: e.APNS_KEY_ID };
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

/** (T996) a dead token carries APNs' answer (`APNs 410 Unregistered`), said in the log and on /status when it is dropped */
type Outcome = "sent" | { dead: string } | { failed: string };
type Fetch = (u: string, i?: RequestInit) => Promise<Response>;

/** APNs: 200 sent; 410 (Unregistered) or 400 BadDeviceToken / DeviceTokenNotForTopic → dead; anything else failed */
export async function apnsOutcome(res: Response): Promise<Outcome> {
  if (res.status === 200) return "sent";
  let reason = "";
  try { reason = String(((await res.json()) as { reason?: string }).reason ?? ""); } catch { /* no body */ }
  const said = `APNs ${res.status}${reason ? ` ${reason}` : ""}`;
  if (res.status === 410 || (res.status === 400 && (reason === "BadDeviceToken" || reason === "DeviceTokenNotForTopic"))) return { dead: said };
  return { failed: said };
}

/** which secrets a platform still lacks ([] = it can send) */
export function pushMissing(env: PushEnv): Record<PushPlatform, string[]> {
  return {
    ios: (["APNS_KEY", "APNS_KEY_ID", "APNS_TEAM_ID"] as const).filter((k) => !env[k]?.trim()),
  };
}

export interface PlatformSend {
  configured: boolean; missing: string[]; tokens: number; sent: number; dead: number; failed: number; errors: string[];
  /** (T996) the first dead tokens dropped, each `<record id>: <APNs' answer>` (the record id = its key's first 12 hex,
   *  never the token); present only when one was dropped */
  dropped?: string[];
}
export interface PushSend {
  at: string; reason: string; ok: boolean; ios: PlatformSend; error?: string;
  /** token records that were stored but did not parse, or are not an iPhone's: a fault (counted, logged, shown on
   *  /status), each deleted */
  unreadable: number;
  /** (T1116) tokens not registered again for PUSH_TOKEN_MAX_AGE_DAYS: never woken, deleted (each logged by record id);
   *  present only when one was */
  stale?: number;
  /** (T1116) stale tokens whose delete failed (a fault: the send is not ok; not woken; the daily sweep tries again) */
  staleNotDeleted?: number;
  /**
   * (T739 L3) a wake too big for one run (over PUSH_SENDS_PER_RUN phones) goes out in parts, one per feed run: which
   * part this was, how many registered phones are still to be told (0: the wake is done), and when the wake began.
   * Absent: the whole wake fit in this run.
   */
  carry?: { part: number; left: number; since: string };
  /** (T739 L3) the kept carry-over could not be read: a fault (deleted, the wake began again from the start), said */
  carryError?: string;
  /** (T996) the APNs provider token this send used (absent: none was needed or it could not be made) */
  apns?: ApnsTokenUsed;
}

const blankSend = (missing: string[]): PlatformSend => ({ configured: !missing.length, missing, tokens: 0, sent: 0, dead: 0, failed: 0, errors: [] });

/**
 * (T739 L3, 2026-11-20) THE SUBREQUEST BUDGET. A Worker invocation on Workers Paid may make 10,000 subrequests by default
 * (fetches AND R2 binding calls count: T225 "Limits checked", docs read 2026-10-28), and the wake runs INSIDE a feed
 * run's invocation, after the run's own work. The gauge run is the heaviest: 1,400–1,900 measured, about 3,000 in the
 * worst case (T225). A wake costs, per phone, one R2 get (its token record), one APNs fetch, and one R2 delete when
 * APNs calls the token dead (3 at most); plus one R2 list per 1,000 registered phones (every key is listed: L2) and a
 * few fixed reads and writes (the carry-over, the status, the release record). Sent all at once, 3,000 phones would
 * already pass the limit beside a worst gauge run, and Cloudflare would fail the rest of the run's subrequests: the
 * status write among them. So ONE RUN TELLS AT MOST PUSH_SENDS_PER_RUN PHONES and keeps a carry-over
 * (PUSH_CARRY_KEY): the next feed run (gauges every 30 min by day) tells the next ones, whether or not it has a reason
 * of its own, until every phone has had the wake. `pushSubrequests` is the count; push.scale.test.ts measures a wake
 * against it and holds it, beside the worst gauge run, under the limit up to a million registered phones.
 */
export const WORKER_SUBREQUESTS = 10_000;
/** the gauge run's worst case (T225 "Limits checked") */
export const GAUGE_RUN_SUBREQUESTS_WORST = 3_000;
/** phones told per run (3 subrequests each at most) */
export const PUSH_SENDS_PER_RUN = 1_500;
/** the fixed reads and writes of a wake: the carry read + write/delete, the status, the release state read + write,
 *  the manifest read, the last DFO index read, the run's own look for a carry, and (T994 #23) the kept provider token's
 *  read + write, and its delete when APNs refused it */
const PUSH_FIXED_SUBREQUESTS = 13;
/** the subrequests a wake makes at most with `registered` phones listed */
export const pushSubrequests = (registered: number, perRun = PUSH_SENDS_PER_RUN): number =>
  Math.max(1, Math.ceil(registered / 1000)) + 3 * Math.min(perRun, registered) + PUSH_FIXED_SUBREQUESTS;

/**
 * (T739 L3) where a wake too big for one run keeps its place: the next feed run tells the next phones.
 * (T977, accepted 2026-11-22) Two runs at once both read it and the later write wins: duplicate silent pushes, or a
 * circle cut short (those phones are told by the next wake). Only possible past PUSH_SENDS_PER_RUN phones; the lease
 * that would stop it is designed in T977 and built when /status first shows a carried wake.
 */
export const PUSH_CARRY_KEY = `${PUSH_PREFIX}carry.json`;
/**
 * The carry-over. Phones are told in key order, as a circle that starts after `start` ("" = the first key): the wake is
 * done when the circle comes back round. `cursor` is the last key told. A NEW wake while one is carried starts its own
 * circle where the carried one stands (`cursor`), so the phones not yet told are told next and nobody waits on a
 * restart: a gauge run that wakes every 30 minutes still reaches every phone.
 */
interface PushCarry { format: "push-carry/1"; reason: string; since: string; start: string; cursor: string; part: number }

/** The kept carry-over, or null (none). Throws when one is kept but cannot be read (the caller says so). */
export async function readPushCarry(bucket: R2BucketLike): Promise<PushCarry | null> {
  const o = await bucket.get(PUSH_CARRY_KEY);
  if (!o) return null;
  const c = JSON.parse(await o.text()) as Partial<PushCarry>;
  if (c?.format !== "push-carry/1" || typeof c.reason !== "string" || typeof c.since !== "string" || typeof c.start !== "string"
      || typeof c.cursor !== "string" || !Number.isSafeInteger(c.part)) throw new Error(`${PUSH_CARRY_KEY} is not a push-carry/1 record`);
  return c as PushCarry;
}

/**
 * Send one wake to the registered phones: every one, or (over PUSH_SENDS_PER_RUN) the next part of them, the rest
 * carried to the next run (`carry`). `reason` null: no new wake, only the carried one goes on (the caller asks only
 * when one is kept). A platform with a secret missing sends nothing and says `missing`; a token the service calls dead
 * is deleted; a failure is counted with its first errors. The result is kept at PUSH_STATUS_KEY.
 */
export async function sendWake(env: PushEnv, fetch_: Fetch, now: Date, asked: string | null,
                               opts: { perRun?: number } = {}): Promise<PushSend> {
  const perRun = opts.perRun ?? PUSH_SENDS_PER_RUN;
  const missing = pushMissing(env);
  let carried: PushCarry | null = null;
  let carryError: string | null = null;
  try { carried = await readPushCarry(env.BUCKET); } catch (e) {
    // a carry-over we wrote that does not read: a fault, said; deleted, and the wake begins at the first phone
    carryError = String((e as Error)?.message ?? e);
    console.error(JSON.stringify({ job: "push", carryError }));
    await env.BUCKET.delete(PUSH_CARRY_KEY);
  }
  // a carried wake whose record did not read: its phones were not all told, so it begins again as a new wake
  const reason = asked ?? (carryError ? "a carried wake (its record did not read)" : null);
  if (reason === null && !carried) throw new Error("no wake to send: no reason and nothing carried");
  // a new wake starts its circle where a carried one stands; a carried one goes on from its cursor
  const start = reason === null ? carried!.start : carried?.cursor ?? "";
  const cursor = reason === null ? carried!.cursor : null;
  const why = reason ?? carried!.reason;
  const since = reason === null ? carried!.since : now.toISOString();
  const part = reason === null ? carried!.part + 1 : 1;
  const out: PushSend = { at: now.toISOString(), reason: why, ok: true, ios: blankSend(missing.ios), unreadable: 0,
                          ...(carryError ? { carryError } : {}) };
  // (T739 L2) EVERY registered phone is listed (a page of 1,000 a call): the cap of 100,000 stopped silently
  const keys = await listKeys(env.BUCKET, TOKENS_PREFIX, Infinity);
  const after = (k: string) => (k > start ? 0 : 1);
  const order = (a: string, b: string) => after(a) - after(b) || (a < b ? -1 : a > b ? 1 : 0);
  const due = keys.sort(order).filter((k) => cursor === null || order(k, cursor) > 0);
  const batch = due.slice(0, perRun);
  const left = due.length - batch.length;

  const recs: { key: string; rec: PushRecord }[] = [];
  for (const key of batch) {
    const o = await env.BUCKET.get(key);
    if (!o) continue;
    let rec: PushRecord;
    try {
      rec = JSON.parse(await o.text()) as PushRecord;
      if (rec?.platform !== "ios") throw new Error(`not an iPhone's token (platform ${JSON.stringify(rec?.platform)})`);
    } catch (e) {
      // a record we wrote that does not parse (or names a platform with no push) is a fault: counted and said; deleted
      // (the iPhone registers again within 7 days, PUSH_REFRESH_MS on the phone)
      out.unreadable++;
      console.error(JSON.stringify({ job: "push", unreadableToken: key, error: String((e as Error)?.message ?? e) }));
      await env.BUCKET.delete(key);
      continue;
    }
    // (T1116) a token not registered again for PUSH_TOKEN_MAX_AGE_DAYS is never woken: deleted here, before the daily
    // sweep comes to it, and logged by record id as a dead one is. A delete that fails is its own fault (counted in
    // `staleNotDeleted`, the send not ok, said); the token is still not woken and the wake goes on (the sweep tries again)
    if (tokenStale(rec, now)) {
      try {
        await env.BUCKET.delete(key);
        out.stale = (out.stale ?? 0) + 1;
        console.log(JSON.stringify({ job: "push", dropped: recordId(key), env: rec.env, why: staleWhy(rec) }));
      } catch (e) {
        out.staleNotDeleted = (out.staleNotDeleted ?? 0) + 1;
        console.error(JSON.stringify({ job: "push", staleNotDeleted: recordId(key), error: String((e as Error)?.message ?? e) }));
      }
      continue;
    }
    recs.push({ key, rec });
  }
  out.ios.tokens = recs.length;
  // one credential per send (made only when there is someone to send to); (T994 #23) the kept one while it may be reused
  let apns: string | null = null;
  let tokenRefused: string | null = null;
  const fail = (p: PlatformSend, why: string) => { p.failed++; if (p.errors.length < KEEP_ERRORS) p.errors.push(why.slice(0, 200)); };
  try {
    if (out.ios.configured && out.ios.tokens) {
      const t = await apnsProviderToken(env.BUCKET, env as ApnsSecrets, now);
      apns = t.jwt;
      out.apns = await apnsTokenUsed(t);
    }
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
    else if ("dead" in r) {
      // (T996) a token APNs calls dead is dropped, and the log says which record and why (never the token itself)
      p.dead++;
      await env.BUCKET.delete(key);
      const id = recordId(key);
      console.log(JSON.stringify({ job: "push", dropped: id, env: rec.env, why: r.dead }));
      p.dropped ??= [];
      if (p.dropped.length < KEEP_ERRORS) p.dropped.push(`${id}: ${r.dead}`);
    }
    else { fail(p, r.failed); if (/^APNs 403 (ExpiredProviderToken|InvalidProviderToken)\b/.test(r.failed)) tokenRefused ??= r.failed; }
  };
  for (let i = 0; i < recs.length; i += PARALLEL) await Promise.all(recs.slice(i, i + PARALLEL).map(one));
  // (T994 #23) APNs refused the provider token: the next wake makes a new one (the failures are counted above). A delete
  // that fails is said in this send's errors and never stops the carry-over or the status below from being kept
  if (tokenRefused) {
    console.error(JSON.stringify({ job: "push", apnsTokenRefused: out.apns?.id ?? null, iat: out.apns?.iat ?? null, why: tokenRefused }));
    try { await env.BUCKET.delete(APNS_JWT_KEY); } catch (e) {
      const why = `the refused APNs token was not dropped (${String((e as Error)?.message ?? e)}); it is made again after ${APNS_JWT_REUSE_S / 60} min`;
      console.error(JSON.stringify({ job: "push", apnsJwtDrop: why }));
      if (out.ios.errors.length < KEEP_ERRORS) out.ios.errors.push(why.slice(0, 200));
    }
  }
  out.ok = !out.ios.failed && !out.unreadable && !carryError && !out.staleNotDeleted;
  // (T739 L3) the place kept for the next run, or the carry-over done. A carry that cannot be kept THROWS, as a lost
  // status does: the rest of the phones would silently never be told
  if (left || part > 1) out.carry = { part, left, since };
  try {
    if (left) {
      const next: PushCarry = { format: "push-carry/1", reason: why, since, start, cursor: batch[batch.length - 1]!, part };
      await env.BUCKET.put(PUSH_CARRY_KEY, JSON.stringify(next), { httpMetadata: { contentType: "application/json" } });
    } else if (carried) await env.BUCKET.delete(PUSH_CARRY_KEY);
  } catch (e) {
    throw Object.assign(new Error(`push: sent (${out.ios.sent} of ${out.ios.tokens}) but the carry-over (${left} phones left) was not kept (${String((e as Error)?.message ?? e)})`), { send: out });
  }
  // (review 2026-11-20, M2) a send whose record cannot be kept THROWS (after the sends): the caller tries the failure
  // record and fails the run, so a lost status is never silent
  try {
    await env.BUCKET.put(PUSH_STATUS_KEY, JSON.stringify(out), { httpMetadata: { contentType: "application/json", cacheControl: "no-store" } });
  } catch (e) {
    throw Object.assign(new Error(`push: sent (${out.ios.sent} of ${out.ios.tokens}) but the status was not written (${String((e as Error)?.message ?? e)})`), { send: out });
  }
  const log = { job: "push", reason: why, ios: { ...out.ios, errors: out.ios.errors.length }, ...(out.apns ? { apns: out.apns } : {}),
                ...(out.carry ? { carry: out.carry } : {}), ...(out.stale ? { stale: out.stale } : {}) };
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
 * (review 2026-11-20, M2) A NEW release is NOT recorded here: `wakePhones` records it (`noteReleaseWake`) once its wake
 * reached a phone, or after RELEASE_WAKE_TRIES wakes; until then each gauge run wakes for it again.
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

/** (re-review 2026-11-20) the gauge runs that may wake for one release before it is recorded anyway */
export const RELEASE_WAKE_TRIES = 3;

/**
 * (re-review 2026-11-20) After a wake for a new release: record it once a phone was reached (`sent > 0`) or there was
 * nobody to reach, or after RELEASE_WAKE_TRIES wakes for it, whichever comes first, so one always-failing token or a
 * broken APNs key cannot re-wake every phone at every gauge run without end (the failures stay on /status). Until then
 * the state keeps the release last told and the tries for this one: `{release, trying: {release, tries}}`.
 */
export async function noteReleaseWake(bucket: R2BucketLike, rel: string, reached: boolean): Promise<{ recorded: boolean; tries: number }> {
  const s = await bucket.get(PUSH_STATE_KEY);
  let st: { release?: string | null; trying?: { release?: string; tries?: number } } = {};
  // newRelease replaced an unreadable record before it named this release; one torn since THROWS (the run says it)
  if (s) st = JSON.parse(await s.text());
  const tries = (st.trying?.release === rel ? Number(st.trying.tries) || 0 : 0) + 1;
  if (reached || tries >= RELEASE_WAKE_TRIES) { await markReleaseSeen(bucket, rel); return { recorded: true, tries }; }
  await bucket.put(PUSH_STATE_KEY, JSON.stringify({ release: st.release ?? null, trying: { release: rel, tries } }),
                   { httpMetadata: { contentType: "application/json" } });
  return { recorded: false, tries };
}

/**
 * (T739) ONE WAKE FROM A FEED RUN (index.ts `run`): the send, its record on /status, and for a new release (`rel`, the
 * gauge run) whether it is now recorded as told. Never throws: a failure is a step error (`logErr`); a wake that threw is
 * kept on /status as {at, reason, ok: false, error}; `statusLost` (the status could not be written) fails the run.
 */
export async function wakePhones(env: PushEnv, fetch_: Fetch, at: Date, why: string | null, rel: string | null,
                                 logErr: (s: string) => void): Promise<{ push: unknown; statusLost: boolean }> {
  let push: unknown = null;
  let statusLost = false;
  let reached = false;
  /** (T739 L3) `why` null: no new wake, the carried one goes on (a wake over PUSH_SENDS_PER_RUN phones) */
  const said = why ?? "the carried wake";
  try {
    const r = await sendWake(env, fetch_, at, why);
    push = { reason: r.reason, ok: r.ok, unreadable: r.unreadable, ios: { sent: r.ios.sent, failed: r.ios.failed, missing: r.ios.missing },
             ...(r.apns ? { apns: r.apns } : {}), ...(r.carry ? { carry: r.carry } : {}), ...(r.stale ? { stale: r.stale } : {}) };
    if (r.ios.failed) logErr(`push: ${r.ios.failed} wake(s) failed (${r.ios.errors.slice(0, 3).join("; ")})`);
    if (r.unreadable) logErr(`push: ${r.unreadable} stored token record(s) did not parse or were not an iPhone's; deleted (those phones register again within 7 days)`);
    if (r.staleNotDeleted) logErr(`push: ${r.staleNotDeleted} token(s) not registered again for ${PUSH_TOKEN_MAX_AGE_DAYS} days could not be deleted (not woken; the daily sweep tries again)`);
    if (r.carryError) logErr(`push: the carry-over did not read (${r.carryError}); deleted, the wake began again at the first phone`);
    reached = r.ios.sent > 0 || (r.ios.tokens === 0 && r.ok);
  } catch (e) {
    const msg = String((e as Error)?.message ?? e);
    logErr(`push: not sent (${msg})`);
    push = { reason: said, ok: false, error: msg };
    const sent = (e as { send?: PushSend })?.send;
    if (sent) { statusLost = true; reached = sent.ios.sent > 0; }   // sendWake's own status (or carry) write failed (after sending)
    try { await writePushFailure(env.BUCKET, at, said, msg); }
    catch (w) { statusLost = true; logErr(`push: the failure was not recorded on /status either (${String((w as Error)?.message ?? w)})`); }
  }
  if (rel) {
    try {
      const n = await noteReleaseWake(env.BUCKET, rel, reached);
      if (!n.recorded) logErr(`push: release ${rel} not recorded as told (no phone reached; try ${n.tries} of ${RELEASE_WAKE_TRIES}); the next gauge run wakes for it again`);
      else if (!reached) logErr(`push: release ${rel} recorded after ${n.tries} wakes that reached no phone (see the push status)`);
    } catch (e) { logErr(`push: release ${rel} not recorded (${String((e as Error)?.message ?? e)}); the next gauge run wakes for it again`); }
  }
  return { push, statusLost };
}

/**
 * (review 2026-11-20, M1) Did a DFO run change what a phone reads? The published answers compared with the last feed's:
 * every place's answers (`locs`), the verdict and gear tables they point into, the issues, `pairs`, and each region's
 * WHOLE record (its header, notes, limits, date, stale mark, holds…; re-review 2026-11-20: a re-typed header note or
 * limit under the same `asof` was missed) less the run's own bookkeeping (REGION_BOOKKEEPING: when it was checked, the
 * fetches that failed, the page hash and when it last moved). Not `at`, not which regions the run asked
 * (`issues.unread`), and not its notices: an unchanged region's notices are re-offered every run (dfo/live.ts,
 * the outbox tells a key once) and a one-region retry counts nothing as changed. No last feed: changed. Compared with
 * keys sorted: the last feed is read back from its published bytes, the new one is the run's object (other key order).
 */
/** a region's own run bookkeeping (dfo/live.ts `reg`): never a reason to wake. `changed` is when the page hash last
 *  moved, so it follows `sha256`; a page whose bytes moved with no other field changing reads the same to a phone.
 *  (T905) and every key that moved to the private state (dfo/live.ts STATE_KEYS: no app reads one): the first run after
 *  that deploy reads a last index that still carries them and publishes one that does not — never a wake on that. */
const REGION_BOOKKEEPING = new Set(["at", "checked", "page", ...STATE_KEYS]);
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
      regions[slug] = Object.fromEntries(Object.entries(r ?? {}).filter(([k]) => !REGION_BOOKKEEPING.has(k)));
    const waters = Object.entries(((ix.issues as { waters?: Record<string, { r?: string }> } | undefined)?.waters) ?? {})
      .filter(([, w]) => read.has(String(w?.r))).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    return stable({ locs: ix.locs ?? null, verdicts: ix.verdicts ?? null, gears: ix.gears ?? null, pairs: ix.pairs ?? null, waters, regions });
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
