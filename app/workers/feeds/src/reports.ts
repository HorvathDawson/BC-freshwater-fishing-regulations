// T237: PROBLEM REPORTS from the app — `POST /report` (public), stored one JSON per report in the PRIVATE part of the
// bucket, and told to the owner in the daily digest (T225 notify policy: a report is not urgent).
//
//   POST /report        a `report/2` payload (app/packages/core/src/menu/report.ts `reportPayload`; a queued or older
//                       app's `report/1` is taken too — each version's own context keys, no other). Validated (size,
//                       shape, limits — the app's limits, enforced again here since anyone can POST), rate-limited per
//                       IP, stored at private/reports/<received day>/<id>.json. Answers {ok, id} — never the content.
//                       The same id re-sent the same day (the app's outbox retrying after a lost answer) is stored once.
//   GET /reports        (Bearer RUN_TOKEN) every stored report since `?since=YYYY-MM-DD` (default: 30 days), newest
//                       first — what the review app's Reports tab syncs.
//   the digest          `reportEvents` (called by the gauge run, every 30 min): every stored report the outbox has not
//                       told yet (T558: no date window) → one digest line each. The reporter's email is never in it.
//
// RATE LIMIT (T249): one report per IP per 10 s and 30 per IP per day — keyed by a salted SHA-256 of the IP
// (private/reports/limits/<hash>.json; the IP itself is never stored), plus the REPORT_LIMITER rate-limit binding
// (wrangler.toml [[ratelimits]], 1 per 10 s per IP at the edge) as the cheap first gate when it is bound.
// PRIVACY: what is stored is the payload and the time it arrived — no IP, no User-Agent, no country.
import type { FeedEvent } from "./events";
import type { R2BucketLike } from "./io";

export const REPORTS_PREFIX = "private/reports/";
/** (T291) reports a TOOL sent (the test bed, smoke runs) carry `test: true` and are stored apart: not in the digest, not in the
 *  default review list, deleted after TEST_RETENTION_DAYS. Honoured only by the staging worker (FEEDS_ENV = "staging"); a
 *  production worker ignores the field, so a real user can never tag a report. */
export const TEST_REPORTS_PREFIX = "private/reports-test/";
export const TEST_RETENTION_DAYS = 7;
const LIMITS_PREFIX = `${REPORTS_PREFIX}limits/`;
/** (T352) the newest format; report/2 adds the map facts and the install id to the context */
export const REPORT_FORMAT = "report/2";
export const REPORT_FORMATS = ["report/1", "report/2"] as const;
/** (T407) "bug" and "feature" beside the data reasons (core REPORT_REASONS) */
export const REASONS = ["rule", "place", "missing", "name", "bug", "feature", "other"] as const;
const REASON_WORDS: Record<(typeof REASONS)[number], string> = {
  rule: "Wrong rule", place: "Wrong place on the map", missing: "Missing water", name: "Wrong name",
  bug: "App bug", feature: "Feature request", other: "Something else",
};
/** the app's limits (core REPORT_LIMITS) */
export const LIMITS = { bytes: 16_384, text: 4000, email: 254, errors: 20, line: 300, field: 200 } as const;
/** one report per IP per this, and at most PER_DAY a day */
export const MIN_GAP_MS = 10_000;
export const PER_DAY = 30;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const STR_FIELDS = ["screen", "tab", "place", "water", "partLabel", "day", "mode", "app", "env", "platform", "release", "tiles"] as const;
const INT_FIELDS = ["part", "section"] as const;
/** (T352) report/2's context: the map's layer and forecast offset, zoom and centre, and the install id */
const V2_STR_FIELDS = ["layer", "horizon"] as const;
/**
 * (T569) a report asked from a water's Conditions: the gauge charted (its station id), the quantity and span picked, and the
 * gauge feed's version (its index's fetch stamp). Strings, report/2 only; no release or section id is added (AGENTS 5).
 */
const V2_GAUGE_FIELDS = ["station", "quantity", "span", "feed"] as const;
const V2_NUM_FIELDS = { zoom: [0, 24], lat: [-90, 90], lon: [-180, 180] } as const;
const INSTALL_RE = /^[0-9a-f]{16}$/;
/** (T430) every context key a report/2 may carry; `app/tools/wire.test.ts` compares it with the app's `ReportContext` */
export const REPORT_CONTEXT_KEYS: readonly string[] = [...STR_FIELDS, ...INT_FIELDS, ...V2_STR_FIELDS, ...V2_GAUGE_FIELDS, ...Object.keys(V2_NUM_FIELDS), "install", "errors"];

export interface StoredReport {
  format: (typeof REPORT_FORMATS)[number];
  id: string;
  created: string;
  received: string;
  reason: (typeof REASONS)[number];
  text: string;
  email: string | null;
  /** (owner 2026-10-29) the reporter ticked "You may contact me" — true only with an email */
  contact: boolean;
  context: Record<string, unknown>;
  /** (T291) sent by a tool: stored under reports-test/, hidden from review by default */
  test?: true;
}

export interface ReportEnv {
  BUCKET: R2BucketLike;
  /** Cloudflare rate-limit binding ([[ratelimits]] in wrangler.toml); absent under test */
  REPORT_LIMITER?: { limit(o: { key: string }): Promise<{ success: boolean }> };
  /** "staging" on the staging worker: the only place the `test` tag is honoured (T291) */
  FEEDS_ENV?: string;
  /** salts the IP hash (a secret; a fixed fallback keeps the hash stable when unset) */
  REPORT_SALT?: string;
}

export const CORS: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Max-Age": "86400",
};
const json = (body: unknown, status: number) =>
  new Response(JSON.stringify(body), { status, headers: { ...CORS, "Content-Type": "application/json", "Cache-Control": "no-store" } });

/** The payload, checked and normalised — or the reason it is refused (a 400 the app shows, never queued). */
export function validateReport(raw: unknown): { ok: true; report: Omit<StoredReport, "received"> } | { ok: false; error: string } {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return { ok: false, error: "not a report" };
  const r = raw as Record<string, unknown>;
  if (!(REPORT_FORMATS as readonly unknown[]).includes(r.format)) return { ok: false, error: `format must be one of ${REPORT_FORMATS.join(", ")}` };
  const v2 = r.format === "report/2";
  if (typeof r.id !== "string" || !/^[0-9a-f]{16}$/.test(r.id)) return { ok: false, error: "bad id" };
  if (typeof r.created !== "string" || Number.isNaN(Date.parse(r.created)) || r.created.length > 40) return { ok: false, error: "bad created" };
  if (typeof r.reason !== "string" || !(REASONS as readonly string[]).includes(r.reason)) return { ok: false, error: "bad reason" };
  if (typeof r.text !== "string" || !r.text.trim()) return { ok: false, error: "the report says nothing" };
  if (r.text.length > LIMITS.text) return { ok: false, error: "report too long" };
  let email: string | null = null;
  if (r.email !== null && r.email !== undefined && r.email !== "") {
    if (typeof r.email !== "string" || r.email.length > LIMITS.email || !EMAIL_RE.test(r.email.trim())) return { ok: false, error: "invalid email address" };
    email = r.email.trim();
  }
  if (r.contact !== undefined && typeof r.contact !== "boolean") return { ok: false, error: "bad contact" };
  if (r.test !== undefined && typeof r.test !== "boolean") return { ok: false, error: "bad test" };
  const contact = !!email && r.contact === true;
  const c = r.context;
  if (!c || typeof c !== "object" || Array.isArray(c)) return { ok: false, error: "bad context" };
  const ctx: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(c as Record<string, unknown>)) {
    if ((STR_FIELDS as readonly string[]).includes(k)) {
      if (typeof v !== "string" || v.length > LIMITS.field) return { ok: false, error: `bad context.${k}` };
      ctx[k] = v;
    } else if ((INT_FIELDS as readonly string[]).includes(k)) {
      if (typeof v !== "number" || !Number.isInteger(v) || v < 0) return { ok: false, error: `bad context.${k}` };
      ctx[k] = v;
    } else if (v2 && ((V2_STR_FIELDS as readonly string[]).includes(k) || (V2_GAUGE_FIELDS as readonly string[]).includes(k))) {
      if (typeof v !== "string" || v.length > LIMITS.field) return { ok: false, error: `bad context.${k}` };
      ctx[k] = v;
    } else if (v2 && k in V2_NUM_FIELDS) {
      const [lo, hi] = V2_NUM_FIELDS[k as keyof typeof V2_NUM_FIELDS];
      if (typeof v !== "number" || !Number.isFinite(v) || v < lo || v > hi) return { ok: false, error: `bad context.${k}` };
      ctx[k] = v;
    } else if (v2 && k === "install") {
      if (typeof v !== "string" || !INSTALL_RE.test(v)) return { ok: false, error: "bad context.install" };
      ctx[k] = v;
    } else if (k === "errors") {
      if (!Array.isArray(v) || v.length > LIMITS.errors || v.some((l) => typeof l !== "string" || l.length > LIMITS.line))
        return { ok: false, error: "bad context.errors" };
      ctx[k] = v;
    } else return { ok: false, error: `unknown context.${k}` };
  }
  return { ok: true, report: { format: r.format as StoredReport["format"], id: r.id, created: r.created, reason: r.reason as StoredReport["reason"], text: r.text.trim(), email, contact, context: ctx, ...(r.test === true ? { test: true as const } : {}) } };
}

/**
 * (T609 W1) The address a limit is keyed on. IPv4 as is; IPv6 by its /64 (one home line owns 2^64 addresses, so a key per
 * full address let one caller look like a new one on every request); an IPv4-mapped IPv6 as its IPv4.
 */
export function ipKey(ip: string): string {
  const s = ip.trim().toLowerCase();
  if (!s.includes(":")) return s;
  const mapped = /^(?:0*:)*:?ffff:(\d+\.\d+\.\d+\.\d+)$/.exec(s) ?? /^::ffff:(\d+\.\d+\.\d+\.\d+)$/.exec(s);
  if (mapped) return mapped[1]!;
  const [head, tail] = s.split("::", 2) as [string, string | undefined];
  const h = head ? head.split(":") : [];
  const t = tail === undefined ? [] : tail ? tail.split(":") : [];
  const groups = tail === undefined ? h : [...h, ...Array(Math.max(0, 8 - h.length - t.length)).fill("0"), ...t];
  return `${groups.slice(0, 4).map((g) => (parseInt(g || "0", 16) || 0).toString(16)).join(":")}::/64`;
}

/** (T609 W1) at most this many reports stored a day, from everyone: past it a 429 and one message to the owner */
export const REPORTS_PER_DAY_TOTAL = 300;
/** the day's total for one endpoint: in its limits/ folder, so `purgeLimits` deletes it once the day is over */
export const totalKey = (limitsPrefix: string, day: string) => `${limitsPrefix}_total-${day}.json`;

/**
 * (T609 W1) Count one more against the day's total; null = allowed, else the refusal. A total that cannot be read is a
 * 503 (THROWS): never "nothing counted yet". Read-then-write: parallel requests may pass a few over the cap.
 */
export async function dailyTotal(bucket: R2BucketLike, limitsPrefix: string, now: Date, cap: number): Promise<string | null> {
  const day = now.toISOString().slice(0, 10);
  const key = totalKey(limitsPrefix, day);
  const o = await bucket.get(key);
  const t = o ? JSON.parse(await o.text()) as { day: string; n: number; capped?: string } : { day, n: 0 };
  if (t.n >= cap) {
    if (!t.capped) await bucket.put(key, JSON.stringify({ ...t, capped: now.toISOString() }), { httpMetadata: { contentType: "application/json" } });
    return "the report service is full for today; your report is kept and sent again later";
  }
  await bucket.put(key, JSON.stringify({ ...t, day, n: t.n + 1 }), { httpMetadata: { contentType: "application/json" } });
  return null;
}

/** (T609 W1) one urgent message per endpoint and day whose total was reached: someone is flooding it */
export async function capEvents(bucket: R2BucketLike, now: Date, at: string): Promise<FeedEvent[]> {
  const out: FeedEvent[] = [];
  const days2 = [0, 1].map((i) => new Date(now.getTime() - i * 86_400_000).toISOString().slice(0, 10));
  for (const [what, prefix, cap] of [["problem reports", LIMIT_PREFIXES[0], REPORTS_PER_DAY_TOTAL], ["new error kinds", LIMIT_PREFIXES[1], CRASH_KINDS_PER_DAY_TOTAL]] as const) {
    for (const day of days2) {
      const o = await bucket.get(totalKey(prefix, day));
      if (!o) continue;
      const t = JSON.parse(await o.text()) as { n: number; capped?: string };
      if (!t.capped) continue;
      out.push({ key: `cap:${prefix}:${day}`, kind: "health", urgent: true, at,
        title: `${day}: the day's limit of ${cap} ${what} was reached at ${t.capped}`,
        text: `POST /${what === "problem reports" ? "report" : "crash"} refused everything past ${cap} ${what} on ${day} (UTC), from ${t.capped}: someone is ` +
          `probably flooding it. The app keeps a refused report and sends it again later; nothing real is lost unless it lasts.\n\n` +
          `Look: GET /${what === "problem reports" ? "reports" : "crashes"}?since=${day} (Bearer RUN_TOKEN) and wrangler tail on the feeds worker.` });
    }
  }
  return out;
}
/** (T609 W19) the staging test store (anyone can tag `test: true` there): at most this many test reports, and new test
 *  error kinds, a day — its own total, so a tool's flood never eats the real one and the test store stays bounded */
export const TEST_PER_DAY_TOTAL = 200;
export const TEST_TOTAL_PREFIX = (limitsPrefix: string) => `${limitsPrefix}test-`;

/** (T609 W1) at most this many NEW error kinds stored a day (a repeat of a known kind adds no object) */
export const CRASH_KINDS_PER_DAY_TOTAL = 500;

/**
 * (T609 W11) The body as text, read with a BYTE cap: null when it is longer than `max` bytes (UTF-8), however it was
 * sent (no Content-Length, or a false one). The old check counted characters after reading everything.
 */
export async function readCapped(req: Request, max: number): Promise<string | null> {
  if (!req.body) return "";
  const reader = req.body.getReader();
  const parts: Uint8Array[] = [];
  let n = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    n += value.byteLength;
    if (n > max) { await reader.cancel().catch(() => {}); return null; }
    parts.push(value);
  }
  const all = new Uint8Array(n);
  let o = 0;
  for (const p of parts) { all.set(p, o); o += p.byteLength; }
  return new TextDecoder("utf-8", { fatal: true }).decode(all);
}

export async function sha256hex(s: string): Promise<string> {
  const d = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s));
  return [...new Uint8Array(d)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/**
 * (T431) REPORT_SALT is a REQUIRED secret (runbooks/secrets.md; `wrangler secret put REPORT_SALT`): without it the per-IP
 * key would be hashed with a constant from the repo. A report or crash is then answered 503 (the app's outbox keeps a
 * report and sends it again) and the miss is logged; `deploy/staging/feeds.sh` refuses to deploy a worker without it.
 */
export function noSalt(env: ReportEnv): Response | null {
  if (env.REPORT_SALT) return null;
  console.error(JSON.stringify({ job: "reports", error: "REPORT_SALT is not set" }));
  return json({ ok: false, error: "the report endpoint is not configured" }, 503);
}

/** The per-IP gate: null = allowed (and recorded), else the refusal. */
export async function rateLimit(env: ReportEnv, ip: string, now: Date,
  o: { prefix?: string; gapMs?: number; perDay?: number; edge?: boolean } = {}): Promise<string | null> {
  const prefix = o.prefix ?? LIMITS_PREFIX, gapMs = o.gapMs ?? MIN_GAP_MS, perDay = o.perDay ?? PER_DAY;
  if (env.REPORT_LIMITER && o.edge !== false) {
    try {
      if (!(await env.REPORT_LIMITER.limit({ key: `report:${ipKey(ip)}` })).success) return "too many reports; try again in a minute";
    } catch { /* the binding failing never blocks a report: the R2 gate still holds */ }
  }
  if (!env.REPORT_SALT) throw new Error("REPORT_SALT is not set");
  const key = `${prefix}${(await sha256hex(`${env.REPORT_SALT}:${ipKey(ip)}`)).slice(0, 32)}.json`;
  const day = now.toISOString().slice(0, 10);
  let st: { last: number; day: string; n: number } = { last: 0, day, n: 0 };
  // (T609 W1) a counter that cannot be read THROWS (the caller answers 503; the app keeps the report): never "fresh"
  const cur = await env.BUCKET.get(key);
  if (cur) {
    try { st = JSON.parse(await cur.text()); } catch (e) { console.error(JSON.stringify({ job: "reports", limitCounterJunk: key, error: String(e) })); }   // our own object: rewritten below
  }
  if (st.day !== day) st = { last: st.last, day, n: 0 };
  if (now.getTime() - st.last < gapMs) return "too many reports; try again in a minute";
  if (st.n >= perDay) return "too many reports today";
  await env.BUCKET.put(key, JSON.stringify({ last: now.getTime(), day, n: st.n + 1 }), { httpMetadata: { contentType: "application/json" } });
  return null;
}

export const reportKey = (day: string, id: string) => `${REPORTS_PREFIX}${day}/${id}.json`;

/** POST /report */
export async function handleReport(req: Request, env: ReportEnv, now: Date = new Date()): Promise<Response> {
  const unset = noSalt(env);
  if (unset) return unset;
  const len = Number(req.headers.get("Content-Length") ?? NaN);
  if (Number.isFinite(len) && len > LIMITS.bytes) return json({ ok: false, error: "report too large" }, 413);
  let raw: string | null;
  try {
    raw = await readCapped(req, LIMITS.bytes);
  } catch {
    return json({ ok: false, error: "unreadable body" }, 400);
  }
  if (raw === null) return json({ ok: false, error: "report too large" }, 413);
  let body: unknown;
  try {
    body = JSON.parse(raw);
  } catch {
    return json({ ok: false, error: "not JSON" }, 400);
  }
  const v = validateReport(body);
  if (!v.ok) return json({ ok: false, error: v.error }, 400);
  const day = now.toISOString().slice(0, 10);
  // (T291) the tag counts on staging only; anywhere else it is dropped and the report is an ordinary one
  const isTest = v.report.test === true && env.FEEDS_ENV === "staging";
  if (!isTest) delete (v.report as { test?: true }).test;
  const key = isTest ? `${TEST_REPORTS_PREFIX}${day}/${v.report.id}.json` : reportKey(day, v.report.id);
  // the app's outbox re-sending after a lost answer: already stored, the same answer, nothing counted. (T558 item 4) A
  // retry that crosses UTC midnight finds it in yesterday's folder: stored once, not twice
  const yday = new Date(now.getTime() - 86_400_000).toISOString().slice(0, 10);
  const yKey = isTest ? `${TEST_REPORTS_PREFIX}${yday}/${v.report.id}.json` : reportKey(yday, v.report.id);
  if (await env.BUCKET.get(key) || await env.BUCKET.get(yKey)) return json({ ok: true, id: v.report.id }, 200);
  const ip = req.headers.get("CF-Connecting-IP") ?? "unknown";
  let refused: string | null;
  try {
    // (T609 W1) per address (IPv6 by its /64), then the day's total from everyone (a test report is not counted)
    refused = await rateLimit(env, ip, now) ?? (isTest
      ? await dailyTotal(env.BUCKET, TEST_TOTAL_PREFIX(LIMITS_PREFIX), now, TEST_PER_DAY_TOTAL)    // (T609 W19)
      : await dailyTotal(env.BUCKET, LIMITS_PREFIX, now, REPORTS_PER_DAY_TOTAL));
  } catch (e) {
    console.error(JSON.stringify({ job: "reports", limitError: String(e) }));
    return json({ ok: false, error: "the report service could not count this report; it is kept and sent again later" }, 503);
  }
  if (refused) return json({ ok: false, error: refused }, 429);
  const stored: StoredReport = { ...v.report, received: now.toISOString() };
  try {
    await env.BUCKET.put(key, JSON.stringify(stored, null, 1), { httpMetadata: { contentType: "application/json" } });
  } catch {
    return json({ ok: false, error: "could not store the report" }, 500);
  }
  return json({ ok: true, id: v.report.id }, 201);
}

const days = (from: Date, n: number) => Array.from({ length: n }, (_, i) => new Date(from.getTime() - i * 86_400_000).toISOString().slice(0, 10));

export async function listKeys(bucket: R2BucketLike, prefix: string, max = 1000): Promise<string[]> {
  if (!bucket.list) return [];
  const out: string[] = [];
  let cursor: string | undefined;
  do {
    const r = await bucket.list({ prefix, cursor, limit: 1000 });
    out.push(...r.objects.map((o) => o.key));
    cursor = r.truncated ? r.cursor : undefined;
  } while (cursor && out.length < max);
  return out;
}

/** (T271) reports are deleted this many days after they are received (privacy page: "180 days after receipt") */
export const RETENTION_DAYS = 180;
export const RETENTION_STATUS_KEY = "private/feeds/report-retention.json";

export interface PurgeResult { ok: boolean; at: string; deleted: number; kept: number; failed: number; error?: string }

/**
 * (T271) The daily purge: delete every stored report received more than RETENTION_DAYS ago. Age is the stored `received`
 * timestamp (the day folder only narrows the listing; a report whose body cannot be read ages by its folder's day).
 * Rate-limit counters (limits/) are not reports: `purgeLimits` deletes them.
 */
export async function purgeReports(bucket: R2BucketLike, now: Date = new Date()): Promise<PurgeResult> {
  const at = now.toISOString();
  if (!bucket.list) return { ok: false, at, deleted: 0, kept: 0, failed: 0, error: "bucket cannot list" };
  const cutoff = now.getTime() - RETENTION_DAYS * 86_400_000;
  const res: PurgeResult = { ok: true, at, deleted: 0, kept: 0, failed: 0 };
  try {
    const keys = await listKeys(bucket, REPORTS_PREFIX, Infinity);
    for (const k of keys) {
      const day = k.slice(REPORTS_PREFIX.length).match(/^(\d{4}-\d{2}-\d{2})\/[^/]+\.json$/)?.[1];
      if (!day) continue;                                   // limits/ and anything else is not a report
      if (Date.parse(day) > cutoff) { res.kept++; continue; } // the folder day starts after the cutoff: certainly newer
      let received = Date.parse(day) + 86_400_000 - 1;
      try {
        const o = await bucket.get(k);
        const t = o ? Date.parse(JSON.parse(await o.text()).received) : NaN;
        if (Number.isFinite(t)) received = t;
      } catch { /* unreadable: the folder day decides */ }
      if (received >= cutoff) { res.kept++; continue; }
      try { await bucket.delete(k); res.deleted++; } catch { res.failed++; }
    }
    if (res.failed) { res.ok = false; res.error = `${res.failed} report(s) could not be deleted`; }
  } catch (e) {
    res.ok = false; res.error = e instanceof Error ? e.message : String(e);
  }
  return res;
}

/** (T559) the per-address counters of POST /report and POST /crash (a salted hash of the address, last time, day, count) */
export const LIMIT_PREFIXES = [LIMITS_PREFIX, "private/crashes/limits/"] as const;

/**
 * (T559) Delete every rate-limit counter whose day is over (privacy page: the salted hash is kept "for a day's counting").
 * Run daily (09:25 UTC): a counter lives at most until the next run after its UTC day ends. One that cannot be read goes.
 */
export async function purgeLimits(bucket: R2BucketLike, now: Date = new Date()): Promise<PurgeResult> {
  const at = now.toISOString();
  const today = at.slice(0, 10);
  if (!bucket.list) return { ok: false, at, deleted: 0, kept: 0, failed: 0, error: "bucket cannot list" };
  const res: PurgeResult = { ok: true, at, deleted: 0, kept: 0, failed: 0 };
  try {
    for (const prefix of LIMIT_PREFIXES) {
      for (const k of await listKeys(bucket, prefix, Infinity)) {
        let day: unknown = null;
        try { const o = await bucket.get(k); day = o ? JSON.parse(await o.text()).day : null; } catch { /* unreadable: deleted */ }
        if (day === today) { res.kept++; continue; }
        try { await bucket.delete(k); res.deleted++; } catch { res.failed++; }
      }
    }
    if (res.failed) { res.ok = false; res.error = `${res.failed} rate-limit counter(s) could not be deleted`; }
  } catch (e) {
    res.ok = false; res.error = e instanceof Error ? e.message : String(e);
  }
  return res;
}

/** GET /reports (authed): every report received on or after `since` (YYYY-MM-DD), newest first, at most `max`. */
export async function listReports(bucket: R2BucketLike, since: string, now: Date = new Date(), max = 500, withTest = false): Promise<StoredReport[]> {
  return (await listReportsCounted(bucket, since, now, max, withTest)).reports;
}

/** (T609 W10) the list and whether it is whole: `truncated` (stopped at `max`, or `since` is past the 366-day reach) and
 *  `skipped` (objects that could not be read), so the review app can say a cut list is cut */
export async function listReportsCounted(bucket: R2BucketLike, since: string, now: Date = new Date(), max = 500, withTest = false):
    Promise<{ reports: StoredReport[]; truncated: boolean; skipped: number }> {
  const today = now.toISOString().slice(0, 10);
  const want = Math.round((Date.parse(today) - Date.parse(since)) / 86_400_000) + 1;
  const n = Math.min(366, Math.max(1, Math.round((Date.parse(today) - Date.parse(since)) / 86_400_000) + 1));
  const keys: string[] = [];
  for (const d of days(now, Number.isFinite(n) ? n : 30)) {
    keys.push(...(await listKeys(bucket, `${REPORTS_PREFIX}${d}/`)));
    if (withTest) keys.push(...(await listKeys(bucket, `${TEST_REPORTS_PREFIX}${d}/`)));
    if (keys.length >= max) break;
  }
  const out: StoredReport[] = [];
  let skipped = 0;
  for (const k of keys.slice(0, max)) {
    const o = await bucket.get(k);
    if (!o) { skipped++; continue; }
    try {
      const rep = JSON.parse(await o.text()) as StoredReport;
      if (k.startsWith(TEST_REPORTS_PREFIX)) rep.test = true;
      out.push(rep);
    } catch { skipped++; }
  }
  return { reports: out.sort((a, b) => (a.received < b.received ? 1 : a.received > b.received ? -1 : 0)),
           truncated: keys.length > max || (Number.isFinite(want) && want > 366), skipped };
}

/** (T558 item 6) at most this many report lines are raised per run; the rest wait for the next run (none is dropped) */
export const REPORT_EVENTS_PER_RUN = 100;

/**
 * One digest line per stored report the outbox has not told (`known`), newest day first. Never the email. (T558 item 6)
 * No date window: a report stored while the gauge run was failing is told when it runs again, not lost after two days
 * (the outbox key `report:<id>` dedupes; reports live 180 days, the outbox remembers 366).
 */
export async function reportEvents(bucket: R2BucketLike, known: (key: string) => boolean, _now: Date, at: string): Promise<FeedEvent[]> {
  const events: FeedEvent[] = [];
  const keys = (await listKeys(bucket, REPORTS_PREFIX, Infinity))
    .filter((k) => /^\d{4}-\d{2}-\d{2}\/[0-9a-f]{16}\.json$/.test(k.slice(REPORTS_PREFIX.length)))
    .sort().reverse();
  for (const k of keys) {
    if (events.length >= REPORT_EVENTS_PER_RUN) break;
    const id = /\/([0-9a-f]{16})\.json$/.exec(k)?.[1];
    if (!id || known(`report:${id}`)) continue;
    const o = await bucket.get(k);
    if (!o) continue;
    let r: StoredReport;
    try {
      r = JSON.parse(await o.text());
    } catch {
      continue;
    }
    events.push(reportEvent(r, at));
  }
  return events;
}

export function reportEvent(r: StoredReport, at: string): FeedEvent {
  const c = r.context as Record<string, string | number | undefined>;
  const where = [c.place, c.water ? `(${c.water})` : null, c.part !== undefined ? `part ${c.part}` : null].filter(Boolean).join(" ") || "no place given";
  const oneLine = r.text.replace(/\s+/g, " ");
  return {
    key: `report:${r.id}`,
    kind: "report",
    title: `${REASON_WORDS[r.reason] ?? r.reason}: ${where} — ${oneLine.slice(0, 90)}${oneLine.length > 90 ? "…" : ""}${r.contact && r.email ? " · reply allowed" : ""}`,
    text:
      `${r.text}\n\n` +
      `Where: ${where}${c.day ? ` · ${c.day}` : ""}${c.mode ? ` · map ${c.mode}` : ""}\n` +
      // (T569) report/2, asked from a water's Conditions: the gauge charted, its quantity and span, the feed's version
      (c.station ? `Gauge: ${[c.station, c.quantity, c.span, c.feed ? `feed ${c.feed}` : null].filter(Boolean).join(" · ")}\n` : "") +
      // (T352) report/2: the map as the reader had it
      (c.layer || c.zoom !== undefined || c.horizon !== undefined
        ? `Map: ${[c.layer, c.horizon !== undefined ? `forecast ${c.horizon}` : null,
                   c.zoom !== undefined ? `zoom ${c.zoom}` : null,
                   c.lat !== undefined && c.lon !== undefined ? `centre ${c.lat}, ${c.lon}` : null].filter(Boolean).join(" · ")}\n`
        : "") +
      `App: ${c.app ?? "?"}${c.env ? ` (${c.env})` : ""} · ${c.platform ?? "?"} · release ${c.release ?? "?"}\n` +
      `Reply: ${r.contact && r.email ? "allowed (Review app → Reports → Reply by email)" : r.email ? "not allowed (email given, contact not ticked)" : "no email"}\n` +
      `Received ${r.received} · id ${r.id}${c.install ? ` · install ${c.install}` : ""}\n` +
      `Review: the review app's Reports tab (Sync).`,
    at,
    // (T627) what the digest's short block is made of; the text above stays the whole record (never the email address)
    data: { id: r.id, received: r.received, reason: REASON_WORDS[r.reason] ?? r.reason, where, words: r.text,
            day: c.day ?? null, mode: c.mode ?? null, app: c.app ?? null, env: c.env ?? null, platform: c.platform ?? null, release: c.release ?? null,
            reply: r.contact && r.email ? "allowed" : r.email ? "not allowed" : "no email",
            // what else the full report holds, by name, so the block can say what it left out
            also: [c.station ? "gauge" : null,
                   c.layer || c.zoom !== undefined || c.horizon !== undefined ? "map position" : null,
                   c.install ? "install id" : null].filter(Boolean) },
  };
}

/** (T291) delete test reports received more than TEST_RETENTION_DAYS ago (by their day folder). (T558 W9) The result says
 *  what could not be deleted; the worker keeps it beside the report purge's and tells a failure. */
export async function purgeTestReports(bucket: R2BucketLike, now: Date = new Date()): Promise<PurgeResult> {
  const at = now.toISOString();
  if (!bucket.list) return { ok: false, at, deleted: 0, kept: 0, failed: 0, error: "bucket cannot list" };
  const cutoff = now.getTime() - TEST_RETENTION_DAYS * 86_400_000;
  const res: PurgeResult = { ok: true, at, deleted: 0, kept: 0, failed: 0 };
  try {
    for (const k of await listKeys(bucket, TEST_REPORTS_PREFIX, Infinity)) {
      const day = k.slice(TEST_REPORTS_PREFIX.length).match(/^(\d{4}-\d{2}-\d{2})\//)?.[1];
      if (!(day && Date.parse(day) + 86_400_000 < cutoff)) { res.kept++; continue; }
      try { await bucket.delete(k); res.deleted++; } catch { res.failed++; }
    }
    if (res.failed) { res.ok = false; res.error = `${res.failed} test report(s) could not be deleted`; }
  } catch (e) {
    res.ok = false; res.error = e instanceof Error ? e.message : String(e);
  }
  return res;
}
