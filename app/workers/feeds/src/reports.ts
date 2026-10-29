// T237: PROBLEM REPORTS from the app — `POST /report` (public), stored one JSON per report in the PRIVATE part of the
// bucket, and told to the owner in the daily digest (T225 notify policy: a report is not urgent).
//
//   POST /report        a `report/1` payload (app/packages/core/src/menu/report.ts `reportPayload`). Validated (size,
//                       shape, limits — the app's limits, enforced again here since anyone can POST), rate-limited per
//                       IP, stored at private/reports/<received day>/<id>.json. Answers {ok, id} — never the content.
//                       The same id re-sent the same day (the app's outbox retrying after a lost answer) is stored once.
//   GET /reports        (Bearer RUN_TOKEN) every stored report since `?since=YYYY-MM-DD` (default: 30 days), newest
//                       first — what the review app's Reports tab syncs.
//   the digest          `reportEvents` (called by the gauge run, every 30 min): reports received today or yesterday that
//                       the outbox has not told yet → one digest line each. The reporter's email is never in it.
//
// RATE LIMIT (T249): one report per IP per 10 s and 30 per IP per day — keyed by a salted SHA-256 of the IP
// (private/reports/limits/<hash>.json; the IP itself is never stored), plus the REPORT_LIMITER rate-limit binding
// (wrangler.toml [[ratelimits]], 1 per 10 s per IP at the edge) as the cheap first gate when it is bound.
// PRIVACY: what is stored is the payload and the time it arrived — no IP, no User-Agent, no country.
import type { FeedEvent } from "./events";
import type { R2BucketLike } from "./io";

export const REPORTS_PREFIX = "private/reports/";
const LIMITS_PREFIX = `${REPORTS_PREFIX}limits/`;
export const REPORT_FORMAT = "report/1";
export const REASONS = ["rule", "place", "missing", "name", "other"] as const;
const REASON_WORDS: Record<(typeof REASONS)[number], string> = {
  rule: "Wrong rule", place: "Wrong place on the map", missing: "Missing water", name: "Wrong name", other: "Something else",
};
/** the app's limits (core REPORT_LIMITS) */
export const LIMITS = { bytes: 16_384, text: 4000, email: 254, errors: 20, line: 300, field: 200 } as const;
/** one report per IP per this, and at most PER_DAY a day */
export const MIN_GAP_MS = 10_000;
export const PER_DAY = 30;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const STR_FIELDS = ["screen", "tab", "place", "water", "partLabel", "day", "mode", "app", "env", "platform", "release", "tiles"] as const;
const INT_FIELDS = ["part", "section"] as const;

export interface StoredReport {
  format: typeof REPORT_FORMAT;
  id: string;
  created: string;
  received: string;
  reason: (typeof REASONS)[number];
  text: string;
  email: string | null;
  context: Record<string, unknown>;
}

export interface ReportEnv {
  BUCKET: R2BucketLike;
  /** Cloudflare rate-limit binding ([[ratelimits]] in wrangler.toml); absent under test */
  REPORT_LIMITER?: { limit(o: { key: string }): Promise<{ success: boolean }> };
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
  if (r.format !== REPORT_FORMAT) return { ok: false, error: `format must be ${REPORT_FORMAT}` };
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
    } else if (k === "errors") {
      if (!Array.isArray(v) || v.length > LIMITS.errors || v.some((l) => typeof l !== "string" || l.length > LIMITS.line))
        return { ok: false, error: "bad context.errors" };
      ctx[k] = v;
    } else return { ok: false, error: `unknown context.${k}` };
  }
  return { ok: true, report: { format: REPORT_FORMAT, id: r.id, created: r.created, reason: r.reason as StoredReport["reason"], text: r.text.trim(), email, context: ctx } };
}

async function sha256hex(s: string): Promise<string> {
  const d = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s));
  return [...new Uint8Array(d)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/** The per-IP gate: null = allowed (and recorded), else the refusal. */
export async function rateLimit(env: ReportEnv, ip: string, now: Date): Promise<string | null> {
  if (env.REPORT_LIMITER) {
    try {
      if (!(await env.REPORT_LIMITER.limit({ key: `report:${ip}` })).success) return "too many reports; try again in a minute";
    } catch { /* the binding failing never blocks a report: the R2 gate still holds */ }
  }
  const key = `${LIMITS_PREFIX}${(await sha256hex(`${env.REPORT_SALT ?? "bc-fishing-report"}:${ip}`)).slice(0, 32)}.json`;
  const day = now.toISOString().slice(0, 10);
  let st: { last: number; day: string; n: number } = { last: 0, day, n: 0 };
  try {
    const o = await env.BUCKET.get(key);
    if (o) st = JSON.parse(await o.text());
  } catch { /* fresh */ }
  if (st.day !== day) st = { last: st.last, day, n: 0 };
  if (now.getTime() - st.last < MIN_GAP_MS) return "too many reports; try again in a minute";
  if (st.n >= PER_DAY) return "too many reports today";
  await env.BUCKET.put(key, JSON.stringify({ last: now.getTime(), day, n: st.n + 1 }), { httpMetadata: { contentType: "application/json" } });
  return null;
}

export const reportKey = (day: string, id: string) => `${REPORTS_PREFIX}${day}/${id}.json`;

/** POST /report */
export async function handleReport(req: Request, env: ReportEnv, now: Date = new Date()): Promise<Response> {
  const len = Number(req.headers.get("Content-Length") ?? NaN);
  if (Number.isFinite(len) && len > LIMITS.bytes) return json({ ok: false, error: "report too large" }, 413);
  let raw: string;
  try {
    raw = await req.text();
  } catch {
    return json({ ok: false, error: "unreadable body" }, 400);
  }
  if (raw.length > LIMITS.bytes) return json({ ok: false, error: "report too large" }, 413);
  let body: unknown;
  try {
    body = JSON.parse(raw);
  } catch {
    return json({ ok: false, error: "not JSON" }, 400);
  }
  const v = validateReport(body);
  if (!v.ok) return json({ ok: false, error: v.error }, 400);
  const day = now.toISOString().slice(0, 10);
  const key = reportKey(day, v.report.id);
  // the app's outbox re-sending after a lost answer: already stored, the same answer, nothing counted
  if (await env.BUCKET.get(key)) return json({ ok: true, id: v.report.id }, 200);
  const ip = req.headers.get("CF-Connecting-IP") ?? "unknown";
  const refused = await rateLimit(env, ip, now);
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

async function listKeys(bucket: R2BucketLike, prefix: string, max = 1000): Promise<string[]> {
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

/** GET /reports (authed): every report received on or after `since` (YYYY-MM-DD), newest first, at most `max`. */
export async function listReports(bucket: R2BucketLike, since: string, now: Date = new Date(), max = 500): Promise<StoredReport[]> {
  const today = now.toISOString().slice(0, 10);
  const n = Math.min(366, Math.max(1, Math.round((Date.parse(today) - Date.parse(since)) / 86_400_000) + 1));
  const keys: string[] = [];
  for (const d of days(now, Number.isFinite(n) ? n : 30)) {
    keys.push(...(await listKeys(bucket, `${REPORTS_PREFIX}${d}/`)));
    if (keys.length >= max) break;
  }
  const out: StoredReport[] = [];
  for (const k of keys.slice(0, max)) {
    const o = await bucket.get(k);
    if (!o) continue;
    try {
      out.push(JSON.parse(await o.text()));
    } catch { /* a broken object is skipped */ }
  }
  return out.sort((a, b) => (a.received < b.received ? 1 : a.received > b.received ? -1 : 0));
}

/** One digest line per report received today or yesterday that the outbox has not told (`told`). Never the email. */
export async function reportEvents(bucket: R2BucketLike, known: (key: string) => boolean, now: Date, at: string): Promise<FeedEvent[]> {
  const events: FeedEvent[] = [];
  for (const d of days(now, 2)) {
    for (const k of await listKeys(bucket, `${REPORTS_PREFIX}${d}/`, 200)) {
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
    title: `${REASON_WORDS[r.reason] ?? r.reason}: ${where} — ${oneLine.slice(0, 90)}${oneLine.length > 90 ? "…" : ""}`,
    text:
      `${r.text}\n\n` +
      `Where: ${where}${c.day ? ` · ${c.day}` : ""}${c.mode ? ` · map ${c.mode}` : ""}\n` +
      `App: ${c.app ?? "?"}${c.env ? ` (${c.env})` : ""} · ${c.platform ?? "?"} · release ${c.release ?? "?"}\n` +
      `Contact email: ${r.email ? "given (in the stored report)" : "none"}\n` +
      `Received ${r.received} · id ${r.id}\n` +
      `Review: the review app's Reports tab (Sync).`,
    at,
  };
}
