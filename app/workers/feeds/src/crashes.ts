// T242: CRASH AND ERROR REPORTS from the app — `POST /crash` (public), everything from our own server (no third-party
// crash service, owner rule). The app sends ONE small payload per new error kind per run, only while online and only
// while "Send crash reports" (More → Data) is on:
//
//   POST /crash         a `crash/1` payload (app/packages/ui/src/crashReport.ts `crashPayload`): the app build, env,
//                       platform, data release, the route KIND (map / conditions / runs / spots / menu / water — never a
//                       water name, coordinate or id), the error's name, a scrubbed message and the stack. Validated
//                       (size, shape, every key known), rate-limited per IP, then folded into ONE object per error kind.
//   one object per kind private/crashes/<hash>.json — {hash, first, last, count, versions, sample}. The hash is the
//                       error's name + message + stack with the numbers taken out, so a kind is one object across
//                       versions and a repeat only moves `last` and `count`. NO IP, User-Agent or country is stored.
//   GET /crashes        (Bearer RUN_TOKEN) every kind seen since `?since=YYYY-MM-DD`, newest first.
//   the digest          `crashEvents` (the gauge run): a kind whose `first` is today or yesterday and that the outbox has not
//                       told → one digest line (not urgent). A repeat of a known kind is never told again.
//   purge               kinds not seen for CRASH_RETENTION_DAYS are deleted (privacy page: "90 days after the last time").
import type { FeedEvent } from "./events";
import type { R2BucketLike } from "./io";
import { CORS, listKeys, rateLimit, sha256hex, type ReportEnv } from "./reports";

export const CRASHES_PREFIX = "private/crashes/";
const LIMITS_PREFIX = `${CRASHES_PREFIX}limits/`;
export const CRASH_FORMAT = "crash/1";
/** (T291) error kinds a TOOL sent (`test: true`): stored apart, not in the digest, deleted 7 days after last seen. Staging only. */
export const TEST_CRASHES_PREFIX = "private/crashes-test/";
export const CRASH_KINDS = ["js", "promise", "render", "native"] as const;
export const ROUTE_KINDS = ["map", "conditions", "runs", "spots", "menu", "water", "other"] as const;
export const CRASH_LIMITS = { bytes: 8192, name: 60, message: 200, stack: 2000, field: 100 } as const;
/** one crash POST per IP per this, and at most PER_DAY a day (the app sends one per new kind per run) */
export const CRASH_GAP_MS = 1_000;
export const CRASH_PER_DAY = 40;
export const CRASH_RETENTION_DAYS = 90;
const STR_FIELDS = ["app", "env", "platform", "release"] as const;

export interface CrashPayload {
  format: typeof CRASH_FORMAT;
  created: string;
  kind: (typeof CRASH_KINDS)[number];
  fatal: boolean;
  route: (typeof ROUTE_KINDS)[number];
  app: string; env: string; platform: string; release: string;
  name: string; message: string; stack: string;
  test?: boolean;
}
export interface CrashKind {
  format: "crash-kind/1";
  test?: true;
  hash: string;
  first: string;
  last: string;
  count: number;
  /** app version → times seen (at most 20 versions kept) */
  versions: Record<string, number>;
  sample: Omit<CrashPayload, "format">;
}

const json = (body: unknown, status: number) =>
  new Response(JSON.stringify(body), { status, headers: { ...CORS, "Content-Type": "application/json", "Cache-Control": "no-store" } });

export function validateCrash(raw: unknown): { ok: true; crash: CrashPayload } | { ok: false; error: string } {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return { ok: false, error: "not a crash report" };
  const r = raw as Record<string, unknown>;
  const known = new Set(["format", "created", "kind", "fatal", "route", "name", "message", "stack", "test", ...STR_FIELDS]);
  for (const k of Object.keys(r)) if (!known.has(k)) return { ok: false, error: `unknown ${k}` };
  if (r.format !== CRASH_FORMAT) return { ok: false, error: `format must be ${CRASH_FORMAT}` };
  if (typeof r.created !== "string" || r.created.length > 40 || Number.isNaN(Date.parse(r.created))) return { ok: false, error: "bad created" };
  if (!(CRASH_KINDS as readonly string[]).includes(r.kind as string)) return { ok: false, error: "bad kind" };
  if (r.test !== undefined && typeof r.test !== "boolean") return { ok: false, error: "bad test" };
  if (typeof r.fatal !== "boolean") return { ok: false, error: "bad fatal" };
  if (!(ROUTE_KINDS as readonly string[]).includes(r.route as string)) return { ok: false, error: "bad route" };
  for (const k of STR_FIELDS) if (typeof r[k] !== "string" || (r[k] as string).length > CRASH_LIMITS.field) return { ok: false, error: `bad ${k}` };
  for (const [k, max] of [["name", CRASH_LIMITS.name], ["message", CRASH_LIMITS.message], ["stack", CRASH_LIMITS.stack]] as const)
    if (typeof r[k] !== "string" || (r[k] as string).length > max) return { ok: false, error: `bad ${k}` };
  if (!(r.name as string).trim()) return { ok: false, error: "no error name" };
  return { ok: true, crash: r as unknown as CrashPayload };
}

/** the kind's identity: name + message + the stack's first function names, with every number taken out (so versions and
 *  line/column shifts are one kind); 16 hex characters */
export async function crashHash(c: Pick<CrashPayload, "name" | "message" | "stack">): Promise<string> {
  const lines = c.stack.split("\n").slice(0, 6).map((l) => l.replace(/https?:\/\/\S+|\(.*\)|[0-9]+/g, "").replace(/\s+/g, " ").trim());
  return (await sha256hex([c.name, c.message.replace(/[0-9]+/g, "#"), ...lines].join("\n"))).slice(0, 16);
}

export const crashKey = (hash: string) => `${CRASHES_PREFIX}${hash}.json`;

/** POST /crash */
export async function handleCrash(req: Request, env: ReportEnv, now: Date = new Date()): Promise<Response> {
  const len = Number(req.headers.get("Content-Length") ?? NaN);
  if (Number.isFinite(len) && len > CRASH_LIMITS.bytes) return json({ ok: false, error: "too large" }, 413);
  let raw: string;
  try { raw = await req.text(); } catch { return json({ ok: false, error: "unreadable body" }, 400); }
  if (raw.length > CRASH_LIMITS.bytes) return json({ ok: false, error: "too large" }, 413);
  let body: unknown;
  try { body = JSON.parse(raw); } catch { return json({ ok: false, error: "not JSON" }, 400); }
  const v = validateCrash(body);
  if (!v.ok) return json({ ok: false, error: v.error }, 400);
  const ip = req.headers.get("CF-Connecting-IP") ?? "unknown";
  const refused = await rateLimit(env, ip, now, { prefix: LIMITS_PREFIX, gapMs: CRASH_GAP_MS, perDay: CRASH_PER_DAY, edge: false });
  if (refused) return json({ ok: false, error: refused }, 429);
  const hash = await crashHash(v.crash);
  const isTest = v.crash.test === true && env.FEEDS_ENV === "staging";   // honoured on staging only
  const key = isTest ? `${TEST_CRASHES_PREFIX}${hash}.json` : crashKey(hash);
  const at = now.toISOString();
  const { format: _format, test: _test, ...sample } = v.crash;
  let kind: CrashKind = { format: "crash-kind/1", hash, first: at, last: at, count: 0, versions: {}, sample };
  try {
    const o = await env.BUCKET.get(key);
    if (o) kind = JSON.parse(await o.text());
  } catch { /* a broken object is replaced */ }
  kind.last = at;
  kind.count++;
  if (kind.versions[v.crash.app] !== undefined || Object.keys(kind.versions).length < 20) kind.versions[v.crash.app] = (kind.versions[v.crash.app] ?? 0) + 1;
  try {
    await env.BUCKET.put(key, JSON.stringify(kind, null, 1), { httpMetadata: { contentType: "application/json" } });
  } catch {
    return json({ ok: false, error: "could not store" }, 500);
  }
  return json({ ok: true }, 201);
}

const kindKeys = async (bucket: R2BucketLike, max: number) =>
  (await listKeys(bucket, CRASHES_PREFIX, max)).filter((k) => !k.startsWith(LIMITS_PREFIX));

/** GET /crashes (authed): the kinds last seen on or after `since` (YYYY-MM-DD), newest first. */
export async function listCrashes(bucket: R2BucketLike, since: string, max = 500, withTest = false): Promise<CrashKind[]> {
  const out: CrashKind[] = [];
  const keys = [...await kindKeys(bucket, max), ...(withTest ? await listKeys(bucket, TEST_CRASHES_PREFIX, max) : [])];
  for (const k of keys) {
    const o = await bucket.get(k);
    if (!o) continue;
    try {
      const c = JSON.parse(await o.text()) as CrashKind;
      if (c.last.slice(0, 10) >= since) out.push(k.startsWith(TEST_CRASHES_PREFIX) ? { ...c, test: true } : c);
    } catch { /* skipped */ }
  }
  return out.sort((a, b) => (a.last < b.last ? 1 : a.last > b.last ? -1 : 0));
}

/** One digest line per NEW error kind (first seen today or yesterday) the outbox has not told. */
export async function crashEvents(bucket: R2BucketLike, known: (key: string) => boolean, now: Date, at: string): Promise<FeedEvent[]> {
  const recent = new Set([0, 1].map((i) => new Date(now.getTime() - i * 86_400_000).toISOString().slice(0, 10)));
  const events: FeedEvent[] = [];
  for (const k of await kindKeys(bucket, 500)) {
    const hash = /\/([0-9a-f]{16})\.json$/.exec(k)?.[1];
    if (!hash || known(`crash:${hash}`)) continue;
    const o = await bucket.get(k);
    if (!o) continue;
    let c: CrashKind;
    try { c = JSON.parse(await o.text()); } catch { continue; }
    if (recent.has(c.first.slice(0, 10))) events.push(crashEvent(c, at));
  }
  return events;
}

export function crashEvent(c: CrashKind, at: string): FeedEvent {
  const s = c.sample;
  const vs = Object.keys(c.versions).join(", ");
  return {
    key: `crash:${c.hash}`,
    kind: "crash",
    title: `${s.name}: ${s.message.slice(0, 90)}${s.message.length > 90 ? "…" : ""} (${s.platform}, ${s.route})`,
    text:
      `${s.name}: ${s.message}\n${s.stack}\n\n` +
      `New error kind ${c.hash}${s.fatal ? " (fatal)" : ""}: ${s.kind === "native" ? "the system ended the app (Android exit record)" : `${s.kind} error`} on the ${s.route} screen.\n` +
      `App ${s.app} (${s.env}) · ${s.platform} · release ${s.release} · versions seen: ${vs}\n` +
      `First seen ${c.first} · ${c.count} so far. Read them all: GET /crashes on the feeds worker (Bearer RUN_TOKEN).`,
    at,
  };
}

/** The daily purge: delete every kind not seen for CRASH_RETENTION_DAYS. */
export async function purgeCrashes(bucket: R2BucketLike, now: Date = new Date()): Promise<{ ok: boolean; deleted: number; failed: number }> {
  const cutoff = now.getTime() - CRASH_RETENTION_DAYS * 86_400_000;
  const testCutoff = now.getTime() - 7 * 86_400_000;
  const res = { ok: true, deleted: 0, failed: 0 };
  try {
    for (const k of [...await kindKeys(bucket, Infinity), ...await listKeys(bucket, TEST_CRASHES_PREFIX, Infinity)]) {
      const lim = k.startsWith(TEST_CRASHES_PREFIX) ? testCutoff : cutoff;
      let last = NaN;
      try { const o = await bucket.get(k); if (o) last = Date.parse(JSON.parse(await o.text()).last); } catch { /* unreadable: deleted below */ }
      if (Number.isFinite(last) && last >= lim) continue;
      try { await bucket.delete(k); res.deleted++; } catch { res.failed++; }
    }
  } catch { res.ok = false; }
  if (res.failed) res.ok = false;
  return res;
}
