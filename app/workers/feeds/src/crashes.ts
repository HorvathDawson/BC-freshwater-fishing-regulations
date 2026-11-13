// T242: CRASH AND ERROR REPORTS from the app — `POST /crash` (public), everything from our own server (no third-party
// crash service, owner rule). The app sends ONE small payload per new error kind per run, only while online and only
// while "Send crash reports" (More → Data) is on:
//
//   POST /crash         a `crash/2` payload (app/packages/ui/src/crashReport.ts `crashPayload`) — or a `crash/1` (0.4.0
//                       and earlier phones keep sending it): the app build, env, platform, data release, the route KIND
//                       (map / conditions / runs / spots / menu / water), the error's name, a scrubbed message and the
//                       stack; (T352) crash/2 adds the open water's item id and its pane (view, part, section handle,
//                       whole, sheet), a random install id and `background`. Validated per version (size, shape, every
//                       key known to THAT version), rate-limited per IP, then folded into ONE object per error kind.
//                       (T352) env "local" (a developer's build, not a tester) is answered 202 and not stored.
//   one object per kind private/crashes/<hash>.json — {hash, first, last, count, versions, installs, sample}. The hash is
//                       the error's name + message + the stack's function names, browser-neutral (`crashHash`), so a kind
//                       is one object across versions and browsers and a repeat only moves `last` and `count`. NO IP,
//                       User-Agent or country is stored.
//   GET /crashes        (Bearer RUN_TOKEN) every kind seen since `?since=YYYY-MM-DD`, newest first; (T352) background OS
//                       kills only with `?background=1`. (T595) Each kind carries a `class` — `defect` (needs a fix),
//                       `network` (a line that failed, already shown to the reader) or `expected` (a state the app knows:
//                       an old app meeting a newer release, a phone with no Play service) — and the list is the DEFECTS
//                       unless `?class=all` (or `?class=network,expected`). Nothing is dropped: the others are stored,
//                       counted (`counts`) and reachable.
//   the digest          `crashEvents` (the gauge run): a defect kind the outbox has not told (T558: whenever first seen)
//                       → one digest line (not urgent). A repeat of a known kind is never told again. (T352) A
//                       background OS kill is never told: it is not a crash.
//   purge               kinds not seen for CRASH_RETENTION_DAYS are deleted (privacy page: "90 days after the last time").
import type { FeedEvent } from "./events";
import type { R2BucketLike } from "./io";
import { CORS, CRASH_KINDS_PER_DAY_TOTAL, dailyTotal, readCapped, TEST_PER_DAY_TOTAL, TEST_TOTAL_PREFIX, LIMIT_PREFIXES, listKeys, noSalt, rateLimit, sha256hex, type ReportEnv } from "./reports";

export const CRASHES_PREFIX = "private/crashes/";
/** (T559) the per-address counters: `purgeLimits` (reports.ts) deletes them once their day is over */
const LIMITS_PREFIX = LIMIT_PREFIXES[1];
/** (T352) the newest format; `CRASH_FORMATS` are every one accepted */
export const CRASH_FORMAT = "crash/2";
export const CRASH_FORMATS = ["crash/1", "crash/2"] as const;
/** (T291) error kinds a TOOL sent (`test: true`): stored apart, not in the digest, deleted 7 days after last seen. Staging only. */
export const TEST_CRASHES_PREFIX = "private/crashes-test/";
export const CRASH_KINDS = ["js", "promise", "render", "native"] as const;
export const ROUTE_KINDS = ["map", "conditions", "runs", "spots", "menu", "water", "other"] as const;
/** (T352) crash/2's pane facts */
export const VIEWS = ["regs", "conditions", "map"] as const;
export const SHEETS = ["search", "spot", "adding"] as const;
/** at most this many distinct install ids kept per kind (enough to count testers; the rest only move `count`) */
export const KIND_INSTALLS = 100;
export const CRASH_LIMITS = { bytes: 8192, name: 60, message: 200, stack: 2000, field: 100 } as const;
/** one crash POST per IP per this, and at most PER_DAY a day (the app sends one per new kind per run) */
export const CRASH_GAP_MS = 1_000;
export const CRASH_PER_DAY = 40;
export const CRASH_RETENTION_DAYS = 90;
const STR_FIELDS = ["app", "env", "platform", "release"] as const;

interface CrashPayloadV1 {
  format: "crash/1";
  created: string;
  kind: (typeof CRASH_KINDS)[number];
  fatal: boolean;
  route: (typeof ROUTE_KINDS)[number];
  app: string; env: string; platform: string; release: string;
  name: string; message: string; stack: string;
  test?: boolean;
}
/** (T352) crash/2: crash/1 and the open water, its pane, the install id and `background` */
interface CrashPayloadV2 extends Omit<CrashPayloadV1, "format"> {
  format: "crash/2";
  background: boolean;
  install: string;
  water: string | null;
  view: (typeof VIEWS)[number] | null;
  part: number | null;
  whole: boolean;
  sheet: (typeof SHEETS)[number] | null;
}
export type CrashPayload = CrashPayloadV1 | CrashPayloadV2;
export interface CrashKind {
  format: "crash-kind/1";
  test?: true;
  /** (T595) set on the way out (`listCrashes`), from the sample: never stored, so a rule change reclassifies old kinds */
  class?: CrashClass;
  hash: string;
  first: string;
  last: string;
  count: number;
  /** app version → times seen (at most 20 versions kept) */
  versions: Record<string, number>;
  /** (T352) the distinct install ids seen (at most KIND_INSTALLS kept); absent on a kind only crash/1 has reached */
  installs?: string[];
  /** (T352) the system closing the app in the background (not a crash): left out of the digest and the default list */
  background?: true;
  sample: Omit<CrashPayloadV1, "format"> | Omit<CrashPayloadV2, "format">;
}

const json = (body: unknown, status: number) =>
  new Response(JSON.stringify(body), { status, headers: { ...CORS, "Content-Type": "application/json", "Cache-Control": "no-store" } });

const V1_KEYS = ["format", "created", "kind", "fatal", "route", "name", "message", "stack", "test", ...STR_FIELDS] as const;
const V2_KEYS = [...V1_KEYS, "background", "install", "water", "view", "part", "whole", "sheet"] as const;
const INSTALL_RE = /^[0-9a-f]{16}$/;
const ITEM_RE = /^[\w:.-]{1,100}$/;
const nullOr = <T>(v: unknown, ok: (x: unknown) => x is T): boolean => v === null || ok(v);
const isInt = (x: unknown): x is number => typeof x === "number" && Number.isInteger(x) && x >= 0 && x <= 2 ** 31;

export function validateCrash(raw: unknown): { ok: true; crash: CrashPayload } | { ok: false; error: string } {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return { ok: false, error: "not a crash report" };
  const r = raw as Record<string, unknown>;
  if (!(CRASH_FORMATS as readonly unknown[]).includes(r.format)) return { ok: false, error: `format must be one of ${CRASH_FORMATS.join(", ")}` };
  const v2 = r.format === "crash/2";
  // every key known to THIS version: a crash/1 with a crash/2 field is refused, as is any unknown key
  const known = new Set<string>(v2 ? V2_KEYS : V1_KEYS);
  for (const k of Object.keys(r)) if (!known.has(k)) return { ok: false, error: `unknown ${k}` };
  if (typeof r.created !== "string" || r.created.length > 40 || Number.isNaN(Date.parse(r.created))) return { ok: false, error: "bad created" };
  if (!(CRASH_KINDS as readonly string[]).includes(r.kind as string)) return { ok: false, error: "bad kind" };
  if (r.test !== undefined && typeof r.test !== "boolean") return { ok: false, error: "bad test" };
  if (typeof r.fatal !== "boolean") return { ok: false, error: "bad fatal" };
  if (!(ROUTE_KINDS as readonly string[]).includes(r.route as string)) return { ok: false, error: "bad route" };
  for (const k of STR_FIELDS) if (typeof r[k] !== "string" || (r[k] as string).length > CRASH_LIMITS.field) return { ok: false, error: `bad ${k}` };
  for (const [k, max] of [["name", CRASH_LIMITS.name], ["message", CRASH_LIMITS.message], ["stack", CRASH_LIMITS.stack]] as const)
    if (typeof r[k] !== "string" || (r[k] as string).length > max) return { ok: false, error: `bad ${k}` };
  if (!(r.name as string).trim()) return { ok: false, error: "no error name" };
  if (v2) {
    // every crash/2 field is required (the app always sends them all; unknown is null)
    if (typeof r.background !== "boolean") return { ok: false, error: "bad background" };
    if (typeof r.whole !== "boolean") return { ok: false, error: "bad whole" };
    if (typeof r.install !== "string" || !INSTALL_RE.test(r.install)) return { ok: false, error: "bad install" };
    if (!nullOr(r.water, (x): x is string => typeof x === "string" && ITEM_RE.test(x))) return { ok: false, error: "bad water" };
    if (!nullOr(r.view, (x): x is string => (VIEWS as readonly unknown[]).includes(x))) return { ok: false, error: "bad view" };
    if (!nullOr(r.sheet, (x): x is string => (SHEETS as readonly unknown[]).includes(x))) return { ok: false, error: "bad sheet" };
    if (!nullOr(r.part, isInt)) return { ok: false, error: "bad part" };
  }
  return { ok: true, crash: r as unknown as CrashPayload };
}

/**
 * (T352) A background OS kill — the system reclaiming the app while it was not in front. crash/2 says so (`background`);
 * a crash/1 (0.4.0 and earlier) is read from its exit line ("LOW_MEMORY while background, …"), the only place it said it.
 */
export function isBackgroundKill(c: CrashPayload): boolean {
  if (c.format === "crash/2") return c.background;
  return c.kind === "native" && /^(LOW_MEMORY|SIGNALED) while (?!foreground\b|visible\b)/.test(c.message);
}

/**
 * A stack frame's FUNCTION NAME only, whichever browser wrote it (T352): Chrome / V8 / Hermes "at fn (file:l:c)" and
 * "at file:l:c", Safari / Firefox "fn@file:l:c" and "@file:l:c". Qualifiers ("Object.", "async ", "new ") and an
 * anonymous frame (no name) go, so the same error is one kind on every browser.
 */
export function frameName(line: string): string {
  let l = line.trim().replace(/^at\s+/, "");
  if (l.includes("@")) l = l.slice(0, l.lastIndexOf("@"));
  else if (/\(.*\)\s*$/.test(l)) l = l.replace(/\s*\(.*\)\s*$/, "");
  else return "";                                            // "at file:l:c": a frame with no name
  l = l.replace(/^(async|new)\s+/, "").replace(/\[as [^\]]*\]/g, "").trim();
  const last = l.split(/[.\s/<]+/).filter(Boolean).pop() ?? "";
  return /^(anonymous|eval|global code|module code)$/.test(last) || /^[0-9]+$/.test(last) ? "" : last;
}

/** the kind's identity: name + message (numbers out) + the stack's first named frames, browser-neutral (`frameName`), so
 *  versions, line/column shifts and browsers are one kind; 16 hex characters. (T352) Used for crash/1 and crash/2 alike:
 *  a kind stored under the older hash ages out after 90 days, and an error still being seen is one new kind, told once. */
export async function crashHash(c: Pick<CrashPayload, "name" | "message" | "stack">): Promise<string> {
  const frames = c.stack.split("\n").map(frameName).filter(Boolean).slice(0, 5);
  return (await sha256hex([c.name, c.message.replace(/[0-9]+/g, "#"), ...frames].join("\n"))).slice(0, 16);
}

/** (T595) what a kind IS: a `defect` needs a fix; `network` is a failed line (shown to the reader already); `expected` is a
 *  state the app knows and says (an old app meeting a newer release, a phone with no Play service). */
export const CRASH_CLASSES = ["defect", "network", "expected"] as const;
export type CrashClass = (typeof CRASH_CLASSES)[number];
const EXPECTED = [
  /Failed to bind to the service/i,                // no Play service to answer the own-version check (T390)
  /release r-[\w-]+ is not one this app reads/i,    // an old app refusing a newer release, and prompting for the update
  /is not one this app reads/i,
  /SecurityException: Unsupported path \/data\/local\/tmp\//i,   // an emulator-only path the OS refuses (T603 item 6, T595)
];
const NETWORK = [
  /UnknownHostException|Unable to resolve host|SocketTimeout|ConnectException|SSLException|ECONNRESET|ETIMEDOUT/i,
  /Failed to fetch|Network request failed|Load failed|NetworkError when attempting to fetch/i,
  /\bHTTP 5\d\d\b/,                                // a server's 5xx: the line or the host, not this app
  /fetch failed|timed out|the manifest could not be read/i,
];
export function classifyCrash(s: Pick<CrashPayload, "name" | "message">): CrashClass {
  const text = `${s.name}: ${s.message}`;
  if (EXPECTED.some((r) => r.test(text))) return "expected";
  if (NETWORK.some((r) => r.test(text))) return "network";
  return "defect";
}

export const crashKey = (hash: string) => `${CRASHES_PREFIX}${hash}.json`;

/** POST /crash */
export async function handleCrash(req: Request, env: ReportEnv, now: Date = new Date()): Promise<Response> {
  const unset = noSalt(env);
  if (unset) return unset;
  const len = Number(req.headers.get("Content-Length") ?? NaN);
  if (Number.isFinite(len) && len > CRASH_LIMITS.bytes) return json({ ok: false, error: "too large" }, 413);
  let raw: string | null;
  try { raw = await readCapped(req, CRASH_LIMITS.bytes); } catch { return json({ ok: false, error: "unreadable body" }, 400); }
  if (raw === null) return json({ ok: false, error: "too large" }, 413);
  let body: unknown;
  try { body = JSON.parse(raw); } catch { return json({ ok: false, error: "not JSON" }, 400); }
  const v = validateCrash(body);
  if (!v.ok) return json({ ok: false, error: v.error }, 400);
  // (T352) a developer's own build is not a tester: answered (so it does not retry) and not stored, wherever it was sent
  if (v.crash.env === "local") return json({ ok: true, stored: false }, 202);
  const ip = req.headers.get("CF-Connecting-IP") ?? "unknown";
  let refused: string | null;
  try {
    refused = await rateLimit(env, ip, now, { prefix: LIMITS_PREFIX, gapMs: CRASH_GAP_MS, perDay: CRASH_PER_DAY, edge: false });
  } catch (e) {
    console.error(JSON.stringify({ job: "crashes", limitError: String(e) }));
    return json({ ok: false, error: "could not count" }, 503);
  }
  if (refused) return json({ ok: false, error: refused }, 429);
  const hash = await crashHash(v.crash);
  const isTest = v.crash.test === true && env.FEEDS_ENV === "staging";   // honoured on staging only
  const key = isTest ? `${TEST_CRASHES_PREFIX}${hash}.json` : crashKey(hash);
  const at = now.toISOString();
  const { format: _format, test: _test, ...sample } = v.crash;
  const background = isBackgroundKill(v.crash);
  let kind: CrashKind = { format: "crash-kind/1", hash, first: at, last: at, count: 0, versions: {}, sample,
                          ...(background ? { background: true as const } : {}) };
  // (T609 W1) a read that fails is a 503, never "a new kind" (it would reset the count); a kind that does not parse is
  // replaced. A NEW kind counts against the day's total from everyone (a test kind does not)
  let o: Awaited<ReturnType<typeof env.BUCKET.get>>;
  try { o = await env.BUCKET.get(key); } catch { return json({ ok: false, error: "could not read" }, 503); }
  let known = false;
  if (o) { try { kind = JSON.parse(await o.text()); known = true; } catch { /* a broken object is replaced */ } }
  if (!known) {
    let full: string | null;
    // (T609 W19) a test kind counts against the test store's own total
    try { full = isTest ? await dailyTotal(env.BUCKET, TEST_TOTAL_PREFIX(LIMITS_PREFIX), now, TEST_PER_DAY_TOTAL)
                        : await dailyTotal(env.BUCKET, LIMITS_PREFIX, now, CRASH_KINDS_PER_DAY_TOTAL); } catch { return json({ ok: false, error: "could not count" }, 503); }
    if (full) return json({ ok: false, error: "too many new error kinds today" }, 429);
  }
  kind.last = at;
  kind.count++;
  // (T609 W11) own keys only: an app value of "constructor" / "__proto__" never reads a prototype member
  const seen = Object.hasOwn(kind.versions, v.crash.app);
  if (seen || Object.keys(kind.versions).length < 20)
    Object.defineProperty(kind.versions, v.crash.app, { value: (seen ? kind.versions[v.crash.app]! : 0) + 1, enumerable: true, writable: true, configurable: true });
  if (v.crash.format === "crash/2") {
    const ins = kind.installs ?? [];
    if (!ins.includes(v.crash.install) && ins.length < KIND_INSTALLS) ins.push(v.crash.install);
    kind.installs = ins;
    if (!("install" in kind.sample)) kind.sample = sample;     // a kind first seen from crash/1: the richer sample
  }
  try {
    await env.BUCKET.put(key, JSON.stringify(kind, null, 1), { httpMetadata: { contentType: "application/json" } });
  } catch {
    return json({ ok: false, error: "could not store" }, 500);
  }
  return json({ ok: true }, 201);
}

const kindKeys = async (bucket: R2BucketLike, max: number) =>
  (await listKeys(bucket, CRASHES_PREFIX, max)).filter((k) => !k.startsWith(LIMITS_PREFIX));

/** `?class=` → the classes asked for: absent = the defects; `all`; or a comma list of class names (unknown names refused). */
export function parseClasses(q: string | null): CrashClass[] | null {
  if (q === null || q === "") return ["defect"];
  if (q === "all") return [...CRASH_CLASSES];
  const want = q.split(",").map((x) => x.trim());
  return want.every((x) => (CRASH_CLASSES as readonly string[]).includes(x)) ? (want as CrashClass[]) : null;
}

/**
 * GET /crashes (authed): the kinds last seen on or after `since` (YYYY-MM-DD), newest first. (T595) Each carries its
 * `class`; only the `classes` asked for are listed (default: the defects). `counts` is how many kinds of each class there
 * were in range, so nothing is hidden by the default.
 */
export async function listCrashes(bucket: R2BucketLike, since: string, max = 500, withTest = false, withBackground = false,
                                  classes: readonly CrashClass[] = ["defect"]): Promise<CrashKind[]> {
  return (await listCrashesCounted(bucket, since, max, withTest, withBackground, classes)).crashes;
}

export async function listCrashesCounted(bucket: R2BucketLike, since: string, max = 500, withTest = false, withBackground = false,
                                         classes: readonly CrashClass[] = ["defect"]):
    Promise<{ crashes: CrashKind[]; counts: Record<CrashClass, number>; truncated: boolean; skipped: number }> {
  const out: CrashKind[] = [];
  const counts: Record<CrashClass, number> = { defect: 0, network: 0, expected: 0 };
  // (T609 W10) one past `max` is listed, so `truncated` says whether the list stopped
  const all = await kindKeys(bucket, max + 1), tests = withTest ? await listKeys(bucket, TEST_CRASHES_PREFIX, max + 1) : [];
  const truncated = all.length > max || tests.length > max;
  const keys = [...all.slice(0, max), ...tests.slice(0, max)];
  let skipped = 0;
  for (const k of keys) {
    const o = await bucket.get(k);
    if (!o) { skipped++; continue; }
    try {
      const c = JSON.parse(await o.text()) as CrashKind;
      if (c.background && !withBackground) continue;      // (T352) not a crash: out of the counts unless asked for
      if (c.last.slice(0, 10) < since) continue;
      const cls = classifyCrash(c.sample);
      counts[cls]++;
      if (classes.includes(cls)) out.push({ ...c, class: cls, ...(k.startsWith(TEST_CRASHES_PREFIX) ? { test: true as const } : {}) });
    } catch { skipped++; }
  }
  return { crashes: out.sort((a, b) => (a.last < b.last ? 1 : a.last > b.last ? -1 : 0)), counts, truncated, skipped };
}

/** One digest line per DEFECT kind the outbox has not told — (T558 item 6) whenever it was first seen, so a kind stored
 *  while the gauge run was failing is told when it runs again; the other classes are one count line for the kinds first
 *  seen today or yesterday. */
export async function crashEvents(bucket: R2BucketLike, known: (key: string) => boolean, now: Date, at: string): Promise<FeedEvent[]> {
  const recent = new Set([0, 1].map((i) => new Date(now.getTime() - i * 86_400_000).toISOString().slice(0, 10)));
  const events: FeedEvent[] = [];
  const others: Record<string, number> = {};
  // (T627) REPEATS, AS A COUNT: the defect kinds already told that were seen again YESTERDAY (a whole UTC day, so the
  // count does not depend on which run takes it). Counted once per day (keyed by that day): only until it is told do the
  // runs read the kinds already told.
  const yesterday = new Date(now.getTime() - 86_400_000).toISOString().slice(0, 10);
  const repeatsKey = `crash-repeats:${yesterday}`;
  const countRepeats = !known(repeatsKey);
  let repeats = 0;
  for (const k of await kindKeys(bucket, 500)) {
    const hash = /\/([0-9a-f]{16})\.json$/.exec(k)?.[1];
    if (!hash) continue;
    const told = known(`crash:${hash}`);
    if (told && !countRepeats) continue;
    const o = await bucket.get(k);
    if (!o) continue;
    let c: CrashKind;
    try { c = JSON.parse(await o.text()); } catch { continue; }
    if (c.background) continue;                             // (T352) a background OS kill is not a crash: never told
    // (T595) only a defect is a line of its own; the others are counted in ONE line below, never dropped
    const cls = classifyCrash(c.sample);
    if (told) {
      if (cls === "defect" && c.last.slice(0, 10) === yesterday && c.first.slice(0, 10) < yesterday) repeats++;
      continue;
    }
    if (cls === "defect") events.push(crashEvent(c, at));
    else if (recent.has(c.first.slice(0, 10))) others[cls] = (others[cls] ?? 0) + 1;
  }
  const n = Object.values(others).reduce((a, b) => a + b, 0);
  const key = `crash-other:${now.toISOString().slice(0, 10)}`;
  if (n > 0 && !known(key)) {
    const parts = Object.entries(others).sort(([a], [b]) => (a < b ? -1 : 1)).map(([cls, k]) => `${k} ${cls}`).join(", ");
    events.push({ key, kind: "crash", title: `${n} new error kind${n === 1 ? "" : "s"} that are not defects (${parts})`,
                  text: `${n} error kind${n === 1 ? "" : "s"} first seen today or yesterday are not defects: ${parts}. ` +
                        `List them: GET /crashes?class=network,expected on the feeds worker (Bearer RUN_TOKEN).`, at });
  }
  // (T627) `fine`: a repeat alone is never the reason for a digest (a known kind is not told again); it rides in one
  if (repeats > 0)
    events.push({ key: repeatsKey, kind: "crash", fine: true, at,
                  title: `${repeats} known error kind${repeats === 1 ? " was" : "s were"} seen again on ${yesterday}`,
                  text: `${repeats} error kind${repeats === 1 ? "" : "s"} already told ${repeats === 1 ? "was" : "were"} reported again on ${yesterday} (UTC). ` +
                        `List them, newest first: GET /crashes on the feeds worker (Bearer RUN_TOKEN).`,
                  data: { repeats, day: yesterday } });
  return events;
}

export function crashEvent(c: CrashKind, at: string): FeedEvent {
  const s = c.sample;
  const vs = Object.keys(c.versions).join(", ");
  // (T352) crash/2: the open water and its pane, and how many installs have seen it
  const pane = "install" in s
    ? [s.water ? `water ${s.water}` : null, s.view ? `${s.view} view` : null, s.part !== null ? `part ${s.part}` : null,
       s.whole ? "whole water" : null, s.sheet ? `${s.sheet} open` : null]
      .filter(Boolean).join(", ")
    : "";
  return {
    key: `crash:${c.hash}`,
    kind: "crash",
    title: `${s.name}: ${s.message.slice(0, 90)}${s.message.length > 90 ? "…" : ""} (${s.platform}, ${s.route})`,
    text:
      `${s.name}: ${s.message}\n${s.stack}\n\n` +
      `New error kind ${c.hash}${s.fatal ? " (fatal)" : ""}: ${s.kind === "native" ? "the system ended the app (Android exit record)" : `${s.kind} error`} on the ${s.route} screen${pane ? ` (${pane})` : ""}.\n` +
      `App ${s.app} (${s.env}) · ${s.platform} · release ${s.release} · versions seen: ${vs}` +
      `${c.installs ? ` · installs: ${c.installs.length}` : ""}\n` +
      `First seen ${c.first} · ${c.count} so far. Read them all: GET /crashes on the feeds worker (Bearer RUN_TOKEN).`,
    at,
    // (T627) what the digest's one line per error kind is made of; the text above stays the whole record
    data: { hash: c.hash, name: s.name, message: s.message, route: s.route, platform: s.platform, app: s.app, env: s.env,
            release: s.release, versions: Object.keys(c.versions), count: c.count, first: c.first, fatal: !!s.fatal,
            native: s.kind === "native", installs: c.installs?.length ?? null, pane },
  };
}

/** The daily purge: delete every kind not seen for CRASH_RETENTION_DAYS. (T558 W9) The result names a failure (`error`);
 *  the worker keeps it beside the report purge's and tells it. */
export async function purgeCrashes(bucket: R2BucketLike, now: Date = new Date()): Promise<{ ok: boolean; deleted: number; failed: number; error?: string }> {
  const cutoff = now.getTime() - CRASH_RETENTION_DAYS * 86_400_000;
  const testCutoff = now.getTime() - 7 * 86_400_000;
  const res: { ok: boolean; deleted: number; failed: number; error?: string } = { ok: true, deleted: 0, failed: 0 };
  try {
    for (const k of [...await kindKeys(bucket, Infinity), ...await listKeys(bucket, TEST_CRASHES_PREFIX, Infinity)]) {
      const lim = k.startsWith(TEST_CRASHES_PREFIX) ? testCutoff : cutoff;
      let last = NaN;
      try { const o = await bucket.get(k); if (o) last = Date.parse(JSON.parse(await o.text()).last); } catch { /* unreadable: deleted below */ }
      if (Number.isFinite(last) && last >= lim) continue;
      try { await bucket.delete(k); res.deleted++; } catch { res.failed++; }
    }
  } catch (e) { res.ok = false; res.error = e instanceof Error ? e.message : String(e); }
  if (res.failed) { res.ok = false; res.error ??= `${res.failed} error kind(s) could not be deleted`; }
  return res;
}
