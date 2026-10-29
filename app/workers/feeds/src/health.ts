// T225 Phase A (owner 2026-10-28): "if a source blocks us / returns bad responses, the run still updates everything
// else" — and every failure is MEASURED.
//
// `resilientFetch` wraps the network for a run: a per-attempt timeout, bounded retries with exponential backoff + jitter
// (network error, timeout, 5xx; 429 only when Retry-After is short), never a retry on 403/404, a per-host circuit
// breaker (after BREAKER_TRIP hard failures in a row the rest of that host's requests this run fail fast as `circuit`,
// so a blocking host can't eat the run's wall time), and the body of a failed attempt is cancelled (Workers allow 6
// open connections). It NEVER throws on an HTTP status: callers keep deciding what a status means.
//
// Every final outcome is tallied per SOURCE (a source = one upstream service: the ECCC hourly CSVs, one DFO region page,
// the in-season page, …; `sourceOf`) and error CLASS: http_4xx, http_404, http_5xx, blocked (403), rate_limited (429),
// timeout, network, parse (the job says the body was unusable), circuit (skipped by the breaker). The run's tally goes
// into status.json (`lastRun.sources`) and is folded into a ROLLING record per source (hourly buckets, 7 days):
// 24 h / 7 d counts per class, success rate, consecutive whole-source failed runs, last error, last ok (`foldHealth`).
// Parts (a station, a DFO region) that failed this run are kept in `parts` with since / runs-in-a-row / class.
import type { Fetch } from "./io";

export type ErrorClass = "http_4xx" | "http_404" | "http_5xx" | "blocked" | "rate_limited" | "timeout" | "network" | "parse" | "circuit";

export interface Policy {
  /** attempts per request (1 = no retry: the caller retries itself) */
  attempts: number;
  /** per-attempt timeout; null = keep the caller's signal */
  timeoutMs: number | null;
  /** a 404 is an expected answer here (a forecast series out of season): counted ok */
  expect404?: boolean;
}

/** source name + its policy for a URL (one table: every upstream the worker calls) */
export function sourceOf(url: string): { source: string; policy: Policy } {
  let u: URL;
  try {
    u = new URL(url);
  } catch {
    return { source: "unknown", policy: { attempts: 1, timeoutMs: 30_000 } };
  }
  const h = u.hostname;
  const p = u.pathname;
  if (h === "dd.weather.gc.ca") {
    if (p.includes("/hourly/")) return { source: "eccc-hourly", policy: { attempts: 2, timeoutMs: 30_000 } };
    if (p.includes("/daily/")) return { source: "eccc-daily", policy: { attempts: 2, timeoutMs: 30_000 } };
    if (p.endsWith("StationList.csv")) return { source: "eccc-station-list", policy: { attempts: 3, timeoutMs: 30_000 } };
    return { source: "eccc-datamart", policy: { attempts: 2, timeoutMs: 30_000 } };
  }
  // its own 4 tries with backoff (publish.fetchTemperatures)
  if (h === "wateroffice.ec.gc.ca") return { source: "eccc-temperature", policy: { attempts: 1, timeoutMs: 30_000 } };
  if (h === "services6.arcgis.com") {
    if (p.endsWith("/query")) return { source: "rfc-forecast", policy: { attempts: 3, timeoutMs: 30_000 } };
    return { source: "rfc-forecast-layers", policy: { attempts: 2, timeoutMs: 30_000 } };
  }
  if (h === "bcrfc.env.gov.bc.ca") return { source: "rfc-series", policy: { attempts: 2, timeoutMs: 25_000, expect404: true } };
  if (h === "collaboration.cmc.ec.gc.ca") return { source: "hydat-listing", policy: { attempts: 2, timeoutMs: 60_000 } };
  if (h === "www.pac.dfo-mpo.gc.ca") {
    const m = /region([0-9a-z]+)-eng\.html$/i.exec(p);
    // fetchRegion retries (4 attempts, Retry-After, body validation) with its own AbortController: each attempt counted
    return { source: m ? `dfo-region-${m[1]}` : "dfo", policy: { attempts: 1, timeoutMs: null } };
  }
  if (h === "www2.gov.bc.ca") return { source: "inseason-page", policy: { attempts: 3, timeoutMs: 30_000 } };
  return { source: h, policy: { attempts: 2, timeoutMs: 30_000 } };
}

export interface SourceRun {
  ok: number;
  fail: number;
  /** extra attempts spent on retries */
  retries: number;
  errors: Partial<Record<ErrorClass, number>>;
  last?: { cls: ErrorClass; detail: string; url: string };
}

/** a source error a job throws (or records) with its class */
export class SourceError extends Error {
  constructor(public cls: ErrorClass, message: string, public source?: string) {
    super(message);
    this.name = "SourceError";
  }
}

export function classOfStatus(status: number): ErrorClass | null {
  if (status < 400 || status === 304) return null;
  if (status === 403 || status === 451) return "blocked";
  if (status === 429) return "rate_limited";
  if (status === 404 || status === 410) return "http_404";
  if (status >= 500) return "http_5xx";
  return "http_4xx";
}

export function classOfError(e: unknown): ErrorClass {
  if (e instanceof SourceError) return e.cls;
  const name = (e as Error)?.name ?? "";
  const msg = String((e as Error)?.message ?? e);
  if (name === "TimeoutError" || name === "AbortError" || /timed? ?out|abort/i.test(msg)) return "timeout";
  return "network";
}

const HARD: ReadonlySet<ErrorClass> = new Set(["http_5xx", "blocked", "rate_limited", "timeout", "network"]);
export const BREAKER_TRIP = 6;
const RETRY_AFTER_MAX_S = 10;

/** One run's measurements. */
export class RunHealth {
  sources: Record<string, SourceRun> = {};
  /** the class each failed URL ended with (a job maps its parts — stations, regions — back to these) */
  failedUrls = new Map<string, ErrorClass>();
  /** hard failures in a row per host (the breaker) */
  private streak = new Map<string, number>();
  /** parts that failed this run: "station 08MF005" / "region 6" → class */
  parts: Record<string, { cls: ErrorClass; detail: string }> = {};

  src(name: string): SourceRun {
    return (this.sources[name] ??= { ok: 0, fail: 0, retries: 0, errors: {} });
  }
  ok(source: string) {
    this.src(source).ok += 1;
  }
  fail(source: string, cls: ErrorClass, detail: string, url = "") {
    const s = this.src(source);
    s.fail += 1;
    s.errors[cls] = (s.errors[cls] ?? 0) + 1;
    s.last = { cls, detail: detail.slice(0, 300), url };
    if (url) this.failedUrls.set(url, cls);
  }
  /** a fetch that answered 200 but whose body the job could not use */
  demote(source: string, cls: ErrorClass, detail: string, url = "") {
    const s = this.src(source);
    if (s.ok > 0) s.ok -= 1;
    this.fail(source, cls, detail, url);
  }
  part(name: string, cls: ErrorClass, detail: string) {
    this.parts[name] = { cls, detail: detail.slice(0, 200) };
  }
  breakerOpen(host: string): boolean {
    return (this.streak.get(host) ?? 0) >= BREAKER_TRIP;
  }
  hit(host: string, cls: ErrorClass | null) {
    if (cls === null) this.streak.set(host, 0);
    else if (HARD.has(cls)) this.streak.set(host, (this.streak.get(host) ?? 0) + 1);
  }
}

export interface ResilientOptions {
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
  /** override the table (tests) */
  policyOf?: (url: string) => { source: string; policy: Policy };
}

function timeoutSignal(ms: number): AbortSignal | undefined {
  return typeof AbortSignal !== "undefined" && "timeout" in AbortSignal ? AbortSignal.timeout(ms) : undefined;
}

/** The run's network: retries, timeouts, breaker, measurement. Returns the last Response (or throws the last error). */
export function resilientFetch(f: Fetch, health: RunHealth, o: ResilientOptions = {}): Fetch {
  const sleep = o.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const rnd = o.random ?? Math.random;
  const lookup = o.policyOf ?? sourceOf;
  return async (url, init) => {
    const { source, policy } = lookup(url);
    let host = "";
    try {
      host = new URL(url).hostname;
    } catch {
      /* the fetch below throws */
    }
    if (health.breakerOpen(host)) {
      health.fail(source, "circuit", `${host}: ${BREAKER_TRIP}+ hard failures in a row this run; not asked`, url);
      throw new SourceError("circuit", `circuit open for ${host}`, source);
    }
    let lastErr: unknown = null;
    for (let attempt = 1; attempt <= policy.attempts; attempt++) {
      if (attempt > 1) health.src(source).retries += 1;
      const sig = policy.timeoutMs === null && init?.signal ? init.signal : policy.timeoutMs !== null ? timeoutSignal(policy.timeoutMs) : init?.signal;
      let res: Response;
      try {
        res = await f(url, { ...(init ?? {}), ...(sig ? { signal: sig } : {}) });
      } catch (e) {
        lastErr = e;
        const cls = classOfError(e);
        health.hit(host, cls);
        if (attempt < policy.attempts && !health.breakerOpen(host)) {
          await sleep(Math.min(5000, 500 * 2 ** (attempt - 1)) + rnd() * 250);
          continue;
        }
        health.fail(source, cls, `${(e as Error)?.name ?? "Error"}: ${String((e as Error)?.message ?? e)}`, url);
        throw e;
      }
      let cls = classOfStatus(res.status);
      if (cls === "http_404" && policy.expect404) cls = null;
      health.hit(host, cls);
      if (cls === null) {
        health.ok(source);
        return res;
      }
      const retryable = cls === "http_5xx" || cls === "timeout" || cls === "rate_limited";
      let wait = Math.min(5000, 500 * 2 ** (attempt - 1)) + rnd() * 250;
      if (cls === "rate_limited" || res.status === 503) {
        const ra = res.headers.get("Retry-After");
        const s = ra && /^\d+$/.test(ra.trim()) ? Number(ra.trim()) : null;
        if (cls === "rate_limited" && (s === null || s > RETRY_AFTER_MAX_S)) {
          health.fail(source, cls, `HTTP ${res.status}${s !== null ? ` (Retry-After ${s}s)` : ""}`, url);
          return res; // asked to back off: we do, until next run
        }
        if (s !== null) wait = Math.min(s, RETRY_AFTER_MAX_S) * 1000;
      }
      if (retryable && attempt < policy.attempts && !health.breakerOpen(host)) {
        // free the connection (6 per invocation); not awaited: a tee'd body's cancel settles only with its twin
        res.body?.cancel().catch(() => {});
        await sleep(wait);
        continue;
      }
      health.fail(source, cls, `HTTP ${res.status}`, url);
      return res;
    }
    throw lastErr ?? new Error("unreachable");
  };
}

// ── rolling record (status.json) ────────────────────────────────────────────────────────────────────────────────────

/** one hour: [hourKey "YYYY-MM-DDTHH", ok, fail, {class: n}] */
export type Bucket = [string, number, number, Partial<Record<ErrorClass, number>>];
export interface SourceHealth {
  hours: Bucket[];
  /** runs in a row where this source answered nothing usable (ok 0, fail > 0) */
  consecutiveFailedRuns: number;
  lastOk: string | null;
  lastError: { at: string; cls: ErrorClass; detail: string } | null;
}
export interface Window {
  ok: number;
  fail: number;
  /** ok / (ok + fail); null with no requests */
  successRate: number | null;
  errors: Partial<Record<ErrorClass, number>>;
}
export interface FeedHealth {
  sources: Record<string, SourceHealth>;
  /** parts failing now (a station, a DFO region): since, failed runs in a row, last class */
  parts: Record<string, { since: string; runs: number; cls: ErrorClass; detail: string }>;
}
export const KEEP_HOURS = 7 * 24;

const hourKey = (iso: string) => iso.slice(0, 13);

/** fold a run's tally into the rolling record (mutates `h`); returns the sources that just reached 3 failed runs */
export function foldHealth(h: FeedHealth | undefined, run: RunHealth, at: string): { health: FeedHealth; newlyDown: string[] } {
  const out: FeedHealth = h ?? { sources: {}, parts: {} };
  const key = hourKey(at);
  const cutoff = hourKey(new Date(Date.parse(at) - KEEP_HOURS * 3600_000).toISOString());
  const newlyDown: string[] = [];
  for (const [name, s] of Object.entries(run.sources)) {
    const r = (out.sources[name] ??= { hours: [], consecutiveFailedRuns: 0, lastOk: null, lastError: null });
    let b = r.hours[r.hours.length - 1];
    if (!b || b[0] !== key) {
      b = [key, 0, 0, {}];
      r.hours.push(b);
    }
    b[1] += s.ok;
    b[2] += s.fail;
    for (const [c, n] of Object.entries(s.errors)) b[3][c as ErrorClass] = (b[3][c as ErrorClass] ?? 0) + (n ?? 0);
    r.hours = r.hours.filter((x) => x[0] > cutoff);
    if (s.ok > 0) r.lastOk = at;
    if (s.last) r.lastError = { at, cls: s.last.cls, detail: s.last.detail };
    if (s.ok === 0 && s.fail > 0) {
      r.consecutiveFailedRuns += 1;
      if (r.consecutiveFailedRuns === 3) newlyDown.push(name);
    } else if (s.ok > 0) r.consecutiveFailedRuns = 0;
  }
  // sources not asked this run keep their record; expire the ones with nothing in 7 days
  for (const [name, r] of Object.entries(out.sources)) {
    r.hours = r.hours.filter((x) => x[0] > cutoff);
    if (!r.hours.length && !(name in run.sources)) delete out.sources[name];
  }
  const parts: FeedHealth["parts"] = {};
  for (const [p, f] of Object.entries(run.parts)) {
    const was = out.parts[p];
    parts[p] = { since: was?.since ?? at, runs: (was?.runs ?? 0) + 1, cls: f.cls, detail: f.detail };
  }
  out.parts = parts;
  return { health: out, newlyDown };
}

/** counts over the last `hours` hours (ending at `now`) */
export function windowOf(r: SourceHealth, now: Date, hours: number): Window {
  const from = hourKey(new Date(now.getTime() - hours * 3600_000).toISOString());
  const w: Window = { ok: 0, fail: 0, successRate: null, errors: {} };
  for (const [k, ok, fail, errs] of r.hours) {
    if (k <= from) continue;
    w.ok += ok;
    w.fail += fail;
    for (const [c, n] of Object.entries(errs)) w.errors[c as ErrorClass] = (w.errors[c as ErrorClass] ?? 0) + (n ?? 0);
  }
  w.successRate = w.ok + w.fail ? Math.round((w.ok / (w.ok + w.fail)) * 10_000) / 10_000 : null;
  return w;
}

/** what status.json shows per source: the windows, consecutive failures, last error (the hourly buckets stay inside) */
export function summarize(h: FeedHealth | undefined, now: Date) {
  const out: Record<string, { last24h: Window; last7d: Window; consecutiveFailedRuns: number; lastOk: string | null; lastError: SourceHealth["lastError"] }> = {};
  for (const [name, r] of Object.entries(h?.sources ?? {}).sort(([a], [b]) => (a < b ? -1 : 1))) {
    out[name] = { last24h: windowOf(r, now, 24), last7d: windowOf(r, now, KEEP_HOURS), consecutiveFailedRuns: r.consecutiveFailedRuns, lastOk: r.lastOk, lastError: r.lastError };
  }
  return out;
}

export const fmtErrors = (e: Partial<Record<ErrorClass, number>>) =>
  Object.entries(e)
    .filter(([, n]) => n)
    .sort((a, b) => (b[1] ?? 0) - (a[1] ?? 0))
    .map(([c, n]) => `${c} ${n}`)
    .join(", ") || "none";
