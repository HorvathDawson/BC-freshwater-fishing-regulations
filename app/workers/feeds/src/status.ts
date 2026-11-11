// T225 follow-up (owner 2026-10-28): the feeds' status — written by every run to R2 private/feeds/status.json, served
// (no auth, small) as GET /status (HTML) and GET /status.json, and the alert hook: FEED_NOTIFY_URL is told ONCE per
// state change of a feed (ok → degraded / failing / stale and back), never once per run.
import { isVersioned, readStored, type Fetch, type Meter, type Store } from "./io";
import { body as notifyBody } from "./notify";
import { fmtErrors, foldHealth, summarize, type FeedHealth, type RunHealth, type SourceRun } from "./health";
import { FORMAT as WORDING_FORMAT } from "./dfo/wording";
import { INSEASON_LINKS_FORMAT } from "../../../packages/core/src/inseason";

export type Job = "gauges" | "dfo" | "inseason";
export type FeedState = "ok" | "degraded" | "failing" | "stale" | "never";

export interface Held {
  /** region slug (DFO) or station (gauges) */
  where: string;
  /** a DFO location id when only one place is held */
  place?: string;
  why: string;
}

export interface RunRecord {
  at: string;
  ok: boolean;
  wallMs: number;
  /** Cloudflare does not expose a request's CPU time inside the Worker: null there (read it from `wrangler tail` /
   *  Workers observability); set by node runs (process.cpuUsage) */
  cpuMs: number | null;
  counts: Record<string, number>;
  held: Held[];
  error: string | null;
  meter: Meter | null;
  /** this run's outcomes per upstream source (src/health.ts): ok / fail / retries / errors by class / last error */
  sources?: Record<string, SourceRun>;
}

export interface FeedStatus {
  job: Job;
  /** expected minutes between runs right now (season-aware for DFO) */
  cadenceMin: number;
  lastRun: RunRecord | null;
  lastSuccess: string | null;
  lastError: { at: string; error: string } | null;
  /** consecutive failed runs */
  failures: number;
  state: FeedState;
  /** the state the alert hook last announced (so it fires once per change) */
  announced: FeedState | null;
  /** "checked at": when the sources were last read, even when nothing changed */
  checkedAt: string | null;
  /** rolling per-source record (hourly buckets, 7 days) + the parts failing now; served summarized (`servedStatus`) */
  health?: FeedHealth;
}

export interface Status {
  format: "feeds-status/1";
  updated: string;
  feeds: Partial<Record<Job, FeedStatus>>;
}

/**
 * (T597 item 2) THE INPUT FORMATS THIS WORKER READS, by R2 key: served on /status.json as `inputs`, so an upload of a
 * curated input (deploy/staging/sync-inputs.sh, deploy/prod/sync-inputs.sh) refuses a format the deployed worker does not list instead of putting a file
 * the live worker cannot read (2026-11-09: matches.json went to inseason-links/3 before the worker that reads it). The DFO
 * entry files and the roster carry no format; they are not listed and not checked.
 */
export const INPUT_FORMATS: Readonly<Record<string, readonly string[]>> = {
  "private/feeds/dfo/wording.json": [WORDING_FORMAT],
  "feeds/inseason/matches.json": [INSEASON_LINKS_FORMAT],
};

export const STATUS_KEY = "status.json";
/** a feed is stale when its last success is older than this many cadences */
export const STALE_CADENCES = 2.5;

/** The cadence wanted (T225 Q-T225c / Q145): gauges hourly; DFO every 6 h Jun 1–Oct 31, daily otherwise. */
export function cadenceMin(job: Job, now: Date): number {
  // wrangler.toml [triggers]: every 30 min 13–05 UTC (06–22 PDT), hourly 06–12 UTC at night (owner 2026-10-28)
  if (job === "gauges") return now.getUTCHours() >= 6 && now.getUTCHours() <= 12 ? 60 : 30;
  // DFO and in-season (as DFO): every 6 h Jun–Oct, daily otherwise
  const m = now.getUTCMonth() + 1;
  return m >= 6 && m <= 10 ? 360 : 1440;
}

export function stateOf(f: Pick<FeedStatus, "lastRun" | "lastSuccess" | "cadenceMin" | "failures">, now: Date): FeedState {
  if (!f.lastRun) return "never";
  if (!f.lastSuccess || now.getTime() - Date.parse(f.lastSuccess) > STALE_CADENCES * f.cadenceMin * 60_000) {
    return f.failures > 0 ? "failing" : "stale";
  }
  if (f.failures > 0) return "failing";
  return f.lastRun.held.length ? "degraded" : "ok";
}

/** The status. Absent: a fresh one. (T558) A failed read or another format THROWS (readStored): written back fresh it
 *  would erase every other feed's last success, failures, announced state and 7-day health. */
export async function readStatus(priv: Store): Promise<Status> {
  return checkStatus(await readStored<Status>(priv, STATUS_KEY));
}
export function checkStatus(s: Status | null): Status {
  if (s === null) return { format: "feeds-status/1", updated: new Date(0).toISOString(), feeds: {} };
  if (s && s.format === "feeds-status/1" && s.feeds && typeof s.feeds === "object") return s;
  throw new Error(`${STATUS_KEY} is not feeds-status/1; fix or delete it`);
}
/** (T558 W13) the status and its version tag (null: absent, or a store that cannot version) */
export async function readStatusAt(priv: Store): Promise<{ st: Status; etag: string | null }> {
  if (!isVersioned(priv)) return { st: await readStatus(priv), etag: null };
  const v = await priv.getVersioned(STATUS_KEY);
  if (!v) return { st: checkStatus(null), etag: null };
  let s: Status;
  try { s = JSON.parse(v.text); } catch (e) { throw new Error(`${STATUS_KEY} is stored but does not parse (${String(e)}); fix or delete it`); }
  return { st: checkStatus(s), etag: v.etag };
}

/** Recompute every feed's state at `now` (staleness moves with the clock, not only with runs). */
export function refresh(s: Status, now: Date): Status {
  for (const f of Object.values(s.feeds)) {
    f.cadenceMin = cadenceMin(f.job, now);
    f.state = stateOf(f, now);
  }
  return s;
}

export interface Alert {
  job: Job;
  from: FeedState | null;
  to: FeedState;
  text: string;
}

/** fold a run's per-source tally into the feed's rolling record; returns the sources that just reached 3 failed runs */
export function recordHealth(s: Status, job: Job, run: RunHealth, at: string): string[] {
  const f = s.feeds[job];
  if (!f) return [];
  const { health, newlyDown } = foldHealth(f.health, run, at);
  f.health = health;
  return newlyDown;
}

/** status.json as served: each feed's rolling health summarized (24 h / 7 d windows), hourly buckets left out */
export function servedStatus(s: Status, now: Date) {
  const feeds: Record<string, unknown> = {};
  for (const [j, f] of Object.entries(s.feeds)) {
    const { health, ...rest } = f!;
    feeds[j] = { ...rest, sources: summarize(health, now), failingParts: health?.parts ?? {} };
  }
  return { ...s, feeds };
}

/** Fold one run into the status, return the alerts due (state changes not yet announced). */
export function record(s: Status, job: Job, run: RunRecord, now: Date): Alert[] {
  const was = s.feeds[job];
  const f: FeedStatus = was ?? {
    job, cadenceMin: cadenceMin(job, now), lastRun: null, lastSuccess: null, lastError: null, failures: 0, state: "never", announced: null, checkedAt: null,
  };
  f.lastRun = run;
  if (run.ok) {
    f.lastSuccess = run.at;
    f.failures = 0;
    f.checkedAt = run.at;
  } else {
    f.failures += 1;
    f.lastError = { at: run.at, error: run.error ?? "unknown error" };
  }
  s.feeds[job] = f;
  s.updated = now.toISOString();
  refresh(s, now);
  return dueAlerts(s);
}

/** Every feed whose state differs from what was last announced (the first "ok" is not news). */
export function dueAlerts(s: Status): Alert[] {
  const out: Alert[] = [];
  for (const f of Object.values(s.feeds)) {
    if (f.state === f.announced || f.state === "never") continue;
    if (f.announced === null && f.state === "ok") {
      f.announced = "ok";
      continue;
    }
    out.push({ job: f.job, from: f.announced, to: f.state, text: alertText(f) });
  }
  return out;
}

function alertText(f: FeedStatus): string {
  const name = f.job === "gauges" ? "Gauge feed" : f.job === "dfo" ? "DFO salmon feed" : "In-season changes check";
  switch (f.state) {
    case "ok":
      return `${name} is OK again (last success ${f.lastSuccess}).`;
    case "degraded":
      return `${name} published with ${f.lastRun!.held.length} held: ${f.lastRun!.held.slice(0, 5).map((h) => `${h.where}${h.place ? ` ${h.place}` : ""} (${h.why.slice(0, 80)})`).join("; ")}`;
    case "failing":
      return `${name} FAILED ${f.failures} run(s) in a row: ${(f.lastError?.error ?? "").slice(0, 200)}${sourceLine(f)}`;
    case "stale":
      return `${name} is STALE: last success ${f.lastSuccess ?? "never"} (cadence ${f.cadenceMin} min).`;
    default:
      return `${name}: ${f.state}`;
  }
}

/** Post the due alerts; an alert that was posted (or that has nowhere to go) is marked announced. */
export async function sendAlerts(s: Status, alerts: Alert[], url: string | null | undefined, fetch: Fetch, log: (s: string) => void = () => {}): Promise<number> {
  let sent = 0;
  for (const a of alerts) {
    const f = s.feeds[a.job]!;
    if (!url) {
      log(`alert (FEED_NOTIFY_URL unset): ${a.text}`);
      f.announced = a.to; // nowhere to send: recorded, so a later URL does not replay history
      continue;
    }
    try {
      const [data, ctype] = notifyBody(url, a.text);
      const r = await fetch(url, { method: "POST", body: data, headers: { "Content-Type": ctype, Title: `BC fishing feed: ${a.job} ${a.to}` } });
      if (r.status < 200 || r.status >= 300) throw new Error(`HTTP ${r.status}`);
      f.announced = a.to;
      sent++;
    } catch (e) {
      log(`alert not sent (${String(e)}); retried next run`);
    }
  }
  return sent;
}

/** "; last 24 h: eccc-hourly 98.1% ok (timeout 12, http_5xx 3), …" — the sources with a failure */
function sourceLine(f: FeedStatus): string {
  const sum = summarize(f.health, new Date(f.lastRun?.at ?? Date.now()));
  const bad = Object.entries(sum).filter(([, w]) => w.last24h.fail > 0);
  if (!bad.length) return "";
  return `; last 24 h: ${bad.map(([n, w]) => `${n} ${w.last24h.successRate === null ? "-" : (w.last24h.successRate * 100).toFixed(1) + "%"} ok (${fmtErrors(w.last24h.errors)})`).join("; ")}`;
}

// ── the page ─────────────────────────────────────────────────────────────────────────────────────────────────────────
const esc = (x: unknown) => String(x ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]!);
const ago = (iso: string | null, now: Date) => {
  if (!iso) return "never";
  const m = Math.round((now.getTime() - Date.parse(iso)) / 60_000);
  return m < 90 ? `${m} min ago` : m < 2880 ? `${Math.round(m / 60)} h ago` : `${Math.round(m / 1440)} d ago`;
};

/** (T558 item 2) the daily backup and purge: when they last ran and how it went (late past 36 h) */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function dailyHtml(d: { backup: any; purge: any }, now: Date): string {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const row = (name: string, j: any, what: (j: any) => string) => {
    if (!j) return `<tr><td>${name}</td><td class="never">never run</td><td></td></tr>`;
    if (j.unreadable) return `<tr><td>${name}</td><td class="failing">unreadable</td><td><small>${esc(j.unreadable)}</small></td></tr>`;
    const late = now.getTime() - Date.parse(j.at) > 36 * 3600_000;
    return `<tr><td>${name}</td><td class="${late ? "stale" : j.ok ? "ok" : "failing"}">${late ? "late" : j.ok ? "ok" : "failed"}</td>` +
      `<td>${esc(j.at)} <small>(${ago(j.at, now)})</small><br><small>${esc(what(j))}${j.error ? ` · ${esc(String(j.error).slice(0, 200))}` : ""}</small></td></tr>`;
  };
  return `<h2>Daily jobs</h2><div class="wrap"><table><tr><th>job</th><th>state</th><th>last run</th></tr>` +
    row("backup", d.backup, (j) => `${j.copied} copied, ${j.bytes} bytes, ${j.pruned ?? 0} pruned, ${j.skipped?.length ?? 0} skipped`) +
    row("purge", d.purge, (j) => `reports ${j.deleted} deleted / ${j.kept} kept` +
      (j.crashes ? ` · error kinds ${j.crashes.deleted} deleted` : "") + (j.limits ? ` · counters ${j.limits.deleted} deleted` : "")) +
    `</table></div>`;
}

/** (T178) the last monthly model score (private/feeds/skill.json): per lead day, the verdict and its reason */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function skillHtml(sk: any, now: Date): string {
  if (!sk) return "<h2>Forecast model scores</h2><p><small>not run yet: monthly, the 1st at 10:55 UTC (T178)</small></p>";
  if (!sk.ok) return `<h2>Forecast model scores</h2><p class="failing">failed ${esc(sk.at)} (${ago(sk.at, now)}): <small>${esc(String(sk.error ?? "").slice(0, 300))}</small></p>`;
  const rows = Object.entries<any>(sk.verdict ?? {}).map(([lead, v]) => {
    const d = sk.table?.[lead]?.paired?.difference;
    return `<tr><td>${esc(lead)}</td><td>${esc(v.weights?.CLEVER)} / ${esc(v.weights?.COFFEE)}</td><td>${d ? `${esc(d.n)} · ${esc(d.mean)} ± ${esc(d.se)}` : "–"}</td><td><small>${esc(v.why)}</small></td></tr>`;
  });
  return `<h2>Forecast model scores (${esc(sk.month)})</h2><p><small>${esc(sk.at)} (${ago(sk.at, now)}) · ${esc(sk.runs)} runs, ${esc(sk.cases)} cases, ${esc(sk.stations)} stations · shipped CLEVER ${esc(sk.current?.CLEVER)} / COFFEE ${esc(sk.current?.COFFEE)}${sk.differs ? ' · <b class="degraded">a verdict differs: a reviewed change applies it (T178)</b>' : ""}</small></p>
<div class="wrap"><table><tr><th>lead (days)</th><th>verdict CLEVER / COFFEE</th><th>paired n · difference ± se</th><th>why</th></tr>${rows.join("") || '<tr><td colspan="4"><small>no scored cases</small></td></tr>'}</table></div>`;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function statusHtml(s: Status, now: Date, skill: any = null, notify: { channel: string; digest: boolean } | null = null,
                           daily: { backup: any; purge: any } | null = null): string {
  const rows = (["gauges", "dfo", "inseason"] as Job[]).map((j) => {
    const f = s.feeds[j];
    if (!f) return `<tr><td>${j}</td><td class="never">never run</td><td colspan="6"></td></tr>`;
    const r = f.lastRun;
    const counts = r ? Object.entries(r.counts).map(([k, v]) => `${esc(k)} ${esc(v)}`).join(", ") : "";
    const held = r && r.held.length ? `<ul>${r.held.map((h) => `<li>${esc(h.where)}${h.place ? ` <code>${esc(h.place)}</code>` : ""}: ${esc(h.why)}</li>`).join("")}</ul>` : "none";
    const meter = r?.meter ? `R2 A ${r.meter.r2ClassA} / B ${r.meter.r2ClassB}, skipped writes ${r.meter.writesSkipped}, unchanged sources ${r.meter.sourcesUnchanged}, subrequests ${r.meter.subrequests}` : "";
    return `<tr>
<td><b>${j}</b><br><small>every ${f.cadenceMin} min</small></td>
<td class="${f.state}">${f.state}</td>
<td>${r ? `${esc(r.at)}<br><small>${ago(r.at, now)} · ${r.ok ? "ok" : "failed"} · ${(r.wallMs / 1000).toFixed(1)} s wall${r.cpuMs !== null ? ` · ${r.cpuMs} ms CPU` : ""}</small>` : ""}</td>
<td>${esc(f.lastSuccess ?? "never")}<br><small>${ago(f.lastSuccess, now)}</small></td>
<td>${esc(counts)}<br><small>${esc(meter)}</small></td>
<td>${held}</td>
<td>${f.lastError ? `${esc(f.lastError.at)}<br><small>${esc(f.lastError.error.slice(0, 300))}</small>` : "none"}</td>
</tr>`;
  });
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Feeds status</title><style>
:root{color-scheme:light dark;--bg:#fff;--fg:#111;--mut:#666;--line:#ddd}
@media (prefers-color-scheme:dark){:root{--bg:#111;--fg:#eee;--mut:#999;--line:#333}}
body{background:var(--bg);color:var(--fg);font:14px/1.4 system-ui,sans-serif;margin:16px}
table{border-collapse:collapse;width:100%}td,th{border-bottom:1px solid var(--line);padding:6px;vertical-align:top;text-align:left}
small{color:var(--mut)}.ok{color:#2a7}.degraded{color:#c80}.failing,.stale{color:#d33;font-weight:600}.never{color:var(--mut)}
ul{margin:0;padding-left:16px}.wrap{overflow-x:auto}
</style></head><body><h1>Feeds status</h1><p><small>${esc(now.toISOString())} · updated ${esc(s.updated)} · <a href="/status.json">status.json</a></small></p>
${notify ? `<p>Owner messages: <b class="${notify.channel === "log" ? "failing" : "ok"}">${esc(notify.channel)}</b>${notify.channel === "log" ? " — nobody is told (NOTIFY_EMAIL_TO / EMAIL binding missing)" : ""}${notify.digest ? " <small>(daily digest)</small>" : ""}</p>` : ""}
<div class="wrap"><table><tr><th>feed</th><th>state</th><th>last run</th><th>last success</th><th>counts</th><th>held</th><th>last error</th></tr>
${rows.join("\n")}</table></div>
<h2>Sources</h2><p><small>per upstream request (after retries): success rate and failures by class; "down" = runs in a row with nothing usable</small></p>
<div class="wrap"><table><tr><th>feed</th><th>source</th><th>this run</th><th>24 h</th><th>7 d</th><th>down</th><th>last ok</th><th>last error</th></tr>
${sourceRows(s, now)}</table></div>
${daily ? dailyHtml(daily, now) : ""}${partsHtml(s, now)}${skillHtml(skill, now)}</body></html>`;
}

const pct = (r: number | null) => (r === null ? "–" : `${(r * 100).toFixed(1)}%`);
function sourceRows(s: Status, now: Date): string {
  const out: string[] = [];
  for (const j of ["gauges", "dfo", "inseason"] as Job[]) {
    const f = s.feeds[j];
    if (!f) continue;
    const sum = summarize(f.health, now);
    const run = f.lastRun?.sources ?? {};
    for (const [name, w] of Object.entries(sum)) {
      const r = run[name];
      const cls = w.consecutiveFailedRuns ? "failing" : w.last24h.fail ? "degraded" : "ok";
      out.push(`<tr><td>${j}</td><td><code>${esc(name)}</code></td>
<td>${r ? `${r.ok} ok / ${r.fail} fail${r.retries ? ` · ${r.retries} retries` : ""}${r.fail ? `<br><small>${esc(fmtErrors(r.errors))}</small>` : ""}` : "<small>not asked</small>"}</td>
<td class="${w.last24h.fail ? cls : "ok"}">${pct(w.last24h.successRate)}<br><small>${w.last24h.ok + w.last24h.fail} req · ${esc(fmtErrors(w.last24h.errors))}</small></td>
<td>${pct(w.last7d.successRate)}<br><small>${w.last7d.ok + w.last7d.fail} req · ${esc(fmtErrors(w.last7d.errors))}</small></td>
<td class="${w.consecutiveFailedRuns ? "failing" : ""}">${w.consecutiveFailedRuns || ""}</td>
<td><small>${ago(w.lastOk, now)}</small></td>
<td>${w.lastError ? `<small>${esc(w.lastError.cls)} · ${ago(w.lastError.at, now)}<br>${esc(w.lastError.detail.slice(0, 160))}</small>` : ""}</td></tr>`);
    }
  }
  return out.join("\n") || `<tr><td colspan="8"><small>no measurements yet</small></td></tr>`;
}
function partsHtml(s: Status, now: Date): string {
  const items: string[] = [];
  for (const j of ["gauges", "dfo", "inseason"] as Job[]) {
    for (const [p, v] of Object.entries(s.feeds[j]?.health?.parts ?? {}).sort((a, b) => b[1].runs - a[1].runs)) {
      items.push(`<li>${j} <code>${esc(p)}</code>: ${esc(v.cls)} · ${v.runs} run(s) in a row since ${ago(v.since, now)} <small>${esc(v.detail)}</small></li>`);
    }
  }
  if (!items.length) return "<h2>Failing parts</h2><p><small>none: every station / region answered on its last run</small></p>";
  return `<h2>Failing parts (${items.length})</h2><p><small>their last good data is kept and marked stale</small></p><ul>${items.slice(0, 200).join("")}</ul>`;
}
