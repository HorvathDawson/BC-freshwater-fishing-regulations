// T225 follow-up (owner 2026-10-28): the feeds' status — written by every run to R2 private/feeds/status.json, served
// (no auth, small) as GET /status (HTML) and GET /status.json, and the alert hook: FEED_NOTIFY_URL is told ONCE per
// state change of a feed (ok → degraded / failing / stale and back), never once per run.
import type { Fetch, Meter, Store } from "./io";
import { body as notifyBody } from "./notify";

export type Job = "gauges" | "dfo";
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
}

export interface Status {
  format: "feeds-status/1";
  updated: string;
  feeds: Partial<Record<Job, FeedStatus>>;
}

export const STATUS_KEY = "status.json";
/** a feed is stale when its last success is older than this many cadences */
export const STALE_CADENCES = 2.5;

/** The cadence wanted (T225 Q-T225c / Q145): gauges hourly; DFO every 6 h Jun 1–Oct 31, daily otherwise. */
export function cadenceMin(job: Job, now: Date): number {
  if (job === "gauges") return 60;
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

export async function readStatus(priv: Store): Promise<Status> {
  try {
    const t = await priv.get(STATUS_KEY);
    const s = t === null ? null : JSON.parse(t);
    if (s && s.format === "feeds-status/1") return s;
  } catch {
    /* rebuilt below */
  }
  return { format: "feeds-status/1", updated: new Date(0).toISOString(), feeds: {} };
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
  const name = f.job === "gauges" ? "Gauge feed" : "DFO salmon feed";
  switch (f.state) {
    case "ok":
      return `${name} is OK again (last success ${f.lastSuccess}).`;
    case "degraded":
      return `${name} published with ${f.lastRun!.held.length} held: ${f.lastRun!.held.slice(0, 5).map((h) => `${h.where}${h.place ? ` ${h.place}` : ""} (${h.why.slice(0, 80)})`).join("; ")}`;
    case "failing":
      return `${name} FAILED ${f.failures} run(s) in a row: ${(f.lastError?.error ?? "").slice(0, 200)}`;
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

// ── the page ─────────────────────────────────────────────────────────────────────────────────────────────────────────
const esc = (x: unknown) => String(x ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]!);
const ago = (iso: string | null, now: Date) => {
  if (!iso) return "never";
  const m = Math.round((now.getTime() - Date.parse(iso)) / 60_000);
  return m < 90 ? `${m} min ago` : m < 2880 ? `${Math.round(m / 60)} h ago` : `${Math.round(m / 1440)} d ago`;
};

export function statusHtml(s: Status, now: Date): string {
  const rows = (["gauges", "dfo"] as Job[]).map((j) => {
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
<div class="wrap"><table><tr><th>feed</th><th>state</th><th>last run</th><th>last success</th><th>counts</th><th>held</th><th>last error</th></tr>
${rows.join("\n")}</table></div></body></html>`;
}
