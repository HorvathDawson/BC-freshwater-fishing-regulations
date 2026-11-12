// T225: the feeds Worker — gauges (pipeline/gauges/feed/publish.py), DFO salmon (pipeline/regs/dfo_salmon/live.py) and
// the provincial in-season changes (archive/pipeline/recurring/in_season) in TypeScript. STAGING ONLY for now: runs on
// wrangler.toml's [triggers] (owner 2026-10-28), or by hand through `POST /run/gauges|dfo|inseason` with
// `Authorization: Bearer <RUN_TOKEN>` (a wrangler secret). Network: src/health.ts (retries, timeouts, breaker, measures).
//
// GET /status (HTML) and GET /status.json (no auth): every feed's last run, last success, counts, held rows / places,
// staleness against its cadence, last error — from private/feeds/status.json, which each run writes (src/status.ts).
//
// NOTIFICATIONS (src/events.ts): every run hands its events — a new HYDAT (asked daily), ECCC roster drift, in-season
// changes (the diff), DFO places to review (unplaced / held / moved, linked to the review app's DFO queue), a feed's
// health changing — to one outbox: deduped by key, email through the EMAIL send_email binding (NOTIFY_EMAIL_FROM →
// NOTIFY_EMAIL_TO), else / on failure the FEED_NOTIFY_URL webhook, else logged. NOTIFY_DIGEST=daily batches all but
// health into one email a day. `?dryNotify=1` on a run, or `POST /notify/preview`, shows the messages without sending.
//
// R2 layout (one bucket): published  feeds/live/  feeds/dfo/  feeds/inseason/   (the data worker serves these)
//                         private    private/feeds/{gauges,dfo,inseason,notify}/, private/feeds/status.json,
//                                    private/reports/<day>/<id>.json (T237: POST /report, GET /reports; src/reports.ts)
import { meteredFetch, meteredStore, newMeter, r2Store, readStored, setDeployId, writeMerged, type Meter, type R2BucketLike } from "./io";
import { runGauges } from "./gauges";
import { runDfo } from "./dfo";
import { QUEUE_ACTION } from "./dfo/live";
import { runInSeason } from "./inseason";
import { channelOf, compose, deliver, readState, sendApk, sendTest, type FeedEvent, type NotifyConfig } from "./events";
import { parseApk, sampleDfoEvents } from "./email";
import { checkStatus, INPUT_FORMATS, readStatus, readStatusAt, record, recordHealth, refresh, servedStatus, statusHtml, STATUS_KEY, type Alert, type Held, type Job } from "./status";
import { classOfError, fmtErrors, resilientFetch, RunHealth, SourceError, summarize } from "./health";
import { pageUrl } from "./dfo/fetch";
import { limited, routeClass, bearerOk, type RateLimiter } from "./guard";
import { forgetReport, runBackup, BACKUP_STATUS_KEY, type DstBucket, type SrcBucket } from "./backup";
import { crashEvents, handleCrash, listCrashesCounted, parseClasses, purgeCrashes } from "./crashes";
import { cardEvents, runCards, type CardBucket, type CardRun } from "./gauges/card/card";
import { runSkill, skillText, type SkillReport } from "./gauges/score";
import { FLOOD_WEIGHTS } from "./gauges/change";
import { STATION_LIST } from "./gauges/liveness";
import { READING_MAX_AGE_HOURS } from "../../../packages/core/src/gauge-policy.generated";
import { checkToken, dispatchOnce, OVERDUE_MS, unfinished, type Dispatch, type DispatchEnv } from "./dispatch";
import { checkRunTiming } from "./runtiming";
import { capEvents, CORS as REPORT_CORS, handleReport, listReportsCounted, purgeLimits, purgeReports, purgeTestReports, reportEvents, RETENTION_DAYS, RETENTION_STATUS_KEY, type ReportEnv } from "./reports";

export interface Env extends ReportEnv, DispatchEnv {
  BUCKET: R2BucketLike;
  RUN_TOKEN?: string;
  FEED_NOTIFY_URL?: string;
  /** Cloudflare Email Workers binding ([[send_email]] in wrangler.toml) */
  EMAIL?: { send(message: unknown): Promise<void> };
  /** the owner's verified Email Routing destination, and a sender on the Email Routing domain (vars or secrets) */
  NOTIFY_EMAIL_TO?: string;
  NOTIFY_EMAIL_FROM?: string;
  /** "daily": one digest a day (health stays immediate) */
  NOTIFY_DIGEST?: string;
  NOTIFY_DIGEST_HOUR_UTC?: string;
  /** the env tag every email carries ("staging" | "production") and the status page link (vars) */
  FEEDS_ENV?: string;
  STATUS_URL?: string;
  /** (T404) this deployment's own names (vars): the worker, its bucket and its backup bucket — said in emails and the
   *  backup manifest, so a production email never names staging's (wrangler.toml / wrangler.prod.toml set them) */
  WORKER_NAME?: string;
  BUCKET_NAME?: string;
  BACKUP_BUCKET_NAME?: string;
  /** (T249) per-IP rate limits ([[ratelimits]] in wrangler.toml; src/guard.ts): absent under test */
  RL_PUBLIC?: RateLimiter;
  RL_AUTH?: RateLimiter;
  /** (T246) the backup bucket (BACKUP_BUCKET_NAME; src/backup.ts) */
  BACKUP?: DstBucket;
  /** (T487) the site whose gauge pages the share cards are for: its /gauge/stations.json names them (var) */
  SITE_ORIGIN?: string;
  /** the deployed version ([version_metadata] in wrangler.toml): every job's reuse key carries it (`rev`, io.ts) */
  CF_VERSION_METADATA?: { id: string };
}

const STATUS_PREFIX = "private/feeds/";
const JOBS: readonly Job[] = ["gauges", "dfo", "inseason"];

export function notifyConfig(env: Env): NotifyConfig {
  return {
    ctx: { env: env.FEEDS_ENV || "staging", statusUrl: env.STATUS_URL || null, worker: env.WORKER_NAME || null },
    emailTo: env.NOTIFY_EMAIL_TO || undefined,
    emailFrom: env.NOTIFY_EMAIL_FROM || undefined,
    webhook: env.FEED_NOTIFY_URL || undefined,
    digest: (env.NOTIFY_DIGEST ?? "").toLowerCase() === "daily",
    digestHourUtc: env.NOTIFY_DIGEST_HOUR_UTC ? Number(env.NOTIFY_DIGEST_HOUR_UTC) : undefined,
    sendEmail: env.EMAIL
      ? async (from, to, raw) => {
          // @ts-ignore — the Workers runtime module (not resolvable under node; only reached when an email is sent)
          const { EmailMessage } = await import("cloudflare:email");
          await env.EMAIL!.send(new EmailMessage(from, to, raw));
        }
      : undefined,
  };
}

/** gauge maintenance notices (publish.maintenanceNotices) → events */
export function gaugeEvents(maintenance: any[], at: string): FeedEvent[] {
  return maintenance.map((n) =>
    n.kind === "hydat"
      ? {
          // (T554 item 4) the envelope in use is in the key: an older clim.json put back over a newer one is told again
          key: `hydat:${n.latest}:${n.current}`,
          kind: "hydat" as const,
          title: `New HYDAT ${n.latest} — run the HYDAT action`,
          text:
            `HYDAT ${n.latest} is out; the gauge envelope (clim.json) is on ${n.current}.\n\n` +
            `1. GitHub → Actions → "feeds — HYDAT → climatology → R2" (upload: true): the feeds Worker's clim.json.\n` +
            `2. On the Mac: scripts/update-hydat.sh --upload — the bundle and release (the build needs the atlas).\n\n` +
            `Noticed ${n.since}.`,
          at,
          data: { latest: n.latest, current: n.current, since: n.since },
        }
      : {
          key: `${n.kind}:${n.latest}`,
          kind: "roster" as const,
          title: n.kind === "roster" ? "ECCC's real-time station list changed — refresh the roster" : `${n.kind}: ${n.latest} is due`,
          text: `${n.kind}: ${n.latest} (feed on ${n.current}). Run: ${n.action}\nThen sync it to R2: Actions → "feeds — sync curated inputs → R2 private/" (what: roster).`,
          at,
          data: { latest: n.latest, current: n.current, action: n.action },
        },
  );
}

/**
 * (T554 item 5) A HYDAT workflow the worker started that did not put the new envelope on R2 (started again, or overdue):
 * the event becomes "did not finish", keyed by the day, so the owner is told once a day while it lasts.
 */
export function hydatUnfinished(e: FeedEvent, dis: Dispatch, at: string): void {
  if (!unfinished(dis) || dis.state === "no-token") return;
  const d = e.data as Record<string, unknown>;
  const n = dis.starts ?? 1;
  e.key = `${e.key}:unfinished:${at.slice(0, 10)}`;
  e.title = dis.state === "failed" ? `HYDAT ${d.latest}: the workflow did not update clim.json, and starting it again failed`
    : dis.overdue ? `HYDAT ${d.latest}: the workflow was started ${n} times and clim.json is still on ${d.current}`
    : `HYDAT ${d.latest}: the first workflow run did not update clim.json — started again`;
  e.text = `${e.title}.\n\nHYDAT ${d.latest} is out; the gauge envelope (clim.json) is still on ${d.current}. ` +
    `The worker started the HYDAT workflow at ${dis.first ?? dis.at}` +
    (dis.state === "failed" ? `; ${OVERDUE_MS / 3600_000} h later clim.json was unchanged, and a second start failed (${dis.error}). It is tried again; or start it by hand in GitHub → Actions.`
      : (n > 1 ? ` and again at ${dis.at}` : "") +
        (dis.overdue ? "; it does not start it a third time. Look at the failed runs in GitHub → Actions, then start it by hand." : ". Check both runs in GitHub → Actions.")) +
    `\n\nNoticed ${d.since}.`;
}

/** DFO notices (unplaced / held / moved / header names / fetch failures) → events naming the review-app tab to open
 *  (the review app runs on the owner's machine: no link, just where to go) and the queue command to run first */
export function dfoEvents(notices: { key: string; text: string; urgent?: boolean; data?: unknown }[], at: string): FeedEvent[] {
  return notices.map((n) => {
    const m = /DFO Region (\w+):/.exec(n.text);
    const lid = /\b(\d[ab]?:[a-z0-9-]+:[a-z0-9-]+)\b/.exec(n.text)?.[1];
    return {
      key: n.key,
      kind: "dfo" as const,
      title: n.text.split(";")[0]!.slice(0, 200),
      // (T366, live.py) a rule-row change on a known place is the DIGEST (`urgent: false`); every other DFO notice urgent
      urgent: n.urgent !== false,
      // (T321) a new wording is not a place to review: its own text says what to do; (T366) nor is a row change; (T549
      // item 4) nor a place that left the page. (T549 item 9) the command reads DFO's page on the Mac first, then queues
      text: n.key.startsWith("dfo-wording:") || n.key.startsWith("dfo-rows") || n.key.startsWith("dfo-gone:") ? n.text :
        `${n.text}\n\n` +
        (m ? `First: ${QUEUE_ACTION(m[1]!)}\n` : "") +
        `Then: Review app → DFO → Extents: ${lid ?? placeOf(n.text) ?? "the new rows"}`,
      at,
      // the email template renders `data` (region · water · the place in full · what it affects); the text above is the
      // webhook / log fallback
      data: n.data ?? { region: m?.[1] ?? "?", kind: "other", place: lid ?? placeOf(n.text) ?? undefined, affects: n.text },
    };
  });
}

/** the place a DFO notice names: "on <water>: '<scope>'" / "header names '<x>'" */
function placeOf(text: string): string | null {
  const m = /\bon ([^:]+?): '([^']*)'/.exec(text) ?? /\bon ([^:]+?): "([^"]*)"/.exec(text);
  if (m) return `${m[1]} — ${m[2]!.slice(0, 120)}`;
  const w = /\bon ([A-Z][^,;:()]+?)(?:,| \(| —|;|$)/.exec(text);
  if (w) return w[1]!;
  const h = /names '([^']+)'/.exec(text);
  return h ? h[1]! : null;
}

/** status alerts (a feed's state changed) → events */
export function healthEvents(alerts: Alert[], at: string): FeedEvent[] {
  return alerts.map((a) => ({
    key: `health:${a.job}:${a.from ?? "-"}>${a.to}:${at}`, kind: "health" as const, title: a.text.slice(0, 160),
    text: `${a.text}\n\nStatus: /status on the feeds worker.`, at, ...(a.to === "ok" ? { fine: true } : {}),
    data: { job: a.job, from: a.from, to: a.to, detail: a.text },
    // owner 2026-10-28: a WHOLE feed failing / stale is emailed at once; degraded and recoveries wait for the digest.
    // owner 2026-10-29: EXCEPT gauges — a gauge failure of any size only ever rides in the daily digest.
    ...((a.to === "failing" || a.to === "stale") && a.job !== "gauges" ? { urgent: true } : {}),
  }));
}

/** a source that answered nothing usable 3 runs running → one digest line (key: the streak) */
export function sourceDownEvents(job: Job, names: string[], st: Awaited<ReturnType<typeof readStatus>>, at: string): FeedEvent[] {
  const sum = summarize(st.feeds[job]?.health, new Date(at));
  return names.map((n) => {
    const w = sum[n]!;
    return {
      key: `source-down:${job}:${n}:${at}`, kind: "health" as const,
      title: `${job}: ${n} failed 3 runs in a row (${w.lastError?.cls ?? "error"})`,
      text: `${job} source ${n} answered nothing usable on its last 3 runs; the rest of the feed still updates and its last good data is kept (marked stale).\n` +
        `Last 24 h: ${w.last24h.ok} ok / ${w.last24h.fail} failed (${fmtErrors(w.last24h.errors)}); last ok ${w.lastOk ?? "never"}.\n` +
        `Last error: ${w.lastError ? `${w.lastError.cls} — ${w.lastError.detail}` : "?"}\n\nStatus: /status on the feeds worker.`,
      at,
    };
  });
}

/** days of 404 in a row after which a listed station is "listed, no data" (out of failures; one digest note) */
export const NODATA_DAYS = 3;
export const NODATA_KEY = "nodata.json";
/** Track station 404 streaks (private/feeds/gauges/nodata.json: {station: first404 ISO}). A station past NODATA_DAYS
 *  has its 404 taken back out of the run's failure counts (`forgive`), is not a failing part, and is told ONCE per
 *  streak in the digest as roster drift; any answer (or a non-404 failure) ends the streak, so it re-enters by itself. */
export async function noDataStations(
  priv: { get(k: string): Promise<string | null>; put(k: string, b: string, o?: any): Promise<void> },
  failed: string[], asked: number, health: RunHealth, at: string, dry = false,
): Promise<{ listedNoData: Set<string>; events: FeedEvent[] }> {
  const out = new Set<string>();
  const events: FeedEvent[] = [];
  if (!asked) return { listedNoData: out, events }; // a run where nothing answered says nothing about single stations
  // (T558) absent = no streaks; a failed read fails the run (readStored): read as empty, every streak would restart
  const was: Record<string, string> = (await readStored<Record<string, string>>(priv, NODATA_KEY)) ?? {};
  const next: Record<string, string> = {};
  for (const st of failed) {
    if (health.failedUrls.get(HOURLY(st)) !== "http_404") continue;
    const first = was[st] ?? at;
    next[st] = first;
    if (Date.parse(at) - Date.parse(first) >= NODATA_DAYS * 86_400_000) {
      out.add(st);
      health.forgive("eccc-hourly", "http_404", HOURLY(st));
      events.push({
        key: `nodata:${st}:${first}`, kind: "roster", at,
        title: `Gauge ${st}: ECCC lists it but has published no data for ${NODATA_DAYS}+ days`,
        text: `${st} is on ECCC's real-time station list, but its hourly CSV has answered 404 since ${first}. It is no longer counted as a failure; it comes back by itself when a file appears.\nIf it stays dark, refresh the roster (realtime: false), then sync it: deploy/staging/sync-inputs.sh or Actions → "feeds — sync curated inputs → R2 private/" (what: roster).`,
      });
    }
  }
  if (!dry && JSON.stringify(next) !== JSON.stringify(was)) await priv.put(NODATA_KEY, JSON.stringify(next), { contentType: "application/json" });
  return { listedNoData: out, events };
}

/** (T550 item 4) minutes after 00:00 UTC within which a whole-run 404 is ECCC's day turn */
export const DAY_TURN_MIN = 30;
/**
 * (T550 item 4) Is this gauge run ECCC's day turn: within DAY_TURN_MIN of 00:00 UTC, nothing answered, every asked station
 * and the station list a 404. Anything else (a 5xx, a timeout, a partial answer, another hour) stays a failure.
 */
export function dayTurn(at: Date, answered: number, failed: readonly string[], classOf: (st: string) => string | undefined,
                        listClass: string | undefined): boolean {
  const min = at.getUTCHours() * 60 + at.getUTCMinutes();
  return min < DAY_TURN_MIN && answered === 0 && failed.length > 0 && listClass === "http_404"
    && failed.every((st) => classOf(st) === "http_404");
}

const HOURLY = (st: string) => `https://dd.weather.gc.ca/today/hydrometric/csv/BC/hourly/BC_${st}_hourly_hydrometric.csv`;
/** share of asked stations that may fail before the gauge feed counts as degraded */
export const GAUGE_DEGRADED_SHARE = 0.1;

/**
 * (T513) A source request never goes through Cloudflare's cache. ECCC sends its CSVs with no Cache-Control, and `.csv`
 * is a cached extension: the edge kept serving old copies (Capilano 08GA031 frozen at Nov 6, 162 of 434 stations stale,
 * some since Oct 28) while every fetch counted as a success.
 */
export const sourceFetch = (u: string, i?: RequestInit) => fetch(u, { ...i, cache: "no-store" });


async function run(job: Job, env: Env, opts: { regions?: string[]; limit?: number; dryNotify?: boolean } = {}) {
  const m: Meter = newMeter();
  // every request: metered (each attempt is a subrequest), then retried / timed out / measured per source (src/health.ts)
  const health = new RunHealth();
  const fetch_ = resilientFetch(meteredFetch(sourceFetch, m), health);
  const logs: string[] = [];
  const log = (s: string) => logs.push(s);
  // (T558 W8) a step that failed inside a run that goes on: kept in the result AND printed (a cron prints nothing else)
  const stepErrors: string[] = [];
  const logErr = (s: string) => { logs.push(s); stepErrors.push(s); console.error(JSON.stringify({ job, stepError: s })); };
  let saveInSeason: (() => Promise<void>) | undefined;
  const t0 = Date.now(); // Workers' clock advances only across I/O: this is wall time, not CPU (CPU: tail / observability)
  const started = new Date();
  const at = started.toISOString();
  let summary: Record<string, unknown> = {};
  let counts: Record<string, number> = {};
  let held: Held[] = [];
  let error: string | null = null;
  const events: FeedEvent[] = [];
  try {
    if (job === "gauges") {
      const s = await runGauges({
        fetch: fetch_,
        feed: meteredStore(r2Store(env.BUCKET, "feeds/live/"), m),
        priv: meteredStore(r2Store(env.BUCKET, "private/feeds/gauges/"), m),
        now: started,
        notifyUrl: null, // the outbox below tells the owner
        limit: opts.limit,
        meter: m,
        log,
      });
      summary = { asked: s.asked, answered: s.answered, withPercentile: s.withPercentile, maintenance: s.maintenance, runArchive: s.runArchive, forecastReused: s.forecastReused };
      counts = { stations: s.answered, asked: s.asked, withPercentile: s.withPercentile, oldReadings: s.oldReadings, forecastRuns: s.forecastRuns, unchanged: m.sourcesUnchanged, forecastReused: s.forecastReused ? 1 : 0 };
      // (T633 item 8) daily records with a gap whose refill waits for ECCC's daily files (before 03:00 UTC): shown on /status
      if (s.archive.deferred) counts.dailyRefillDeferred = s.archive.deferred;
      const gev = gaugeEvents(s.maintenance as any[], at);
      // (T413) a new HYDAT starts the HYDAT workflow itself when the owner's token is set; the email says what happened
      for (const e of gev) {
        if (e.kind !== "hydat" || opts.dryNotify) continue;
        const d = e.data as Record<string, unknown>;
        // (T554 item 5) this loop runs only while clim.json is behind `latest`: a start older than OVERDUE_MS did not
        // put the new envelope on R2. It is started once more, then never; either way the owner is TOLD AGAIN (the key
        // carries the day), where the first notice (`hydat:<latest>:<current>`, told once) used to be the last word
        const dis = await dispatchOnce(env, (u, i) => fetch(u, i), r2Store(env.BUCKET, "private/feeds/dispatch/"),
          "feeds-hydat.yml", String(d.latest), { upload: "true" }, started, { redoAfterMs: OVERDUE_MS });
        d.dispatch = dis;
        hydatUnfinished(e, dis, at);
      }
      events.push(...gev);
      // owner 2026-10-28: a station ECCC lists but has 404'd for NODATA_DAYS is "listed, no data" — not a failure
      const nd = await noDataStations(r2Store(env.BUCKET, "private/feeds/gauges/"), s.answered ? s.failed ?? [] : [], s.answered ? s.asked : 0, health, at, opts.dryNotify);
      events.push(...nd.events);
      // (T487) each gauge page's share card, drawn here (never per request): only the ones whose reading moved
      let cards: CardRun | null = null;
      try {
        cards = await runCards({ fetch: meteredFetch((u, i) => fetch(u, i), m), feed: meteredStore(r2Store(env.BUCKET, "feeds/live/"), m),
                                 bucket: env.BUCKET as unknown as CardBucket, siteOrigin: env.SITE_ORIGIN, now: started, limit: opts.limit, log });
      } catch (e) {
        cards = { ok: false, listed: 0, drawn: 0, unchanged: 0, failed: [], missingGlyphs: {}, ms: 0, error: e instanceof Error ? e.message : String(e) };
      }
      summary.cards = { ok: cards.ok, listed: cards.listed, drawn: cards.drawn, unchanged: cards.unchanged, failed: cards.failed.length, ms: cards.ms, ...(cards.error ? { error: cards.error } : {}) };
      counts.cardsDrawn = cards.drawn;
      counts.cardsFailed = cards.failed.length + (cards.ok ? 0 : 1);
      if (!cards.ok) logErr(`cards: not drawn (${cards.error})`);
      // (T237) problem reports received since the last run: one digest line each (not urgent)
      try {
        const ns = await readState(r2Store(env.BUCKET, STATUS_PREFIX));
        const pend = new Set(ns.pending.map((e) => e.key));
        events.push(...(await reportEvents(env.BUCKET, (k) => !!ns.told[k] || pend.has(k), started, at)));
        // (T242) and the app's NEW error kinds (one line each, once)
        events.push(...(await crashEvents(env.BUCKET, (k) => !!ns.told[k] || pend.has(k), started, at)));
        // (T487) the card run's failures and the cards the site found missing (one line each, once)
        events.push(...(await cardEvents(env.BUCKET as unknown as CardBucket, cards, (k) => !!ns.told[k] || pend.has(k), at)));
      } catch (e) {
        logErr(`reports: not read (${String(e)})`);
      }
      // (T550 item 4, T597 item 1) ECCC'S DAY TURN: at 00:05 UTC the whole /today/ directory answers 404 (the station
      // list and every hourly file; staging 2026-11-10 and 2026-11-11 00:05, ~6 of 7 nights by the 7-day 404 count).
      // Such a run is the source's turnover, not 428 failures: they are forgiven, the last good index stands, and the run
      // is degraded with one line. The schedule no longer asks at 00:05 (wrangler.toml); this covers a late cron.
      const turn = dayTurn(started, s.answered, s.failed ?? [], (st) => health.failedUrls.get(HOURLY(st)), health.failedUrls.get(STATION_LIST));
      if (turn) {
        for (const st of s.failed ?? []) health.forgive("eccc-hourly", "http_404", HOURLY(st));
        health.forgive("eccc-station-list", "http_404", STATION_LIST);
        held.push({ where: "source day turn", why: `ECCC's today/ directory answered 404 for all ${s.asked} stations at ${at.slice(11, 16)} UTC (its day turn); last good index kept` });
      }
      // owner 2026-10-29: a station the breaker skipped was never asked: not a failure, its last good entry is carried
      const skipped = (s.failed ?? []).filter((st) => health.failedUrls.get(HOURLY(st)) === "circuit");
      const failed = turn ? [] : (s.failed ?? []).filter((st) => !nd.listedNoData.has(st) && health.failedUrls.get(HOURLY(st)) !== "circuit");
      counts.skippedByBreaker = skipped.length;
      counts.failed = failed.length;
      counts.listedNoData = nd.listedNoData.size;
      counts.carriedStale = s.carried ?? 0;
      for (const st of failed) {
        const cls = health.failedUrls.get(HOURLY(st));
        if (cls) health.part(`station ${st}`, cls, health.sources["eccc-hourly"]?.last?.url === HOURLY(st) ? health.sources["eccc-hourly"]!.last!.detail : cls);
        else {
          health.demote("eccc-hourly", "parse", `station ${st}: CSV had no readable rows`, HOURLY(st));
          health.part(`station ${st}`, "parse", "CSV had no readable rows");
        }
      }
      if (failed.length > GAUGE_DEGRADED_SHARE * s.asked)
        held.push({ where: "stations", why: `${failed.length} of ${s.asked} failed this run; ${s.carried ?? 0} kept their last good entry (stale)` });
      // (T550) THE READING-AGE WATCHDOG: entries whose last reading is past READING_MAX_AGE_MS are written unranked; past a
      // share of the asked stations the feed is degraded (T513: 162 frozen CSVs counted as successes and nothing said so)
      if (s.oldReadings > GAUGE_DEGRADED_SHARE * s.asked)
        held.push({ where: "readings", why: `${s.oldReadings} of ${s.asked} stations' last reading is older than ${READING_MAX_AGE_HOURS} h: not ranked` });
      if (s.forecastFailed?.length) held.push({ where: "forecast", why: `${s.forecastFailed.join(", ")} summary unavailable; last good runs carried` });
      if (s.listShrunk) held.push({ where: "station list", why: `ECCC listed ${s.listShrunk.now} BC stations, the last index ${s.listShrunk.was}: not believed, the roster's realtime stations asked` });
      if (s.answered === 0 && !turn) throw new Error(`no station answered (asked ${s.asked}); index.json ${s.indexKept ? "kept (last good)" : "rewritten"}`);
      if (s.rankDrop) throw new Error(`${s.rankDrop.lost} of ${s.rankDrop.had} stations ranked last run lost their rank with a current reading (clim.json broken or for another roster); index.json kept (last good)`);
    } else if (job === "dfo") {
      const s = await runDfo({
        fetch: fetch_,
        feed: meteredStore(r2Store(env.BUCKET, "feeds/dfo/"), m),
        priv: meteredStore(r2Store(env.BUCKET, "private/feeds/dfo/"), m),
        regions: opts.regions ?? null,
        notifyUrl: null,
        meter: m,
        log,
      });
      const regions = (s.index as any).regions ?? {};
      held = dfoHeld(regions);
      for (const [slug, r] of Object.entries<any>(regions)) {
        const src = `dfo-region-${slug}`;
        const url = pageUrl(slug);
        if (r.stale && (opts.regions == null || opts.regions.includes(slug))) {
          const cls = health.failedUrls.get(url);
          if (cls) health.part(`region ${slug}`, cls, health.sources[src]?.last?.detail ?? cls);
          else if (health.sources[src]) {
            // every attempt answered 200 but no body passed validation
            health.demote(src, "parse", "page failed validation on every attempt", url);
            health.part(`region ${slug}`, "parse", "page failed validation");
          }
        } else if (r.held && health.sources[src]?.ok) {
          health.demote(src, "parse", String(r.held).slice(0, 200), url);
          health.part(`region ${slug}`, "parse", String(r.held));
        }
      }
      counts = {
        locations: Object.keys((s.index as any).locs ?? {}).length, regions: Object.keys(regions).length, unplaced: s.unplaced.length,
        notices: s.notices.length, unchangedRegions: (s.skipped ?? []).length,
      };
      summary = { ...counts, skipped: s.skipped };
      events.push(...dfoEvents(s.notices, at));
    } else {
      const s = await runInSeason({
        fetch: fetch_,
        priv: meteredStore(r2Store(env.BUCKET, "private/feeds/inseason/"), m),
        feed: meteredStore(r2Store(env.BUCKET, "feeds/inseason/"), m),
        meter: m,
        now: started,
        log,
        holdSnapshot: true,   // (T558) saved below, once the outbox has its events; a dry run saves nothing
      });
      saveInSeason = s.save;
      counts = {
        rows: s.rows, sections: s.sections, pageChanged: s.changed ? 1 : 0,
        unmatched: s.feed?.unmatched ?? 0, new: s.feed?.new ?? 0, changed: s.feed?.changed ?? 0, removed: s.feed?.removed ?? 0,
      };
      summary = { ...counts, first: s.first, diff: s.diff };
      events.push(...s.events);
    }
  } catch (e) {
    error = String((e as Error)?.stack ?? e).slice(0, 2000);
    // a body the job refused after the fetch itself succeeded (in-season block page / empty read)
    if (e instanceof SourceError && e.source && (health.sources[e.source]?.ok ?? 0) > 0) {
      health.demote(e.source, e.cls, e.message);
      health.part(e.source, e.cls, e.message);
    } else if (e instanceof SourceError && e.source) health.part(e.source, e.cls, e.message);
    else summary = { ...summary, errorClass: classOfError(e) };
  }
  const wallMs = Date.now() - t0;
  // (T558 item 2) the gauge run (the most frequent) also watches the daily jobs
  if (job === "gauges") {
    try { events.push(...await lateJobEvents(env.BUCKET, started, at)); } catch (e) { logErr(`daily jobs: not checked (${String((e as Error)?.message ?? e)})`); }
    // (T609 W1) a day whose report / new-error-kind total was reached
    try { events.push(...await capEvents(env.BUCKET, started, at)); } catch (e) { logErr(`report caps: not checked (${String((e as Error)?.message ?? e)})`); }
  }
  // (T558) the status (private/feeds/status.json) and the outbox. The status is written whatever the outbox does; a step
  // that fails is printed and kept in the result (`stepErrors`), never only in a log no cron prints
  const priv = r2Store(env.BUCKET, STATUS_PREFIX);
  let notify: unknown = null;
  let st: Awaited<ReturnType<typeof readStatus>> | null = null;
  let stEtag: string | null = null;
  try {
    ({ st, etag: stEtag } = await readStatusAt(priv));
  } catch (e) {
    logErr(`status: not read, so not written (${String((e as Error)?.message ?? e)})`);
  }
  // a source that answered nothing usable this run (and is not expected to): the feed published without it → degraded
  for (const [n, r] of Object.entries(health.sources))
    if (r.ok === 0 && r.fail > 0 && !held.some((h) => h.where === `source ${n}`))
      held.push({ where: `source ${n}`, why: `nothing usable this run (${fmtErrors(r.errors)}); last good data kept` });
  let alerts: Alert[] = [];
  const runRecord = { at, ok: error === null, wallMs, cpuMs: null, counts, held, error, meter: { ...m }, sources: health.sources };
  if (st) {
    alerts = record(st, job, runRecord, new Date());
    const down = recordHealth(st, job, health, at);
    events.push(...healthEvents(alerts, at), ...sourceDownEvents(job, down, st, at));
  }
  let queued = false;
  try {
    notify = await deliver(events, { store: priv, config: notifyConfig(env), fetch, now: new Date(), dryRun: opts.dryNotify, log });
    queued = !opts.dryNotify;
  } catch (e) {
    logErr(`notify: the outbox could not take this run's ${events.length} message(s) (${String((e as Error)?.message ?? e)}); they are raised again next run`);
  }
  // (T558) the in-season snapshot moves on only once its diff is in the outbox: otherwise the next run computes it again
  if (saveInSeason && queued) {
    try { await saveInSeason(); } catch (e) { logErr(`in-season: snapshot not saved (${String((e as Error)?.message ?? e)}); the same diff is told again next run`); }
  }
  if (st) {
    // an alert counts as announced only once the outbox has it (sent, or queued and retried)
    const announce = (s: NonNullable<typeof st>) => { if (queued) for (const a of alerts) if (s.feeds[a.job]) s.feeds[a.job]!.announced = a.to; return s; };
    announce(st);
    try {
      // (T558 W13) only over the status read; if another job wrote it meanwhile, this run is recorded on top of that
      await writeMerged(priv, STATUS_KEY, st, stEtag, (fresh) => {
        const f = checkStatus(fresh);
        record(f, job, runRecord, new Date());
        recordHealth(f, job, health, at);
        return announce(f);
      }, { contentType: "application/json", cacheControl: "no-store" });
    } catch (e) {
      logErr(`status: not written (${String((e as Error)?.message ?? e)})`);
    }
  }
  return { job, ok: error === null, error, wallMs, meter: m, sources: health.sources, failingParts: Object.keys(health.parts).length, summary, notify, stepErrors, log: logs.slice(-40) };
}

/** what a DFO run left held: whole regions (page / structure), stale regions (fetch failed), single places */
export function dfoHeld(regions: Record<string, any>): Held[] {
  const out: Held[] = [];
  for (const [slug, r] of Object.entries<any>(regions)) {
    if (r.held) out.push({ where: `region ${slug}`, why: r.held });
    else if (r.stale) out.push({ where: `region ${slug}`, why: `fetch failed ${r.failures}× in a row; the last answers stand` });
    for (const h of r.holds ?? []) out.push({ where: `region ${slug}`, place: h.location_id, why: `${h.kept}: ${h.why}` });
  }
  return out;
}

/** (T609 W12) the open status routes: no sniffing, no framing */
const SAFE_HEADERS = { "X-Content-Type-Options": "nosniff", "X-Frame-Options": "DENY" };
const firstLine = (s: string) => s.split("\n")[0]!.slice(0, 300);
/** (T609 W12) /status and /status.json are open to anyone: an error is served as its first line (no stack frames, no
 *  internal file names); the full text stays in R2, the run's answer to the token holder and the owner's email */
export function publicStatus(st: Awaited<ReturnType<typeof readStatus>>): void {
  for (const f of Object.values(st.feeds)) {
    if (!f) continue;
    if (f.lastError) f.lastError = { ...f.lastError, error: firstLine(f.lastError.error) };
    if (f.lastRun?.error) f.lastRun = { ...f.lastRun, error: firstLine(f.lastRun.error) };
  }
}
function publicErrors<T extends Record<string, unknown> | null>(j: T): T {
  if (j && typeof j.error === "string") return { ...j, error: firstLine(j.error) };
  return j;
}

// (T249) constant-time compare (src/guard.ts)
const authed = (req: Request, env: Env) => bearerOk(req, env.RUN_TOKEN);

/**
 * (T431) An owner message from a daily / weekly / monthly job. A send that fails (counted by `deliver`, retried next run) or
 * an outbox that cannot be read or written is LOGGED with the job's name (console.error: the worker's logs, `wrangler
 * tail`), never swallowed: it used to be a bare catch "logged below" with nothing below logging it.
 */
async function tell(job: string, env: Env, now: Date, events: FeedEvent[]): Promise<{ failed: number; error?: string }> {
  if (!events.length) return { failed: 0 };
  try {
    const r = await deliver(events, { store: r2Store(env.BUCKET, STATUS_PREFIX), config: notifyConfig(env), fetch, now,
                                      log: (s) => console.log(JSON.stringify({ job, notify: s })) });
    if (r.failed) console.error(JSON.stringify({ job, notifyFailed: r.failed }));
    return { failed: r.failed };
  } catch (e) {
    const error = String((e as Error)?.message ?? e).slice(0, 500);
    console.error(JSON.stringify({ job, notifyError: error }));
    return { failed: events.length, error };
  }
}

/** (T558) the daily backup and purge run at 09:25 UTC: a last run older than this is told (urgent), once per last run */
export const DAILY_JOB_LATE_MS = 36 * 3600_000;
/**
 * (T558 item 2) Nothing noticed when the daily crons stopped: backup.json said "ok" forever. The gauge run (every 30-60
 * min) reads each daily job's last result; one older than DAILY_JOB_LATE_MS is one urgent health message. A job that
 * has never run (a new deployment) says nothing here; the status page shows "never".
 */
export async function lateJobEvents(bucket: R2BucketLike, now: Date, at: string): Promise<FeedEvent[]> {
  const out: FeedEvent[] = [];
  for (const [name, key, retry] of [["backup", BACKUP_STATUS_KEY, "/run/backup"], ["report purge", RETENTION_STATUS_KEY, "/run/purge-reports"]] as const) {
    const j = await readStored<{ at?: string }>(r2Store(bucket, ""), key);
    const last = Date.parse(j?.at ?? "");
    if (!j || !Number.isFinite(last) || now.getTime() - last <= DAILY_JOB_LATE_MS) continue;
    out.push({
      key: `late:${name}:${j.at}`, kind: "health", urgent: true, at,
      title: `The daily ${name} has not run since ${j.at}`,
      text: `The daily ${name} (cron "25 9 * * *", 09:25 UTC) last ran ${j.at}, more than ${DAILY_JOB_LATE_MS / 3600_000} h ago: its cron has stopped or every run fails before it records.\n\n` +
        `Look: wrangler tail on the feeds worker; run it by hand: POST ${retry} (Bearer RUN_TOKEN).`,
    });
  }
  return out;
}

/** (T246) the daily backup; a failure is told at once (urgent health event) */
async function backup(env: Env, now = new Date()) {
  if (!env.BACKUP) return { ok: false, error: "no BACKUP binding" };
  const r = await runBackup(env.BUCKET as unknown as SrcBucket, env.BACKUP, now, env.BUCKET_NAME ?? null);
  const told = r.ok ? null : await tell("backup", env, now, [{ key: `backup:${r.day}`, kind: "health", title: `Backup ${r.day} failed`, urgent: true, at: r.at,
    text: `The daily R2 backup (daily/${r.day}/ in ${env.BACKUP_BUCKET_NAME ?? "the backup bucket (BACKUP_BUCKET_NAME is not set)"}) failed after ${r.copied} objects:\n${r.error}\n\nRetry: POST /run/backup on the feeds worker (Bearer RUN_TOKEN).` }]);
  console.log(JSON.stringify({ job: "backup", ok: r.ok, copied: r.copied, bytes: r.bytes, skipped: r.skipped.length, wallMs: r.wallMs, told }));
  return r;
}

/** (T271) the daily report purge (reports older than 180 days); the count is kept for /status, a failure rides the daily digest */
async function purge(env: Env, now = new Date()) {
  const r = await purgeReports(env.BUCKET, now);
  // (T291) test reports after 7 days, (T242) error kinds unseen for 90 days. (T558 W9) Both results are kept in
  // report-retention.json and a failure is told, like the report purge's (the 90-day promise must not lapse unseen)
  const testReports = await purgeTestReports(env.BUCKET, now);
  const crashes = await purgeCrashes(env.BUCKET, now);
  // (T559) the per-address counters, once their day is over (privacy page: "for a day's counting")
  const limits = await purgeLimits(env.BUCKET, now);
  const out = { ...r, ok: r.ok && limits.ok && crashes.ok && testReports.ok, limits, crashes, testReports };
  try {
    await env.BUCKET.put(RETENTION_STATUS_KEY, JSON.stringify(out), { httpMetadata: { contentType: "application/json", cacheControl: "no-store" } });
  } catch (e) { console.error(JSON.stringify({ job: "purge-reports", statusError: String(e) })); }
  const day = r.at.slice(0, 10);
  const told = await tell("purge-reports", env, now, [
    ...(r.ok ? [] : [{ key: `report-purge:${day}`, kind: "health" as const, title: `Report purge ${day} failed`, urgent: false, at: r.at,
      text: `The daily deletion of problem reports older than ${RETENTION_DAYS} days failed (${r.deleted} deleted, ${r.failed} not): ${r.error}\n\nRetry: POST /run/purge-reports on the feeds worker (Bearer RUN_TOKEN).` }]),
    ...(crashes.ok ? [] : [{ key: `crash-purge:${day}`, kind: "health" as const, title: `Error-kind purge ${day} failed`, urgent: false, at: r.at,
      text: `The daily deletion of app error kinds not seen for 90 days failed (${crashes.deleted} deleted, ${crashes.failed} not): ${crashes.error}\n\nRetry: POST /run/purge-reports on the feeds worker (Bearer RUN_TOKEN).` }]),
    ...(testReports.ok ? [] : [{ key: `test-report-purge:${day}`, kind: "health" as const, title: `Test-report purge ${day} failed`, urgent: false, at: r.at,
      text: `The daily deletion of test reports older than 7 days failed (${testReports.deleted} deleted, ${testReports.failed} not): ${testReports.error}\n\nRetry: POST /run/purge-reports on the feeds worker (Bearer RUN_TOKEN).` }]),
    ...(limits.ok ? [] : [{ key: `limits-purge:${day}`, kind: "health" as const, title: `Rate-limit counter purge ${day} failed`, urgent: false, at: r.at,
      text: `The daily deletion of yesterday's rate-limit counters (salted address hashes; the privacy page keeps them "for a day's counting") failed (${limits.deleted} deleted, ${limits.failed} not): ${limits.error}\n\nRetry: POST /run/purge-reports on the feeds worker (Bearer RUN_TOKEN).` }]),
  ]);
  console.log(JSON.stringify({ job: "purge-reports", ok: out.ok, deleted: r.deleted, kept: r.kept, failed: r.failed, limits: { deleted: limits.deleted, failed: limits.failed },
    crashes: { deleted: crashes.deleted, failed: crashes.failed }, testReports: { deleted: testReports.deleted, failed: testReports.failed }, told }));
  return out;
}

/** (T178) where the last monthly model score is kept (GET /status shows it) */
export const SKILL_STATUS_KEY = "private/feeds/skill.json";

/** (T178) the monthly model score: CLEVER vs COFFEE on the archive month that just closed (src/gauges/score.ts). It
 *  REPORTS — status + one digest line; applying new weights stays a reviewed change (owner 2026-11-04). */
async function skill(env: Env, now = new Date(), month?: string): Promise<{ ok: boolean; error?: string; report?: SkillReport }> {
  const m = newMeter();
  let report: SkillReport | undefined;
  let error: string | undefined;
  try {
    report = await runSkill({
      feed: meteredStore(r2Store(env.BUCKET, "feeds/live/"), m),
      priv: meteredStore(r2Store(env.BUCKET, "private/feeds/gauges/"), m),
      now, month, weights: FLOOD_WEIGHTS,
    });
  } catch (e) {
    error = String((e as Error)?.stack ?? e).slice(0, 2000);
  }
  const at = now.toISOString();
  try {
    await env.BUCKET.put(SKILL_STATUS_KEY, JSON.stringify(report ? { ok: true, ...report, meter: m } : { ok: false, at, month: month ?? null, error, meter: m }),
      { httpMetadata: { contentType: "application/json", cacheControl: "no-store" } });
  } catch (e) { console.error(JSON.stringify({ job: "skill", statusError: String(e) })); }
  const ev: FeedEvent = report
    ? { key: `skill:${report.month}`, kind: "skill", at, title: `Forecast model scores for ${report.month}: ${report.differs ? "a verdict differs from the shipped weights" : "no change"}`, text: skillText(report) }
    : { key: `skill-failed:${at.slice(0, 7)}`, kind: "skill", at, title: "The monthly forecast model score failed", text: `${error}\n\nRetry: POST /run/skill on the feeds worker (Bearer RUN_TOKEN).` };
  const told = await tell("skill", env, now, [ev]);
  console.log(JSON.stringify({ job: "skill", ok: !!report, month: report?.month, runs: report?.runs, cases: report?.cases, meter: m, told }));
  return report ? { ok: true, report } : { ok: false, error };
}

/** (T412) the weekly run-timing source check (src/runtiming.ts): a newer NuSEDS starts its workflow and tells the owner;
 *  a failed check is told too (`runtiming-failed:<day>`). (T431) `told` says whether that message went out. */
async function runTimingCheck(env: Env, now = new Date()): Promise<{ ok: boolean; error?: string; event?: FeedEvent | null; told: { failed: number; error?: string } }> {
  let event: FeedEvent | null = null;
  let error: string | undefined;
  try {
    event = await checkRunTiming(env, (u, i) => fetch(u, i), env.BUCKET, r2Store(env.BUCKET, "private/feeds/dispatch/"), now);
  } catch (e) {
    error = String((e as Error)?.message ?? e).slice(0, 500);
  }
  const at = now.toISOString();
  const ev: FeedEvent | null = event ?? (error
    ? { key: `runtiming-failed:${at.slice(0, 10)}`, kind: "runtiming", at, title: "The weekly run-timing source check failed",
        text: `${error}\n\nRetry: POST /run/runtiming on the feeds worker (Bearer RUN_TOKEN).` }
    : null);
  const told = await tell("runtiming", env, now, ev ? [ev] : []);
  console.log(JSON.stringify({ job: "runtiming", ok: !error, event: event?.key ?? null, error, told }));
  return error ? { ok: false, error, told } : { ok: true, event, told };
}

/** (T431) which job a cron string starts: its minute field names it (wrangler.toml [triggers]; test/scheduled.test.ts). */
export function cronJob(cron: string): Job | "backup" | "skill" | "runtiming" {
  if (cron.startsWith("25 ")) return "backup";   // + the report purge
  if (cron.startsWith("55 ")) return "skill";
  if (cron.startsWith("40 ")) return "runtiming";
  return cron.startsWith("15 ") ? "dfo" : cron.startsWith("45 ") ? "inseason" : "gauges";
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    setDeployId(env.CF_VERSION_METADATA?.id);
    const url = new URL(req.url);
    // (T249) every route is rate-limited per IP before anything else runs (the token routes more tightly)
    const cls = routeClass(url.pathname);
    const slow = await limited(cls === "auth" ? env.RL_AUTH : env.RL_PUBLIC, req, cls);
    if (slow) return slow;
    // (T237) problem reports from the app: public POST (validated, rate-limited, stored private), authed GET for review
    if (url.pathname === "/report") {
      if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: REPORT_CORS });
      if (req.method !== "POST") return new Response("Method Not Allowed", { status: 405, headers: { ...REPORT_CORS, Allow: "POST, OPTIONS" } });
      return handleReport(req, env);
    }
    // (T242) app error reports: public POST (validated, rate-limited, folded into one object per error kind), authed GET
    if (url.pathname === "/crash") {
      if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: REPORT_CORS });
      if (req.method !== "POST") return new Response("Method Not Allowed", { status: 405, headers: { ...REPORT_CORS, Allow: "POST, OPTIONS" } });
      return handleCrash(req, env);
    }
    if (url.pathname === "/crashes" && req.method === "GET") {
      if (!authed(req, env)) return new Response("Forbidden", { status: 403 });
      const since = url.searchParams.get("since") ?? new Date(Date.now() - 30 * 86_400_000).toISOString().slice(0, 10);
      if (!/^\d{4}-\d{2}-\d{2}$/.test(since)) return new Response("since must be YYYY-MM-DD", { status: 400 });
      // (T595) the defects unless `?class=all` / `?class=network,expected`; `counts` says what the default left out
      const classes = parseClasses(url.searchParams.get("class"));
      if (!classes) return new Response("class must be all, or a list of defect, network, expected", { status: 400 });
      return Response.json(await listCrashesCounted(env.BUCKET, since, 500, url.searchParams.get("test") === "1", url.searchParams.get("background") === "1", classes), { headers: { "Cache-Control": "no-store" } });
    }
    if (url.pathname === "/reports" && req.method === "GET") {
      if (!authed(req, env)) return new Response("Forbidden", { status: 403 });
      const since = url.searchParams.get("since") ?? new Date(Date.now() - 30 * 86_400_000).toISOString().slice(0, 10);
      if (!/^\d{4}-\d{2}-\d{2}$/.test(since)) return new Response("since must be YYYY-MM-DD", { status: 400 });
      // (T609 W10) `truncated` / `skipped`: a cut or partly unreadable list says so
      return Response.json(await listReportsCounted(env.BUCKET, since, new Date(), 500, url.searchParams.get("test") === "1"), { headers: { "Cache-Control": "no-store" } });
    }
    if (req.method === "GET" && (url.pathname === "/status" || url.pathname === "/status.json")) {
      const now = new Date();
      // (T558) a status that cannot be read is a 503 naming why, never "every feed: never run" with a 200
      let st: Awaited<ReturnType<typeof readStatus>>;
      try {
        st = refresh(await readStatus(r2Store(env.BUCKET, STATUS_PREFIX)), now);
      } catch (e) {
        const error = `the feeds' status could not be read: ${e instanceof Error ? e.message : String(e)}`;
        console.error(JSON.stringify({ job: "status", error }));
        return url.pathname === "/status.json"
          ? Response.json({ ok: false, error, inputs: INPUT_FORMATS }, { status: 503, headers: { "Cache-Control": "no-store" } })
          : new Response(error, { status: 503, headers: { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store" } });
      }
      // (T246) the last backup run beside the feeds (null: none yet)
      // (T609 W12) a daily job's file that cannot be read says so, never "not run yet"; served errors are first lines only
      const side = async (key: string) => { try { return publicErrors(await readStored<Record<string, unknown>>(r2Store(env.BUCKET, ""), key)); } catch (e) { return { unreadable: firstLine(String((e as Error)?.message ?? e)) }; } };
      const bk = await side(BACKUP_STATUS_KEY);
      // (T271) the last report purge: when, and how many reports it deleted
      const rt = await side(RETENTION_STATUS_KEY);
      // (T178) the last monthly model score
      const sk = await side(SKILL_STATUS_KEY);
      publicStatus(st);
      // (T558) how owner messages go out: "log" means nobody is told
      const nc = notifyConfig(env);
      const notifyInfo = { channel: channelOf(nc), digest: !!nc.digest };
      if (url.pathname === "/status.json") return Response.json({ ...servedStatus(st, now), notify: notifyInfo, backup: bk, reportRetention: rt, skill: sk, inputs: INPUT_FORMATS }, { headers: { "Cache-Control": "no-store", ...SAFE_HEADERS } });
      return new Response(statusHtml(st, now, sk, notifyInfo, { backup: bk, purge: rt }), { headers: { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store", ...SAFE_HEADERS,
        "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; frame-ancestors 'none'" } });
    }
    if (url.pathname === "/notify/test" && req.method === "POST") {
      if (!authed(req, env)) return new Response("Forbidden", { status: 403 });
      const lines: string[] = [];
      // {"sample": "dfo"}: the real template on a recorded DFO payload (marked TEST)
      const body = (await req.json().catch(() => ({}))) as { sample?: string };
      const sample = body.sample === "dfo" ? sampleDfoEvents() : [];
      const via = await sendTest(notifyConfig(env), fetch, new Date(), env.FEEDS_ENV || "staging", (s) => lines.push(s), sample);
      return Response.json({ via, log: lines }, { status: via ? 200 : 502 });
    }
    if (url.pathname === "/notify/apk" && req.method === "POST") {
      // (owner 2026-10-29) deploy/staging/build-apk.sh: a new dev APK is up — one urgent email, at once
      if (!authed(req, env)) return new Response("Forbidden", { status: 403 });
      const a = parseApk(await req.json().catch(() => null));
      if (!a) return new Response("Bad Request", { status: 400 });
      const lines: string[] = [];
      const via = await sendApk(notifyConfig(env), fetch, new Date(), a, (s) => lines.push(s));
      return Response.json({ via, log: lines }, { status: via ? 200 : 502 });
    }
    if (url.pathname === "/notify/preview" && req.method === "POST") {
      // what the owner would get: the queued digest (if any) — nothing sent, nothing recorded
      if (!authed(req, env)) return new Response("Forbidden", { status: 403 });
      const s = await readState(r2Store(env.BUCKET, STATUS_PREFIX));
      const c = notifyConfig(env);
      return Response.json({ channel: channelOf(c), digest: !!c.digest, pending: s.pending.length, told: Object.keys(s.told).length, lastDigest: s.lastDigest, messages: compose(s.pending, true, c.ctx) });
    }
    if (url.pathname === "/run/purge-reports" && req.method === "POST") {
      if (!authed(req, env)) return new Response("Forbidden", { status: 403 });
      const r = await purge(env);
      return Response.json(r, { status: r.ok ? 200 : 500 });
    }
    if (url.pathname === "/run/skill" && req.method === "POST") {
      // (T178) the monthly model score by hand; ?month=YYYY-MM scores that archive month
      if (!authed(req, env)) return new Response("Forbidden", { status: 403 });
      const month = url.searchParams.get("month") ?? undefined;
      if (month !== undefined && !/^\d{4}-\d{2}$/.test(month)) return new Response("month must be YYYY-MM", { status: 400 });
      const r = await skill(env, new Date(), month);
      return Response.json(r, { status: r.ok ? 200 : 500 });
    }
    if (url.pathname === "/run/check-github" && req.method === "POST") {
      // (T413) is the dispatch token good? a read with it; starts nothing
      if (!authed(req, env)) return new Response("Forbidden", { status: 403 });
      const r = await checkToken(env, (u, i) => fetch(u, i));
      return Response.json(r, { status: r.ok ? 200 : 500 });
    }
    if (url.pathname === "/run/runtiming" && req.method === "POST") {
      // (T412) the weekly run-timing source check by hand
      if (!authed(req, env)) return new Response("Forbidden", { status: 403 });
      const r = await runTimingCheck(env);
      return Response.json(r, { status: r.ok ? 200 : 500 });
    }
    if (url.pathname === "/run/forget-report" && req.method === "POST") {
      // (T559) a report deleted on request (privacy page: "within 30 days"): the live object and every backup copy
      if (!authed(req, env)) return new Response("Forbidden", { status: 403 });
      const id = url.searchParams.get("id") ?? "";
      if (!/^[0-9a-f]{16}$/.test(id)) return new Response("id must be the report's 16 hex characters", { status: 400 });
      if (!env.BACKUP) return Response.json({ ok: false, error: "no BACKUP binding: the backup copies were not deleted" }, { status: 500 });
      try {
        const r = await forgetReport(env.BUCKET as unknown as SrcBucket & { delete(k: string): Promise<void> }, env.BACKUP, id, new Date());
        return Response.json({ ok: true, id, ...r });
      } catch (e) {
        return Response.json({ ok: false, id, error: String((e as Error)?.message ?? e) }, { status: 500 });
      }
    }
    if (url.pathname === "/run/backup" && req.method === "POST") {
      if (!authed(req, env)) return new Response("Forbidden", { status: 403 });
      const r = await backup(env);
      return Response.json(r, { status: r.ok ? 200 : 500 });
    }
    const job = url.pathname.match(/^\/run\/(gauges|dfo|inseason)$/)?.[1] as Job | undefined;
    if (!job || req.method !== "POST") return new Response("Not Found", { status: 404 });
    if (!authed(req, env)) return new Response("Forbidden", { status: 403 });
    const regions = url.searchParams.get("regions")?.split(",").filter(Boolean);
    const limit = Number(url.searchParams.get("limit")) || undefined;
    const r = await run(job, env, { regions, limit, dryNotify: url.searchParams.has("dryNotify") });
    return Response.json(r, { status: r.ok ? 200 : 500 });
  },
  async scheduled(ev: { cron: string }, env: Env, ctx: { waitUntil(p: Promise<unknown>): void }): Promise<void> {
    setDeployId(env.CF_VERSION_METADATA?.id);
    // cron strings are in wrangler.toml [triggers]: the minute field names the job
    //   "15 …" DFO, "45 …" in-season, "25 …" the daily backup (T246) + report purge (T271), "55 …" the monthly model
    //   score (T178), "40 …" the weekly run-timing source check (T412), anything else the gauges
    const job = cronJob(ev.cron);
    if (job === "backup") { ctx.waitUntil(Promise.allSettled([backup(env), purge(env)])); return; }
    if (job === "skill") { ctx.waitUntil(skill(env)); return; }
    if (job === "runtiming") { ctx.waitUntil(runTimingCheck(env)); return; }
    // (T558 W8) a failed run prints its error and the tail of its log (the step errors were printed as they happened)
    ctx.waitUntil(run(job, env).then((r) => {
      console.log(JSON.stringify({ job, ok: r.ok, wallMs: r.wallMs, meter: r.meter, stepErrors: r.stepErrors.length }));
      if (!r.ok) console.error(JSON.stringify({ job, error: r.error, log: r.log.slice(-10) }));
    }));
  },
};
export { JOBS };
