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
//                         private    private/feeds/{gauges,dfo,inseason,notify}/, private/feeds/status.json
import { meteredFetch, meteredStore, newMeter, r2Store, type Meter, type R2BucketLike } from "./io";
import { runGauges } from "./gauges";
import { runDfo } from "./dfo";
import { runInSeason } from "./inseason";
import { compose, deliver, readState, sendTest, type FeedEvent, type NotifyConfig } from "./events";
import { readStatus, record, recordHealth, refresh, servedStatus, statusHtml, STATUS_KEY, type Alert, type Held, type Job } from "./status";
import { classOfError, fmtErrors, resilientFetch, RunHealth, SourceError, summarize } from "./health";
import { pageUrl } from "./dfo/fetch";

export interface Env {
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
}

const STATUS_PREFIX = "private/feeds/";
const JOBS: readonly Job[] = ["gauges", "dfo", "inseason"];

export function notifyConfig(env: Env): NotifyConfig {
  return {
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
          key: `hydat:${n.latest}`,
          kind: "hydat" as const,
          title: `New HYDAT ${n.latest} — run the HYDAT action`,
          text:
            `HYDAT ${n.latest} is out; the gauge envelope (clim.json) is on ${n.current}.\n\n` +
            `1. GitHub → Actions → "feeds — HYDAT → climatology → R2" (upload: true): the feeds Worker's clim.json.\n` +
            `2. On the Mac: scripts/update-hydat.sh --upload — the bundle and release (the build needs the atlas).\n\n` +
            `Noticed ${n.since}.`,
          at,
        }
      : {
          key: `${n.kind}:${n.latest}`,
          kind: "roster" as const,
          title: n.kind === "roster" ? "ECCC's real-time station list changed — refresh the roster" : `${n.kind}: ${n.latest} is due`,
          text: `${n.kind}: ${n.latest} (feed on ${n.current}). Run: ${n.action}\nThen sync it to R2: Actions → "feeds — sync curated inputs → R2 private/" (what: roster).`,
          at,
        },
  );
}

/** DFO notices (unplaced / held / moved / header names / fetch failures) → events naming the review-app tab to open
 *  (the review app runs on the owner's machine: no link, just where to go) and the queue command to run first */
export function dfoEvents(notices: { key: string; text: string }[], at: string): FeedEvent[] {
  return notices.map((n) => {
    const m = /DFO Region (\w+):/.exec(n.text);
    const lid = /\b(\d[ab]?:[a-z0-9-]+:[a-z0-9-]+)\b/.exec(n.text)?.[1];
    return {
      key: n.key,
      kind: "dfo" as const,
      title: n.text.split(";")[0]!.slice(0, 200),
      text:
        `${n.text}\n\n` +
        (m ? `First: python -m pipeline.regs.dfo_salmon.live queue --regions ${m[1]}\n` : "") +
        `Then: Review app → DFO → Extents: ${lid ?? placeOf(n.text) ?? "the new rows"}`,
      at,
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
    // owner 2026-10-28: a WHOLE feed failing / stale is emailed at once; degraded and recoveries wait for the digest
    ...(a.to === "failing" || a.to === "stale" ? { urgent: true } : {}),
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

const HOURLY = (st: string) => `https://dd.weather.gc.ca/today/hydrometric/csv/BC/hourly/BC_${st}_hourly_hydrometric.csv`;
/** share of asked stations that may fail before the gauge feed counts as degraded */
export const GAUGE_DEGRADED_SHARE = 0.1;


async function run(job: Job, env: Env, opts: { regions?: string[]; limit?: number; dryNotify?: boolean } = {}) {
  const m: Meter = newMeter();
  // every request: metered (each attempt is a subrequest), then retried / timed out / measured per source (src/health.ts)
  const health = new RunHealth();
  const fetch_ = resilientFetch(meteredFetch((u, i) => fetch(u, i), m), health);
  const logs: string[] = [];
  const log = (s: string) => logs.push(s);
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
      counts = { stations: s.answered, asked: s.asked, withPercentile: s.withPercentile, forecastRuns: s.forecastRuns, unchanged: m.sourcesUnchanged, forecastReused: s.forecastReused ? 1 : 0 };
      events.push(...gaugeEvents(s.maintenance as any[], at));
      counts.failed = s.failed?.length ?? 0;
      counts.carriedStale = s.carried ?? 0;
      for (const st of s.failed ?? []) {
        const cls = health.failedUrls.get(HOURLY(st));
        if (cls) health.part(`station ${st}`, cls, health.sources["eccc-hourly"]?.last?.url === HOURLY(st) ? health.sources["eccc-hourly"]!.last!.detail : cls);
        else {
          health.demote("eccc-hourly", "parse", `station ${st}: CSV had no readable rows`, HOURLY(st));
          health.part(`station ${st}`, "parse", "CSV had no readable rows");
        }
      }
      if ((s.failed?.length ?? 0) > GAUGE_DEGRADED_SHARE * s.asked)
        held.push({ where: "stations", why: `${s.failed!.length} of ${s.asked} not answered this run; ${s.carried ?? 0} kept their last good entry (stale)` });
      if (s.forecastFailed?.length) held.push({ where: "forecast", why: `${s.forecastFailed.join(", ")} summary unavailable; last good runs carried` });
      if (s.answered === 0) throw new Error(`no station answered (asked ${s.asked}); index.json ${s.indexKept ? "kept (last good)" : "rewritten"}`);
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
      });
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
  // the status (private/feeds/status.json) and the outbox — never fail the run
  let notify: unknown = null;
  try {
    const priv = r2Store(env.BUCKET, STATUS_PREFIX);
    const st = await readStatus(priv);
    // a source that answered nothing usable this run (and is not expected to): the feed published without it → degraded
    for (const [n, r] of Object.entries(health.sources))
      if (r.ok === 0 && r.fail > 0 && !held.some((h) => h.where === `source ${n}`))
        held.push({ where: `source ${n}`, why: `nothing usable this run (${fmtErrors(r.errors)}); last good data kept` });
    const alerts = record(st, job, { at, ok: error === null, wallMs, cpuMs: null, counts, held, error, meter: { ...m }, sources: health.sources }, new Date());
    const down = recordHealth(st, job, health, at);
    if (!opts.dryNotify) for (const a of alerts) st.feeds[a.job]!.announced = a.to; // the outbox retries a failed send
    events.push(...healthEvents(alerts, at), ...sourceDownEvents(job, down, st, at));
    notify = await deliver(events, { store: r2Store(env.BUCKET, STATUS_PREFIX), config: notifyConfig(env), fetch, now: new Date(), dryRun: opts.dryNotify, log });
    await priv.put(STATUS_KEY, JSON.stringify(st), { contentType: "application/json", cacheControl: "no-store" });
  } catch (e) {
    log(`status/notify: not written (${String(e)})`);
  }
  return { job, ok: error === null, error, wallMs, meter: m, sources: health.sources, failingParts: Object.keys(health.parts).length, summary, notify, log: logs.slice(-40) };
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

const authed = (req: Request, env: Env) => !!env.RUN_TOKEN && req.headers.get("Authorization") === `Bearer ${env.RUN_TOKEN}`;

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    if (req.method === "GET" && (url.pathname === "/status" || url.pathname === "/status.json")) {
      const now = new Date();
      const st = refresh(await readStatus(r2Store(env.BUCKET, STATUS_PREFIX)), now);
      if (url.pathname === "/status.json") return Response.json(servedStatus(st, now), { headers: { "Cache-Control": "no-store" } });
      return new Response(statusHtml(st, now), { headers: { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" } });
    }
    if (url.pathname === "/notify/test" && req.method === "POST") {
      if (!authed(req, env)) return new Response("Forbidden", { status: 403 });
      const lines: string[] = [];
      const via = await sendTest(notifyConfig(env), fetch, new Date(), "staging", (s) => lines.push(s));
      return Response.json({ via, log: lines }, { status: via ? 200 : 502 });
    }
    if (url.pathname === "/notify/preview" && req.method === "POST") {
      // what the owner would get: the queued digest (if any) — nothing sent, nothing recorded
      if (!authed(req, env)) return new Response("Forbidden", { status: 403 });
      const s = await readState(r2Store(env.BUCKET, STATUS_PREFIX));
      const c = notifyConfig(env);
      return Response.json({ channel: c.emailTo && c.emailFrom && c.sendEmail ? "email" : c.webhook ? "webhook" : "log", digest: !!c.digest, pending: s.pending.length, told: Object.keys(s.told).length, lastDigest: s.lastDigest, messages: compose(s.pending, true) });
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
    // cron strings are in wrangler.toml [triggers]: the minute field names the job
    //   "15 …" DFO, "45 …" in-season, anything else the gauges
    const job: Job = ev.cron.startsWith("15 ") ? "dfo" : ev.cron.startsWith("45 ") ? "inseason" : "gauges";
    ctx.waitUntil(run(job, env).then((r) => console.log(JSON.stringify({ job, ok: r.ok, wallMs: r.wallMs, meter: r.meter }))));
  },
};
export { JOBS };
