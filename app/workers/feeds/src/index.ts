// T225: the feeds Worker — gauges (pipeline/gauges/feed/publish.py) and DFO salmon (pipeline/regs/dfo_salmon/live.py)
// ported to TypeScript. STAGING ONLY for now and MEASURING ONLY: wrangler.toml declares NO cron trigger; a run is started
// by hand through `POST /run/gauges` or `POST /run/dfo` with `Authorization: Bearer <RUN_TOKEN>` (a wrangler secret).
// `scheduled` is wired so switching on is a [triggers] block, not code (prod switch-on is the owner's).
//
// GET /status (HTML) and GET /status.json (no auth): every feed's last run, last success, counts, held rows / places,
// staleness against its cadence, last error — from private/feeds/status.json, which each run writes (src/status.ts).
//
// R2 layout (one bucket): published  feeds/live/  feeds/dfo/   (the data worker serves these)
//                         private    private/feeds/gauges/  private/feeds/dfo/   (the data worker must NOT serve these)
import { meteredFetch, meteredStore, newMeter, r2Store, type Meter, type R2BucketLike } from "./io";
import { runGauges } from "./gauges";
import { runDfo } from "./dfo";
import { readStatus, record, refresh, sendAlerts, statusHtml, STATUS_KEY, type Held, type Job } from "./status";

interface Env {
  BUCKET: R2BucketLike;
  RUN_TOKEN?: string;
  FEED_NOTIFY_URL?: string;
}

const STATUS_PREFIX = "private/feeds/";

async function run(job: Job, env: Env, opts: { regions?: string[]; limit?: number } = {}) {
  const m: Meter = newMeter();
  const fetch_ = meteredFetch((u, i) => fetch(u, i), m);
  const logs: string[] = [];
  const log = (s: string) => logs.push(s);
  const t0 = Date.now(); // Workers' clock advances only across I/O: this is wall time, not CPU (CPU: tail / observability)
  const started = new Date();
  let summary: Record<string, unknown> = {};
  let counts: Record<string, number> = {};
  let held: Held[] = [];
  let error: string | null = null;
  try {
    if (job === "gauges") {
      const s = await runGauges({
        fetch: fetch_,
        feed: meteredStore(r2Store(env.BUCKET, "feeds/live/"), m),
        priv: meteredStore(r2Store(env.BUCKET, "private/feeds/gauges/"), m),
        now: started,
        notifyUrl: env.FEED_NOTIFY_URL ?? null,
        limit: opts.limit,
        meter: m,
        log,
      });
      summary = { asked: s.asked, answered: s.answered, withPercentile: s.withPercentile, maintenance: s.maintenance, runArchive: s.runArchive };
      counts = { stations: s.answered, asked: s.asked, withPercentile: s.withPercentile, forecastRuns: s.forecastRuns, unchanged: m.sourcesUnchanged };
      if (s.answered === 0) throw new Error(`no station answered (asked ${s.asked})`);
    } else {
      const s = await runDfo({
        fetch: fetch_,
        feed: meteredStore(r2Store(env.BUCKET, "feeds/dfo/"), m),
        priv: meteredStore(r2Store(env.BUCKET, "private/feeds/dfo/"), m),
        regions: opts.regions ?? null,
        notifyUrl: env.FEED_NOTIFY_URL ?? null,
        meter: m,
        log,
      });
      const regions = (s.index as any).regions ?? {};
      held = dfoHeld(regions);
      counts = {
        locations: Object.keys((s.index as any).locs ?? {}).length, regions: Object.keys(regions).length, unplaced: s.unplaced.length,
        notices: s.notices.length, unchangedRegions: (s.skipped ?? []).length,
      };
      summary = { ...counts, sent: s.sent, skipped: s.skipped };
    }
  } catch (e) {
    error = String((e as Error)?.stack ?? e).slice(0, 2000);
  }
  const wallMs = Date.now() - t0;
  // the status (private/feeds/status.json) and the alert hook — never fails the run
  try {
    const priv = r2Store(env.BUCKET, STATUS_PREFIX);
    const st = await readStatus(priv);
    const alerts = record(st, job, { at: started.toISOString(), ok: error === null, wallMs, cpuMs: null, counts, held, error, meter: { ...m } }, new Date());
    await sendAlerts(st, alerts, env.FEED_NOTIFY_URL, fetch, log);
    await priv.put(STATUS_KEY, JSON.stringify(st), { contentType: "application/json", cacheControl: "no-store" });
  } catch (e) {
    log(`status: not written (${String(e)})`);
  }
  return { job, ok: error === null, error, wallMs, meter: m, summary, log: logs.slice(-40) };
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

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    if (req.method === "GET" && (url.pathname === "/status" || url.pathname === "/status.json")) {
      const now = new Date();
      const st = refresh(await readStatus(r2Store(env.BUCKET, STATUS_PREFIX)), now);
      if (url.pathname === "/status.json") return Response.json(st, { headers: { "Cache-Control": "no-store" } });
      return new Response(statusHtml(st, now), { headers: { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" } });
    }
    const job = url.pathname.match(/^\/run\/(gauges|dfo)$/)?.[1] as Job | undefined;
    if (!job || req.method !== "POST") return new Response("Not Found", { status: 404 });
    if (!env.RUN_TOKEN || req.headers.get("Authorization") !== `Bearer ${env.RUN_TOKEN}`)
      return new Response("Forbidden", { status: 403 });
    const regions = url.searchParams.get("regions")?.split(",").filter(Boolean);
    const limit = Number(url.searchParams.get("limit")) || undefined;
    const r = await run(job, env, { regions, limit });
    return Response.json(r, { status: r.ok ? 200 : 500 });
  },
  async scheduled(ev: { cron: string }, env: Env, ctx: { waitUntil(p: Promise<unknown>): void }): Promise<void> {
    // cron strings are chosen in wrangler.toml [triggers] (none yet); a DFO cron names "dfo" by its minute field
    const job: Job = ev.cron.startsWith("15 ") ? "dfo" : "gauges";
    ctx.waitUntil(run(job, env).then((r) => console.log(JSON.stringify({ job, ok: r.ok, wallMs: r.wallMs, meter: r.meter }))));
  },
};
