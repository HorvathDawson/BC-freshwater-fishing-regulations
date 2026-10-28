// T225: the feeds Worker — gauges (pipeline/gauges/feed/publish.py) and DFO salmon (pipeline/regs/dfo_salmon/live.py)
// ported to TypeScript. STAGING ONLY for now and MEASURING ONLY: wrangler.toml declares NO cron trigger; a run is started
// by hand through `POST /run/gauges` or `POST /run/dfo` with `Authorization: Bearer <RUN_TOKEN>` (a wrangler secret).
// `scheduled` is wired so switching on is a [triggers] block, not code (prod switch-on is the owner's).
//
// R2 layout (one bucket): published  feeds/live/  feeds/dfo/   (the data worker serves these)
//                         private    private/feeds/gauges/  private/feeds/dfo/   (the data worker must NOT serve these)
import { meteredFetch, meteredStore, newMeter, r2Store, type Meter, type R2BucketLike } from "./io";
import { runGauges } from "./gauges";
import { runDfo } from "./dfo";

interface Env {
  BUCKET: R2BucketLike;
  RUN_TOKEN?: string;
  FEED_NOTIFY_URL?: string;
}

type Job = "gauges" | "dfo";

async function run(job: Job, env: Env, opts: { regions?: string[]; limit?: number } = {}) {
  const m: Meter = newMeter();
  const fetch_ = meteredFetch((u, i) => fetch(u, i), m);
  const logs: string[] = [];
  const log = (s: string) => logs.push(s);
  const t0 = Date.now(); // Workers' clock advances only across I/O: this is wall time, not CPU (CPU: tail / observability)
  let summary: unknown;
  if (job === "gauges") {
    const s = await runGauges({
      fetch: fetch_,
      feed: meteredStore(r2Store(env.BUCKET, "feeds/live/"), m),
      priv: meteredStore(r2Store(env.BUCKET, "private/feeds/gauges/"), m),
      now: new Date(),
      notifyUrl: env.FEED_NOTIFY_URL ?? null,
      limit: opts.limit,
      log,
    });
    summary = { asked: s.asked, answered: s.answered, withPercentile: s.withPercentile, maintenance: s.maintenance };
  } else {
    const s = await runDfo({
      fetch: fetch_,
      feed: meteredStore(r2Store(env.BUCKET, "feeds/dfo/"), m),
      priv: meteredStore(r2Store(env.BUCKET, "private/feeds/dfo/"), m),
      regions: opts.regions ?? null,
      notifyUrl: env.FEED_NOTIFY_URL ?? null,
      log,
    });
    summary = { locs: Object.keys((s.index as any).locs ?? {}).length, unplaced: s.unplaced.length, notices: s.notices.length, sent: s.sent };
  }
  return { job, wallMs: Date.now() - t0, meter: m, summary, log: logs.slice(-40) };
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    const job = url.pathname.match(/^\/run\/(gauges|dfo)$/)?.[1] as Job | undefined;
    if (!job || req.method !== "POST") return new Response("Not Found", { status: 404 });
    if (!env.RUN_TOKEN || req.headers.get("Authorization") !== `Bearer ${env.RUN_TOKEN}`)
      return new Response("Forbidden", { status: 403 });
    const regions = url.searchParams.get("regions")?.split(",").filter(Boolean);
    const limit = Number(url.searchParams.get("limit")) || undefined;
    try {
      return Response.json(await run(job, env, { regions, limit }));
    } catch (e) {
      return Response.json({ job, error: String((e as Error)?.stack ?? e) }, { status: 500 });
    }
  },
  async scheduled(ev: { cron: string }, env: Env, ctx: { waitUntil(p: Promise<unknown>): void }): Promise<void> {
    // cron strings are chosen in wrangler.toml [triggers] (none yet); a DFO cron names "dfo" by its minute field
    const job: Job = ev.cron.startsWith("15 ") ? "dfo" : "gauges";
    ctx.waitUntil(run(job, env).then((r) => console.log(JSON.stringify({ job, wallMs: r.wallMs, meter: r.meter }))));
  },
};
