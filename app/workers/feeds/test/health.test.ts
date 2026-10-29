// T225 Phase A (owner 2026-10-28): a source that blocks us / answers badly never stops the run; every failure is measured.
import { mkdtempSync, cpSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { foldHealth, resilientFetch, RunHealth, SourceError, sourceOf, summarize, windowOf, BREAKER_TRIP, type FeedHealth } from "../src/health";
import type { Fetch } from "../src/io";
import { runGauges } from "../src/gauges/publish";
import { runInSeason } from "../src/inseason";
import { cadenceMin, servedStatus, statusHtml, recordHealth, record, type Status } from "../src/status";
import { healthEvents } from "../src/index";
import { dirStore, fixtureFetch, loadRecording } from "../parity/gauges/node";
import { MemStore } from "./dfo.scenario";

const noSleep = async () => {};
const seq = (...rs: Array<Response | Error>): { fetch: Fetch; calls: () => number } => {
  let i = 0;
  return {
    fetch: async () => {
      const r = rs[Math.min(i++, rs.length - 1)]!;
      if (r instanceof Error) throw r;
      return r.clone();
    },
    calls: () => i,
  };
};
const H = "https://dd.weather.gc.ca/today/hydrometric/csv/BC/hourly/BC_08MF005_hourly_hydrometric.csv";

describe("resilientFetch", () => {
  it("retries a 503 and a network error with backoff, then succeeds; counted once ok with retries", async () => {
    const h = new RunHealth();
    const s = seq(new Response("", { status: 503 }), new Response("ok"));
    const f = resilientFetch(s.fetch, h, { sleep: noSleep });
    expect((await f(H)).status).toBe(200);
    expect(s.calls()).toBe(2);
    expect(h.sources["eccc-hourly"]).toMatchObject({ ok: 1, fail: 0, retries: 1 });
  });

  it("classifies: 403 blocked (no retry), 429 with a long Retry-After not retried, 404, timeout, network", async () => {
    const cases: Array<[Response | Error, string]> = [
      [new Response("", { status: 403 }), "blocked"],
      [new Response("", { status: 429, headers: { "Retry-After": "120" } }), "rate_limited"],
      [new Response("", { status: 404 }), "http_404"],
      [Object.assign(new Error("The operation was aborted due to timeout"), { name: "TimeoutError" }), "timeout"],
      [new TypeError("fetch failed"), "network"],
    ];
    for (const [r, cls] of cases) {
      const g = new RunHealth();
      const s = seq(r, r, r);
      const f = resilientFetch(s.fetch, g, { sleep: noSleep });
      try {
        await f("https://www2.gov.bc.ca/x");
      } catch {
        /* errors rethrown after the retries */
      }
      expect(Object.keys(g.sources["inseason-page"]!.errors)).toEqual([cls]);
      expect(s.calls()).toBe(cls === "blocked" || cls === "rate_limited" || cls === "http_404" ? 1 : 3);
    }
  });

  it("a 404 is ok for a forecast series (not in season)", async () => {
    const h = new RunHealth();
    await resilientFetch(seq(new Response("", { status: 404 })).fetch, h, { sleep: noSleep })("https://bcrfc.env.gov.bc.ca/freshet/clever/08MF005.CSV");
    expect(h.sources["rfc-series"]).toMatchObject({ ok: 1, fail: 0 });
  });

  it("the breaker: after a run of hard failures on one host the rest fail fast as `circuit`", async () => {
    const h = new RunHealth();
    const s = seq(new Response("", { status: 403 }));
    const f = resilientFetch(s.fetch, h, { sleep: noSleep });
    for (let i = 0; i < BREAKER_TRIP + 10; i++) {
      try {
        await f(H.replace("08MF005", `ST${i}`));
      } catch (e) {
        expect(e).toBeInstanceOf(SourceError);
      }
    }
    expect(s.calls()).toBe(BREAKER_TRIP);
    expect(h.sources["eccc-hourly"]!.errors).toEqual({ blocked: BREAKER_TRIP, circuit: 10 });
  });

  it("the source table covers every upstream", () => {
    expect(sourceOf("https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/fresh-douce/region6-eng.html").source).toBe("dfo-region-6");
    expect(sourceOf("https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/fresh-douce/region6-eng.html").policy.attempts).toBe(1);
    expect(sourceOf("https://services6.arcgis.com/a/arcgis/rest/services/x/FeatureServer/0/query?f=json").source).toBe("rfc-forecast");
    expect(sourceOf("https://dd.weather.gc.ca/today/hydrometric/doc/hydrometric_StationList.csv").source).toBe("eccc-station-list");
  });
});

describe("rolling health", () => {
  it("hourly buckets → 24 h / 7 d windows, success rate, consecutive failed runs, 3 runs → newly down once", () => {
    let fh: FeedHealth | undefined;
    const down: string[][] = [];
    const t0 = Date.parse("2026-10-20T00:05:00Z");
    for (let k = 0; k < 8 * 24; k++) {
      const r = new RunHealth();
      const at = new Date(t0 + k * 3600_000).toISOString();
      if (k >= 8 * 24 - 4) r.fail("eccc-hourly", "timeout", "x");
      else {
        r.ok("eccc-hourly");
        r.ok("eccc-hourly");
        if (k % 2) r.fail("eccc-hourly", "http_5xx", "HTTP 502");
      }
      const out = foldHealth(fh, r, at);
      fh = out.health;
      down.push(out.newlyDown);
    }
    const s = fh!.sources["eccc-hourly"]!;
    expect(s.hours.length).toBe(7 * 24);
    expect(s.consecutiveFailedRuns).toBe(4);
    expect(down.flat()).toEqual(["eccc-hourly"]);
    const now = new Date(t0 + (8 * 24 - 1) * 3600_000 + 60_000);
    const w = windowOf(s, now, 24);
    expect(w.errors.timeout).toBe(4);
    expect(w.ok + w.fail).toBeGreaterThan(0);
    expect(summarize(fh, now)["eccc-hourly"]!.last7d.successRate).toBeLessThan(1);
  });

  it("parts carry since / runs in a row and clear on recovery", () => {
    const a = new RunHealth();
    a.part("station X", "timeout", "t");
    let fh = foldHealth(undefined, a, "2026-10-28T10:00:00Z").health;
    fh = foldHealth(fh, a, "2026-10-28T11:00:00Z").health;
    expect(fh.parts["station X"]).toMatchObject({ since: "2026-10-28T10:00:00Z", runs: 2 });
    fh = foldHealth(fh, new RunHealth(), "2026-10-28T12:00:00Z").health;
    expect(fh.parts).toEqual({});
  });

  it("status: served summarized (no buckets), the page lists sources and failing parts; gauges 3-hourly at night", () => {
    const st: Status = { format: "feeds-status/1", updated: "", feeds: {} };
    const at = "2026-10-28T10:00:00Z";
    const r = new RunHealth();
    r.ok("eccc-hourly");
    r.fail("eccc-hourly", "blocked", "HTTP 403", H);
    r.part("station 08MF005", "blocked", "HTTP 403");
    record(st, "gauges", { at, ok: true, wallMs: 1, cpuMs: null, counts: {}, held: [], error: null, meter: null, sources: r.sources }, new Date(at));
    recordHealth(st, "gauges", r, at);
    const j = servedStatus(st, new Date(at)) as any;
    expect(j.feeds.gauges.health).toBeUndefined();
    expect(j.feeds.gauges.sources["eccc-hourly"].last24h).toMatchObject({ ok: 1, fail: 1, successRate: 0.5, errors: { blocked: 1 } });
    expect(j.feeds.gauges.failingParts["station 08MF005"].cls).toBe("blocked");
    const html = statusHtml(st, new Date(at));
    expect(html).toContain("eccc-hourly");
    expect(html).toContain("Failing parts (1)");
    expect(cadenceMin("gauges", new Date("2026-10-28T09:00:00Z"))).toBe(180);
    expect(cadenceMin("gauges", new Date("2026-10-28T18:00:00Z"))).toBe(60);
  });

  it("emails: a whole feed failing / stale is urgent, degraded is not", () => {
    const ev = healthEvents([{ job: "dfo", from: "ok", to: "failing", text: "x" }, { job: "gauges", from: "ok", to: "degraded", text: "y" }], "2026-10-28T10:00:00Z");
    expect(ev[0]!.urgent).toBe(true);
    expect(ev[1]!.urgent).toBeUndefined();
  });
});

describe("gauges: a failed station keeps its last good entry, marked stale", () => {
  const FIX = join(__dirname, "fixtures/gauges");
  it("one station's CSV blocked: the run publishes, that station is carried `stale`; all blocked: index.json kept", async () => {
    const dir = mkdtempSync(join(tmpdir(), "gauges-health-"));
    cpSync(join(FIX, "seed"), dir, { recursive: true });
    const rec = loadRecording(FIX);
    const base = fixtureFetch(rec).fetch;
    const feed = dirStore(join(dir, "feed"));
    const priv = dirStore(join(dir, "priv"));
    const roster = JSON.parse(readFileSync(join(FIX, "roster.json"), "utf-8"));
    const go = (fetch: Fetch, now = new Date(rec.now)) => runGauges({ fetch, feed, priv, now, roster, sleep: noSleep });
    await go(base);
    const idx1 = JSON.parse((await feed.get("index.json"))!);
    const victim = Object.keys(idx1.stations).sort()[0]!;
    const blockOne: Fetch = async (u, i) => (u.includes(`BC_${victim}_hourly`) ? new Response("", { status: 403 }) : base(u, i));
    const s2 = await go(blockOne, new Date(Date.parse(rec.now) + 3600_000));
    expect(s2.failed).toContain(victim);
    expect(s2.carried).toBeGreaterThanOrEqual(1);
    const idx2 = JSON.parse((await feed.get("index.json"))!);
    expect(idx2.stations[victim]).toMatchObject({ ...idx1.stations[victim], stale: true });
    expect(Object.keys(idx2.stations).length).toBe(Object.keys(idx1.stations).length);
    // every station CSV blocked: nothing answered, the last good index.json stands
    const before = await feed.get("index.json");
    const blockAll: Fetch = async (u, i) => (u.includes("_hourly_hydrometric.csv") ? new Response("", { status: 403 }) : base(u, i));
    const s3 = await go(blockAll, new Date(Date.parse(rec.now) + 7200_000));
    expect(s3.answered).toBe(0);
    expect(s3.indexKept).toBe(true);
    expect(await feed.get("index.json")).toBe(before);
  });
});

describe("in-season: an empty / gutted page is refused, the last good feed stands", () => {
  it("a block page fails `blocked`, nothing written", async () => {
    const priv = new MemStore();
    const feed = new MemStore();
    await priv.put("latest.json", JSON.stringify({ format: "inseason/1", at: "x", url: "u", sha: "s", rows: Array.from({ length: 30 }, () => ({})) }));
    await expect(runInSeason({ fetch: async () => new Response(""), priv, feed, html: "<html>Access Denied. Reference #18.abc</html>" })).rejects.toMatchObject({ cls: "blocked" });
    expect(await feed.get("index.json")).toBeNull();
  });
});
