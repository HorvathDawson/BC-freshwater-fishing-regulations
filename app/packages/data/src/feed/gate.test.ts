/**
 * (T676 item 2, the acceptance) A FEED FORMAT BUMP DOES NOT STRAND THE PREVIOUS APP: the bumped index names a newer
 * `minAppVersion`; the previous app (which carries the gate) keeps showing its last capture AND raises the update prompt
 * — no "no feed", no failure report, the feed not marked seen. Every reader given the gate does the same on its own
 * fetch (DFO, in-season, gauges, run timing).
 */
import { describe, expect, it } from "vitest";
import { appTooOld, appUpdateVM, APP_UPDATE_WORDS, createFeedGate, FeedNeedsNewerApp, GAUGE_FEED_FORMAT, INSEASON_FEED_FORMAT, INSEASON_LINKS_FORMAT,
         resolveAppVersion, SALMON_FEED_FORMAT, setFailureSink, type UpdateEvent } from "@app/core";
import { sha256Hex } from "../sha256";
import type { StationId } from "../index";
import { createUpdateService } from "../update/service";
import { createNotifySettings, NOTIFY_DEFAULT } from "../notify/settings";
import { runBackgroundCheck } from "../notify/check";
import { fetchObs, httpFeed } from "./http";
import { httpInSeason } from "./inseason";
import { httpRunTiming } from "./runtiming";
import { httpSalmonFeed } from "./salmon";

const APP = resolveAppVersion({ version: "0.8.4" });
const A = "A" as StationId;
const enc = new TextEncoder();
/** every index's stamp today (core FEED_MIN_APP; `feedGate.test.ts` holds that each passes every app with the gate) */
const TODAY = "0.0.0";

/** A whole dfo-feed index (one location), as the worker writes it today: stamped with the table's minimum. */
const DFO = { format: SALMON_FEED_FORMAT, minAppVersion: TODAY, at: "2026-11-20T18:00:00Z",
  verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 }, { s: "open", t: "1 per day, hatchery marked only (Sep 15–Jan 31)" }],
  gears: [["No natural bait"]], locs: { "2:squamish": { r: "2", sp: [[0, -1], [0, 1], [0, -1], [0, -1], [0, -1]], g: [0, -1] } },
  unplaced: 0, regions: { "2": { name: "Lower Mainland", url: "u", asof: "2026-11-20", sha256: "x", changed: "2026-11-20T18:00:00Z",
                                 checked: "2026-11-20T18:00:00Z", stale: false, held: null, failures: 0 } } };
/** The same feed after a format bump that 0.8.4 cannot read: the writer raised the minimum with it. */
const BUMPED = { ...DFO, format: "dfo-feed/4", minAppVersion: "0.9.0", at: "2026-12-01T18:00:00Z" };

const mem = (t: string | null = null) => ({ t, read() { return this.t; }, write(x: string) { this.t = x; } });

/** An origin: url → body (a 200 with an ETag of the body; a matching If-None-Match: 304). */
function origin(files: Record<string, unknown>) {
  /** every answer's status, in order */
  const log: number[] = [];
  const f = (async (u: RequestInfo | URL, i?: RequestInit) => {
    const body = files[String(u)];
    if (body === undefined) { log.push(404); return new Response("", { status: 404 }); }
    const text = typeof body === "string" ? body : JSON.stringify(body);
    const etag = `"${sha256Hex(enc.encode(text)).slice(0, 12)}"`;
    if (new Headers(i?.headers).get("If-None-Match") === etag) { log.push(304); return new Response(null, { status: 304, headers: { ETag: etag } }); }
    log.push(200);
    return new Response(text, { status: 200, headers: { ETag: etag } });
  }) as typeof fetch;
  return { files, fetch: f, log };
}

describe("(T676 item 2) a format bump on staging, the previous app", () => {
  it("keeps showing its last capture AND raises the update prompt; nothing is reported; a readable index clears it", async () => {
    const reported: unknown[] = [];
    const undo = setFailureSink((e) => reported.push(e));
    try {
      const URL_ = "https://o.test/feeds/dfo/index.json";
      const o = origin({ [URL_]: DFO });
      const gate = createFeedGate(APP);
      const store = mem();
      const salmon = httpSalmonFeed("https://o.test/feeds/dfo/", store, o.fetch, { gate, ttlMs: 0 });
      const events: UpdateEvent[] = [];
      const service = createUpdateService({ fetch: o.fetch, appVersion: APP, feedGate: gate, onEvent: (e) => events.push(e),
        feeds: [{ id: "dfo", url: URL_, apply: (text) => salmon.offer!(text) }] });

      // today: the stamped feed is read and captured
      await service.check();
      expect((await salmon.latest())?.at).toBe(DFO.at);
      expect([service.snapshot().needsApp, appTooOld(service.snapshot())]).toEqual([null, false]);

      // the bump is published
      o.files[URL_] = BUMPED;
      await service.check();
      const snap = service.snapshot();
      expect(snap.needsApp).toBe("0.9.0");
      expect(snap.feeds[0]).toMatchObject({ id: "dfo", error: null, needsApp: "0.9.0" });
      expect(events.at(-1)).toMatchObject({ source: "dfo", kind: "app-too-old", note: "needs app ≥ 0.9.0" });
      // the last capture answers (the service's offer was refused, and so is the reader's own fetch)
      expect((await salmon.latest())?.at).toBe(DFO.at);
      expect(JSON.parse(store.t!).at).toBe(DFO.at);
      // the strong prompt, the release gate's own
      const vm = appUpdateVM({ channel: "play", check: { state: "current" }, tooOld: appTooOld(snap), start: "idle" });
      expect([vm?.strength, vm?.text, vm?.button?.immediate]).toEqual(["strong", APP_UPDATE_WORDS.needed, true]);
      // a known state, not a failure: nothing reported, no error on the sync badge
      expect(reported).toEqual([]);

      // its validator is remembered: the next check is a 304 and the same refusal, nothing downloaded
      o.log.length = 0;
      await service.check();
      expect(o.log).toEqual([304]);
      expect(events.at(-1)).toMatchObject({ source: "dfo", kind: "app-too-old", bytes: 0, note: "needs app ≥ 0.9.0 (unchanged)" });
      expect(service.snapshot().needsApp).toBe("0.9.0");

      // the origin goes back to a feed this app reads: the need clears
      o.files[URL_] = { ...DFO, at: "2026-12-02T18:00:00Z" };
      await service.check();
      expect([service.snapshot().needsApp, appTooOld(service.snapshot()), (await salmon.latest())?.at])
        .toEqual([null, false, "2026-12-02T18:00:00Z"]);
    } finally { undo(); }
  });

  it("a restart keeps the refusal (a 304, the prompt up at once); the updated app reads the index whole", async () => {
    const URL_ = "https://o.test/feeds/dfo/index.json";
    const o = origin({ [URL_]: DFO });
    const store = mem(), capture = mem();
    const start = (version: string) => {
      const gate = createFeedGate(resolveAppVersion({ version }));
      const salmon = httpSalmonFeed("https://o.test/feeds/dfo/", capture, o.fetch, { gate, ttlMs: 0 });
      return createUpdateService({ fetch: o.fetch, appVersion: resolveAppVersion({ version }), feedGate: gate, store,
                                   feeds: [{ id: "dfo", url: URL_, apply: (text) => salmon.offer!(text) }] });
    };
    let s = start("0.8.4");
    await s.check();
    o.files[URL_] = BUMPED;
    await s.check();
    expect(s.snapshot().needsApp).toBe("0.9.0");
    s = start("0.8.4");                                   // the next launch
    expect([s.snapshot().needsApp, s.snapshot().feeds[0]!.needsApp]).toEqual(["0.9.0", "0.9.0"]);
    o.log.length = 0;
    await s.check();
    expect([o.log, s.snapshot().needsApp]).toEqual([[304], "0.9.0"]);
    // an app new enough for the stamp forgets the refusal and reads the index whole (this one still cannot parse /4:
    // a failure of its own, said as one; the point is that it was downloaded, not answered from the refusal)
    const undo = setFailureSink(() => {});
    try {
      s = start("0.9.0");
      expect(s.snapshot().needsApp).toBeNull();
      o.log.length = 0;
      await s.check();
      expect(o.log).toEqual([200]);
    } finally { undo(); }
  });

  it("(item 4) a 304 of the index in use clears a need a reader's own fetch set (the origin no longer serves it)", async () => {
    const URL_ = "https://o.test/feeds/dfo/index.json";
    const o = origin({ [URL_]: DFO });
    const gate = createFeedGate(APP);
    const salmon = httpSalmonFeed("https://o.test/feeds/dfo/", mem(), o.fetch, { gate, ttlMs: -1 });
    const service = createUpdateService({ fetch: o.fetch, appVersion: APP, feedGate: gate,
                                          feeds: [{ id: "dfo", url: URL_, apply: (text) => salmon.offer!(text) }] });
    await service.check();
    o.files[URL_] = BUMPED;
    await salmon.latest();                                // the reader's own fetch meets the bump first
    expect(service.snapshot().needsApp).toBe("0.9.0");
    o.files[URL_] = DFO;                                  // the bump is taken back
    o.log.length = 0;
    await service.check();
    expect([o.log, service.snapshot().needsApp, service.snapshot().feeds[0]!.needsApp, gate.needs()]).toEqual([[304], null, null, null]);
  });

  it("the mutation: the same bump to an app WITHOUT the gate reads as a refused feed, reported, and no prompt", async () => {
    const reported: unknown[] = [];
    const undo = setFailureSink((e) => reported.push(e));
    try {
      const URL_ = "https://o.test/feeds/dfo/index.json";
      const o = origin({ [URL_]: BUMPED });
      const salmon = httpSalmonFeed("https://o.test/feeds/dfo/", mem(JSON.stringify(DFO)), o.fetch);
      const service = createUpdateService({ fetch: o.fetch, appVersion: APP,
        feeds: [{ id: "dfo", url: URL_, apply: (text) => salmon.offer!(text) }] });
      await service.check();
      expect(appTooOld(service.snapshot())).toBe(false);
      expect(service.snapshot().feeds[0]!.error).toMatch(/not a dfo-feed\/3 feed/);
      expect(reported.length).toBeGreaterThan(0);
    } finally { undo(); }
  });
});

describe("(T676 item 2) every reader given the gate refuses an index for a newer app and keeps what it holds", () => {
  const quiet = async (fn: () => Promise<void>) => {
    const reported: unknown[] = [];
    const undo = setFailureSink((e) => reported.push(e));
    try { await fn(); } finally { undo(); }
    expect(reported).toEqual([]);
  };
  const serve = (files: Record<string, unknown>): typeof fetch => (async (u: string) => {
    const name = String(u).split("/").pop()!;
    const f = files[name];
    if (f === undefined) return new Response("", { status: 404 });
    return new Response(typeof f === "string" || f instanceof Uint8Array ? (f as BodyInit) : JSON.stringify(f));
  }) as unknown as typeof fetch;

  it("in-season: the capture answers, no unreadable-feed line, the need held", () => quiet(async () => {
    const FEED = { format: INSEASON_FEED_FORMAT, minAppVersion: TODAY, fetchedAt: "2026-11-20T12:00:00Z",
                   source: "s", removed: [], rows: [{ id: "is-a", region: "4", water: "W", effective: "", from: null, to: null, change: "No fishing" }] };
    const LINKS = { format: INSEASON_LINKS_FORMAT, notices: { "is-a": { waters: { "gnis:1": { all: true } } } } };
    const gate = createFeedGate(APP);
    const store = mem();
    expect((await httpInSeason("http://x", store, serve({ "index.json": FEED, "matches.json": LINKS }), { gate }).latest())?.feed.rows.length).toBe(1);
    const bumped = { ...FEED, format: "inseason-feed/2", minAppVersion: "0.9.0", fetchedAt: "2026-12-01T12:00:00Z" };
    const src = httpInSeason("http://x", store, serve({ "index.json": bumped, "matches.json": LINKS }), { gate });
    expect((await src.latest())?.feed.fetchedAt).toBe(FEED.fetchedAt);
    expect([src.failure!(), gate.refused()]).toEqual([null, { inseason: "0.9.0" }]);
    expect(() => src.offer!(JSON.stringify(bumped))).toThrow(/needs app 0.9.0/);
  }));

  it("gauges: the index, a station file and obs.json — the session's copy answers, nothing reported; the offer is refused", () => quiet(async () => {
    const IDX = { format: GAUGE_FEED_FORMAT, minAppVersion: TODAY, fetchedAt: "2026-11-20T12:00:00Z", stations: { A: {} } };
    const ST = { format: GAUGE_FEED_FORMAT, minAppVersion: TODAY, fetchedAt: "2026-11-20T12:00:00Z", station: "A",
                 now: { at: "2026-11-20T11:00:00-08:00", discharge: 12, level: 1.1 } };
    const OBS = { format: GAUGE_FEED_FORMAT, minAppVersion: TODAY, fetchedAt: "2026-11-20T12:00:00Z", windowHours: 24,
                  stations: { A: [["2026-11-20T11:00:00-08:00", 1.1, 12]] } };
    const gate = createFeedGate(APP);
    const files: Record<string, unknown> = { "index.json": IDX, "A.json": ST, "obs.json": OBS };
    const f = httpFeed("http://x/feeds/live", serve(files), { gate });
    expect(await f.live()).toEqual(new Set(["A"]));
    expect(await f.lastReport(A)).toBe(ST.now.at);
    expect((await fetchObs("http://x/feeds/live", serve(files), undefined, undefined, gate)).stations).toEqual(OBS.stations);
    // the bump: every gauge file in gauge-feed/2, stamped 0.9.0
    const bump = (d: object) => ({ ...d, format: "gauge-feed/2", minAppVersion: "0.9.0" });
    files["index.json"] = bump({ ...IDX, stations: { B: {} } });
    files["A.json"] = bump({ ...ST, now: { ...ST.now, at: "2026-12-01T11:00:00-08:00" } });
    files["obs.json"] = bump(OBS);
    f.refresh!();
    expect(await f.live()).toEqual(new Set(["A"]));
    expect(await f.lastReport(A)).toBe(ST.now.at);           // the station panel keeps the session's copy
    expect(gate.needs()).toBe("0.9.0");
    expect(() => f.offer!(files["index.json"])).toThrow(/needs app 0.9.0/);
    await expect(fetchObs("http://x/feeds/live", serve(files), undefined, undefined, gate)).rejects.toThrow(FeedNeedsNewerApp);
    // the background check's own gate (a fresh one per run, no index read): obs.json's own stamp is enough
    await expect(fetchObs("http://x/feeds/live", serve(files), undefined, undefined, createFeedGate(APP))).rejects.toThrow(FeedNeedsNewerApp);
  }));

  it("(review M1/M3) ONLY a file's own stamp excuses it: a broken gauge file in this app's format, or another format with no stamp, is REPORTED with a gate present — held or not", async () => {
    const reported: string[] = [];
    const undo = setFailureSink((e) => reported.push(String((e as Error).message ?? e)));
    try {
      const IDX = { format: GAUGE_FEED_FORMAT, minAppVersion: TODAY, fetchedAt: "2026-11-20T12:00:00Z", stations: { A: {} } };
      const brokenStation = { format: GAUGE_FEED_FORMAT, minAppVersion: TODAY, fetchedAt: "2026-11-20T12:00:00Z" };   // no station
      const brokenObs = { format: GAUGE_FEED_FORMAT, minAppVersion: TODAY, fetchedAt: "2026-11-20T12:00:00Z" };       // no stations
      for (const held of [false, true]) {
        reported.length = 0;
        const gate = createFeedGate(APP);
        if (held) gate.note("gauges", "0.9.0");                // a hold (say a stale one) excuses nothing
        const files: Record<string, unknown> = { "index.json": IDX, "A.json": brokenStation, "obs.json": brokenObs };
        const f = httpFeed("http://x/feeds/live", serve(files), { gate });
        expect(await f.lastReport(A)).toBeUndefined();
        expect(reported.filter((m) => m.includes("A.json"))).toHaveLength(1);
        // obs.json: the background check reports it (a plain failure, never the gate's known state)
        const settings = createNotifySettings({ read: () => JSON.stringify({ ...NOTIFY_DEFAULT, on: true,
          rules: [{ id: "r1", station: "A", stationName: "A", quantity: "discharge", direction: "above",
                    threshold: { kind: "value", value: 1 }, on: true, water: null, createdAt: 0 }] }), write: () => {} });
        const r = await runBackgroundCheck({ settings, obs: () => fetchObs("http://x/feeds/live", serve(files), undefined, undefined, gate),
          manifest: async () => { throw new Error("unused"); }, notify: async () => {}, now: () => Date.parse("2026-11-20T12:00:00Z") });
        expect(r.failed).toHaveLength(1);
        expect(reported.filter((m) => m.includes("obs.json"))).toHaveLength(1);
        // and a file in another format WITHOUT a stamp: reported too (every gauge file is stamped; one missing it is a defect)
        reported.length = 0;
        files["A.json"] = { ...brokenStation, station: "A", format: "gauge-feed/2", minAppVersion: undefined };
        f.refresh!();
        await f.lastReport(A);
        expect(reported.filter((m) => m.includes("A.json"))).toHaveLength(1);
      }
    } finally { undo(); }
  });

  it("(review M2) a bad kept refusal never crashes the start: not an object → none; an entry without a semver need → dropped", async () => {
    const URL_ = "https://o.test/feeds/dfo/index.json";
    const o = origin({ [URL_]: DFO });
    const start = (refused: unknown) => {
      const store = mem(JSON.stringify({ v: 1, seen: {}, at: {}, refused }));
      const gate = createFeedGate(APP);
      const salmon = httpSalmonFeed("https://o.test/feeds/dfo/", mem(), o.fetch, { gate });
      return createUpdateService({ fetch: o.fetch, appVersion: APP, feedGate: gate, store,
                                   feeds: [{ id: "dfo", url: URL_, apply: (text) => salmon.offer!(text) }] });
    };
    for (const bad of ["ab", 7, ["x"], null, { dfo: { need: 5 } }, { dfo: "0.9.0" }, { dfo: { need: "soon", etag: null, sha256: null } },
                       { dfo: { need: "0.9.0", etag: 3, sha256: null } }, { dfo: { need: "0.9.0" } }]) {
      const s = start(bad);
      expect([s.snapshot().needsApp ?? null, s.snapshot().feeds[0]!.needsApp ?? null]).toEqual([null, null]);
      await s.check();                                     // the index is read whole, as with no refusal kept
      expect(s.snapshot().feeds[0]).toMatchObject({ error: null, needsApp: null });
    }
    // a well-formed one is kept
    const good = start({ dfo: { need: "0.9.0", etag: null, sha256: null } });
    expect(good.snapshot().needsApp).toBe("0.9.0");
  });

  it("run timing: the capture answers", () => quiet(async () => {
    const runs = enc.encode(JSON.stringify({ format: "runtiming-runs/1", slots: ["Steelhead"], default: 0,
      relations: ["spawning", "passage", "observed"], relation_words: {}, sources: {}, runs: [
        { id: "psf:1", src: "psf", sp: "Steelhead", slot: 0, name: "X", months: 1, pentads: "1".padStart(19, "0") }] }));
    const paint = enc.encode(JSON.stringify({ format: "runtiming-paint/1", section_handles: "h", tile_version: null,
      runs_sha256: sha256Hex(runs), profiles: [{ r: [0], s: 1, m: [[0, 1, 0]] }], sid: [7], p: [0], km: [0] }));
    const index = { format: "runtiming-index/1", minAppVersion: TODAY, built_at: "t", section_handles: "h",
      tile_version: null, sources: {}, files: { runs: { url: "runs-a.json.gz", bytes: runs.length, sha256: sha256Hex(runs) },
                                                paint: { url: "paint-b.json.gz", bytes: paint.length, sha256: sha256Hex(paint) } } };
    const s = { text: null as string | null, async read() { return s.text; }, async write(t: string) { s.text = t; } };
    const gate = createFeedGate(APP);
    const files: Record<string, unknown> = { "index.json": JSON.stringify(index), "runs-a.json.gz": runs, "paint-b.json.gz": paint };
    expect((await httpRunTiming("https://o/runtiming", s, serve(files), { gate, ttlMs: 0 }).latest())?.from).toBe("live");
    files["index.json"] = JSON.stringify({ ...index, format: "runtiming-index/2", minAppVersion: "0.9.0" });
    const got = await httpRunTiming("https://o/runtiming", s, serve(files), { gate, ttlMs: 0 }).latest();
    expect([got?.from, got?.facts.state(7, [0], 0), gate.needs()]).toEqual(["capture", "spawn", "0.9.0"]);
  }));
});
