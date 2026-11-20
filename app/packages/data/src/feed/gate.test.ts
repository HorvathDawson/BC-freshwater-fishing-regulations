/**
 * (T676 item 2, the acceptance) A FEED FORMAT BUMP DOES NOT STRAND THE PREVIOUS APP: the bumped index names a newer
 * `minAppVersion`; the previous app (which carries the gate) keeps showing its last capture AND raises the update prompt
 * — no "no feed", no failure report, the feed not marked seen. Every reader given the gate does the same on its own
 * fetch (DFO, in-season, gauges, run timing).
 */
import { describe, expect, it } from "vitest";
import { appTooOld, appUpdateVM, APP_UPDATE_WORDS, createFeedGate, GAUGE_FEED_FORMAT, INSEASON_FEED_FORMAT, INSEASON_LINKS_FORMAT,
         resolveAppVersion, SALMON_FEED_FORMAT, setFailureSink, type UpdateEvent } from "@app/core";
import { sha256Hex } from "../sha256";
import { createUpdateService } from "../update/service";
import { httpFeed } from "./http";
import { httpInSeason } from "./inseason";
import { httpRunTiming } from "./runtiming";
import { httpSalmonFeed } from "./salmon";

const APP = resolveAppVersion({ version: "0.8.4" });
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
  const f = (async (u: RequestInfo | URL, i?: RequestInit) => {
    const body = files[String(u)];
    if (body === undefined) return new Response("", { status: 404 });
    const text = typeof body === "string" ? body : JSON.stringify(body);
    const etag = `"${sha256Hex(enc.encode(text)).slice(0, 12)}"`;
    if (new Headers(i?.headers).get("If-None-Match") === etag) return new Response(null, { status: 304, headers: { ETag: etag } });
    return new Response(text, { status: 200, headers: { ETag: etag } });
  }) as typeof fetch;
  return { files, fetch: f };
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

      // not "seen": the next check reads it again (and refuses it again)
      await service.check();
      expect(events.at(-1)).toMatchObject({ source: "dfo", kind: "app-too-old" });

      // the origin goes back to a feed this app reads: the need clears
      o.files[URL_] = { ...DFO, at: "2026-12-02T18:00:00Z" };
      await service.check();
      expect([service.snapshot().needsApp, appTooOld(service.snapshot()), (await salmon.latest())?.at])
        .toEqual([null, false, "2026-12-02T18:00:00Z"]);
    } finally { undo(); }
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

  it("gauges: the session's copy answers, the service's offer is refused", () => quiet(async () => {
    const IDX = { format: GAUGE_FEED_FORMAT, minAppVersion: TODAY, fetchedAt: "2026-11-20T12:00:00Z", stations: { A: {} } };
    const gate = createFeedGate(APP);
    const files: Record<string, unknown> = { "index.json": IDX };
    const f = httpFeed("http://x/feeds/live", serve(files), { gate });
    expect(await f.live()).toEqual(new Set(["A"]));
    files["index.json"] = { ...IDX, format: "gauge-feed/2", minAppVersion: "0.9.0", stations: { B: {} } };
    f.refresh!();
    expect(await f.live()).toEqual(new Set(["A"]));
    expect(gate.needs()).toBe("0.9.0");
    expect(() => f.offer!(files["index.json"])).toThrow(/needs app 0.9.0/);
  }));

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
