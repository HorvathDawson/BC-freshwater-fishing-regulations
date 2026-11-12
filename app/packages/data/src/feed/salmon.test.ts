/** (T211 part 2) The DFO feed reader: the fetched feed captured, the last capture offline, nothing without one. */
import { describe, expect, it } from "vitest";
import { httpSalmonFeed, withLiveSalmon } from "./salmon";
import { SALMON_FEED_FORMAT, salmonFeedLine, salmonFeedOf, setFailureSink, type SalmonFacts } from "@app/core";

/** A whole dfo-feed/3, one location (cut from core's salmonFeed.golden.ts). */
const FEED = {"format": "dfo-feed/3", "at": "2026-10-27T18:00:00Z", "verdicts": [{"s": "closed", "t": "Not opened by DFO: closed to salmon fishing", "d": 1}, {"s": "open", "t": "4 per day (Apr 1\u2013Mar 31)"}, {"s": "closed", "t": "No fishing for coho (Apr 1\u2013Mar 31)"}, {"s": "open", "t": "4 per day, only 2 over 50 cm (Aug 1\u2013Sep 15)"}], "gears": [["No natural bait"]], "locs": {"6:resort": {"r": "6", "sp": [[0, -1], [0, 2], [0, -1], [0, -1], [0, -1]], "g": [0, -1]}}, "unplaced": 0, "regions": {"6": {"name": "Skeena", "url": "https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/fresh-douce/region6-eng.html", "asof": "2026-10-20", "sha256": "x", "changed": "2026-10-27T18:00:00Z", "checked": "2026-10-27T18:00:00Z", "stale": false, "held": null, "failures": 0}}};
const RELEASE = { chains: [{ r: [], w: "", l: [], sp: [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0]], g: [0, -1], m: [0, 3] }],
                  regions: {}, unmapped: [] };
const mem = (t: string | null = null) => ({ t, read() { return this.t; }, write(x: string) { this.t = x; } });
const ok = (body: unknown, seen: string[] = []) =>
  (async (u: string) => { seen.push(u); return new Response(JSON.stringify(body)); }) as unknown as typeof fetch;
const offline = (async () => { throw new TypeError("offline"); }) as unknown as typeof fetch;

describe("httpSalmonFeed", () => {
  it("fetches the index, keeps it, and asks again only after the ttl", async () => {
    const seen: string[] = [], store = mem();
    let t = 0;
    const f = httpSalmonFeed("http://x/feeds/dfo/", store, ok(FEED, seen), { ttlMs: 10, now: () => t });
    expect((await f.latest())?.at).toBe(FEED.at);
    await f.latest();
    t = 11;
    await f.latest();
    expect(seen).toEqual(["http://x/feeds/dfo/index.json", "http://x/feeds/dfo/index.json"]);
    expect(JSON.parse(store.t!).at).toBe(FEED.at);
  });

  it("offline: the last capture answers; never captured: null", async () => {
    expect((await httpSalmonFeed("http://x", mem(JSON.stringify(FEED)), offline).latest())?.at).toBe(FEED.at);
    expect(await httpSalmonFeed("http://x", mem(), offline).latest()).toBeNull();
    expect(await httpSalmonFeed("http://x", undefined, offline).latest()).toBeNull();
  });

  it("a broken or older feed is never kept over a good capture", async () => {
    const store = mem(JSON.stringify(FEED));
    expect((await httpSalmonFeed("http://x", store, ok({ format: "nope" })).latest())?.at).toBe(FEED.at);
    const older = { ...FEED, at: "2026-01-01T00:00:00Z" };
    expect((await httpSalmonFeed("http://x", store, ok(older)).latest())?.at).toBe(FEED.at);
    expect(JSON.parse(store.t!).at).toBe(FEED.at);
  });

  it("(T556) a feed in a format this app does not read is refused AND reported; the capture answers", async () => {
    const got: unknown[] = [];
    const undo = setFailureSink((e) => got.push(e));
    try {
      const store = mem(JSON.stringify(FEED));
      const bumped = { ...FEED, format: "dfo-feed/4", at: "2026-12-01T00:00:00Z" };
      expect((await httpSalmonFeed("http://x", store, ok(bumped)).latest())?.at).toBe(FEED.at);
      expect(got.map((e) => (e as Error).message)).toEqual([`dfo index.json: not a readable ${SALMON_FEED_FORMAT} feed — refused`]);
      expect(SALMON_FEED_FORMAT).toBe(FEED.format);
      expect(JSON.parse(store.t!).at).toBe(FEED.at);
      // a good feed says nothing
      got.length = 0;
      await httpSalmonFeed("http://x", mem(), ok(FEED)).latest();
      expect(got).toEqual([]);
    } finally { undo(); }
  });
});

describe("withLiveSalmon", () => {
  const rel = (): SalmonFacts => ({ handles: "x", chains: 1, payload: RELEASE as unknown as SalmonFacts["payload"], chainOf: () => 0, loaded: [] });
  it("lays the feed over the loader's salmon once; every other read is the loader's", async () => {
    let asked = 0;
    const loader = { salmon: async () => { asked++; return rel(); }, other: () => "own" };
    const live = withLiveSalmon(loader, httpSalmonFeed("http://x", mem(JSON.stringify(FEED)), offline));
    const [a, b] = [await live.salmon(), await live.salmon()];
    expect([a, asked, live.other(), salmonFeedOf(a)?.at]).toEqual([b, 1, "own", FEED.at]);
    expect(withLiveSalmon(loader)).toBe(loader);
  });
});

// (T521) DFO's places no curated location knows: read beside the index in the same fetch, kept in the same capture;
// a failed read reported and said on the feed line, never an empty list.
describe("httpSalmonFeed: the unplaced list (T521)", () => {
  const ROW = { region: "6", items: ["gnis:9999"], water: "Babine River", scope: "below the counting fence",
                rows: ["All | Aug 1 until further notice | No fishing for salmon"], kind: "new", fingerprint: "ab" };
  const routes = (by: Record<string, () => Response>, seen: string[] = []) =>
    (async (u: string) => { seen.push(u); const k = Object.keys(by).find((x) => u.endsWith(x)); if (!k) throw new Error(`no route ${u}`); return by[k]!(); }) as unknown as typeof fetch;

  it("one unplaced: unplaced.json read beside the index and kept in the capture (offline answers with it)", async () => {
    const seen: string[] = [], store = mem();
    const fetchImpl = routes({ "/index.json": () => new Response(JSON.stringify({ ...FEED, unplaced: 1 })),
                               "/unplaced.json": () => new Response(JSON.stringify([ROW])) }, seen);
    const f = await httpSalmonFeed("http://x/feeds/dfo", store, fetchImpl).latest();
    expect(seen).toEqual(["http://x/feeds/dfo/index.json", "http://x/feeds/dfo/unplaced.json"]);
    expect(f?.unplacedRows).toEqual([{ fingerprint: "ab", region: "6", items: ["gnis:9999"], water: "Babine River", scope: "below the counting fence",
                                       rows: ["All | Aug 1 until further notice | No fishing for salmon"] }]);
    expect((await httpSalmonFeed("http://x", store, offline).latest())?.unplacedRows).toEqual(f?.unplacedRows);
  });

  it("zero unplaced: unplaced.json is not asked for", async () => {
    const seen: string[] = [];
    const f = await httpSalmonFeed("http://x", mem(), ok(FEED, seen)).latest();
    expect([seen, f?.unplacedRows]).toEqual([["http://x/index.json"], []]);
  });

  it("a failed read is reported, and the card's feed line says it", async () => {
    const got: unknown[] = [];
    const undo = setFailureSink((e) => got.push(e));
    try {
      const fetchImpl = routes({ "/index.json": () => new Response(JSON.stringify({ ...FEED, unplaced: 1 })),
                                 "/unplaced.json": () => new Response("down", { status: 503 }) });
      const live = withLiveSalmon({ salmon: async () => ({ handles: "x", chains: 1, payload: RELEASE as unknown as SalmonFacts["payload"], chainOf: () => 0, loaded: [] }) },
                                  httpSalmonFeed("http://x", mem(), fetchImpl));
      const s = await live.salmon();
      expect(got.map((e) => (e as Error).message)).toEqual(["dfo unplaced.json: HTTP 503"]);
      expect(salmonFeedLine(s)).toMatch(/^Checked with DFO Oct 27, 2026, \d{1,2}:\d\d [ap]m PT \(1 DFO place not yet on the map could not be read\)$/);
    } finally { undo(); }
  });

  it("offer: a feed the update service took reads its unplaced list too", async () => {
    const seen: string[] = [];
    const src = httpSalmonFeed("http://x", mem(), routes({ "/unplaced.json": () => new Response(JSON.stringify([ROW])) }, seen));
    src.offer!(JSON.stringify({ ...FEED, unplaced: 1 }));
    expect((await src.latest())?.unplacedRows?.length).toBe(1);
    expect(seen).toEqual(["http://x/unplaced.json"]);
  });
});
