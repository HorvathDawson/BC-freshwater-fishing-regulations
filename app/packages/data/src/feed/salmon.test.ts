/** (T211 part 2) The DFO feed reader: the fetched feed captured, the last capture offline, nothing without one. */
import { describe, expect, it } from "vitest";
import { httpSalmonFeed, withLiveSalmon } from "./salmon";
import { salmonFeedOf, type SalmonFacts } from "@app/core";

/** A whole dfo-feed/1, one location (cut from core's salmonFeed.golden.ts). */
const FEED = {"format": "dfo-feed/1", "at": "2026-10-27T18:00:00Z", "verdicts": [{"s": "closed", "t": "Not opened by DFO: closed to salmon fishing", "d": 1}, {"s": "open", "t": "4 per day (Apr 1\u2013Mar 31)"}, {"s": "closed", "t": "No fishing for coho (Apr 1\u2013Mar 31)"}, {"s": "open", "t": "4 per day, only 2 over 50 cm (Aug 1\u2013Sep 15)"}], "gears": [["No natural bait"]], "locs": {"6:resort": {"r": "6", "sp": [[0, -1], [0, 2], [0, -1], [0, -1], [0, -1]], "g": [0, -1]}}, "unplaced": 0, "regions": {"6": {"name": "Skeena", "url": "https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/fresh-douce/region6-eng.html", "asof": "2026-10-20", "sha256": "x", "changed": "2026-10-27T18:00:00Z", "checked": "2026-10-27T18:00:00Z", "stale": false, "held": null, "failures": 0}}};
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
});

describe("withLiveSalmon", () => {
  const rel = (): SalmonFacts => ({ handles: "x", chains: 1, payload: RELEASE as unknown as SalmonFacts["payload"], chainOf: () => 0 });
  it("lays the feed over the loader's salmon once; every other read is the loader's", async () => {
    let asked = 0;
    const loader = { salmon: async () => { asked++; return rel(); }, other: () => "own" };
    const live = withLiveSalmon(loader, httpSalmonFeed("http://x", mem(JSON.stringify(FEED)), offline));
    const [a, b] = [await live.salmon(), await live.salmon()];
    expect([a, asked, live.other(), salmonFeedOf(a)?.at]).toEqual([b, 1, "own", FEED.at]);
    expect(withLiveSalmon(loader)).toBe(loader);
  });
});
