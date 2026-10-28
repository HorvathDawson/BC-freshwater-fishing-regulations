/**
 * (T211 part 2) THE LIVE DFO FEED over the release's salmon: each release chain re-composed from the feed's per-location
 * answers exactly as the pipeline composes it (`salmonFeed.golden.ts`, written and checked by
 * `pipeline/tests/test_dfo_live.py`), the captured feed preferred, the release's own file only without one.
 */
import { describe, expect, it } from "vitest";
import { SALMON_FEED_GOLDEN } from "./salmonFeed.golden";
import { salmonCard, salmonChainStatus, type SalmonFacts } from "./salmon";
import { composeChain, parseSalmonFeed, pickSalmon, salmonFeedLine, salmonFeedOf, withSalmonFeed } from "./salmonFeed";

// eslint-disable-next-line @typescript-eslint/no-explicit-any -- the golden is the pipeline's JSON, read as data
const G = SALMON_FEED_GOLDEN as any;
const release = (): SalmonFacts => {
  const by = new Map<number, number>(G.sections.map(([s, c]: [number, number]) => [s, c]));
  return { handles: "147b20dce7d8576c", chains: G.release.chains.length, payload: G.release,
           chainOf: (sid: number) => by.get(sid) ?? 0 };
};
const feed = () => parseSalmonFeed(G.feed)!;

describe("the DFO feed over the release", () => {
  it("re-composes every release chain as the pipeline does (narrowest location first, not listed = closed)", () => {
    const f = feed();
    expect(f).not.toBeNull();
    G.release.chains.forEach((c: { l: string[]; u?: string[] }, k: number) => expect(composeChain(c.l, f, c.u ?? [])).toEqual(G.expect[k]));
    const live = withSalmonFeed(release(), f);
    expect(live.payload.chains.map((c) => ({ sp: c.sp, g: c.g, m: c.m }))).toEqual(G.expect);
    // the geometry is the release's: same chains, same sections
    expect([live.chainOf(12), live.chains, live.payload.chains[2]!.l]).toEqual([release().chainOf(12),
      release().chains, G.release.chains[2].l]);
  });

  it("the card reads the feed: the Kispiox's coho moved to Aug 1–Sep 15, and the page date and check time show", () => {
    const live = withSalmonFeed(release(), feed());
    const was = salmonCard(release(), { sids: [12], item: null }, new Date(2026, 8, 10));
    const now = salmonCard(live, { sids: [12], item: null }, new Date(2026, 8, 10));
    const coho = (c: typeof was) => c.rows.find((r) => r.label.includes("Coho"))!.status;
    expect([coho(was), coho(now)]).toEqual(["closed", "open"]);
    expect(now.asOf).toBe("DFO page dated Oct 20, 2026");
    expect(salmonFeedLine(live)).toMatch(/^Checked with DFO Oct 27, 2026, \d{1,2}:\d\d [ap]m$/);
    expect([salmonFeedLine(release()), salmonFeedOf(release())]).toEqual([null, null]);
  });

  it("PARTIAL COVERAGE: a water with a DFO place not drawn yet says check DFO on every section, never a confident answer", () => {
    const by = new Map<number, number>(G.sections.map(([s, c]: [number, number]) => [s, c]));
    const live = withSalmonFeed(release(), feed());
    for (const f of [release(), live]) {
      for (const sid of [11, 14]) expect(salmonChainStatus(f, by.get(sid)!, 250)).toBe("check");
      const card = salmonCard(f, { sids: [11], item: "gnis:4674" }, new Date(2026, 8, 10));
      expect([card.headlineStatus, card.partial?.text, card.partial?.url]).toEqual(
        ["check", "Salmon rules on parts of this water aren't mapped yet — check DFO", G.release.regions["6"].url]);
      expect(card.rows.every((r) => r.status === "check") && !card.allClosed).toBe(true);
      // a section off that water keeps its own answer
      expect(salmonCard(f, { sids: [10], item: null }, new Date(2026, 8, 10)).partial).toBeNull();
    }
  });

  it("uses the captured feed, else the release's own file; a release cut from newer pages wins", () => {
    const r = release();
    expect(pickSalmon(r, null)).toBe(r);
    expect(salmonFeedOf(pickSalmon(r, feed()))?.at).toBe(G.feed.at);
    const old = { ...G.feed, at: "2026-08-01T00:00:00Z" };
    expect(pickSalmon(r, parseSalmonFeed(old))).toBe(r);
  });

  it("a place not drawn yet shows while DFO lists it: dormant at build, off the card until the feed lists it", () => {
    const r = release();
    const dormant = { ...r, payload: { ...r.payload, unmapped: r.payload.unmapped.map((u) => ({ ...u, d: 1 })) } };
    expect(pickSalmon(dormant, null).payload.unmapped).toEqual([]);
    expect(pickSalmon(dormant, feed()).payload.unmapped.length).toBe(1);          // the feed lists 6:resort today
    const { "6:resort": _gone, ...rest } = G.feed.locs;
    expect(pickSalmon(r, parseSalmonFeed({ ...G.feed, locs: rest })).payload.unmapped).toEqual([]);
  });

  it("refuses what is not a whole dfo-feed/1", () => {
    expect([parseSalmonFeed(null), parseSalmonFeed({ ...G.feed, format: "x" }),
            parseSalmonFeed({ ...G.feed, locs: { a: { r: "6", sp: [[0, 99]], g: [] } } }),
            parseSalmonFeed({ ...G.feed, locs: { a: { r: "6", sp: [[0, 0], [0, 0], [0, 0], [0, 0], [0, 999]], g: [0, -1] } } })])
      .toEqual([null, null, null, null]);
  });
});
