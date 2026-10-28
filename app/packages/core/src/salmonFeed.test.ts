/**
 * (T211 part 2) THE LIVE DFO FEED over the release's salmon: each release chain re-composed from the feed's per-location
 * answers exactly as the pipeline composes it (`salmonFeed.golden.ts`, written and checked by
 * `pipeline/tests/test_dfo_live.py`), the captured feed preferred, the release's own file only without one.
 */
import { describe, expect, it } from "vitest";
import { SALMON_FEED_GOLDEN } from "./salmonFeed.golden";
import { salmonCard, salmonChainStatus, salmonNotes, type SalmonFacts } from "./salmon";
import { composeChain, parseSalmonFeed, pickSalmon, salmonFeedLine, salmonFeedOf, withSalmonFeed } from "./salmonFeed";

// eslint-disable-next-line @typescript-eslint/no-explicit-any -- the golden is the pipeline's JSON, read as data
const G = SALMON_FEED_GOLDEN as any;
const release = (): SalmonFacts => {
  const by = new Map<number, number>(G.sections.map(([s, c]: [number, number]) => [s, c]));
  return { handles: "147b20dce7d8576c", chains: G.release.chains.length, payload: G.release,
           chainOf: (sid: number) => by.get(sid) ?? 0, loaded: [] };
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

  it("PARTIAL COVERAGE: a water with a DFO place not drawn yet says check DFO wherever that place answers otherwise", () => {
    const by = new Map<number, number>(G.sections.map(([s, c]: [number, number]) => [s, c]));
    const live = withSalmonFeed(release(), feed());
    for (const f of [release(), live]) {
      for (const sid of [11, 14]) expect(salmonChainStatus(f, by.get(sid)!, 250)).toBe("check");
      const card = salmonCard(f, { sids: [11], item: "gnis:4674" }, new Date(2026, 8, 10));
      expect([card.headlineStatus, card.partial?.text, card.partial?.url]).toEqual(
        ["check", "Salmon rules on parts of this water aren't mapped yet — check DFO", G.release.regions["6"].url]);
      // (T227) a row keeps its answer only where the undrawn place says the same for its species that day: the place
      // closes every salmon, so the closed coho / sockeye · pink · chum stand; the open chinook reads check DFO
      expect([card.rows.map((r) => [r.label, r.status]), card.allClosed]).toEqual(
        [[["Chinook", "check"], ["Coho", "closed"], ["Sockeye · pink · chum", "closed"]], false]);
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

  it("THE REGION HEADER: the notes a page shows are the pipeline's (`notes_for`), the feed's words over the release's", () => {
    const live = withSalmonFeed(release(), feed());
    for (const c of G.notes as { regions: string[]; item: string | null; day: number; expect: { region: string[]; water: string[] } }[]) {
      const got = salmonNotes(live.payload, c.regions, c.item, c.day);
      expect([c, { region: got.region.map((n) => n.id), water: got.water.map((n) => n.id) }]).toEqual([c, c.expect]);
    }
    // the feed's daily limit (2) is the card's, not the release's (4)
    const daily = (f: SalmonFacts) => salmonNotes(f.payload, ["6"], null, 0).region.find((n) => n.k === "daily")!.n;
    expect([daily(release()), daily(live)]).toEqual([4, 2]);
  });

  it("the card: region rules in one block, a water's own note on its open row, sizes, and the provincial gear line", () => {
    const live = withSalmonFeed(release(), feed());
    // Oct 1 on the Kispiox (gnis:4674): the 62 cm adult chinook window is open; a bait ban marks part of it
    const card = salmonCard(live, { sids: [12], item: "gnis:4674", gear: ["bait", "none"] }, new Date(2026, 9, 1));
    expect(card.regionRules.map((g) => g.label)).toEqual(["Region 6 Skeena"]);
    const rules = card.regionRules[0]!.notes;
    expect(rules.some((t) => t.startsWith("You can only fish for salmon in Region 6 during daylight"))).toBe(true);
    expect(rules.some((t) => t.startsWith("Annual bait ban"))).toBe(false);          // the Kispiox is excepted by name
    expect(card.waterRules.some((t) => t.includes("over 62 cm: the Kispiox River"))).toBe(true);
    const open = card.rows.filter((r) => r.status === "open");
    for (const r of open) if (r.species.includes("CH")) expect(r.sizes).toContain("adult chinook over 62 cm");
    expect(card.gearProvincial).toBe("B.C. rules, also for salmon (on parts of this water): Bait ban: no bait of any kind.");
    // Jul 1: the window is shut — the region's 50 cm holds
    const jul = salmonCard(live, { sids: [12], item: "gnis:4674" }, new Date(2026, 6, 1));
    expect(jul.waterRules.some((t) => t.includes("62 cm"))).toBe(false);
    expect(jul.gearProvincial).toBeNull();
  });
});
