/**
 * (T668 + T549, the app half) DFO salmon places from two regions, and the places the release does not show.
 *
 *  - PARITY: a two-region chain (`s`) re-composed from the live feed exactly as `live.compose` composes it — the merged
 *    verdict and both regions' gear lines added to the tables in the same order — and `a` (the answering place) as
 *    `salmon_who.answering_places`. Pinned by `salmonStraddle.golden.ts`, cut by the pipeline
 *    (`python -m pipeline.tests.test_t668_salmon_who --write`; `--check` and its pytest fail when stale).
 *  - Fix 1: the card's "not yet mapped" caveat lists only the undrawn places the page's chains name (`u`).
 *  - Fix 4: each row names where its answer comes from ("Region 6 · Skeena River · B(ii)"); a release without `a`/`p`
 *    (before fix 4, e.g. r-303b4007a70a) names nothing — the card as it was.
 *  - T549: an `off` place on the water is a text row; feed places the release neither shows nor states, and held rows,
 *    are said; a missing chain has one answer (SalmonError).
 */
import { describe, expect, it } from "vitest";
import { SALMON_STRADDLE_GOLDEN } from "./salmonStraddle.golden";
import { SALMON_FEED_GOLDEN } from "./salmonFeed.golden";
import { salmonCard, salmonChainStatus, salmonFacts, salmonStatusFromSpecies, salmonTable, SalmonError, type SalmonFacts, type SalmonShard } from "./salmon";
import { composeChain, parseSalmonFeed, withSalmonFeed } from "./salmonFeed";

// eslint-disable-next-line @typescript-eslint/no-explicit-any -- the goldens are the pipeline's JSON, read as data
const G = SALMON_STRADDLE_GOLDEN as any;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const K = SALMON_FEED_GOLDEN as any;
const facts = (payload: SalmonFacts["payload"], sections: [number, number][]): SalmonFacts => {
  const by = new Map<number, number>(sections);
  return { handles: "x", chains: payload.chains.length, payload, chainOf: (sid) => by.get(sid) ?? 0, loaded: [] };
};
const release = () => facts(G.release, G.sections);
const feed = () => parseSalmonFeed(G.feed)!;
const MAR = new Date(2026, 2, 1), JUL = new Date(2026, 6, 19);
const rowsOf = (f: SalmonFacts, sid: number, on: Date, item: string | null = null) =>
  salmonCard(f, { sids: [sid], item }, on).rows.map((r) => [r.label, r.status, r.from ?? null]);

describe("(T668) a two-region chain under the live feed: `live.compose`, pinned", () => {
  it("every chain re-composed as the pipeline does, the merged verdict and gear list added in its order", () => {
    const live = withSalmonFeed(release(), feed());
    expect(live.payload.chains.map((c) => ({ sp: c.sp, g: c.g, m: c.m, ...(c.c ? { c: c.c } : {}), ...(c.n ? { n: c.n } : {}),
                                             ...(c.a ? { a: c.a } : {}) }))).toEqual(G.expect);
    expect([live.payload.verdicts, live.payload.gears]).toEqual([G.verdicts, G.gears]);
    // the feed's own tables are never written
    expect([feed().verdicts.length, feed().gears.length]).toEqual([G.feed.verdicts.length, G.feed.gears.length]);
  });

  it("a merge with no tables to add it to stops, never a guess", () => {
    const c = G.release.chains[2];
    expect(() => composeChain(c.l, feed(), [], false, c.s)).toThrow("two regions' openings need a merged verdict");
  });
});

describe("(T668 fix 4) each row names where its answer comes from", () => {
  it("the straddler, the release: chinook closed by Region 7, the rest closed to Region 6's section E", () => {
    expect(rowsOf(release(), 67, MAR)).toEqual([["Salmon", "closed", "Region 7; Region 6 · E"]]);
  });

  it("one cascade naming two regions: the Sustut (Region 6, section B(i)) opens chinook, Region 7 summer coho", () => {
    expect(rowsOf(release(), 70, JUL)).toEqual([
      ["Chinook", "open", "Region 6 · Sustut River · B(i)"], ["Coho", "open", "Region 7"], ["Sockeye · pink · chum", "closed", null]]);
  });

  it("under the feed: both regions' words together in March, the first region on an equal opening in July", () => {
    const live = withSalmonFeed(release(), feed());
    expect(rowsOf(live, 67, MAR)).toEqual([["Chinook", "open", "Region 6 and Region 7 together"],
                                           ["Coho · sockeye · pink · chum", "closed", "Region 6 · E"]]);
    expect(rowsOf(live, 67, JUL)[0]).toEqual(["Chinook", "open", "Region 6 · E"]);
    // both regions' gear lines on the straddler
    expect(salmonCard(live, { sids: [67], item: null }, JUL).gear).toEqual(["Barbless hooks only (Jan 1–Dec 31)", "No bait (Jan 1–Dec 31)"]);
  });

  it("THE SKEENA's lettered sections, as DFO prints them (owner 2026-11-11): A, B(i), B(ii)", () => {
    // the curated places' own region / water / section (data/curated/dfo_salmon/places/region-6.json)
    const P = G.release;
    const skeena = { ...P, p: { "6:e": { r: "6", sec: "A" }, "6:sustut": { r: "6", water: "Skeena River", sec: "B(ii)" },
                                "7:default": { r: "6", sec: "B(i)" } } };
    const f = facts(skeena, G.sections);
    expect(rowsOf(f, 70, JUL).map((r) => r[2])).toEqual(["Region 6 · Skeena River · B(ii)", "Region 6 · B(i)", null]);
    expect(rowsOf(f, 60, JUL)[0]).toEqual(["Chinook", "open", "Region 6 · A"]);
  });

  it("a release before fix 4 (no `a`, no `p`: r-303b4007a70a) names nothing; the rest of the card is as it was", () => {
    const { p: _p, ...old } = G.release;
    const before = facts({ ...old, chains: G.release.chains.map(({ a: _a, ...c }: { a?: unknown }) => c) }, G.sections);
    const now = salmonCard(release(), { sids: [70], item: null }, JUL), was = salmonCard(before, { sids: [70], item: null }, JUL);
    expect(was.rows.map((r) => r.from)).toEqual([undefined, undefined, undefined]);
    expect(was.rows.map(({ from: _f, fromBy: _b, ...r }) => r)).toEqual(now.rows.map(({ from: _f, fromBy: _b, ...r }) => r));
    expect(salmonTable(was).species.map((s) => s.from)).toEqual([undefined, undefined, undefined, undefined, undefined]);
  });

  it("the table's species rows say it each on their own (the Sustut stretch, Jul 19)", () => {
    expect(salmonTable(salmonCard(release(), { sids: [70], item: null }, JUL)).species.map((s) => [s.name, s.from ?? null]))
      .toEqual([["Chinook", "Region 6 · Sustut River · B(i)"], ["Coho", "Region 7"], ["Sockeye", null], ["Pink", null], ["Chum", null]]);
  });
});

describe("(T668 fix 1) the caveat lists only the undrawn places the page's chains name", () => {
  it("a section whose chain names no undrawn place has no caveat, though its water has one", () => {
    const by = new Map<number, number>(K.sections);
    const f = facts(K.release, K.sections);
    expect(by.get(10)).toBeDefined();
    expect(K.release.chains[by.get(10)!].u).toBeUndefined();
    // gnis:4674 holds the undrawn resort place; sid 10's chain does not name it (another region's place, say)
    expect(salmonCard(f, { sids: [10], item: "gnis:4674" }, JUL).caveat).toBeNull();
    expect(salmonCard(f, { sids: [11], item: "gnis:4674" }, JUL).caveat?.items.map((x) => x.where)).toEqual(
      ["Where: Kispiox River — above the resort signs"]);
  });
});

describe("(T549) places the release does not show, feed places it does not hold, held rows", () => {
  it("an `off` place on this water is a text row; with the feed, its answer today", () => {
    const page = { sids: [70], item: "gnis:S" };
    const off = "DFO also lists Sustut River — below the falls (not on the map yet). Check DFO's page.";
    expect(salmonCard(release(), page, JUL).waterRules).toEqual([off]);
    expect(salmonCard(withSalmonFeed(release(), feed()), page, JUL).waterRules).toEqual([
      "DFO also lists Sustut River — below the falls (not on the map yet): Chinook open · Coho · sockeye · pink · chum closed. Check DFO's page."]);
    // another water's page does not show it
    expect(salmonCard(release(), { sids: [70], item: "gnis:other" }, JUL).waterRules).toEqual([]);
  });

  it("a feed place the release neither shows nor states is counted on its region's pages", () => {
    const f = { ...feed(), locs: { ...feed().locs, "7:new": feed().locs["7:default"]! } };
    expect(salmonCard(withSalmonFeed(release(), f), { sids: [70], item: null }, JUL).waterRules).toEqual([
      "DFO lists 1 place in Region 7 that this app's data does not hold yet. Check DFO's page."]);
    expect(salmonCard(withSalmonFeed(release(), f), { sids: [60], item: null }, JUL).waterRules).toEqual([]);
  });

  it("a held row (its last answers carried) is said on the pages of its place", () => {
    const f = parseSalmonFeed({ ...G.feed, regions: { ...G.feed.regions, "6": { ...G.feed.regions["6"], holds: [
      { location_id: "6:e", fingerprint: "x", why: "ValueError", kept: "carried" }] } } })!;
    const held = "DFO changed a row here that the app could not read, so its earlier answer is shown. Check DFO's page.";
    expect(salmonCard(withSalmonFeed(release(), f), { sids: [60], item: null }, JUL).waterRules).toEqual([held]);
    expect(salmonCard(withSalmonFeed(release(), f), { sids: [70], item: null }, JUL).waterRules).toEqual([]);
    expect(parseSalmonFeed({ ...G.feed, regions: { "6": { ...G.feed.regions["6"], holds: [{ location_id: "6:e" }] } } })).toBeNull();
  });

  it("a release without `off` (before T549) shows no such row", () => {
    const { off: _o, ...old } = G.release;
    expect(salmonCard(facts(old, G.sections), { sids: [70], item: "gnis:S" }, JUL).waterRules).toEqual([]);
  });
});

describe("(T549 item 11) a missing chain has one answer: SalmonError", () => {
  it("the status readers and a slice naming a chain the head lacks", () => {
    const f = release();
    expect(() => salmonStatusFromSpecies(f, 99, 0)).toThrow(SalmonError);
    expect(() => salmonChainStatus(f, 99, 0)).toThrow(SalmonError);
    const shard: SalmonShard = { region: "6", starts: Uint32Array.of(60), lens: Uint16Array.of(1), chain: Uint16Array.of(99) };
    expect(() => salmonFacts(G.release, "x", [shard])).toThrow("slice 6 run 0 names chain 99, the head holds 4");
  });
});
