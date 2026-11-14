/**
 * (T211 part 2) THE LIVE DFO FEED over the release's salmon: each release chain re-composed from the feed's per-location
 * answers exactly as the pipeline composes it (`salmonFeed.golden.ts`, written and checked by
 * `pipeline/tests/test_dfo_live.py`), the captured feed preferred, the release's own file only without one.
 */
import { describe, expect, it } from "vitest";
import { SALMON_FEED_GOLDEN } from "./salmonFeed.golden";
import { salmonCard, salmonChainStatus, salmonNotes, type SalmonFacts } from "./salmon";
import { composeChain, dfoNoticeRows, parseDfoUnplaced, parseSalmonFeed, pickSalmon, salmonFeedLine, salmonCheckedLine, salmonFeedOf, withSalmonFeed } from "./salmonFeed";

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
    G.release.chains.forEach((c: { l: string[]; u?: string[]; n?: number[] }, k: number) => expect(composeChain(c.l, f, c.u ?? [], !!c.n)).toEqual(G.expect[k]));
    const live = withSalmonFeed(release(), f);
    expect(live.payload.chains.map((c) => ({ sp: c.sp, g: c.g, m: c.m, ...(c.n ? { n: c.n } : {}) }))).toEqual(G.expect);
    // the geometry is the release's: same chains, same sections
    expect([live.chainOf(12), live.chains, live.payload.chains[2]!.l]).toEqual([release().chainOf(12),
      release().chains, G.release.chains[2].l]);
  });

  it("(T716) composed once per release head and feed: another set of slices over the same head reuses it, a new feed does not", () => {
    const f = feed();
    // two sets of slices over ONE head (a page's regions, the map's every region): two facts, one payload
    const a = release(), b = { ...release(), chainOf: () => 0 };
    const la = withSalmonFeed(a, f), lb = withSalmonFeed(b, f);
    expect([la.payload === lb.payload, la.chainOf(12), lb.chainOf(12)]).toEqual([true, a.chainOf(12), 0]);
    // the feed's time and words per answer, from that feed
    expect(salmonFeedLine(lb)).toBe(salmonFeedLine(la));
    // a feed taken later (another object): composed afresh, never the earlier answer
    const later = parseSalmonFeed({ ...G.feed, at: "2026-10-28T18:04:00Z" })!;
    const lc = withSalmonFeed(a, later);
    expect([lc.payload === la.payload, salmonFeedLine(lc) === salmonFeedLine(la)]).toEqual([false, false]);
    // and the same answer as composing it alone
    expect(lc.payload).toEqual(withSalmonFeed(release(), parseSalmonFeed({ ...G.feed, at: "2026-10-28T18:04:00Z" })!).payload);
  });

  it("the card reads the feed: the Kispiox's coho moved to Aug 1–Sep 15, and the page date and check time show", () => {
    const live = withSalmonFeed(release(), feed());
    const was = salmonCard(release(), { sids: [12], item: null }, new Date(2026, 8, 10));
    const now = salmonCard(live, { sids: [12], item: null }, new Date(2026, 8, 10));
    const coho = (c: typeof was) => c.rows.find((r) => r.label.includes("Coho"))!.status;
    expect([coho(was), coho(now)]).toEqual(["closed", "open"]);
    expect(now.asOf).toBe("DFO page dated Oct 20, 2026");
    expect(salmonFeedLine(live)).toMatch(/^Checked with DFO Oct 27, 2026, \d{1,2}:\d\d [ap]m PT$/);
    expect([salmonFeedLine(release()), salmonFeedOf(release())]).toEqual([null, null]);
  });

  it("(T372, the trout way) PARTIAL COVERAGE: a water with a DFO place not drawn yet keeps its answer; the place is the caveat", () => {
    const by = new Map<number, number>(G.sections.map(([s, c]: [number, number]) => [s, c]));
    const live = withSalmonFeed(release(), feed());
    for (const f of [release(), live]) {
      // (T372, owner 2026-11-04: the trout way) the undrawn place no longer paints the map "check": the map keeps the
      // chain's own answer, and so does the card: its rows are the drawn answer, the place its in-part caveat
      expect([11, 14].map((sid) => salmonChainStatus(f, by.get(sid)!, 250))).toEqual(["open", "closed"]);
      const card = salmonCard(f, { sids: [11], item: "gnis:4674" }, new Date(2026, 8, 10));
      expect([card.headlineStatus, card.rows.map((r) => [r.label, r.status]), card.allClosed]).toEqual(
        ["open", [["Chinook", "open"], ["Coho", "closed"], ["Sockeye · pink · chum", "closed"]], false]);
      // DFO speaks for the place that day (its `n`, composed from the feed as the pipeline does): in force, first
      expect([card.caveat?.hot, card.caveat?.items.map((x) => x.where)]).toEqual(
        [true, ["Where: Kispiox River — above the resort signs"]]);
      // a section off that water keeps its own answer, and has no caveat
      expect(salmonCard(f, { sids: [10], item: null }, new Date(2026, 8, 10)).caveat).toBeNull();
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

  it("refuses what is not a whole dfo-feed/3 (an older capture too)", () => {
    expect([parseSalmonFeed(null), parseSalmonFeed({ ...G.feed, format: "x" }), parseSalmonFeed({ ...G.feed, format: "dfo-feed/1" }),
            parseSalmonFeed({ ...G.feed, locs: { a: { r: "6", sp: [[0, 99]], g: [] } } }),
            parseSalmonFeed({ ...G.feed, locs: { a: { r: "6", sp: [[0, 0], [0, 0], [0, 0], [0, 0], [0, 999]], g: [0, -1] } } })])
      .toEqual([null, null, null, null, null]);
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
    const rules = card.regionRules.flatMap((g) => g.notes);
    // (T482) the daylight note is the card's `hours` line, the FEED's (never left loose in the region block)
    expect(rules.some((t) => t.startsWith("You can only fish for salmon in Region 6 during daylight"))).toBe(false);
    expect(card.hours).toBe("Salmon fishing in daylight only: one hour before sunrise to one hour after sunset");
    expect(rules.some((t) => t.startsWith("Annual bait ban"))).toBe(false);          // the Kispiox is excepted by name
    // (T372) the chinook row keeps its drawn answer (open), so its sizes line says the water's 62 cm (below), never
    // repeated in the water's lines
    const open = card.rows.filter((r) => r.status === "open");
    expect(open.some((r) => r.species.includes("CH"))).toBe(true);
    expect(card.waterRules.some((t) => t.includes("over 62 cm: the Kispiox River"))).toBe(false);
    for (const r of open) if (r.species.includes("CH")) expect(r.sizes).toContain("adult chinook over 62 cm");
    expect(card.gearProvincial).toBe("B.C. rules, also for salmon (on parts of this water): Bait ban: no bait of any kind.");
    // Jul 1: the window is shut — the region's 50 cm holds
    const jul = salmonCard(live, { sids: [12], item: "gnis:4674" }, new Date(2026, 6, 1));
    expect(jul.waterRules.some((t) => t.includes("62 cm"))).toBe(false);
    expect(jul.gearProvincial).toBeNull();
  });
});

describe("(T350) the bucket goes with the notes it is read from", () => {
  const BUCKET = { tree: { node: "tier", id: "t0", tier: "total", n: 4, says: "Up to 4 salmon a day in total", by: 0, children: [] }, t: "4 a day" };
  const withRegion = (fr: object) => {
    const f = feed();
    return { ...f, regions: { ...f.regions, "6": { ...f.regions["6"]!, ...fr } } } as never;
  };
  const rel = (): SalmonFacts => {
    const r = release();
    return { ...r, payload: { ...r.payload, regions: { ...r.payload.regions, "6": { ...r.payload.regions["6"]!, bucket: BUCKET as never } } } };
  };
  it("a feed region with notes and no bucket has none (never the release's)", () => {
    const f = withRegion({ notes: [], bucket: undefined });
    expect(withSalmonFeed(rel(), f).payload.regions["6"]!.bucket).toBeUndefined();
  });
  it("a feed region without notes keeps the release's notes and bucket together", () => {
    const f = withRegion({ notes: undefined, bucket: undefined });
    const r6 = withSalmonFeed(rel(), f).payload.regions["6"]!;
    expect([r6.bucket, r6.notes]).toEqual([BUCKET, rel().payload.regions["6"]!.notes]);
  });
});

describe("the line by the mode switch (T276)", () => {
  it("says when the feed was fetched, in the feed's own time; nothing for the release's own file", () => {
    const f = feed();
    expect(salmonCheckedLine(withSalmonFeed(release(), { ...f, at: "2026-10-29T09:11:27Z" }))).toBe("DFO rules checked Oct 29");
    expect(salmonCheckedLine(release())).toBeNull();
    expect(salmonCheckedLine(null)).toBeNull();
  });

  it("T551: the legend's date and the card's line are ONE B.C. stamp, whatever zone the device is in", () => {
    const f = feed();
    // 00:15 UTC Aug 15 is 17:15 PT Aug 14: the legend used the UTC date (Aug 15) beside a card saying Aug 14
    const at = "2026-08-15T00:15:00Z";
    const withAt = withSalmonFeed(release(), { ...f, at });
    expect(salmonCheckedLine(withAt)).toBe("DFO rules checked Aug 14");
    expect(salmonFeedLine(withAt)).toBe("Checked with DFO Aug 14, 2026, 5:15 pm PT");
    // and the two agree on a fetch just past B.C. midnight (07:05 UTC = 00:05 PT)
    const late = withSalmonFeed(release(), { ...f, at: "2026-11-10T07:05:00Z" });
    expect([salmonCheckedLine(late), salmonFeedLine(late)]).toEqual(["DFO rules checked Nov 10", "Checked with DFO Nov 10, 2026, 12:05 am PT"]);
  });
});

describe("(T321) the feed's rows in DFO's words", () => {
  it("a location the feed carries answers with the feed's rows; the others keep the release's", () => {
    const r = release();
    const f = feed();
    const [carried, other] = [Object.keys(f.locs)[0]!, "zz:not-in-the-feed"];
    const rel = { ...r, chainOf: r.chainOf, payload: { ...r.payload, x: {
      [carried]: [{ k: "note", sp: "Coho", d: "", t: "old words" }], [other]: [{ k: "new", sp: "Coho", d: "", t: "kept" }] } } } as SalmonFacts;
    const fx = { ...f, locs: { ...f.locs, [carried]: { ...f.locs[carried]!, x: [{ k: "new", sp: "Coho", d: "", t: "new words" }] } } } as typeof f;
    expect(withSalmonFeed(rel, fx).payload.x).toEqual({
      [other]: [{ k: "new", sp: "Coho", d: "", t: "kept" }], [carried]: [{ k: "new", sp: "Coho", d: "", t: "new words" }] });
    const plain = withSalmonFeed(rel, f).payload.x!;
    expect(plain[carried]).toBeUndefined();                                       // the feed says nothing now: gone
  });
});

describe("(T356) a header closure from the feed: the pipeline's `c`, composed as `live.compose`", () => {
  const closed = () => parseSalmonFeed(G.closedFeed)!;
  it("every chain the closure answers every day carries the note ids; the card is the closure, in its words", () => {
    const f = closed();
    G.release.chains.forEach((c: { l: string[]; u?: string[]; n?: number[] }, k: number) => expect(composeChain(c.l, f, c.u ?? [], !!c.n)).toEqual(G.closedExpect[k]));
    expect(G.closedExpect.filter((c: { c?: string[] }) => c.c).length).toBeGreaterThan(0);
    const live = withSalmonFeed(release(), f);
    const card = salmonCard(live, { sids: [12], item: "gnis:4674" }, new Date(2026, 6, 20));
    expect([card.closedOnly, card.caveat, card.rows.map((r) => [r.label, r.status, r.notes])]).toEqual([true, null,
      [["Salmon", "closed", ["There is no fishing for salmon in Region 6 until further notice."]]]]);
  });
  it("the release's closure never outlives the feed's verdicts", () => {
    const shut = { ...release(), payload: { ...G.release, chains: G.release.chains.map((c: object) => ({ ...c, c: ["6:closed:t356"] })) } };
    expect(withSalmonFeed(shut, feed()).payload.chains.some((c) => c.c)).toBe(false);
  });
  it("a feed region with notes and without its sizes / shared limits is not read", () => {
    const { sizes: _s, ...r6 } = G.feed.regions["6"];
    expect(parseSalmonFeed({ ...G.feed, regions: { "6": r6 } })).toBeNull();
    expect(parseSalmonFeed({ ...G.feed, format: "dfo-feed/2" })).toBeNull();
  });
});

// (T521, owner 2026-11-09) a DFO place no curated location knows: (a) on a matched water, a line with the water's
// other DFO rules on its Salmon card, "Location not yet on the map: <DFO's words>"; (b) on a water no binding knows,
// a global bell notice naming DFO's region. No paint, no part change. Zero shows nothing; an unread list is said.
describe("(T521) the feed's unplaced places", () => {
  const matched = { fingerprint: "aa11", region: "6", items: ["gnis:9999"], water: "Babine River", scope: "below the counting fence",
                    rows: ["All | Aug 1 until further notice | No fishing for salmon"] };
  const newWater = { fingerprint: "bb22", region: "6", items: [], water: "Brand New Creek", scope: "",
                     rows: ["Coho | Sep 1 to Oct 31 | 2 per day"] };
  const day = new Date(2026, 8, 10);
  const page = { sids: [10], item: "gnis:9999" };
  const withRows = (rows: unknown[]) => withSalmonFeed(release(), { ...feed(), unplaced: rows.length, unplacedRows: parseDfoUnplaced(rows) });

  it("(a) a matched water: in its Salmon card's water rules, in DFO's words; no caveat, rows and map unchanged", () => {
    const plain = withRows([]), live = withRows([matched, newWater]);
    const was = salmonCard(plain, page, day), now = salmonCard(live, page, day);
    expect(now.waterRules).toEqual([...was.waterRules,
      "Location not yet on the map: below the counting fence — All · Aug 1 until further notice · No fishing for salmon"]);
    expect([now.caveat, now.notListed]).toEqual([was.caveat, null]);
    expect(now.rows).toEqual(was.rows);
    const by = new Map<number, number>(G.sections.map(([s, c]: [number, number]) => [s, c]));
    for (const sid of by.keys()) expect(salmonChainStatus(live, by.get(sid)!, 250)).toBe(salmonChainStatus(plain, by.get(sid)!, 250));
    // another water's page does not show it, and the matched place is never the bell's
    expect(salmonCard(live, { sids: [10], item: "gnis:1" }, day).waterRules).toEqual(salmonCard(plain, { sids: [10], item: "gnis:1" }, day).waterRules);
    expect(dfoNoticeRows({ ...feed(), unplaced: 1, unplacedRows: parseDfoUnplaced([matched]) })).toEqual(
      { rows: [], unread: null, count: 0, label: null });
  });

  it("(b) a water no binding knows: a bell notice naming DFO's region and its words", () => {
    const vm = dfoNoticeRows({ ...feed(), unplaced: 2, unplacedRows: parseDfoUnplaced([matched, newWater]) });
    expect(vm).toEqual({ rows: [{ id: "6:bb22:0", where: "DFO Region 6 Skeena · Brand New Creek", text: "Coho · Sep 1 to Oct 31 · 2 per day" }],
                         unread: null, count: 1, label: "1 DFO water not on the map yet" });
    // never on a water page
    expect(salmonCard(withRows([newWater]), page, day).waterRules).toEqual(salmonCard(withRows([]), page, day).waterRules);
  });

  it("zero unplaced shows nothing", () => {
    const live = withRows([]);
    expect(live.payload.feedUnplaced).toBeUndefined();
    expect(salmonCard(live, page, day)).toEqual(salmonCard(withSalmonFeed(release(), feed()), page, day));
    expect(dfoNoticeRows({ ...feed(), unplaced: 0, unplacedRows: [] })).toEqual({ rows: [], unread: null, count: 0, label: null });
    expect(dfoNoticeRows(null)).toEqual({ rows: [], unread: null, count: 0, label: null });
  });

  it("an unplaced list that was not read is shown: on the card's feed line and in the bell", () => {
    const f = { ...feed(), unplaced: 2 };
    expect(salmonFeedLine(withSalmonFeed(release(), f))).toMatch(
      /^Checked with DFO Oct 27, 2026, \d{1,2}:\d\d [ap]m PT \(2 DFO places not yet on the map could not be read\)$/);
    const msg = "2 DFO places not yet on the map could not be read. They will show when the next check reads them.";
    expect(dfoNoticeRows(f)).toEqual({ rows: [], unread: msg, count: 1, label: msg });
  });

  it("parseDfoUnplaced refuses a row without its water's items (a pre-T521 worker's list)", () => {
    const { items: _i, ...old } = matched;
    expect(() => parseDfoUnplaced([old])).toThrow("dfo unplaced.json: row 0 is not a place with fingerprint, region, water, rows and items");
    expect(() => parseDfoUnplaced({})).toThrow("dfo unplaced.json: not a list");
  });
});
