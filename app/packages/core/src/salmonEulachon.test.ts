/**
 * T796 / T770 eulachon (owner 2026-11-16 19:09: "As a closure line (Recommended)"). DFO's "All | No fishing for
 * eulachon" row (Region 6 B(ii)) is no gear line: the chain's `o` (runs of an index into `gears`, -1 none, absent where
 * no line), in Python's words, "Eulachon closed all year: No fishing for eulachon" (`deliver.salmon.closures_on`). The
 * card draws it as a closure line (`closures`), never in `gear`; under a live feed `composeChain` merges `o` exactly as
 * `g` (`live.compose`): every place's, narrowest first, a line once; two regions' groups the same way.
 *
 * The chain is the final scratch release r-1603e71cc92c's chain 279 (Kitsumkalum River — downstream of railway bridge),
 * cut to its gear and one verdict: places narrowest first, the bait line Jan 1–Jun 15 (book days 0..166), `o` all year.
 */
import { describe, expect, it } from "vitest";
import { decodeSalmonHead, salmonCard, SalmonError, type SalmonFacts } from "./salmon";
import { composeChain, composeChainEveryDay, feedTables, parseSalmonFeed, withSalmonFeed, type SalmonFeed } from "./salmonFeed";

const EULACHON = "Eulachon closed all year: No fishing for eulachon";
const BAIT = "No natural bait allowed (Jan 1–Jun 15)";
const RAIL = "6:kitsumkalum-river:downstream-of-railway-bridge", WHOLE = "6:kitsumkalum-river:whole", BII = "6:b(ii):section";
const five = <T>(x: T) => [x, x, x, x, x];
const CHINOOK = "No fishing for chinook (Apr 1–Mar 31)";
const PAYLOAD = {
  format: "salmon/4", credit: "Fisheries and Oceans Canada (DFO), freshwater salmon sport fishing regulations", default: 0,
  regions: { "6": { name: "Skeena", asof: "2026-08-28", url: "https://example/6", notes: [], shared: [], sizes: [] } },
  scopes: {}, unmapped: [],
  verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 }, { s: "closed", t: CHINOOK }],
  gears: [[BAIT], [EULACHON]],
  chains: [
    { r: [], w: "", l: [], sp: five([0, 0]), g: [0, -1], m: [0, 3] },
    { r: ["6"], w: "Kitsumkalum River — downstream of railway bridge.", l: [RAIL, WHOLE, BII], sp: five([0, 1]),
      g: [0, 0, 167, -1], o: [0, 1], m: [0, 3] },
  ],
};
const HANDLES = "0123456789abcdef";

/** the head as the release ships it (`salmon/4`), decoded — `checkPayload` runs */
function head(payload: object): ReturnType<typeof decodeSalmonHead> {
  const json = new TextEncoder().encode(JSON.stringify(payload));
  const b = new Uint8Array(17 + json.length);
  b.set([0x42, 0x43, 0x53, 0x41, 4]);
  for (let i = 0; i < 8; i++) b[5 + i] = parseInt(HANDLES.slice(2 * i, 2 * i + 2), 16);
  new DataView(b.buffer).setUint32(13, json.length, true);
  b.set(json, 17);
  return decodeSalmonHead(b, HANDLES);
}
const facts = (payload: object = PAYLOAD): SalmonFacts =>
  ({ handles: HANDLES, chains: 2, payload: head(payload), chainOf: (sid) => (sid === 1 ? 1 : 0), loaded: [] });

/** the live feed: each place its own year (`live.location_calendar`), its own tables — other indices than the release */
type Locs = Record<string, { r: string; sp: number[][]; g: number[]; o?: number[] }>;
const FEED = (locs: Locs, gears: string[][] = [["padding"], [EULACHON], [BAIT]]): SalmonFeed => parseSalmonFeed({
  format: "dfo-feed/3", at: "2026-10-09T18:00:00Z",
  regions: { "6": { name: "Skeena", url: "", asof: "2026-10-01", checked: null, stale: false, held: null } },
  verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 }, { s: "closed", t: CHINOOK }],
  gears, locs,
})!;
// the release's places as the feed carries them: the bait line on the river's whole, the eulachon row on B(ii)
const LIVE: Locs = {
  [RAIL]: { r: "6", sp: five([0, -1]), g: [0, -1] },
  [WHOLE]: { r: "6", sp: five([0, 1]), g: [0, 2, 167, -1] },
  [BII]: { r: "6", sp: five([0, -1]), g: [0, -1], o: [0, 1] },
};
const JAN_10 = new Date(2026, 0, 10), JUL_15 = new Date(2026, 6, 15);

describe("(T770) the eulachon closure is the chain's `o`: decoded, drawn as a closure line, never a gear line", () => {
  it("the head decodes `o`; an `o` index past the gear table is refused, never a quiet line", () => {
    expect(facts().payload.chains[1]!.o).toEqual([0, 1]);
    expect(facts().payload.chains[0]!.o).toBeUndefined();
    const bad = { ...PAYLOAD, chains: [PAYLOAD.chains[0], { ...PAYLOAD.chains[1], o: [0, 2] }] };
    expect(() => head(bad)).toThrow(SalmonError);
    expect(() => head(bad)).toThrow("salmon: chain 1 closure lines: index 2 of 2");
  });

  for (const [how, f] of [["offline (the release)", () => facts()], ["under a live feed", () => withSalmonFeed(facts(), FEED(LIVE))]] as const)
    it(`${how}: the Kitsumkalum card's closure line is the eulachon's, every day; the gear lines are the bait line only`, () => {
      const F = f();
      const jan = salmonCard(F, { sids: [1], item: null }, JAN_10), jul = salmonCard(F, { sids: [1], item: null }, JUL_15);
      expect([jan.closures, jan.gear]).toEqual([[EULACHON], [BAIT]]);
      expect([jul.closures, jul.gear]).toEqual([[EULACHON], []]);
      // an unlisted water carries none
      expect(salmonCard(F, { sids: [9], item: null }, JAN_10).closures).toEqual([]);
    });

  it("under a live feed the release's `o` never outlives the feed: a feed whose place has no eulachon row has none", () => {
    const { o: _o, ...bii } = LIVE[BII]!;
    const F = withSalmonFeed(facts(), FEED({ ...LIVE, [BII]: bii }));
    expect([F.payload.chains[1]!.o, salmonCard(F, { sids: [1], item: null }, JAN_10).closures]).toEqual([undefined, []]);
  });
});

describe("(T770) composeChain merges `o` exactly as `g` (`live.compose`)", () => {
  const G = [["padding"], [EULACHON], ["Smelt closed all year: No fishing for smelt"], [BAIT]];
  const words = (feed: SalmonFeed, tables: ReturnType<typeof feedTables>, runs: readonly number[] | undefined) =>
    runs?.map((x, k) => (k % 2 ? (x >= 0 ? (tables.gears[x] ?? feed.gears[x]) : null) : x));

  it("one place's line is its own index; two places' lines are one list, narrowest first, a line once", () => {
    const one = FEED(LIVE, G);
    const t1 = feedTables(one);
    expect(words(one, t1, composeChain([RAIL, WHOLE, BII], one, [], false, undefined, t1).o)).toEqual([0, [EULACHON]]);
    const two = FEED({ ...LIVE, [RAIL]: { ...LIVE[RAIL]!, o: [0, 2] } }, G);
    const t2 = feedTables(two);
    expect(words(two, t2, composeChain([RAIL, WHOLE, BII], two, [], false, undefined, t2).o))
      .toEqual([0, ["Smelt closed all year: No fishing for smelt", EULACHON]]);
    // the same line at two places is said once
    const same = FEED({ ...LIVE, [WHOLE]: { ...LIVE[WHOLE]!, o: [0, 1] } }, G);
    expect(words(same, feedTables(same), composeChain([RAIL, WHOLE, BII], same, [], false, undefined, feedTables(same)).o))
      .toEqual([0, [EULACHON]]);
    // two places' lines with no tables to add the list to STOPS, as the gear does
    expect(() => composeChain([RAIL, WHOLE, BII], two)).toThrow(/gear lines need tables/);
  });

  it("two regions' groups (`s`): every group's lines, in group order; none anywhere: no `o`", () => {
    const two = FEED({ ...LIVE, [RAIL]: { ...LIVE[RAIL]!, r: "6", o: [0, 2] } }, G);
    const t = feedTables(two);
    expect(words(two, t, composeChain([RAIL, WHOLE, BII], two, [], false, [[0], [1, 2]], t).o))
      .toEqual([0, ["Smelt closed all year: No fishing for smelt", EULACHON]]);
    expect(composeChain([RAIL, WHOLE], two, [], false, [[0], [1]], t).o).toEqual([0, 2]);
    expect("o" in composeChain([WHOLE], two, [], false, undefined, t)).toBe(false);
  });

  it("the change-day pass: a line starting on a day nothing else changes is there, as on every day", () => {
    // the eulachon row from Nov 1 (book day 304) only; no species or gear run changes that day
    const late = FEED({ ...LIVE, [BII]: { ...LIVE[BII]!, o: [0, -1, 304, 1] } }, G);
    const t = feedTables(late);
    const got = composeChain([RAIL, WHOLE, BII], late, [], false, undefined, t);
    expect(got.o).toEqual([0, -1, 304, 1]);
    expect(got).toEqual(composeChainEveryDay([RAIL, WHOLE, BII], late, [], false, undefined, feedTables(late)));
  });

  it("a feed place whose `o` indexes past its gears is no feed (never a half-read one)", () => {
    expect(parseSalmonFeed({ ...FEED(LIVE), locs: { ...LIVE, [BII]: { ...LIVE[BII]!, o: [0, 9] } } })).toBeNull();
  });
});
