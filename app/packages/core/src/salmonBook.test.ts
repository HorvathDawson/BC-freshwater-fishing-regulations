/**
 * T796 A5 (owner 2026-11-16, question 29) in core, offline AND under a live feed.
 *
 * A5 — A STRETCH THE RULE BOOK CLOSES READS CLOSED FOR SALMON (book p.77). The release puts the book's full closure first
 * in the stretch's chains, a static place (`p[id].static`, `p[id].book`) whose year is in payload `static`. Owner 12:53:
 * "the salmon closed because the river is closed in synopsis applies to the map too" — so on a book-closed stretch the
 * Salmon map's line colour (`salmonChainStatus` / `salmonDayProfiles`), the year bar (`card.year`, `salmonStripOf`) and
 * the card's alerts (its headline, the folded row `salmonGlance`) read closed, both ways. The payload is the Python's:
 * `with_book_closures` over `pipeline/tests/test_t796_book_closures.py` `_built()` (Cowichan below the Mile 66 trestle,
 * DFO "catch and release" all year, the book closed Sep 1–Nov 15), values pasted from `.venv/bin/python`; the feed's
 * place year is `live.location_calendar` over the same DFO rule with the feed's own verdict table (other indices).

 */
import { describe, expect, it } from "vitest";
import { decodeSalmonHead, salmonCard, salmonChainStatus, salmonDayProfiles, salmonStripOf, SalmonError, type SalmonFacts } from "./salmon";
import { salmonGlance } from "./glance";
import { composeChain, feedTables, parseSalmonFeed, withSalmonFeed, type SalmonFeed } from "./salmonFeed";

const BOOK = "book:r1:cowichan@1::cowichan_river.r6", DFO = "1:cowichan:66-mile";
const BOOK_WORDS = "Closed to all fishing by the rule book (p.15): No Fishing downstream of the CNR Mile 66 Trestle between Sept 1-Nov 15 (Sep 1–Nov 15)";
const RELEASE_WORDS = "Open for salmon catch and release";
const five = <T>(x: T) => [x, x, x, x, x];
// `with_book_closures(...)` (the Python's), pasted
const PAYLOAD = {
  format: "salmon/4", credit: "Fisheries and Oceans Canada (DFO), freshwater salmon sport fishing regulations", default: 0,
  regions: { "1": { name: "R1", asof: null, url: "https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/fresh-douce/region1-eng.html", notes: [], shared: [], sizes: [] } },
  scopes: {}, unmapped: [], gears: [],
  verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 }, { s: "release", t: RELEASE_WORDS },
             { s: "closed", t: BOOK_WORDS }],
  chains: [
    { r: [], w: "", l: [], sp: five([0, 0]), g: [0, -1], m: [0, 3] },
    { r: ["1"], w: "Cowichan River — 66 Mile Trestle", l: [DFO], sp: five([0, 1]), g: [0, -1], m: [0, 2], a: five([0, 0]) },
    { r: ["1"], w: "Cowichan River — 66 Mile Trestle", l: [BOOK, DFO], sp: five([0, 1, 244, 2, 320, 1]), g: [0, -1],
      m: [0, 2, 244, 3, 320, 2], a: five([0, 1, 244, 0, 320, 1]) },
  ],
  p: { [DFO]: { r: "1", water: "Cowichan River" },
       [BOOK]: { r: "1", water: "COWICHAN RIVER", static: 1, book: { rule: "r1:cowichan@1::cowichan_river.r6", pages: [15] } } },
  static: { [BOOK]: { sp: five([0, -1, 244, 2, 320, -1]), g: [0, -1] } },
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
  ({ handles: HANDLES, chains: 3, payload: head(payload), chainOf: (sid) => (sid === 1 ? 1 : sid === 2 ? 2 : 0), loaded: [] });
/** the live feed: DFO's place only (the book place is never in it), its own verdict table — other indices */
const FEED = (over: Partial<SalmonFeed> = {}): SalmonFeed => parseSalmonFeed({
  format: "dfo-feed/3", at: "2026-10-09T18:00:00Z",
  regions: { "1": { name: "R1", url: "", asof: "2026-10-01", checked: null, stale: false, held: null } },
  verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 }, { s: "open", t: "padding" }, { s: "release", t: RELEASE_WORDS }],
  gears: [], locs: { [DFO]: { r: "1", sp: five([0, 2]), g: [0, -1] } }, ...over,
})!;
const OCT_10 = new Date(2026, 9, 10), JUL_15 = new Date(2026, 6, 15);
const DAY = { oct10: 283, jul15: 196 };

describe("(T796 A5) a stretch the rule book closes reads closed for salmon: map, year bar, card — offline and live", () => {
  for (const [how, f] of [["offline (the release)", () => facts()], ["under a live feed", () => withSalmonFeed(facts(), FEED())]] as const) {
    it(`${how}: Oct 10 the map paints it closed, the bar and the card read closed and name the book; Jul 15 DFO's answer`, () => {
      const F = f();
      // THE MAP: the book-closed section's chain closed on Oct 10; its neighbour (no book closure) release only
      expect([salmonChainStatus(F, 2, DAY.oct10), salmonChainStatus(F, 1, DAY.oct10), salmonChainStatus(F, 2, DAY.jul15)])
        .toEqual(["closed", "release", "release"]);
      const prof = salmonDayProfiles(F, OCT_10);
      expect([prof.closed.includes("s2"), prof.release.includes("s1")]).toEqual([true, true]);
      // THE CARD: closed, the book's words, named by the book's page
      const card = salmonCard(F, { sids: [2], item: null }, OCT_10);
      expect([card.headline, card.headlineStatus, card.allClosed]).toEqual(["Closed today", "closed", true]);
      expect(card.rows.map((r) => [r.label, r.statusLabel, r.notes, r.from])).toEqual(
        [["Salmon", "Closed", [BOOK_WORDS], "Rule book p.15 · COWICHAN RIVER"]]);
      // THE ALERTS: the folded row says closed
      expect(salmonGlance(card)).toMatchObject({ value: "closed", tone: "closed" });
      // THE YEAR BAR: release only, closed Sep 1–Nov 15 (book days 243..318), release only again; today closed
      const bar = card.year.bars[0]!;
      expect([bar.today, bar.todayWord, bar.runs]).toEqual(["closed", "closed",
        [{ status: "release", days: 243 }, { status: "closed", days: 76 }, { status: "release", days: 46 }]]);
      expect(salmonStripOf(card, { runs: [], line: [], todayAt: 282 / 365 })).toMatchObject({ status: "closed", word: "Closed" });
      // Jul 15: the book says nothing; DFO's catch and release answers, named by DFO's place
      const jul = salmonCard(F, { sids: [2], item: null }, JUL_15);
      expect([jul.headline, jul.rows.map((r) => [r.label, r.notes, r.from])]).toEqual(
        ["Release only today", [["Chinook · coho · sockeye · pink · chum", [RELEASE_WORDS], "Region 1 · Cowichan River"]]]);
    });
  }

  it("under a live feed the static place's year is the release's, composed as the Python composes it (`live.compose`)", () => {
    const live = withSalmonFeed(facts(), FEED());
    const [rel, got] = [facts().payload.chains[2]!, live.payload.chains[2]!];
    const words = (P: SalmonFacts["payload"], runs: readonly number[]) => runs.map((x, k) => (k % 2 ? P.verdicts[x]!.t : x));
    expect(got.sp.map((r) => words(live.payload, r))).toEqual(rel.sp.map((r) => words(facts().payload, r)));
    expect([got.m, got.a]).toEqual([rel.m, rel.a]);
    // composed without the static places, the chain reads DFO's answer on the book's days (the defect this fixes)
    const bare = composeChain(rel.l, FEED(), [], false, undefined, feedTables(FEED()), true);
    expect(bare.m).toEqual([0, 2]);
  });

  it("a book place without its year, or a feed naming a static place, is refused — never a quiet answer", () => {
    const { static: _s, ...noYear } = PAYLOAD;
    expect(() => head(noYear)).toThrow(SalmonError);
    expect(() => head({ ...PAYLOAD, static: { [BOOK]: { sp: five([0, 7]), g: [0, -1] } } })).toThrow(/static .* index 7 of 3/);
    const clash = FEED({ locs: { [DFO]: { r: "1", sp: five([0, 2]), g: [0, -1] }, [BOOK]: { r: "1", sp: five([0, 0]), g: [0, -1] } } });
    expect(() => withSalmonFeed(facts(), clash).payload.chains[2]).toThrow(/static place/);
  });
});
