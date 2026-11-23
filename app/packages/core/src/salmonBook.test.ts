/**
 * T796 (owner 2026-11-16, questions 29 and 30) in core, offline AND under a live feed.
 *
 * A5 — A STRETCH THE RULE BOOK CLOSES READS CLOSED FOR SALMON (book p.77). The release puts the book's full closure first
 * in the stretch's chains, a static place (`p[id].static`, `p[id].book`) whose year is in payload `static`. Owner 12:53:
 * "the salmon closed because the river is closed in synopsis applies to the map too" — so on a book-closed stretch the
 * Salmon map's line colour (`salmonChainStatus` / `salmonDayProfiles`), the year bar (`card.year`, `salmonStripOf`) and
 * the card's alerts (its headline, the folded row `salmonGlance`) read closed, both ways. The payload is the Python's:
 * `with_book_closures` over `pipeline/tests/test_t796_book_closures.py` `_built()` (Cowichan below the Mile 66 trestle,
 * DFO "catch and release" all year, the book closed Sep 1–Nov 15), values pasted from `.venv/bin/python`; the feed's
 * place year is `live.location_calendar` over the same DFO rule with the feed's own verdict table (other indices).
 *
 * A6 — A DFO "FINFISH CLOSURE" CLOSES THE WHOLE PAGE: a verdict's `f` (the pipeline's and the feeds worker's) on the
 * page's drawn chains that day; `closeForFinfish` closes the provincial page from the card's `finfish`.
 */
import { describe, expect, it } from "vitest";
import { closeForFinfish, type RegsPage } from "./regs";
import { decodeSalmonHead, salmonCard, salmonChainStatus, salmonDayIndex, salmonDayProfiles, salmonStripOf, salmonTable, SalmonError,
         type SalmonFacts } from "./salmon";
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

// ── A6 ──────────────────────────────────────────────────────────────────────────────────────────────────────────────

const FINFISH = "Finfish closure (Aug 25–Oct 15)";
/** chain 1 (sid 1): DFO's finfish closure Aug 25–Oct 15 (its verdict `f: 1`), release only otherwise */
const FIN_PAYLOAD = {
  ...PAYLOAD, static: undefined, p: { [DFO]: { r: "1", water: "Stamp River" } },
  verdicts: [...PAYLOAD.verdicts.slice(0, 2), { s: "closed", t: FINFISH, f: 1 }],
  chains: [PAYLOAD.chains[0]!, { ...PAYLOAD.chains[1]!, sp: five([0, 1, 237, 2, 289, 1]), m: [0, 2, 237, 3, 289, 2], a: five([0, 0]) }],
};
const finFacts = (): SalmonFacts => ({ ...facts(FIN_PAYLOAD), chains: 2, chainOf: (sid) => (sid === 1 ? 1 : 0) });
const finFeed = () => FEED({ verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 }, { s: "release", t: RELEASE_WORDS },
                                        { s: "closed", t: FINFISH, f: 1 }] as never,
                             locs: { [DFO]: { r: "1", sp: five([0, 1, 237, 2, 289, 1]), g: [0, -1] } } });

/** an open provincial page, as `buildRegsPage` shapes the parts `closeForFinfish` reads */
const OPEN_PAGE = {
  status: "open", who: { age: "adult", residency: "resident" },
  header: { name: "Stamp River", dot: "open", dayWord: "Today", date: "Oct 10", dateAria: "", place: null, tabs: [] },
  keep: { order: ["strip", { group: 0 }, "otherFish", "possession"], inherited: null, banner: null, whereNot: null, outside: null,
          strip: { status: "open", line: [{ kind: "status", text: "Open" }, { kind: "when", text: "today" }, { kind: "soon", text: "Closed from Nov 1" }],
                   runs: [], todayAt: 0.77, months: [], legend: [] },
          groups: [{ title: "Trout" }], plainRows: [], otherFish: [{}], otherFishKey: [], caveats: [], possession: {} },
  licence: { any: 1 }, gear: null, methods: null,
} as unknown as RegsPage;

describe("(T796 A6) a DFO \"Finfish closure\" closes the whole page, trout included — offline and live", () => {
  for (const [how, f] of [["offline (the release)", finFacts], ["under a live feed", () => withSalmonFeed(finFacts(), finFeed())]] as const) {
    it(`${how}: Oct 10 the card carries DFO's words and the page is closed; Nov 1 the page is its own`, () => {
      const F = f();
      const card = salmonCard(F, { sids: [1], item: null }, OCT_10);
      expect(card.finfish).toEqual([FINFISH]);
      const page = closeForFinfish(OPEN_PAGE, card.finfish);
      expect([page.status, page.header.dot, page.licence, page.keep.strip!.status, page.keep.strip!.line]).toEqual(
        ["closed", "closed", null, "closed", [{ kind: "status", text: "Closed" }, { kind: "when", text: "today" }]]);
      expect([page.keep.order, page.keep.groups, page.keep.otherFish, page.keep.possession]).toEqual([["strip", "banner"], [], null, null]);
      expect(page.keep.banner!.paras).toEqual(
        ["DFO closes this water to all fishing today (a finfish closure), trout and other fish included: Finfish closure (Aug 25–Oct 15)"]);
      const nov = salmonCard(F, { sids: [1], item: null }, new Date(2026, 10, 1));
      expect([nov.finfish, closeForFinfish(OPEN_PAGE, nov.finfish)]).toEqual([[], OPEN_PAGE]);
    });
  }
  it("a plain DFO closure (no `f`) closes salmon only: the page stays open", () => {
    const P = { ...FIN_PAYLOAD, verdicts: [...PAYLOAD.verdicts.slice(0, 2), { s: "closed", t: FINFISH }] };
    const card = salmonCard({ ...facts(P), chains: 2, chainOf: () => 1 }, { sids: [1], item: null }, OCT_10);
    expect([card.headline, card.finfish, closeForFinfish(OPEN_PAGE, card.finfish).status]).toEqual(["Closed today", [], "open"]);
  });
});

// ── (T796, owner 2026-11-16 15:19) EVERY SURFACE NAMES WHERE A BOOK CLOSURE IS FROM ─────────────────────────────────────
// "we need to make sure it says where the closure is from though": wherever the app shows the rule book's closure for
// salmon it names the rule book, the page, the book's own line and its dates — and never calls it DFO's. The stretches
// are three of the owner's (lane change list on the rebuilt salmon step of r-46e4355a7277): the book's words as
// `deliver.salmon.book_rule` + `live.location_calendar` write them, its row and pages as `with_book_closures` puts them in
// `p`; DFO's answer is one verdict for every species (its coho row), for brevity. Offline AND under a live feed.

type Span = readonly [Date, Date];
const on = (m: number, d: number) => new Date(2026, m - 1, d);
const inSpans = (spans: readonly Span[], i: number) => spans.some(([a, b]) => i >= salmonDayIndex(a) && i <= salmonDayIndex(b));
/** runs [start, value, …] over the file's 366 days */
const runsBy = (v: (i: number) => number): number[] => {
  const r: number[] = [];
  for (let i = 0; i < 366; i++) { const x = v(i); if (!r.length || r[r.length - 1] !== x) r.push(i, x); }
  return r;
};
const NOT_OPENED = { s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 } as const;
const MAP = { open: 1, release: 2, closed: 3 } as const;
interface Stretch {
  book: { rule: string; r?: string; water: string; pages: number[]; words: string; on: readonly Span[] };
  dfo: { lid: string; r: string; water?: string; sec?: string; v: { s: "open" | "release"; t: string }; on: readonly Span[] };
}
/** sid 1: DFO's place only; sid 2: the book's closure first, then DFO's (`with_book_closures`' chain) */
function stretch(o: Stretch): { facts: () => SalmonFacts; feed: () => SalmonFeed } {
  const B = `book:${o.book.rule}`, isB = (i: number) => inSpans(o.book.on, i), isD = (i: number) => inSpans(o.dfo.on, i);
  const payload = {
    format: "salmon/4", credit: PAYLOAD.credit, default: 0, scopes: {}, unmapped: [], gears: [],
    regions: { [o.dfo.r]: { name: "Skeena", asof: "2026-08-28", url: "https://www.pac.dfo-mpo.gc.ca/", notes: [], shared: [], sizes: [] } },
    verdicts: [NOT_OPENED, o.dfo.v, { s: "closed", t: o.book.words }],
    chains: [
      { r: [], w: "", l: [], sp: five([0, 0]), g: [0, -1], m: [0, 3] },
      { r: [o.dfo.r], w: "w", l: [o.dfo.lid], sp: five(runsBy((i) => (isD(i) ? 1 : 0))), g: [0, -1],
        m: runsBy((i) => (isD(i) ? MAP[o.dfo.v.s] : 3)), a: five(runsBy((i) => (isD(i) ? 0 : -1))) },
      { r: [o.dfo.r], w: "w", l: [B, o.dfo.lid], sp: five(runsBy((i) => (isB(i) ? 2 : isD(i) ? 1 : 0))), g: [0, -1],
        m: runsBy((i) => (isB(i) ? 3 : isD(i) ? MAP[o.dfo.v.s] : 3)), a: five(runsBy((i) => (isB(i) ? 0 : isD(i) ? 1 : -1))) },
    ],
    p: { [o.dfo.lid]: { r: o.dfo.r, ...(o.dfo.water ? { water: o.dfo.water } : {}), ...(o.dfo.sec ? { sec: o.dfo.sec } : {}) },
         [B]: { ...(o.book.r ? { r: o.book.r } : {}), water: o.book.water, static: 1, book: { rule: o.book.rule, pages: o.book.pages } } },
    static: { [B]: { sp: five(runsBy((i) => (isB(i) ? 2 : -1))), g: [0, -1] } },
  };
  return {
    facts: () => facts(payload),
    // the live feed: DFO's place only, its own verdict table (other indices)
    feed: () => FEED({ regions: { [o.dfo.r]: { name: "Skeena", url: "", asof: "2026-10-01", checked: null, stale: false, held: null } },
                       verdicts: [NOT_OPENED, { s: "open", t: "padding" }, o.dfo.v] as never,
                       locs: { [o.dfo.lid]: { r: o.dfo.r, sp: five(runsBy((i) => (isD(i) ? 2 : 0))), g: [0, -1] } } }),
  };
}

const BOOK_HEAD = "Closed to all fishing by the rule book";
// the Bell-Irving, a Nass side stream in DFO's section C: the Skeena/Nass winter closure, Jan 1–Jun 15 (book p.49)
const NASS_LINE = "No fishing: in any stream in the watersheds of the Skeena River upstream of Cedarvale or Nass River upstream of "
  + "Kitsault Bridge, Jan 1-June 15 (NOTE: Nass River mainstem is EXEMPT and Skeena River mainstem upstream of Cedarvale is only "
  + "closed Jan 1-May 31). See tables for other exceptions.";
const NASS = stretch({
  book: { rule: "z6:skeena_nass_winter_closure::skeena_nass_winter_closure.r2", r: "6", water: "Skeena and Nass watersheds",
          pages: [49], words: `${BOOK_HEAD} (p.49): ${NASS_LINE} (Jan 1–Jun 15)`, on: [[on(1, 1), on(6, 15)]] },
  dfo: { lid: "6:nass:c", r: "6", sec: "C", v: { s: "open", t: "4 per day, only 2 over 50 cm (Jan 1–Oct 31)" }, on: [[on(1, 1), on(10, 31)]] },
});
// the Skeena below the CNR bridge at Terrace, in an ecological reserve: closed all year (book p.9); DFO 4 a day Jul 29–Aug 16
const RESERVE_LINE = "Fishing is prohibited in Ecological Reserves in B.C.";
const RESERVE = stretch({
  book: { rule: "zp:superior_closures::superior_closures.r3", water: "National Parks and Ecological Reserves", pages: [9],
          words: `${BOOK_HEAD} (p.9): ${RESERVE_LINE}`, on: [[on(1, 1), on(12, 31)]] },
  dfo: { lid: "6:skeena:cnr", r: "6", water: "Skeena River", v: { s: "open", t: "4 per day (Jul 29–Aug 16)" }, on: [[on(7, 29), on(8, 16)]] },
});
// the Yakoun, Yakoun Lake down ~13 km: closed Oct 1–Apr 30 (book p.19); DFO's section D open Apr 1–Oct 31
const YAKOUN_LINE = "No Fishing from Yakoun Lake downstream approximately 13 km to fishing boundary signs, Oct 1-Apr 30";
const YAKOUN = stretch({
  book: { rule: "r1:yakoun_river@6-13::yakoun_river.r1", r: "1", water: "Yakoun River", pages: [19],
          words: `${BOOK_HEAD} (p.19): ${YAKOUN_LINE} (Oct 1–Apr 30)`, on: [[on(1, 1), on(4, 30)], [on(10, 1), on(12, 31)]] },
  dfo: { lid: "6:qci:d", r: "6", sec: "D", v: { s: "open", t: "4 per day, only 1 over 50 cm (Apr 1–Oct 31)" }, on: [[on(4, 1), on(10, 31)]] },
});

/** every surface a page shows for salmon, as its words: the card's head, its rows, the table's words and places, the
 *  source fold, the year bar's name and day, the folded Keep row (and what a plain one says under it), and the map's colour */
function surfaces(F: SalmonFacts, day: Date) {
  const card = salmonCard(F, { sids: [2], item: null }, day);
  const t = salmonTable(card);
  const strip = salmonStripOf(card, { runs: [], line: [], todayAt: 0 })!;
  return {
    head: [card.title, card.badge, card.headline],
    rows: card.rows.map((r) => [r.label, r.statusLabel, r.notes, r.from]),
    table: { says: t.says.flatMap((x) => x.lines), from: [...new Set(t.species.map((r) => r.from))] },
    fold: { title: card.sourceTitle, book: card.book, source: card.source, note: card.note },
    bar: [strip.title, strip.word],
    glance: salmonGlance(card).value,
    // (T796 follow-up) a plain closed row (closed all year: no Details, the card never drawn) says the book's closure itself
    glanceRow: (({ fold, book }) => ({ fold: fold ?? true, book: book ?? null }))(salmonGlance(card)),
    map: salmonChainStatus(F, 2, salmonDayIndex(day)),
  };
}
const FOLD_BOOK = { title: "Sources (rule book and DFO)", source: "Fisheries and Oceans Canada · Region 6 Skeena",
                    note: "Where the rule book closes a water to all fishing, salmon are closed there too (rule book p.77). "
                      + "Elsewhere salmon are set by DFO, not the provincial regulations above. Both apply." };

describe("(T796, owner 15:19) a book closure names the rule book — page, line, dates — everywhere, never DFO", () => {
  for (const [how, live] of [["offline (the release)", false], ["under a live feed", true]] as const) {
    const of = (s: ReturnType<typeof stretch>) => (live ? withSalmonFeed(s.facts(), s.feed()) : s.facts());

    it(`${how}: the Bell-Irving (Nass, section C) Feb 15 — the Skeena/Nass winter closure, p.49, Jan 1–Jun 15`, () => {
      const words = `${BOOK_HEAD} (p.49): ${NASS_LINE} (Jan 1–Jun 15)`, from = "Rule book p.49 · Skeena and Nass watersheds";
      expect(surfaces(of(NASS), on(2, 15))).toEqual({
        head: ["Salmon", "Rule book", "Closed today"],
        rows: [["Salmon", "Closed", [words], from]],
        table: { says: [words], from: [from] },
        fold: { ...FOLD_BOOK, book: [{ from, words }] },
        bar: ["Salmon (DFO and rule book)", "Closed"], glance: "closed", glanceRow: { fold: true, book: null }, map: "closed",
      });
    });

    it(`${how}: the Skeena in the ecological reserve below Terrace Aug 15 — p.9, all year, though DFO opens sockeye`, () => {
      const words = `${BOOK_HEAD} (p.9): ${RESERVE_LINE} (all year)`, from = "Rule book p.9 · National Parks and Ecological Reserves";
      expect(surfaces(of(RESERVE), on(8, 15))).toEqual({
        head: ["Salmon", "Rule book", "Closed today"],
        rows: [["Salmon", "Closed", [words], from]],
        table: { says: [words], from: [from] },
        fold: { ...FOLD_BOOK, book: [{ from, words }] },
        bar: ["Salmon (rule book)", "Closed"], glance: "closed",
        // closed all year: the Keep tab's Salmon row is plain (nothing behind it), so it names the book, page, line, dates
        glanceRow: { fold: false, book: [{ from, words }] }, map: "closed",
      });
    });

    it(`${how}: the Yakoun's closed stretch Nov 15 is the book's (p.19, Oct 1–Apr 30); Jul 15 DFO's own answer, named DFO's`, () => {
      const words = `${BOOK_HEAD} (p.19): ${YAKOUN_LINE} (Oct 1–Apr 30)`, from = "Rule book p.19 · Yakoun River";
      expect(surfaces(of(YAKOUN), on(11, 15))).toEqual({
        head: ["Salmon", "Rule book", "Closed today"],
        rows: [["Salmon", "Closed", [words], from]],
        table: { says: [words], from: [from] },
        fold: { ...FOLD_BOOK, book: [{ from, words }] },
        bar: ["Salmon (DFO and rule book)", "Closed"], glance: "closed", glanceRow: { fold: true, book: null }, map: "closed",
      });
      const dfo = "4 per day, only 1 over 50 cm (Apr 1–Oct 31)";
      expect(surfaces(of(YAKOUN), on(7, 15))).toEqual({
        head: ["Salmon", "DFO", "Open today"],
        rows: [["Chinook · coho · sockeye · pink · chum", "Open", [dfo], "Region 6 · D"]],
        table: { says: [dfo], from: ["Region 6 · D"] },
        fold: { title: "Source (DFO)", book: [], source: FOLD_BOOK.source,
                note: "Salmon are set by DFO, not the provincial regulations above. Both apply." },
        // the bar's year holds the book's days too: it is never named DFO's alone
        bar: ["Salmon (DFO and rule book)", "Open"], glance: "open", glanceRow: { fold: true, book: null }, map: "open",
      });
    });
  }

  it("a stretch with no book closure keeps DFO's names: the badge, the fold, the bar", () => {
    const s = surfaces(YAKOUN.facts(), on(7, 15));
    const dfoOnly = salmonCard(YAKOUN.facts(), { sids: [1], item: null }, on(11, 15));
    expect([s.head[1], dfoOnly.badge, dfoOnly.sourceTitle, dfoOnly.book, salmonStripOf(dfoOnly, null)!.title])
      .toEqual(["DFO", "DFO", "Source (DFO)", [], "Salmon (DFO)"]);
  });
});

// ── (T796 W2) EACH BOOK CLOSURE CARRIES ITS OWN DATES ──────────────────────────────────────────────────────────────────
// The adversarial review's W2 (row-22, chain 568 of r-685b700b2802, values pasted): Statlu Creek's closure is all year
// (p.26, "No Fishing"), but the Chehalis's May closure (p.22) sits first on the same chain and answers May 1–31. The card
// said "(all year)" only when the place answered EVERY day of the chain, so on the days Statlu answers its words had no
// dates at all. "All year" is the closure's own year (`payload.static`), never which place answers the chain.
const CHEHALIS = "book:r2:chehalis_river@2-19::chehalis_river.r2", STATLU = "book:r2:statlu_creek@2-19::statlu_creek.r1";
const CHEHALIS_DFO = "2:chehalis-river:downstream-of-the-logging-bridge-2";
const CHEHALIS_WORDS = `${BOOK_HEAD} (p.22): No Fishing downstream of the main logging road bridge situated approximately 2.4 km `
  + "downstream of Chehalis Lake, May 1-31 (May 1–31)";
const STATLU_WORDS = `${BOOK_HEAD} (p.26): No Fishing`;
const CHEHALIS_OPEN = { s: "open", t: "2 per day (Sep 1–Dec 31)" } as const;
const isMay = (i: number) => i >= 121 && i < 152, isFall = (i: number) => i >= 244;
const TWO_BOOKS = {
  format: "salmon/4", credit: PAYLOAD.credit, default: 0, scopes: {}, unmapped: [], gears: [],
  regions: { "2": { name: "Lower Mainland", asof: "2026-08-28", url: "https://www.pac.dfo-mpo.gc.ca/", notes: [], shared: [], sizes: [] } },
  verdicts: [NOT_OPENED, CHEHALIS_OPEN, { s: "closed", t: CHEHALIS_WORDS }, { s: "closed", t: STATLU_WORDS }],
  chains: [
    { r: [], w: "", l: [], sp: five([0, 0]), g: [0, -1], m: [0, 3] },
    { r: ["2"], w: "w", l: [CHEHALIS_DFO], sp: five(runsBy((i) => (isFall(i) ? 1 : 0))), g: [0, -1],
      m: runsBy((i) => (isFall(i) ? 1 : 3)), a: five(runsBy((i) => (isFall(i) ? 0 : -1))) },
    // chain 568 as the release ships it: the Chehalis's May closure first, Statlu's all-year one, then DFO's place
    { r: ["2"], w: "w", l: [CHEHALIS, STATLU, CHEHALIS_DFO], sp: five([0, 3, 121, 2, 152, 3]), g: [0, -1], m: [0, 3],
      a: five([0, 1, 121, 0, 152, 1]) },
  ],
  p: { [CHEHALIS_DFO]: { r: "2", water: "Chehalis River" },
       [CHEHALIS]: { r: "2", water: "Chehalis River", static: 1, book: { rule: "r2:chehalis_river@2-19::chehalis_river.r2", pages: [22] } },
       [STATLU]: { r: "2", water: "Statlu Creek", static: 1, book: { rule: "r2:statlu_creek@2-19::statlu_creek.r1", pages: [26] } } },
  static: { [CHEHALIS]: { sp: five(runsBy((i) => (isMay(i) ? 2 : -1))), g: [0, -1] }, [STATLU]: { sp: five([0, 3]), g: [0, -1] } },
};
const twoBooksFeed = () => FEED({ regions: { "2": { name: "Lower Mainland", url: "", asof: "2026-10-01", checked: null, stale: false, held: null } },
                                  verdicts: [NOT_OPENED, { s: "release", t: "padding" }, CHEHALIS_OPEN] as never,
                                  locs: { [CHEHALIS_DFO]: { r: "2", sp: five(runsBy((i) => (isFall(i) ? 2 : 0))), g: [0, -1] } } });

describe("(T796 W2) two book closures on one stretch: each one's words carry its own dates", () => {
  for (const [how, f] of [["offline (the release)", () => facts(TWO_BOOKS)], ["under a live feed", () => withSalmonFeed(facts(TWO_BOOKS), twoBooksFeed())]] as const) {
    it(`${how}: Statlu Creek (p.26) says "(all year)" on Feb 15 and Oct 10; May 15 the Chehalis's own May 1–31`, () => {
      const statlu = { from: "Rule book p.26 · Statlu Creek", words: `${STATLU_WORDS} (all year)` };
      const chehalis = { from: "Rule book p.22 · Chehalis River", words: CHEHALIS_WORDS };
      for (const [day, b] of [[on(2, 15), statlu], [OCT_10, statlu], [on(5, 15), chehalis]] as const) {
        const s = surfaces(f(), day);
        expect([s.rows, s.table.says, s.fold.book, s.glanceRow, s.map]).toEqual([
          [["Salmon", "Closed", [b.words], b.from]], [b.words], [b], { fold: false, book: [b] }, "closed"]);
      }
    });
  }
});

// ── (T796 X1) A REGION ITS DFO PAGE CLOSES KEEPS THE REGION-CLOSED CARD WHERE THE BOOK CLOSES IT TOO ───────────────────────
// The adversarial review's X1 (320,369 sections of Regions 4, 5 and 7): Region 4's page says "There is no fishing for
// salmon in Region 4 … until further notice" (the header's closure, `c`), and the book's spring closure (p.34) now stands
// first on the same chains. `c` means "every day is the header's", so the pipeline (`_book_chain`) and the live feed
// (`composeChain`) both drop it there, and the card lost T227's one closed statement for a year bar and per-day rows.
// Chain 1 is the release before the book (r-46e4355a7277 chain 139); chain 2 the candidate's chain 696 as shipped (the
// spring closure Apr 1–Jun 14, `c` dropped); chain 3 a book closure every day (no header day left): the book's own card.
const R4_NOTE = "4:closed:21698e56", R4_WORDS = "There is no fishing for salmon in Region 4 - Kootenays until further notice.";
const SPRING = "book:z4:spring_stream_closure::spring_stream_closure.r1", R4_REGION = "4:region:section";
const SPRING_WORDS = `${BOOK_HEAD} (p.34): No Fishing: in any stream in Region 4 from Apr 1-June 14 (see tables for additional closed `
  + "times or exceptions). (Apr 1–Jun 14)";
const PARKS = "book:zp:superior_closures::superior_closures.r1", PARKS_WORDS = `${BOOK_HEAD} (p.9): Freshwater fishing is prohibited in National Parks`;
const R4_HEADER = { s: "closed", t: R4_WORDS, c: R4_NOTE } as const;
const REGION_4 = {
  format: "salmon/4", credit: PAYLOAD.credit, default: 0, scopes: {}, unmapped: [], gears: [],
  regions: { "4": { name: "Kootenays", asof: "2026-08-01", url: "https://www.pac.dfo-mpo.gc.ca/",
                    notes: [{ id: R4_NOTE, k: "closed", t: R4_WORDS, fold: "closure" }], shared: [], sizes: [] } },
  verdicts: [NOT_OPENED, R4_HEADER, { s: "closed", t: SPRING_WORDS }, { s: "closed", t: PARKS_WORDS }],
  chains: [
    { r: [], w: "", l: [], sp: five([0, 0]), g: [0, -1], m: [0, 3] },
    { r: ["4"], w: "All Region 4 waters", l: [R4_REGION], sp: five([0, 1]), g: [0, -1], m: [0, 3], a: five([0, 0]), c: [R4_NOTE] },
    { r: ["4"], w: "All Region 4 waters", l: [SPRING, R4_REGION], sp: five([0, 1, 91, 2, 166, 1]), g: [0, -1], m: [0, 3],
      a: five([0, 1, 91, 0, 166, 1]) },
    { r: ["4"], w: "All Region 4 waters", l: [PARKS, R4_REGION], sp: five([0, 3]), g: [0, -1], m: [0, 3], a: five([0, 0]) },
  ],
  p: { [R4_REGION]: { r: "4", sec: "region" },
       [SPRING]: { r: "4", water: "Every stream in Region 4", static: 1, book: { rule: "z4:spring_stream_closure::spring_stream_closure.r1", pages: [34] } },
       [PARKS]: { water: "National Parks", static: 1, book: { rule: "zp:superior_closures::superior_closures.r1", pages: [9] } } },
  static: { [SPRING]: { sp: five([0, -1, 91, 2, 166, -1]), g: [0, -1] }, [PARKS]: { sp: five([0, 3]), g: [0, -1] } },
};
/** (T820) a page closed every day: one bar, every day closed; its strip (no provincial bar: no when, no marker) */
const ALL_CLOSED_BAR = { label: null, unmapped: false, today: "closed", todayWord: "closed", runs: [{ status: "closed", days: 365 }] };
const STRIP_ALL_CLOSED = (title: string, by: "dfo" | "both" | "book") => ({ title, by, status: "closed", mark: "✕", word: "Closed", when: null, soon: null,
  runs: [{ drawn: "closed", days: 365, gear: 0 }], generalTitle: "Game fish (B.C. rules)", legend: [{ drawn: "closed", label: "✕ Closed" }] });
const r4Facts = (): SalmonFacts => ({ ...facts(REGION_4), chains: 4, chainOf: (sid) => (sid >= 1 && sid <= 3 ? sid : 0) });
/** Region 4's page today: the header's closure every day, its own verdict table (other indices) */
const r4Feed = () => FEED({ regions: { "4": { name: "Kootenays", url: "", asof: "2026-10-01", checked: null, stale: false, held: null } },
                            verdicts: [NOT_OPENED, { s: "open", t: "padding" }, R4_HEADER] as never,
                            locs: { [R4_REGION]: { r: "4", sp: five([0, 2]), g: [0, -1] } } });

describe("(T796 X1) a region closed by its DFO page keeps its one closed statement where the book closes it too", () => {
  for (const [how, f] of [["offline (the release)", r4Facts], ["under a live feed", () => withSalmonFeed(r4Facts(), r4Feed())]] as const) {
    it(`${how}: Jul 1 (no book day) the card is the region closure's, whole; May 1 the same with the book's line as a note`, () => {
      const F = f();
      const card = (sid: number, day: Date) => salmonCard(F, { sids: [sid], item: null }, day);
      // a non-book day: the card of the region before the book — one closed row in the header's words — and (T820, owner
      // 2026-11-16 21:37: "the calendar bar doesnt have a full red line for salmon when closed but instead it is empty with
      // no color on the calendar bar..") X1's "no bar" overruled: the full closed bar, named by both (the book closes
      // Apr 1–Jun 14 of its year, the header the rest)
      const jul = card(2, on(7, 1)), julBefore = card(1, on(7, 1));
      expect({ ...jul, year: null }).toEqual({ ...julBefore, year: null });
      expect([julBefore.year, jul.year]).toEqual([{ bars: [ALL_CLOSED_BAR], by: "dfo" }, { bars: [ALL_CLOSED_BAR], by: "both" }]);
      expect([jul.badge, jul.rows.map((r) => [r.label, r.statusLabel, r.notes, r.from ?? null]), jul.closedOnly,
              salmonStripOf(jul, null), salmonGlance(jul)]).toEqual(["DFO", [["Salmon", "Closed", [R4_WORDS], null]], true,
        STRIP_ALL_CLOSED("Salmon (DFO and rule book)", "both"), { source: "dfo", thumb: "CO", name: "Salmon", value: "closed", tone: "closed", fold: false }]);
      // a book day: still the region's one statement (no per-species rows), and the book names its closure — source,
      // page, line, dates — on the row, in the source fold, on the plain Keep row and in the badge; the same full bar
      const book = { from: "Rule book p.34 · Every stream in Region 4", words: SPRING_WORDS };
      const before = card(1, on(5, 1)), may = card(2, on(5, 1));
      expect(may).toEqual({ ...before, badge: "Rule book · DFO", rows: [{ ...before.rows[0]!, notes: [R4_WORDS, SPRING_WORDS] }], book: [book],
                            sourceTitle: "Sources (rule book and DFO)", note: FOLD_BOOK.note, year: { bars: [ALL_CLOSED_BAR], by: "both" },
                            // (T1031) each species' sources: the region's closure, then the book's closure by its rule
                            whoBy: Object.fromEntries(Object.entries(before.whoBy!).map(([sp, w]) => [sp, [...w!, { k: "book", from: book.from,
                              rule: "z4:spring_stream_closure::spring_stream_closure.r1" }]])) });
      expect([may.closedOnly, may.year.bars, salmonStripOf(may, null), salmonGlance(may)]).toEqual([true, [ALL_CLOSED_BAR],
        STRIP_ALL_CLOSED("Salmon (DFO and rule book)", "both"), { source: "dfo", thumb: "CO", name: "Salmon", value: "closed", tone: "closed", fold: false, book: [book] }]);
      expect([salmonChainStatus(F, 2, salmonDayIndex(on(5, 1))), salmonChainStatus(F, 2, salmonDayIndex(on(7, 1)))]).toEqual(["closed", "closed"]);
    });

    it(`${how}: a book closure on every day leaves no day the header's — the book's own card, its bar named by the book`, () => {
      const park = salmonCard(f(), { sids: [3], item: null }, on(7, 1));
      const words = `${PARKS_WORDS} (all year)`;
      expect([park.badge, park.rows.map((r) => [r.notes, r.from]), park.closedOnly, salmonStripOf(park, null)?.title])
        .toEqual(["Rule book", [[[words], "Rule book p.9 · National Parks"]], true, "Salmon (rule book)"]);
    });
  }

  // as `c` asks: a DFO place on the water not drawn yet (`u`) must be closed by the header too, or the card keeps its caveat
  const OTHER = "4:kootenay-river:whole";
  const withUndrawn = (v: number, closed: boolean) => ({
    ...REGION_4, verdicts: [...REGION_4.verdicts, { s: "open", t: "2 per day (Jan 1–Dec 31)" }],
    p: { ...REGION_4.p, [OTHER]: { r: "4", water: "Kootenay River" } },
    unmapped: [{ items: ["gnis:1"], r: "4", w: "Kootenay River", chain: 4 }],
    chains: [...REGION_4.chains.slice(0, 2), { ...REGION_4.chains[2]!, u: [OTHER] }, REGION_4.chains[3]!,
             { r: ["4"], w: "Kootenay River", l: [OTHER], sp: five([0, v]), g: [0, -1], m: [0, v === 1 ? 3 : 1], a: five([0, 0]), n: [0, 1],
               ...(closed ? { c: [R4_NOTE] } : {}) }],
  });
  const undrawnFeed = (open: boolean) => FEED({ ...r4Feed(), verdicts: [NOT_OPENED, { s: "open", t: "2 per day (Jan 1–Dec 31)" }, R4_HEADER] as never,
    locs: { [R4_REGION]: { r: "4", sp: five([0, 2]), g: [0, -1] }, [OTHER]: { r: "4", sp: five([0, open ? 1 : 2]), g: [0, -1] } } });
  for (const live of [false, true]) {
    it(`${live ? "under a live feed" : "offline (the release)"}: an undrawn place open keeps the in-part card; closed by the header, the region's`, () => {
      const card = (open: boolean) => {
        const r = { ...facts(withUndrawn(open ? 4 : 1, !open)), chains: 5, chainOf: (sid: number) => (sid >= 1 && sid <= 4 ? sid : 0) };
        return salmonCard(live ? withSalmonFeed(r, undrawnFeed(open)) : r, { sids: [2], item: null }, on(7, 1));
      };
      const [open, shut] = [card(true), card(false)];
      expect([open.closedOnly, open.headline, open.rows[0]!.notes]).toEqual([false, "Closed today · open in part", [R4_WORDS]]);
      // (T820, owner 2026-11-16 21:37) the region's statement draws the full closed bar, never none
      expect([shut.closedOnly, shut.year.bars, shut.rows.map((r) => [r.notes, r.from ?? null])]).toEqual([true, [ALL_CLOSED_BAR], [[[R4_WORDS], null]]]);
    });
  }
});
