/**
 * T1052 (owner 2026-11-24 ~12:33, "One block, both calendars (Recommended)"): a water ONE closure shuts to all fishing,
 * salmon included, all year, says it once — the closed banner, for both; every other case keeps its two statements. The
 * decision table over hand-built halves (the page's banner, the salmon card, the part's rules); the same through the water
 * screen over the fixture release and the salmon card core builds: `WaterScreen.oneClosure.test.tsx`.
 */
import { describe, expect, it } from "vitest";
import type { SalmonCardVM } from "../salmon";
import type { RuleIndex, RuleView } from "./inputs";
import type { BannerVM, RegsPage } from "./model";
import { closedWithSalmon, OneClosureError } from "./withSalmon";

const NELSON = "r2:nelson_creek@2-8::nelson_creek.r1";
const BOOK_LINE = [{ t: "book", printed: 25, pdf: 27, box: [] }];
const banner = (over: Partial<BannerVM> = {}): BannerVM => ({
  kind: "closed", big: ["Closed all year"], paras: [],
  chain: [{ text: "No fishing for any game fish.", scope: "This water", rule: NELSON, book: BOOK_LINE as unknown as BannerVM["chain"][0]["book"] }],
  reopen: null, merged: null, ...over,
});
const page = (b: BannerVM | null = banner(), status = "closed"): RegsPage =>
  ({ status, keep: { banner: b, order: ["strip", "banner"] } }) as unknown as RegsPage;
const card = (over: Partial<SalmonCardVM> = {}): SalmonCardVM => ({
  todayBy: "book", closedOnly: true, issues: [], finfish: [], caveat: null, side: null,
  book: [{ rule: NELSON, from: "B.C. Synopsis p.25 · Nelson Creek", words: "Closed to all fishing by the B.C. Synopsis (p.25): No Fishing (all year)",
           water: "Nelson Creek", pages: [{ printed: 25, pdf: null }] }],
  bookAllYear: [NELSON], ...over,
}) as unknown as SalmonCardVM;
const rule = (key: string, over: { via?: "reach" | "trib"; row?: string; line?: string; unless?: string } = {}): RuleView => ({
  key, via: over.via ?? "reach",
  export: { verbatim: over.line ?? "No Fishing", prov: { rank: 0, entry_name: over.row ?? "Nelson Creek" } },
  facts: { kind: "gate", unless_opened: over.unless ?? null },
}) as unknown as RuleView;
const index = (...rs: RuleView[]): RuleIndex => ({
  get() { throw new Error("not asked"); }, has: () => false, size: rs.length,
  byKey: (k) => rs.find((r) => r.key === k) ?? null,
});
const RULES = index(rule(NELSON));

/** what the Keep tab draws of the banner: headline, paragraphs, each chain line with its level, the salmon flag */
const said = (p: RegsPage) => {
  const b = p.keep.banner!;
  return { big: b.big, paras: b.paras, chain: b.chain.map((c) => [c.text, c.scope, c.rule, c.about ?? null]), salmon: b.salmon ?? null };
};
const UNMERGED = { big: ["Closed all year"], paras: [], chain: [["No fishing for any game fish.", "This water", NELSON, null]], salmon: null };

describe("(T1052) closedWithSalmon: one statement where one closure shuts every fish, salmon included, all year", () => {
  it("Nelson Creek (p.25 \"No Fishing\", the same rule on the banner and every salmon species): one block, both said", () => {
    const out = closedWithSalmon(page(), card(), RULES);
    expect(said(out)).toEqual({
      big: ["Closed to all fishing all year, salmon included"], paras: [],
      // (#131 review) the level the banner gave the closure stays; the Source links are about the row, not the long line
      chain: [["The B.C. Synopsis row for Nelson Creek reads “No Fishing”.", "This water", NELSON, "the Nelson Creek row"]], salmon: true,
    });
    // the closure's book line (its page and highlight) is the banner's own: the source line is unchanged
    expect(out.keep.banner!.chain[0]!.book).toBe(BOOK_LINE);
    // (T1207, owner 2026-11-26) ONE status block for both fish, tagged with the rule book that closes them (never "DFO")
    expect(out.keep.banner!.schedule).toEqual({ title: "Salmon and game fish", tag: "B.C. Synopsis" });
  });

  it("a walked tributary names the row it inherits; a book line ending its own sentence takes no second full stop", () => {
    const trib = closedWithSalmon(page(), card(), index(rule(NELSON, { via: "trib", row: "Little Qualicum River", line: "No Fishing - All tributaries" })));
    expect(said(trib).chain).toEqual([["Little Qualicum River’s row covers its tributaries: “No Fishing - All tributaries”.", "This water", NELSON, "the Little Qualicum River row"]]);
    // (#131 review MED 3) a park reserve's closure keeps its "Federal / parks" level
    const parks = page(banner({ chain: [{ ...banner().chain[0]!, scope: "Federal / parks" }] }));
    const own = closedWithSalmon(parks, card(), index(rule(NELSON, { row: "National Park Reserves", line: "All fresh waters within Pacific Rim\nNational Park Reserve are closed to fishing." })));
    expect(said(own).chain).toEqual([["The B.C. Synopsis row for National Park Reserves reads “All fresh waters within Pacific Rim National Park Reserve are closed to fishing.”", "Federal / parks", NELSON, "the National Park Reserves row"]]);
  });

  // each case keeps the page AS IT IS (the same object): today's two statements
  const kept: [string, (p: RegsPage) => RegsPage, RegsPage?][] = [
    ["closed for part of the year (the banner reopens)", (p) => closedWithSalmon(p, card(), RULES), page(banner({ reopen: { text: "Opens again Apr 16.", day: { month: 4, day: 16 } } }))],
    ["the salmon's book closure for part of its year only (not all year)", (p) => closedWithSalmon(p, card({ bookAllYear: [] }), RULES), page()],
    ["salmon closed by DFO (\"Not opened by DFO\"), game fish by the book: two sources", (p) => closedWithSalmon(p, card({ todayBy: "dfo", book: [], bookAllYear: [] }), RULES), page()],
    ["salmon by the book AND DFO on the page today", (p) => closedWithSalmon(p, card({ todayBy: "both" }), RULES), page()],
    ["salmon closed by ANOTHER book rule (Still Creek: Brunette River's row)", (p) => closedWithSalmon(p, card({ book: [{ ...card().book[0]!, rule: "r2:brunette_river@2-8::brunette_river.r1" }], bookAllYear: ["r2:brunette_river@2-8::brunette_river.r1"] }), RULES), page()],
    // (#131 review MED 1) NEVER DROP A SOURCE: the Salmon row's second closure is not on the banner, so merging would
    // take it off the page and out of the every-source sheet
    ["the salmon stand on two all-year closures, the banner cites one of them", (p) => closedWithSalmon(p, card({ book: [card().book[0]!, { ...card().book[0]!, rule: "r2:brunette_river@2-8::brunette_river.r1" }], bookAllYear: [NELSON, "r2:brunette_river@2-8::brunette_river.r1"] }), index(rule(NELSON), rule("r2:brunette_river@2-8::brunette_river.r1"))), page()],
    // (#131 review LOW 5) the banner's second closure is not the salmon's, though the card's all-year list names it: the
    // all-year list counts only for closures the salmon stand on (a hand-built card; a built card's list is a subset)
    ["the banner cites a closure the card's all-year list names but the salmon do not stand on", (p) => closedWithSalmon(p, card({ bookAllYear: [NELSON, "r2:x@2-8::x.r1"] }), index(rule(NELSON), rule("r2:x@2-8::x.r1"))), page(banner({ chain: [...banner().chain, { ...banner().chain[0]!, rule: "r2:x@2-8::x.r1" }] }))],
    ["the banner cites two closures, the salmon one of them", (p) => closedWithSalmon(p, card(), index(rule(NELSON), rule("r2:x@2-8::x.r1"))), page(banner({ chain: [...banner().chain, { ...banner().chain[0]!, rule: "r2:x@2-8::x.r1" }] }))],
    ["a live DFO notice on the card (its row carries the issue mark)", (p) => closedWithSalmon(p, card({ issues: [{}] as unknown as SalmonCardVM["issues"] }), RULES), page()],
    ["a DFO finfish closure on the card", (p) => closedWithSalmon(p, card({ finfish: ["Finfish closure"] }), RULES), page()],
    ["an undrawn DFO place (the card's in-part caveat)", (p) => closedWithSalmon(p, card({ caveat: {} as SalmonCardVM["caveat"] }), RULES), page()],
    ["a one-bank DFO place", (p) => closedWithSalmon(p, card({ side: {} as SalmonCardVM["side"] }), RULES), page()],
    ["the card is not one closed-all-year statement", (p) => closedWithSalmon(p, card({ closedOnly: false }), RULES), page()],
    ["a closure another authority may lift (a National Park, \"unless opened\")", (p) => closedWithSalmon(p, card(), index(rule(NELSON, { unless: "Closed unless opened by Parks Canada." }))), page()],
    ["a merged \"closed all year on N stretches\" banner", (p) => closedWithSalmon(p, card(), RULES), page(banner({ merged: { label: "Closed all year on 2 stretches (3 sections)", items: [] } }))],
    ["a card built by hand (no todayBy)", (p) => closedWithSalmon(p, card({ todayBy: undefined }), RULES), page()],
  ];
  for (const [what, run, p = page()] of kept) it(`kept as two statements: ${what}`, () => {
    expect(run(p)).toBe(p);
    expect(run(p).keep.banner?.schedule).toBeUndefined();   // (T1207) and as two status blocks: no one-block schedule
  });
  it("kept as it is, the banner says game fish and the Salmon row stays", () => { expect(said(page())).toEqual(UNMERGED); });

  it("no salmon card (a water with no salmon, a release without them): no \"salmon included\" words", () => {
    const p = page();
    expect(closedWithSalmon(p, null, RULES)).toBe(p);
    expect(said(p)).toEqual(UNMERGED);
  });

  it("a part that is not closed (another part of a several-part water is), a tidal page, a closed day with no closure named: as it is", () => {
    const tidal = page(banner({ kind: "tidal", big: ["Tidal water"], chain: [] }), "tidal");
    const bare = page(banner({ big: ["Closed"], chain: [] }));   // a closed day whose frame names no broad closure
    for (const p of [page(null, "open"), page(null, "closed"), tidal, bare]) expect(closedWithSalmon(p, card(), RULES)).toBe(p);
    // (#131 review LOW 5) nothing on either side: no closure to say, never an empty "salmon included" block
    expect(closedWithSalmon(bare, card({ book: [], bookAllYear: [] }), RULES)).toBe(bare);
  });

  it("a closure the part's rules do not hold, or one with no book row: refused, never words made up", () => {
    expect(() => closedWithSalmon(page(), card(), index())).toThrow(OneClosureError);
    expect(() => closedWithSalmon(page(), card(), index(rule(NELSON, { row: "" })))).toThrow(/no book row name or line/);
  });
});
