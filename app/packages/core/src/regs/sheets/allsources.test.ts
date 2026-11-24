/**
 * (T990) the page's every-source sheet: the rules by source, no duplicates; in-season after the synopsis. (T1036) Built from
 * what the screen DRAWS (`drawnRegsOf`), never from `page.gear` / `page.methods` or the card model: the "if fished" gear on a
 * day only salmon may be fished, nothing of a closed day's gear, a drawn line's own rule, the drawn trout table's rules.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { ALL_SOURCES_KEY, AllSourcesRuleMissing, allSourcesSheet, drawnCitations, drawnRegsOf, drawnSourceRules, reachableSheets, withDrawnSources, withInSeason } from "./allsources";
import { ALL_SOURCES } from "../words";
import { sheetOf } from "../sheets";
import { parseSheetKey } from "../sheetKey";
import { setFailureSink } from "../../failure";
import { gearTabsFor } from "../../salmon";
import type { InSeasonBannerVM } from "../../inseason";
import type { RegsPage, SheetCtx, SheetVM } from "../model";

const rule = (key: string, extra: Record<string, unknown> = {}, f: Record<string, unknown> = {}) => ({ key, missing: false, f, export: { verbatim: `words ${key}` },
  facts: { title: `Rule ${key}`, book: [[40, 42]], ...extra } });
const RULES: Record<string, unknown> = {
  "a::1": rule("a::1"),
  "b::1": rule("b::1", {}, { source: "dfo" }),
  "c::1": rule("c::1", { book: [], cited: { link: "StanleyParkVan.com", url: "https://stanleyparkvan.com/x.html", quote: "q" } }),
  "m::1": { ...rule("m::1"), missing: true },   // the index's stand-in for a rule it does not hold
  "e::1": { ...rule("e::1"), notes: [] }, "f::1": rule("f::1"), "n::1": rule("n::1"), "q::1": rule("q::1"), "u::1": rule("u::1"), "v::1": rule("v::1"),
  "x::1": rule("x::1"), "y::1": rule("y::1"), "z::1": rule("z::1"), "o::1": rule("o::1"), "d::1": rule("d::1", {}, { source: "dfo" }),
  "g::1": rule("g::1"), "h::1": rule("h::1"), "w::1": rule("w::1"), "p::1": rule("p::1"), "t::1": rule("t::1"), "k::1": rule("k::1"),
};
const BY_IX = ["t::1"];
const INDEX = { byKey: (k: string) => RULES[k] ?? null, get: (i: number) => RULES[BY_IX[i]!], has: (i: number) => i < BY_IX.length } as never;
const src = (keys: string[], open = "srcs:gear") => ({ who: "Province ›", aria: "Sources: Province", open, keys, pages: [] });
const gear = (keys: string[], closed: string | null = null) =>
  ({ closed, closedMore: [], day: null, mode: null, lines: [{ src: src(["k::1"], "srcs:gl|hook") }], bait: { src: src(["a::1"], "srcs:bait") }, src: src(keys) });
const methods = () => ({ ways: { src: src(["c::1", "b::1"], "srcs:ways") }, boats: null, reference: null });
const KEEP = { trout: null };
const page = (over: Record<string, unknown> = {}) => ({ status: "open", gear: gear(["a::1", "b::1"]), methods: methods(), keep: KEEP,
  boats: null, licence: null, header: { name: "Lost Lagoon" }, day: { month: 7, day: 15 }, ...over });
const ctx = (p = page()): SheetCtx => ({ inp: { rules: INDEX }, page: p, today: { month: 7, day: 15 } } as unknown as SheetCtx);
/** the page's own every-source sheet, its sheets the page's own (`sheetOf`), as sheets.ts builds it */
const pageSheet = (c: SheetCtx): SheetVM => allSourcesSheet(c, (k) => sheetOf(c, k));
type Groups = { where: string; rows: { key: string | null; title: string | null; quote: string; book: unknown }[] }[];
const rowsOf = (vm: SheetVM) => (vm.blocks[0] as unknown as { k: string; groups: Groups }).groups;
const keysOf = (vm: SheetVM) => (vm.blocks[0]!.k === "sources" ? rowsOf(vm).flatMap((g) => g.rows.map((r) => r.key)) : []);
const BANNER = { id: "i1", head: "In-season change (Jul 1)", text: "Closed", url: "https://gov.bc.ca/n" } as InSeasonBannerVM;

describe("the page's every-source sheet (T990)", () => {
  it("lists book, DFO and the local authority's citation once each, however many sections cite them", () => {
    const g = rowsOf(pageSheet(ctx()));
    expect(g.map((x) => [x.where, x.rows.map((r) => r.key)])).toEqual([
      [ALL_SOURCES.BOOK, ["a::1"]], [ALL_SOURCES.DFO, ["b::1"]], [ALL_SOURCES.LOCAL, ["c::1"]]]);
    expect(g[2]!.rows[0]!.book).toEqual([{ t: "web", s: "StanleyParkVan.com", url: "https://stanleyparkvan.com/x.html" }]);
  });
  it("a water with book + in-season + DFO lists all three; a repeated notice is one row", () => {
    const sheet = withInSeason((k) => sheetOf(ctx(), k), [BANNER, BANNER])(ALL_SOURCES_KEY)!;
    const g = rowsOf(sheet);
    expect(g.map((x) => x.where)).toEqual([ALL_SOURCES.BOOK, ALL_SOURCES.INSEASON, ALL_SOURCES.DFO, ALL_SOURCES.LOCAL]);
    expect(g[1]!.rows).toHaveLength(1);
  });
  it("is a key the page opens, and other keys pass through", () => {
    expect(parseSheetKey("src:all")).toBe("src:all");
    expect(sheetOf(ctx(), ALL_SOURCES_KEY)?.title).toBe(ALL_SOURCES.TITLE);
    expect(withInSeason(() => null, [BANNER])("hatch")).toBeNull();
  });
});

describe("the every-source sheet lists what the drawn screen cites (T1036)", () => {
  it("a closed day: the page's own Gear and Methods are not drawn, so none of their rules is listed", () => {
    const vm = pageSheet(ctx(page({ status: "closed", gear: gear(["a::1", "b::1"], "Closed to fishing for game fish") })));
    expect([vm.blocks, vm.sub]).toEqual([[{ k: "para", text: [{ t: "text", s: ALL_SOURCES.NONE }] }], "Lost Lagoon · Jul 15"]);
  });
  it("a day only salmon may be fished: the \"if fished\" gear's rules are listed, the closed day's gear's are not", () => {
    const p = page({ status: "closed", gear: gear(["h::1"], "Closed to fishing for game fish"), methods: null });
    const tabs = gearTabsFor(p as never, { gear: gear(["g::1"]) as never, methods: null }, { status: "open" } as never, null);
    const sheet = withDrawnSources({ ...p, sheet: () => null } as unknown as RegsPage, drawnRegsOf(p as never, tabs, null, []), INDEX);
    expect(keysOf(sheet(ALL_SOURCES_KEY)!)).toEqual(["a::1", "g::1"]);
  });
  it("a drawn line's own rule is listed where it shows the rule's page; a row that only opens a sheet, and a per-row gear line, are not", () => {
    const keep = { trout: null,
      whereNot: { heading: "Where you can’t fish", scope: null, items: [{ rule: "w::1", text: [{ t: "text", s: "No fishing" }, { t: "book", printed: 9, pdf: 11 }] }] },
      possession: { label: "Possession", open: "pos:pos", rule: "p::1", says: "Possession: 2 days’ quota.", value: "2 days’ quota", also: [] } };
    // (this hand-made context holds no glossary for Possession's sheet: the sheets one tap away are the second group's test)
    expect(keysOf(allSourcesSheet(ctx(page({ keep })), () => null))).toEqual(["w::1", "a::1", "b::1", "c::1"]);
  });
  it("the drawn trout table's rules are listed (its Source line opens them)", () => {
    expect(keysOf(pageSheet(ctx(page({ keep: { trout: { kind: "table", src: [0] } }, gear: null, methods: null }))))).toEqual(["t::1"]);
  });
  it("a rule cited twice is one row; a key the index lacks names nothing", () => {
    expect(drawnSourceRules([src(["a::1", "zz::9"]), src(["a::1"], "srcs:always")], INDEX).map((r) => r.key)).toEqual(["a::1"]);
  });
});

describe("a cited rule the page's index lacks (T1036, W46)", () => {
  let reports: unknown[] = [], undo = () => {};
  beforeEach(() => { reports = []; undo = setFailureSink((e) => reports.push(e)); });
  afterEach(() => undo());
  const said = () => reports.map((e) => [(e as Error).name, (e as AllSourcesRuleMissing).rule, (e as AllSourcesRuleMissing).by]);
  it("a source line's key, a line's rule and a trout table's index: each reported once, and no row for any", () => {
    const drawn = [src(["a::1", "zz::9", "m::1"]), src(["zz::9"], "srcs:always"),
      { rule: "yy::1", text: [{ t: "book", printed: 9, pdf: 11 }] }, { kind: "table", src: [0, 5] }];
    expect(drawnCitations(drawn, INDEX)).toEqual({ rules: [RULES["a::1"], RULES["t::1"]], missing: ["zz::9", "m::1", "yy::1", "#5"] });
    const host = { sheet: () => null, header: { name: "Lost Lagoon" }, day: { month: 7, day: 15 } } as unknown as RegsPage;
    expect(keysOf(withDrawnSources(host, drawn, INDEX)(ALL_SOURCES_KEY)!)).toEqual(["a::1", "t::1"]);
    expect(said()).toEqual([["AllSourcesRuleMissing", "zz::9", "line"], ["AllSourcesRuleMissing", "m::1", "line"], ["AllSourcesRuleMissing", "yy::1", "line"],
      ["AllSourcesRuleMissing", "#5", "line"]]);
  });
  it("the page's own sheet reports them too; a page citing only what its index holds reports nothing", () => {
    pageSheet(ctx(page({ gear: gear(["a::1", "gone::1"]) })));
    pageSheet(ctx());
    expect(said()).toEqual([["AllSourcesRuleMissing", "gone::1", "line"]]);
  });
});

describe("the salmon card's book closures in the every-source sheet (T1036)", () => {
  let reports: unknown[] = [], undo = () => {};
  beforeEach(() => { reports = []; undo = setFailureSink((e) => reports.push(e)); });
  afterEach(() => undo());
  const host = { sheet: () => null, header: { name: "Skeena River" }, day: { month: 9, day: 10 } } as unknown as RegsPage;
  const closure = (r: string) => ({ rule: r, water: "Skeena River", words: "Closed to all fishing (all year)", pages: [{ printed: 77, pdf: 83 }, { printed: 78, pdf: null }] });
  it("a closure's rule is listed once, beside the drawn lines' rules", () => {
    const sheet = withDrawnSources(host, [src(["a::1"])], INDEX, [closure("k::1"), closure("a::1")]);
    expect([keysOf(sheet(ALL_SOURCES_KEY)!), reports]).toEqual([["a::1", "k::1"], []]);
  });
  it("a closure whose rule the page lacks is said as its card line says it, its pages linked, and reported (W46)", () => {
    const g = rowsOf(withDrawnSources(host, [], INDEX, [closure("gone::1")])(ALL_SOURCES_KEY)!);
    expect(g).toEqual([{ where: ALL_SOURCES.BOOK, rows: [{ key: null, title: "Skeena River", quote: "Closed to all fishing (all year)", role: "",
      book: [{ t: "book", printed: 77, pdf: 83 }, { t: "text", s: " " }, { t: "book", printed: 78, pdf: null }] }] }]);
    expect(reports.map((e) => [(e as Error).name, (e as AllSourcesRuleMissing).rule, (e as AllSourcesRuleMissing).by])).toEqual([["AllSourcesRuleMissing", "gone::1", "closure"]]);
  });
  it("any other key is the page's own sheet", () => {
    const own = { key: "hatch", title: "t", sub: null, blocks: [] } as SheetVM;
    expect(withDrawnSources({ ...host, sheet: (k: string) => (k === "hatch" ? own : null) } as unknown as RegsPage, [], INDEX)("hatch")).toBe(own);
  });
});

/**
 * (T1036, owner 2026-11-24: "Add back but maybe have them separate?") THE SECOND GROUP: under its own head, below the drawn
 * lines' rules, every rule a sheet one tap from the drawn screen cites (a fish row's, both its tabs; Possession's) that no
 * drawn line cites. Never an "Overruled" card, never a sheet two taps away, never a sheet no drawn line opens.
 */
describe("the every-source sheet's second group: what the sheets one tap away cite (T1036, owner 2026-11-24)", () => {
  let reports: unknown[] = [], undo = () => {};
  beforeEach(() => { reports = []; undo = setFailureSink((e) => reports.push(e)); });
  afterEach(() => undo());
  const book = (printed: number) => ({ t: "book", printed, pdf: printed + 2 });
  const link = (open: string) => ({ t: "link", s: "Who says so ›", open });
  const rows = (keys: string[], overruled = false) =>
    ({ k: "sources", ...(overruled ? { overruled: true } : {}), groups: [{ where: "B.C. Synopsis", rows: keys.map((key) => ({ key, title: null, quote: "q", role: "", book: [] })) }] });
  const SHEETS: Record<string, unknown> = {
    // a fish row's sheet, its Sources tab: two listed (a::1 is drawn already), one overruled, a link two taps away
    "other:o0": { key: "other:o0", title: "Kokanee", sub: null, tabs: [{ label: "Identification", open: "other:o0|id", on: false }, { label: "Sources", open: "other:o0", on: true }],
                  blocks: [rows(["g::1", "a::1", "d::1"]), rows(["h::1"], true), { k: "link", line: [{ t: "link", s: "See every regulation line ›", open: "src:r1" }] }] },
    // its Identification tab: a source line naming a rule beside its page
    "other:o0|id": { key: "other:o0|id", title: "Kokanee", sub: null, blocks: [{ k: "source", line: [link("rule:x::1"), { t: "text", s: " " }, book(49)] }] },
    // two taps away (the row's every line): not followed
    "src:r1": { key: "src:r1", title: "t", sub: null, blocks: [rows(["w::1"])] },
    // Possession: its "Who says so ›" beside p.13; a rule link with no page beside it cites nothing; a rule the index lacks
    "pos:pos": { key: "pos:pos", title: "Possession", sub: null, blocks: [{ k: "source", line: [link("rule:p::1"), { t: "text", s: " " }, book(13)] },
                 { k: "link", line: [link("rule:t::1")] }, { k: "fold", summary: "s", open: false, body: [rows(["m::1"])] }] },
    // a per-row gear line's sheet (not drawn since T900) and a sheet no drawn line opens (a closed day's card): never read
    "srcs:gl|hook": { key: "srcs:gl|hook", title: "t", sub: null, blocks: [rows(["y::1"])] },
    "other:o5": { key: "other:o5", title: "t", sub: null, blocks: [rows(["z::1"])] },
  };
  const keep = { trout: null, otherFish: [{ name: "Kokanee", open: "other:o0", id: "other:o0|id" }],
                 possession: { label: "Possession", open: "pos:pos", rule: "p::1", says: "Possession: 2 days’ quota.", value: "2 days’ quota", also: [] } };
  const p = page({ keep, methods: null });
  const sheetOfPage = (k: string) => (SHEETS[k] ?? null) as SheetVM | null;
  const built = () => withDrawnSources({ ...p, sheet: sheetOfPage } as unknown as RegsPage,
    drawnRegsOf(p as never, gearTabsFor(p as never, null, null, null), null, []), INDEX)(ALL_SOURCES_KEY)!;
  const groupsOf = (b: unknown) => (b as { groups: Groups }).groups.map((g) => [g.where, g.rows.map((r) => r.key)]);

  it("below the drawn rules, under its head: each rule a reachable sheet cites that no drawn line does, by source", () => {
    const vm = built();
    expect(vm.blocks.map((b) => b.k)).toEqual(["sources", "head", "sources"]);
    expect(groupsOf(vm.blocks[0])).toEqual([[ALL_SOURCES.BOOK, ["a::1"]], [ALL_SOURCES.DFO, ["b::1"]]]);
    expect(vm.blocks[1]).toEqual({ k: "head", text: "Also in the fish and possession sheets" });
    // g::1 and d::1 (the fish sheet's list), x::1 (its other tab's line), p::1 (Possession's): never a::1 again, never
    // h::1 (overruled), w::1 (two taps), t::1 (no page beside it), y::1 / z::1 (no drawn line opens their sheets)
    expect(groupsOf(vm.blocks[2])).toEqual([[ALL_SOURCES.BOOK, ["g::1", "x::1", "p::1"]], [ALL_SOURCES.DFO, ["d::1"]]]);
    // m::1, cited by Possession's sheet and not held by the index: reported, no row (W46)
    expect(reports.map((e) => [(e as Error).name, (e as AllSourcesRuleMissing).rule, (e as AllSourcesRuleMissing).by]))
      .toEqual([["AllSourcesRuleMissing", "m::1", "sheet"]]);
  });
  it("(review of PR 126) a sheet the screen opens from a key it BUILDS (OPEN): trout rows, hatchery, rule lines, terms, salmon rows", () => {
    const S: Record<string, unknown> = {
      "tf:RB": { key: "tf:RB", title: "t", sub: null, tabs: [{ label: "i", open: "tf:RB|id", on: false }, { label: "s", open: "tf:RB", on: true }], blocks: [rows(["e::1"])] },
      "tf:RB|id": { key: "tf:RB|id", title: "t", sub: null, blocks: [] },
      hatch: { key: "hatch", title: "t", sub: null, blocks: [rows(["f::1"])] },
      "rule:n::1": { key: "rule:n::1", title: "t", sub: null, blocks: [] },          // a caveat line's rule: its own sheet
      "rule:q::1": { key: "rule:q::1", title: "t", sub: null, blocks: [] },          // a closed banner's "Who says so ›"
      "rule:u::1": { key: "rule:u::1", title: "t", sub: null, blocks: [] },          // a boat chip
      "gl:bait_ban": { key: "gl:bait_ban", title: "t", sub: null, blocks: [rows(["v::1"])] },
      "gl:fly_fishing": { key: "gl:fly_fishing", title: "t", sub: null, blocks: [rows(["x::1"])] },
      "sp:CO|src": { key: "sp:CO|src", title: "t", sub: null, blocks: [rows(["d::1"])] },
      "sp:CO": { key: "sp:CO", title: "t", sub: null, blocks: [] },
    };
    const drawn = [
      { trout: { kind: "table", boxes: [{ rows: [{ key: "RB", name: "Rainbow trout" }] }], hatchLink: "Hatchery or wild? ›" } },
      { items: [{ what: [{ t: "text", s: "No fishing near the fence" }], where: null, rule: "n::1", fish: null, book: [] }] },
      // a closure that names no page (with one, its rule is a drawn line's: the first group's)
      { chain: [{ text: "Closed all year", scope: "this water", rule: "q::1", book: [] }] },
      [{ kind: "motor", text: "No powered boats", rule: "u::1" }],
      { line: [{ t: "text", s: "Bait ban" }, { t: "term", ids: ["bait_ban"] }] },
      { pill: { term: "fly_fishing" } },
      { species: [{ code: "CO", statusLabel: "Open" }] },
    ];
    const vm = withDrawnSources({ ...page({ methods: null }), sheet: (k: string) => (S[k] ?? null) as SheetVM | null } as unknown as RegsPage,
                                drawn, INDEX)(ALL_SOURCES_KEY)!;
    expect(vm.blocks.map((b) => b.k)).toEqual(["para", "head", "sources"]);
    expect(groupsOf(vm.blocks[2])).toEqual([[ALL_SOURCES.BOOK, ["e::1", "f::1", "n::1", "q::1", "u::1", "v::1", "x::1"]], [ALL_SOURCES.DFO, ["d::1"]]]);
    expect(reachableSheets(drawn)).toEqual(["tf:RB", "tf:RB|id", "hatch", "rule:n::1", "rule:q::1", "rule:u::1", "gl:bait_ban", "gl:fly_fishing", "sp:CO|src", "sp:CO"]);
    // a row that opens its own sheet draws no ⓘ for its term (Possession): no gl: sheet of it
    expect(reachableSheets([{ open: "pos:pos", term: "possession_quota" }])).toEqual(["pos:pos"]);
  });
  it("(review of PR 126) the page's own every-source sheet always has its sheets: the second group, never left out", () => {
    // sheets.ts builds it with the page's own sheetOf; a lookup that holds a reachable sheet lists its rules
    const keepP = { trout: null, possession: { label: "Possession", open: "pos:pos", rule: "p::1", says: "s", value: "v", also: [] } };
    const vm = allSourcesSheet(ctx(page({ keep: keepP, methods: null })), (k) => (k === "pos:pos" ? SHEETS["pos:pos"] as SheetVM : null));
    expect(groupsOf(vm.blocks[2])).toEqual([[ALL_SOURCES.BOOK, ["p::1"]]]);
    // and the page's sheets build it so (sheets.ts `sheetOf`): a caveat line's rule sheet, one tap away, is in the second group
    const c = ctx(page({ keep: { trout: null, caveats: [{ items: [{ what: [{ t: "text", s: "w" }], where: null, rule: "e::1", fish: null, book: [] }] }] }, methods: null }));
    expect(groupsOf(sheetOf(c, ALL_SOURCES_KEY)!.blocks[2])).toEqual([[ALL_SOURCES.BOOK, ["e::1"]]]);
  });
  it("the sheets one tap away: what drawn lines open, never the every-source sheet itself or a per-row gear line's", () => {
    expect(reachableSheets([{ open: "src:all" }, { open: "srcs:gl|hook" }, { open: "pos:pos" }, { open: "other:o0" }, { open: "gear" }, { open: "pos:pos" }]))
      .toEqual(["pos:pos", "other:o0"]);
  });
  it("nothing reachable cites more: no head, no second group; the in-season changes keep the second group after the first", () => {
    const none = withDrawnSources({ ...page({ methods: null }), sheet: () => null } as unknown as RegsPage,
      drawnRegsOf(page({ methods: null }) as never, gearTabsFor(page({ methods: null }) as never, null, null, null), null, []), INDEX)(ALL_SOURCES_KEY)!;
    expect(none.blocks.map((b) => b.k)).toEqual(["sources"]);
    const vm = withInSeason(() => built(), [BANNER])(ALL_SOURCES_KEY)!;
    expect([vm.blocks.map((b) => b.k), groupsOf(vm.blocks[0]).map((g) => g[0])]).toEqual([["sources", "head", "sources"], [ALL_SOURCES.BOOK, ALL_SOURCES.INSEASON, ALL_SOURCES.DFO]]);
  });
  it("a page whose drawn lines cite nothing still lists what its sheets cite, under the head", () => {
    const bare = page({ keep, gear: null, methods: null });
    const vm = withDrawnSources({ ...bare, sheet: sheetOfPage } as unknown as RegsPage,
      drawnRegsOf(bare as never, gearTabsFor(bare as never, null, null, null), null, []), INDEX)(ALL_SOURCES_KEY)!;
    expect(vm.blocks.map((b) => b.k)).toEqual(["para", "head", "sources"]);
    expect(groupsOf(vm.blocks[2])).toEqual([[ALL_SOURCES.BOOK, ["g::1", "a::1", "x::1", "p::1"]], [ALL_SOURCES.DFO, ["d::1"]]]);
  });
});
