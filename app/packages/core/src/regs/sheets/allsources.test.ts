/**
 * (T990) the page's every-source sheet: the rules by source, no duplicates; in-season after the synopsis. (T1036) Built from
 * what the screen DRAWS (`drawnRegsOf`), never from `page.gear` / `page.methods` or the card model: the "if fished" gear on a
 * day only salmon may be fished, nothing of a closed day's gear, a drawn line's own rule, the drawn trout table's rules.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { ALL_SOURCES_KEY, AllSourcesRuleMissing, allSourcesSheet, drawnRegsOf, drawnSourceRules, withDrawnSources, withInSeason } from "./allsources";
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
type Groups = { where: string; rows: { key: string | null; title: string | null; quote: string; book: unknown }[] }[];
const rowsOf = (vm: SheetVM) => (vm.blocks[0] as unknown as { k: string; groups: Groups }).groups;
const keysOf = (vm: SheetVM) => (vm.blocks[0]!.k === "sources" ? rowsOf(vm).flatMap((g) => g.rows.map((r) => r.key)) : []);
const BANNER = { id: "i1", head: "In-season change (Jul 1)", text: "Closed", url: "https://gov.bc.ca/n" } as InSeasonBannerVM;

describe("the page's every-source sheet (T990)", () => {
  it("lists book, DFO and the local authority's citation once each, however many sections cite them", () => {
    const g = rowsOf(allSourcesSheet(ctx()));
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
    const vm = allSourcesSheet(ctx(page({ status: "closed", gear: gear(["a::1", "b::1"], "Closed to fishing for game fish") })));
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
    expect(keysOf(allSourcesSheet(ctx(page({ keep }))))).toEqual(["w::1", "a::1", "b::1", "c::1"]);
  });
  it("the drawn trout table's rules are listed (its Source line opens them)", () => {
    expect(keysOf(allSourcesSheet(ctx(page({ keep: { trout: { kind: "table", src: [0] } }, gear: null, methods: null }))))).toEqual(["t::1"]);
  });
  it("a rule cited twice is one row; a key the index lacks names nothing", () => {
    expect(drawnSourceRules([src(["a::1", "zz::9"]), src(["a::1"], "srcs:always")], INDEX).map((r) => r.key)).toEqual(["a::1"]);
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
    expect(reports.map((e) => [(e as Error).name, (e as AllSourcesRuleMissing).rule])).toEqual([["AllSourcesRuleMissing", "gone::1"]]);
  });
  it("any other key is the page's own sheet", () => {
    const own = { key: "hatch", title: "t", sub: null, blocks: [] } as SheetVM;
    expect(withDrawnSources({ ...host, sheet: (k: string) => (k === "hatch" ? own : null) } as unknown as RegsPage, [], INDEX)("hatch")).toBe(own);
  });
});
