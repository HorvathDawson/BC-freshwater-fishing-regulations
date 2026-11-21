/** (T990) the page's every-source sheet: the union of the sections' rules by source, no duplicates; in-season after the synopsis. */
import { describe, expect, it } from "vitest";
import { ALL_SOURCES_KEY, allSourcesSheet, withInSeason } from "./allsources";
import { ALL_SOURCES } from "../words";
import { sheetOf } from "../sheets";
import { parseSheetKey } from "../sheetKey";
import type { InSeasonBannerVM } from "../../inseason";
import type { SheetCtx, SheetVM } from "../model";

const rule = (key: string, extra: Record<string, unknown> = {}, f: Record<string, unknown> = {}) => ({ key, missing: false, f, export: { verbatim: `words ${key}` },
  facts: { title: `Rule ${key}`, book: [[40, 42]], ...extra } });
const RULES: Record<string, unknown> = {
  "a::1": rule("a::1"),
  "b::1": rule("b::1", {}, { source: "dfo" }),
  "c::1": rule("c::1", { book: [], cited: { link: "StanleyParkVan.com", url: "https://stanleyparkvan.com/x.html", quote: "q" } }),
};
const src = (keys: string[]) => ({ who: "Province ›", aria: "Sources: Province", open: "srcs:gear", keys, pages: [] });
const ctx = (): SheetCtx => ({
  inp: { rules: { byKey: (k: string) => RULES[k] ?? null, get: () => { throw new Error("no"); } } },
  page: { gear: { src: src(["a::1", "b::1"]), mode: null, lines: [], bait: { src: src(["a::1"]) } },
    methods: { ways: { src: src(["c::1", "b::1"]) }, boats: null, reference: null }, keep: { trout: null },
    header: { name: "Lost Lagoon" }, day: { month: 7, day: 15 } },
  today: { month: 7, day: 15 },
} as unknown as SheetCtx);
const rowsOf = (vm: SheetVM) => (vm.blocks[0] as unknown as { groups: { where: string; rows: { key: string | null; book: unknown }[] }[] }).groups;
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
