/**
 * (T991 round 3) The per-tab Source row: the whole composed row is pinned as one string (AGENTS 33), and each synopsis
 * page's highlight boxes are pinned as data (owner 2026-11-22: "make sure highlighting is still there for each").
 */
import { describe, expect, it } from "vitest";
import { LICENCE_ROWS } from "./menu/licences";
import type { RegsPage, SheetVM } from "./regs/model";
import { ALL_SOURCES } from "./regs/words";
import { authorityUrl, conditionsSourcesOf, regsSourcesOf, salmonShowsDfo, SOURCE_AUTHORITIES, sourcesText, type SourceLinkVM } from "./tabSources";

const box = (pdf: number, y: number) => ({ pdf, box: [10, y, 200, y + 12] as [number, number, number, number], size: [612, 792] as [number, number] });
const book = (printed: number, pdf: number | null, b?: ReturnType<typeof box>[]) => ({ t: "book", printed, pdf, ...(b ? { box: b } : {}) });

/** A page: its drawn VM (`body`) and its every-source sheet's groups (`all`: where → rows' book Lines). */
function page(body: Record<string, unknown>, all: Record<string, unknown[][]> = {}): RegsPage {
  const groups = Object.entries(all).map(([where, rows]) => ({ where, rows: rows.map((b) => ({ key: null, title: null, quote: "", role: "", book: b })) }));
  const sheet = (k: string): SheetVM | null => (k === "src:all"
    ? { key: "src:all", title: "Sources", sub: null, blocks: groups.length ? [{ k: "sources", groups }] : [{ k: "para", text: [] }] } as unknown as SheetVM
    : null);
  return { ...body, sheet } as unknown as RegsPage;
}
const none = { inSeason: false, dfo: false };

describe("regsSourcesOf: one link per synopsis page the page cites (owner 2026-11-22: \"one for each page referenced\")", () => {
  // Skeena River, Class II part (live app 2026-11-22): its gear lines cite p.8 p.49 p.53, its closures p.9, its caveat p.53
  const skeena = page({
    keep: { whereNot: [[{ t: "text", s: "No fishing: within 23 m" }, book(9, 11, [box(11, 100)])], [book(9, 11, [box(11, 140)])]],
            caveats: [{ items: [{ line: [book(53, 55, [box(55, 400)])] }] }] },
    gear: { src: { who: "Province · Region 6 · This water’s ›", aria: "", open: "srcs:gear", keys: [],
                   pages: [{ printed: 8, pdf: 10, box: [box(10, 20)] }, { printed: 49, pdf: 51 }, { printed: 53, pdf: 55, box: [box(55, 400)] }] } },
  }, { [ALL_SOURCES.BOOK]: [[book(49, 51, [box(51, 600)])], [book(9, 11, [box(11, 100)])]] });

  it("lists every page in page order, the first named, each once, then All sources", () => {
    expect(sourcesText(regsSourcesOf(skeena, none))).toBe("Source B.C. Synopsis p.8 ↗ p.9 ↗ p.49 ↗ p.53 ↗ All sources ›");
  });
  it("each page link carries every box its citing rules hold on that page, once each (the opened copy marks them all)", () => {
    const links = regsSourcesOf(skeena, none).links.filter((l): l is Extract<SourceLinkVM, { k: "book" }> => l.k === "book");
    expect(links.map((l) => [l.printed, l.pdf, l.box ?? []])).toEqual([
      [8, 10, [box(10, 20)]],
      [9, 11, [box(11, 100), box(11, 140)]],
      [49, 51, [box(51, 600)]],
      [53, 55, [box(55, 400)]],
    ]);
  });
  it("each page's spoken name says what it opens: the page, and its marked passages where it has any (T60: not a rule's own link)", () => {
    expect(regsSourcesOf(skeena, none).links.flatMap((l) => (l.k === "book" ? [l.aria] : []))).toEqual([
      "B.C. Synopsis page 8, every passage this page cites marked, opens the PDF",
      "B.C. Synopsis page 9, every passage this page cites marked, opens the PDF",
      "B.C. Synopsis page 49, every passage this page cites marked, opens the PDF",
      "B.C. Synopsis page 53, every passage this page cites marked, opens the PDF",
    ]);
    expect(regsSourcesOf(page({ a: [book(7, 9)] }), none).links[0]).toEqual(
      { k: "book", label: "B.C. Synopsis p.7 ↗", printed: 7, pdf: 9, aria: "B.C. Synopsis page 7, opens the PDF" });
  });
  it("the water's own rule-book row is among them (T474: always linked), not repeated where the page already cites it (T60)", () => {
    expect(sourcesText(regsSourcesOf(skeena, { ...none, waterPages: [book(9, 11), book(98, 100, [box(100, 5)])] as never })))
      .toBe("Source B.C. Synopsis p.8 ↗ p.9 ↗ p.49 ↗ p.53 ↗ p.98 ↗ All sources ›");
  });
  it("a per-row gear source (srcs:gl|…, not drawn since T900) cites nothing", () => {
    const p = page({ gear: { lines: [{ src: { who: "", aria: "", open: "srcs:gl|hook", keys: [], pages: [{ printed: 22, pdf: 24 }] } }] } });
    expect(sourcesText(regsSourcesOf(p, none))).toBe("Source All sources ›");
  });
  it("a page the PDF map lacks is said unlinked, as every book run says it", () => {
    expect(sourcesText(regsSourcesOf(page({ a: [book(7, null)] }), none))).toBe("Source B.C. Synopsis p.7 All sources ›");
  });
});

describe("regsSourcesOf: only the authorities this page shows (owner 2026-11-22: \"condition sources don't need to be on regulations\")", () => {
  const p = page({ a: [book(9, 11)] });
  it("the Province's in-season changes only where the page shows one", () => {
    expect(sourcesText(regsSourcesOf(p, { inSeason: true, dfo: false }))).toBe("Source B.C. Synopsis p.9 ↗ B.C. in-season changes ↗ All sources ›");
  });
  it("DFO where the page shows DFO's salmon, or a rule of the page is DFO's", () => {
    expect(sourcesText(regsSourcesOf(p, { inSeason: true, dfo: true })))
      .toBe("Source B.C. Synopsis p.9 ↗ B.C. in-season changes ↗ DFO notices ↗ All sources ›");
    const q = page({}, { [ALL_SOURCES.DFO]: [[]] });
    expect(sourcesText(regsSourcesOf(q, none))).toBe("Source DFO notices ↗ All sources ›");
  });
  it("a local authority's cited page, by the name the rule gives it (T981)", () => {
    const q = page({}, { [ALL_SOURCES.LOCAL]: [[{ t: "web", s: "StanleyParkVan.com", url: "https://stanleyparkvan.com/x" }]] });
    const vm = regsSourcesOf(q, none);
    expect(sourcesText(vm)).toBe("Source StanleyParkVan.com ↗ All sources ›");
    expect(vm.links[0]).toEqual({ k: "web", label: "StanleyParkVan.com ↗", url: "https://stanleyparkvan.com/x" });
  });
  it("links each authority to its Sources-and-licences page, and All sources to the every-source sheet", () => {
    const vm = regsSourcesOf(p, { inSeason: true, dfo: true });
    expect(vm.links.slice(1)).toEqual([
      { k: "web", label: "B.C. in-season changes ↗", url: LICENCE_ROWS.find((r) => r.k === "inseason")!.url },
      { k: "web", label: "DFO notices ↗", url: LICENCE_ROWS.find((r) => r.k === "dfo")!.url },
      { k: "sheet", label: "All sources ›", open: "src:all", aria: "All sources for this page" },
    ]);
  });
});

describe("salmonShowsDfo", () => {
  it("a bar some of whose days are DFO's; a rule-book-only bar is not; no bar: the glance's DFO row", () => {
    expect([salmonShowsDfo({ by: "dfo" }, false), salmonShowsDfo({ by: "both" }, false), salmonShowsDfo({ by: "book" }, true),
            salmonShowsDfo(null, true), salmonShowsDfo(null, false)]).toEqual([true, true, false, true, false]);
  });
});

describe("conditionsSourcesOf", () => {
  it("names the gauges, the forecasts and the weather, never the synopsis or DFO", () => {
    expect(sourcesText(conditionsSourcesOf())).toBe("Source Environment Canada ↗ BC River Forecast Centre ↗ Open-Meteo ↗");
    expect(conditionsSourcesOf().links.map((l) => (l.k === "web" ? l.url : null))).toEqual([
      "https://wateroffice.ec.gc.ca",
      "https://www2.gov.bc.ca/gov/content/environment/air-land-water/water/drought-flooding-dikes-dams/river-forecast-centre",
      "https://open-meteo.com",
    ]);
  });
});

describe("SOURCE_AUTHORITIES", () => {
  it("every authority has a page on Sources and licences (a missing one throws, never a blank link)", () => {
    for (const id of Object.keys(SOURCE_AUTHORITIES) as (keyof typeof SOURCE_AUTHORITIES)[]) expect(authorityUrl(id)).toMatch(/^https:\/\//);
  });
});
