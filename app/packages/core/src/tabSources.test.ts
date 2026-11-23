/**
 * (T991 round 3) The per-tab Source row: the whole composed row is pinned as one string (AGENTS 33), and each synopsis
 * page's highlight boxes are pinned as data (owner 2026-11-22: "make sure highlighting is still there for each").
 */
import { describe, expect, it } from "vitest";
import { LICENCE_ROWS } from "./menu/licences";
import type { RegsPage } from "./regs/model";
import { authorityUrl, conditionsSourcesOf, drawnRegsOf, regsSourcesOf, salmonShowsDfo, SOURCE_AUTHORITIES, sourcesText, type SourceLinkVM } from "./tabSources";

const box = (pdf: number, y: number) => ({ pdf, box: [10, y, 200, y + 12] as [number, number, number, number], size: [612, 792] as [number, number] });
const book = (printed: number, pdf: number | null, b?: ReturnType<typeof box>[]) => ({ t: "book", printed, pdf, ...(b ? { box: b } : {}) });


/** A drawn Gear / Methods pair (`gearTabsFor`'s output) with its source line citing `pages`. */
const gearCiting = (pages: { printed: number; pdf: number; box?: ReturnType<typeof box>[] }[]) =>
  ({ src: { who: "Province ›", aria: "", open: "srcs:gear", keys: [], pages } }) as never;
const tabs = (gear: unknown, methods: unknown = null) => ({ gear, methods, salmonGear: null }) as never;
const page = (o: Record<string, unknown> = {}) =>
  ({ header: {}, keep: {}, boats: null, methods: null, gear: null, licence: null, sheet: () => { throw new Error("a sheet is not drawn"); }, ...o }) as unknown as RegsPage;
/** A rule index: `dfo` names the keys whose rule is DFO's own (`source: "dfo"`), and rix 7 is one (a trout table's `src`). */
const index = (dfo: readonly string[] = []) => {
  const view = (key: string, source: string) => ({ key, missing: false, f: { source } }) as never;
  return { get: (i: number) => view(`ix::${i}`, i === 7 && dfo.includes("#7") ? "dfo" : "book"), has: (i: number) => i === 7,
           byKey: (k: string) => (k.startsWith("e::") ? view(k, dfo.includes(k) ? "dfo" : "book") : null) };
};
const none = { waterPages: [] as never[], inSeason: false, dfo: false, rules: index() };

describe("regsSourcesOf: one link per synopsis page the DRAWN tabs cite (owner 2026-11-22: \"one for each page referenced\"; #63 review)", () => {
  // Skeena River, Class II part (live app 2026-11-22): its closures cite p.9, its caveat p.53, its gear line p.8 p.49 p.53
  const skeena = page({
    keep: { whereNot: [[{ t: "text", s: "No fishing: within 23 m" }, book(9, 11, [box(11, 100)])], [book(9, 11, [box(11, 140)])]],
            caveats: [{ items: [{ line: [book(53, 55, [box(55, 400)])] }] }] },
  });
  const drawn = drawnRegsOf(skeena, tabs(gearCiting([{ printed: 8, pdf: 10, box: [box(10, 20)] }, { printed: 49, pdf: 51 }, { printed: 53, pdf: 55, box: [box(55, 400)] }])), null, []);

  it("lists every page in page order, the first named, each once, then All sources", () => {
    expect(sourcesText(regsSourcesOf(drawn, none))).toBe("Source B.C. Synopsis p.8 ↗ p.9 ↗ p.49 ↗ p.53 ↗ All sources ›");
  });
  it("each page link carries every box the drawn lines cite on that page, once each (the opened copy marks them all)", () => {
    const links = regsSourcesOf(drawn, none).links.filter((l): l is Extract<SourceLinkVM, { k: "book" }> => l.k === "book");
    expect(links.map((l) => [l.printed, l.pdf, l.box ?? []])).toEqual([
      [8, 10, [box(10, 20)]],
      [9, 11, [box(11, 100), box(11, 140)]],
      [49, 51, []],
      [53, 55, [box(55, 400)]],
    ]);
  });
  it("each page's spoken name says what it opens (T60: not a rule's own link)", () => {
    expect(regsSourcesOf(drawn, none).links.flatMap((l) => (l.k === "book" ? [l.aria] : []))).toEqual([
      "B.C. Synopsis page 8, every passage this page cites marked, opens the PDF",
      "B.C. Synopsis page 9, every passage this page cites marked, opens the PDF",
      "B.C. Synopsis page 49, opens the PDF",
      "B.C. Synopsis page 53, every passage this page cites marked, opens the PDF",
    ]);
  });
  it("(owner: \"Page 9 is not the water body page\") the water's own row is marked, and listed even where no drawn line links it (T474)", () => {
    const vm = regsSourcesOf(drawn, { ...none, waterPages: [book(53, 55, [box(55, 400)]), book(98, 100)] as never });
    expect(sourcesText(vm)).toBe("Source B.C. Synopsis p.8 ↗ p.9 ↗ p.49 ↗ p.53 (this water) ↗ p.98 (this water) ↗ All sources ›");
    expect(vm.links.flatMap((l) => (l.k === "book" ? [l.aria] : [])).slice(3)).toEqual([
      "B.C. Synopsis page 53, this water's own entry, every passage this page cites marked, opens the PDF",
      "B.C. Synopsis page 98, this water's own entry, opens the PDF",
    ]);
    expect(sourcesText(regsSourcesOf([], { ...none, waterPages: [book(22, 24)] as never }))).toBe("Source B.C. Synopsis p.22 (this water) ↗ All sources ›");
  });
  it("a per-row gear source (srcs:gl|…, not drawn since T900) cites nothing", () => {
    const g = { lines: [{ src: { who: "", aria: "", open: "srcs:gl|hook", keys: [], pages: [{ printed: 22, pdf: 24 }] } }] };
    expect(sourcesText(regsSourcesOf(drawnRegsOf(page(), tabs(g), null, []), none))).toBe("Source All sources ›");
  });
  it("a page the PDF map lacks is said unlinked, as every book run says it", () => {
    expect(sourcesText(regsSourcesOf([[book(7, null)]], none))).toBe("Source B.C. Synopsis p.7 All sources ›");
    expect(sourcesText(regsSourcesOf([], { ...none, waterPages: [book(7, null)] as never }))).toBe("Source B.C. Synopsis p.7 (this water) All sources ›");
  });
});

describe("drawnRegsOf: what the Regulations tab draws (#63 review)", () => {
  it("a day only salmon may be fished: the Gear / Methods drawn (the day's gear \"if fished\"), never page.gear (the closed day's)", () => {
    const closedDay = page({ gear: gearCiting([{ printed: 64, pdf: 66 }]) });
    expect(sourcesText(regsSourcesOf(drawnRegsOf(closedDay, tabs(gearCiting([{ printed: 8, pdf: 10 }])), null, []), none)))
      .toBe("Source B.C. Synopsis p.8 ↗ All sources ›");
  });
  it("Methods' boats and the Handling fish reference only while Methods is drawn; the Licence tab as drawn; never a sheet", () => {
    const p = page({ boats: [[book(70, 72)]], methods: { reference: { lines: [[book(6, 8)]] } } });
    const lic = { lines: [[book(10, 12)]] } as never;
    expect(sourcesText(regsSourcesOf(drawnRegsOf(p, tabs(null, {}), lic, []), none))).toBe("Source B.C. Synopsis p.6 ↗ p.10 ↗ p.70 ↗ All sources ›");
    expect(sourcesText(regsSourcesOf(drawnRegsOf(p, tabs(null, null), null, []), none))).toBe("Source All sources ›");
    // (#63 re-review probe) the Methods tab as drawn carries its own `reference` (the "if fished" day's): not drawn, never cited
    expect(sourcesText(regsSourcesOf(drawnRegsOf(page(), tabs(null, { reference: { lines: [[book(10, 12)]] } }), null, []), none))).toBe("Source All sources ›");
  });
});

describe("regsSourcesOf: only the authorities this page shows (owner 2026-11-22: \"condition sources don't need to be on regulations\")", () => {
  const drawn = [[book(9, 11)]];
  it("the Province's in-season changes only where the page shows one", () => {
    expect(sourcesText(regsSourcesOf(drawn, { ...none, inSeason: true }))).toBe("Source B.C. Synopsis p.9 ↗ B.C. in-season changes ↗ All sources ›");
  });
  it("DFO only where the page shows DFO's salmon", () => {
    expect(sourcesText(regsSourcesOf(drawn, { ...none, inSeason: true, dfo: true })))
      .toBe("Source B.C. Synopsis p.9 ↗ B.C. in-season changes ↗ DFO notices ↗ All sources ›");
  });
  it("(#63 re-review) DFO, too, where a drawn rule is DFO's own (a DFO override): named by a sheet key, a source line's keys or a trout row's index", () => {
    const line = (k: string) => [{ src: { who: "", aria: "", open: "srcs:gear", keys: [k], pages: [] } }];
    expect(sourcesText(regsSourcesOf(line("e::r1"), { ...none, rules: index(["e::r1"]) }))).toBe("Source DFO notices ↗ All sources ›");
    expect(sourcesText(regsSourcesOf([[{ t: "link", s: "x", open: "rule:e::r2" }]], { ...none, rules: index(["e::r2"]) }))).toBe("Source DFO notices ↗ All sources ›");
    expect(sourcesText(regsSourcesOf([{ src: [7] }], { ...none, rules: index(["#7"]) }))).toBe("Source DFO notices ↗ All sources ›");
    // a drawn rule that is the book's, and a DFO rule nothing drawn names: no DFO
    expect(sourcesText(regsSourcesOf(line("e::r1"), { ...none, rules: index(["e::r9"]) }))).toBe("Source All sources ›");
  });
  it("a local authority's cited page a drawn rule links, by the name the rule gives it (T981)", () => {
    const vm = regsSourcesOf([[{ t: "web", s: "StanleyParkVan.com", url: "https://stanleyparkvan.com/x" }]], none);
    expect(sourcesText(vm)).toBe("Source StanleyParkVan.com ↗ All sources ›");
    expect(vm.links[0]).toEqual({ k: "web", label: "StanleyParkVan.com ↗", url: "https://stanleyparkvan.com/x" });
  });
  it("links each authority to its Sources-and-licences page, and All sources to the every-source sheet", () => {
    const vm = regsSourcesOf(drawn, { ...none, inSeason: true, dfo: true });
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
