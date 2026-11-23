/**
 * (T1031 item 3, owner 2026-11-22: "clean up our inline sources. Right now the way they display is inconsistent") THE ONE
 * SOURCE LINE: every inline source is composed by `sourceLineOf` — label "Source", the synopsis pages (the name on the
 * first only, ascending, once each, highlights joined), the web pages, at most one sheet. Each call site's composer is
 * pinned here by its whole composed string (AGENTS 33) and by what each link opens and says aloud (T60).
 */
import { describe, expect, it } from "vitest";
import type { RegsPage, SheetVM } from "./regs/model";
import type { SalmonCardVM } from "./salmon";
import {
  inSeasonSourcesOf, lineSourcesOf, salmonBookSourcesOf, salmonDfoSourcesOf, salmonWithPdf, sectionSourcesOf, sourceLineOf, sourcesText,
  troutSourcesOf, type SourceLinkVM,
} from "./tabSources";

const box = (pdf: number, y: number) => ({ pdf, box: [10, y, 200, y + 12] as [number, number, number, number], size: [612, 792] as [number, number] });
const arias = (links: readonly SourceLinkVM[]) => links.map((l) => (l.k === "web" ? l.url : l.aria));

describe("sourceLineOf: the one format", () => {
  it("pages ascending, each once with its boxes joined, the name on the first; then web pages once each; then the sheet", () => {
    const vm = sourceLineOf({
      pages: [{ printed: 49, pdf: 51 }, { printed: 8, pdf: 10, box: [box(10, 1)] }, { printed: 8, pdf: 10, box: [box(10, 1), box(10, 2)] }],
      marks: "the passages behind Gear",
      web: [{ name: "B.C. in-season changes", url: "https://a" }, { name: "B.C. in-season changes", url: "https://a" }],
      sheet: { label: "Who says so ›", open: "srcs:gear", aria: "Who says so: the rules behind Gear" },
    });
    expect(sourcesText(vm)).toBe("Source B.C. Synopsis p.8 ↗ p.49 ↗ B.C. in-season changes ↗ Who says so ›");
    expect(vm.links[0]).toEqual({ k: "book", label: "B.C. Synopsis p.8 ↗", printed: 8, pdf: 10, box: [box(10, 1), box(10, 2)],
                                  aria: "B.C. Synopsis page 8, the passages behind Gear marked, opens the PDF" });
    // a page with no box marks nothing, so it says nothing of marks
    expect(arias(vm.links)).toEqual(["B.C. Synopsis page 8, the passages behind Gear marked, opens the PDF",
      "B.C. Synopsis page 49, opens the PDF", "https://a", "Who says so: the rules behind Gear"]);
  });
  it("a page the PDF map lacks is said unlinked; nothing to cite is no links", () => {
    expect(sourcesText(sourceLineOf({ pages: [{ printed: 9, pdf: null }] }))).toBe("Source B.C. Synopsis p.9");
    expect(sourceLineOf({}).links).toEqual([]);
  });
});

describe("each surface's line", () => {
  it("a section (Gear, Ways to fish, Boats, Reference): its pages then Who says so ›, named for the section", () => {
    const vm = sectionSourcesOf({ open: "srcs:boats", pages: [{ printed: 43, pdf: 45, box: [box(45, 3)] }] }, "Boats");
    expect(sourcesText(vm)).toBe("Source B.C. Synopsis p.43 ↗ Who says so ›");
    expect(arias(vm.links)).toEqual(["B.C. Synopsis page 43, the passages behind Boats marked, opens the PDF", "Who says so: the rules behind Boats"]);
    expect(vm.links[1]).toMatchObject({ k: "sheet", open: "srcs:boats" });
  });
  it("a Line's source runs (a sheet's source block, a closure on the closed banner): pages, a cited page, its sheet link", () => {
    const line = [{ t: "link" as const, s: "Who says so ›", open: "rule:z2:p::p.r1" as const }, { t: "text" as const, s: " " },
                  { t: "book" as const, printed: 21, pdf: 23, box: [box(23, 4)] }];
    expect(sourcesText(lineSourcesOf(line, "Possession"))).toBe("Source B.C. Synopsis p.21 ↗ Who says so ›");
    expect(arias(lineSourcesOf(line, "Possession").links)).toEqual([
      "B.C. Synopsis page 21, the passages behind Possession marked, opens the PDF", "Who says so: the rules behind Possession"]);
    // a closure's line has no link run of its own: the banner hands its rule's sheet
    const lagoon = [{ t: "web" as const, s: "StanleyParkVan.com", url: "https://stanleyparkvan.com/x" }];
    expect(sourcesText(lineSourcesOf(lagoon, "No fishing", "rule:r2:lost_lagoon::lost_lagoon.r1"))).toBe("Source StanleyParkVan.com ↗ Who says so ›");
    // a fish sheet's "All sources ›" keeps its own words, spoken for its sheet
    const all = lineSourcesOf([{ t: "link", s: "All sources ›", open: "src:trout" }], "Rainbow trout");
    expect([sourcesText(all), arias(all.links)]).toEqual(["Source All sources ›", ["All sources: the rules behind Rainbow trout"]]);
    expect(() => lineSourcesOf([{ t: "text", s: "Release." }], "x")).toThrow(/a source line holds a "text" run/);
    expect(() => lineSourcesOf([{ t: "link", s: "Today ›", open: { day: { month: 1, day: 1 } } }], "x")).toThrow(/opens no sheet/);
  });
  it("an in-season change: its page by the table's name; a change with no page draws nothing", () => {
    expect(sourcesText(inSeasonSourcesOf({ url: "https://www2.gov.bc.ca/x" }))).toBe("Source B.C. in-season changes ↗");
    expect(inSeasonSourcesOf({ url: "" }).links).toEqual([]);
  });
  it("the trout table: Who says so › alone, opening its every-rule sheet (it draws no page of its own); no such sheet: thrown", () => {
    const sheet: SheetVM = { key: "src:trout", title: "Sources", sub: null, blocks: [{ k: "sources", groups: [{ where: "", rows: [
      { key: "a", title: null, quote: "", role: "", book: [{ t: "book", printed: 6, pdf: 8, box: [box(8, 5)] }] }] }] }] };
    const page = { sheet: (k: string) => (k === "src:trout" ? sheet : null) } as unknown as RegsPage;
    // (#69 review) the sheet's p.6 (a quota the table never says) is not cited on the table's line
    expect(sourcesText(troutSourcesOf(page))).toBe("Source Who says so ›");
    expect(arias(troutSourcesOf(page).links)).toEqual(["Who says so: the rules behind Trout and char"]);
    expect(troutSourcesOf(page).links[0]).toMatchObject({ k: "sheet", open: "src:trout" });
    // a page with a trout table and no every-rule sheet is a programming error, said, never a line with no pages
    expect(() => troutSourcesOf({ sheet: () => null } as unknown as RegsPage)).toThrow(/no src:trout sheet/);
  });
  it("the salmon card: a book closure's page, linked once the page's PDF map is given; DFO's region pages", () => {
    const card = { book: [{ from: "B.C. Synopsis p.9 · Skeena River", words: "w", water: "Skeena River", pages: [{ printed: 9, pdf: null }] }],
                   dfoPages: [{ region: "Region 6 Skeena", url: "https://dfo/6" }] } as unknown as SalmonCardVM;
    expect(sourcesText(salmonBookSourcesOf(card.book[0]!))).toBe("Source B.C. Synopsis p.9");
    const linked = salmonWithPdf(card, { 9: 11 });
    expect(linked.book[0]!.pages).toEqual([{ printed: 9, pdf: 11 }]);
    expect(sourcesText(salmonBookSourcesOf(linked.book[0]!))).toBe("Source B.C. Synopsis p.9 ↗");
    expect(sourcesText(salmonDfoSourcesOf(card))).toBe("Source DFO notices, Region 6 Skeena ↗");
    expect(salmonDfoSourcesOf(card).links).toEqual([{ k: "web", label: "DFO notices, Region 6 Skeena ↗", url: "https://dfo/6" }]);
  });
});
