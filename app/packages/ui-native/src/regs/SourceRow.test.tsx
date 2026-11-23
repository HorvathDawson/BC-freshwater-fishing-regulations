/**
 * (T991 round 3, owner 2026-11-22) The Regulations tab's Source row: the last block of the page, just above "Report a
 * problem", naming the synopsis pages the page cites (T474: the water's own among them — so its page is always linked,
 * and once, T60), each opening the rule book at that page with its highlights; "All sources ›" opens the breakdown sheet.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Text } from "react-native";
import type { RegsPage, SourceBox, TabSourcesVM } from "@app/core";
import { LIGHT } from "../theme";
import { WaterFrame } from "../WaterFrame";
import { RegsScreen, type RegsScreenProps } from "./RegsScreen";
import vedder1010 from "./fixtures/vedder_1010.json";
import { TEST_SOURCES } from "./testing/sources";

afterEach(cleanup);

const books: [number, readonly SourceBox[] | undefined][] = [];
const sheets: string[] = [];
const NOOP = { onSheet: (k: string) => { sheets.push(k); }, onDay() {}, onBook: (pdf: number, box?: readonly SourceBox[]) => { books.push([pdf, box]); } };
const mount = (p: Partial<RegsScreenProps> = {}) =>
  render(<WaterFrame palette={LIGHT} face="regulations">
           <RegsScreen sources={TEST_SOURCES} page={vedder1010 as unknown as RegsPage} palette={LIGHT} actions={NOOP as never} onDate={() => {}}
                       gear={<Text>gear</Text>} methods={<Text>methods</Text>} licence={<Text>licence</Text>} {...p} />
         </WaterFrame>);
const BOX = { pdf: 101, box: [10, 20, 200, 32] as [number, number, number, number], size: [612, 792] as [number, number] };
const SOURCES: TabSourcesVM = { label: "Source", links: [
  { k: "book", label: "B.C. Synopsis p.22 ↗", printed: 22, pdf: 24, aria: "B.C. Synopsis page 22, opens the PDF" },
  { k: "book", label: "p.99 ↗", printed: 99, pdf: 101, box: [BOX], aria: "B.C. Synopsis page 99, every passage this page cites marked, opens the PDF" },
  { k: "web", label: "DFO notices ↗", url: "https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/fresh-douce/index-eng.html" },
  { k: "sheet", label: "All sources ›", open: "src:all", aria: "All sources for this page" },
] };

describe("the Regulations tab's Source row (T991 round 3)", () => {
  it("is the page's last block before the report link, after the legal foot, and draws core's row as is", () => {
    mount({ asOf: "Regulations as of Oct 10, 2026", sources: SOURCES });
    const row = screen.getByTestId("regs-sources");
    expect(row.textContent).toBe("SourceB.C. Synopsis p.22 ↗p.99 ↗DFO notices ↗All sources ›");
    // (T60) a page here marks every passage the page cites: its own name, not a second "Rule book page 99" link
    expect([...row.querySelectorAll("[role=link]")].map((e) => e.getAttribute("aria-label")))
      .toEqual(["B.C. Synopsis page 22, opens the PDF", "B.C. Synopsis page 99, every passage this page cites marked, opens the PDF", null]);
    expect(screen.getByTestId("regs-legal").compareDocumentPosition(row) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(screen.getByTestId("regs-legal").contains(row)).toBe(false);
    expect(screen.getByTestId("regs-all-sources").contains(row)).toBe(true);
  });
  it("a page opens the rule book at its PDF page with that page's highlights (owner: \"make sure highlighting is still there\")", () => {
    mount({ sources: SOURCES });
    books.length = 0;
    fireEvent.click(screen.getByText("p.99 ↗"));
    fireEvent.click(screen.getByText("B.C. Synopsis p.22 ↗"));
    expect(books).toEqual([[101, [BOX]], [24, undefined]]);
  });
  it("All sources › opens the every-source sheet", () => {
    const page = vedder1010 as unknown as RegsPage & { sheets: Record<string, unknown> };
    mount({ sources: SOURCES, page: { ...page, sheet: (k: string) => page.sheets[k] ?? null } as RegsPage });
    sheets.length = 0;
    fireEvent.click(screen.getByTestId("regs-all-sources-button"));
    expect(sheets).toEqual(["src:all"]);
  });
});
