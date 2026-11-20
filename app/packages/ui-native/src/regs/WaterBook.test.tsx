/**
 * (T474) The water's own rule-book page: one "This water in the rule book p.N ↗" link in a fixed spot on the Regulations
 * page; a water with no entry of its own draws none. (T916, owner 2026-11-19: "the location that was awkward at the top")
 * the spot is the page's foot, under the "not legal advice" and "as of" lines, never above the first status card.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Text } from "react-native";
import type { RegsPage } from "@app/core";
import { LIGHT } from "../theme";
import { WaterFrame } from "../WaterFrame";
import { RegsScreen, type RegsScreenProps } from "./RegsScreen";
import vedder1010 from "./fixtures/vedder_1010.json";

afterEach(cleanup);

const books: number[] = [];
const NOOP = { onSheet() {}, onDay() {}, onBook: (printed: number) => { books.push(printed); } };
const mount = (p: Partial<RegsScreenProps> = {}) =>
  render(<WaterFrame palette={LIGHT} face="regulations">
           <RegsScreen page={vedder1010 as unknown as RegsPage} palette={LIGHT} actions={NOOP as never} onDate={() => {}}
                       gear={<Text>gear</Text>} methods={<Text>methods</Text>} licence={<Text>licence</Text>} {...p} />
         </WaterFrame>);

describe("the water's own rule-book link (T474)", () => {
  it("is drawn once, in the page's foot after the as-of line (T916), and opens its page", () => {
    mount({ asOf: "Regulations as of Oct 10, 2026", waterBook: [{ t: "text", s: "This water in the rule book" }, { t: "text", s: " " }, { t: "book", printed: 21, pdf: 23, bare: true }] });
    const link = screen.getByTestId("regs-water-book");
    expect(screen.getAllByTestId("regs-water-book")).toHaveLength(1);
    expect(link.textContent).toBe("This water in the rule book p.21 ↗");
    const foot = screen.getByTestId("regs-legal");
    expect(foot.contains(link)).toBe(true);
    expect(foot.lastElementChild).toBe(link);                                   // under "not legal advice" and "as of"
    expect(screen.getByTestId("regs-content").firstElementChild!.contains(link)).toBe(false);
    // nothing of the page's own content comes after the foot's line: it is no longer above the status cards
    expect(screen.getByTestId("keep-tab").compareDocumentPosition(link) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    fireEvent.click(screen.getByText("p.21 ↗"));
    expect(books.length).toBe(1);
  });
  it("none: nothing drawn", () => {
    mount({ waterBook: [] });
    expect(screen.queryByTestId("regs-water-book")).toBeNull();
  });
});
