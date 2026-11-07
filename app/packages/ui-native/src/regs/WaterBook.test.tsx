/**
 * (T474) The water's own rule-book page: one "This water in the rule book p.N ↗" link in a fixed spot at the top of the
 * Regulations page; a water with no entry of its own draws none.
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
  it("is drawn once, leading the page, and opens its page", () => {
    mount({ waterBook: [{ t: "text", s: "This water in the rule book" }, { t: "text", s: " " }, { t: "book", printed: 21, pdf: 23, bare: true }] });
    const link = screen.getByTestId("regs-water-book");
    expect(link.textContent).toBe("This water in the rule book p.21 ↗");
    expect(screen.getByTestId("regs-content").firstElementChild!.contains(link)).toBe(true);
    fireEvent.click(screen.getByText("p.21 ↗"));
    expect(books.length).toBe(1);
  });
  it("none: nothing drawn", () => {
    mount({ waterBook: [] });
    expect(screen.queryByTestId("regs-water-book")).toBeNull();
  });
});
