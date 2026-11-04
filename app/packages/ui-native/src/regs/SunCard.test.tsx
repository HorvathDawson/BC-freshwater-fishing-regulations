/**
 * (T382) The day's sunrise and sunset: the first small card of the Regulations page, in a card (not loose text), the
 * line drawn whole; none, no card.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { Text } from "react-native";
import type { RegsPage } from "@app/core";
import { LIGHT } from "../theme";
import { WaterFrame } from "../WaterFrame";
import { RegsScreen, type RegsScreenProps } from "./RegsScreen";
import vedder1010 from "./fixtures/vedder_1010.json";

afterEach(cleanup);

const NOOP = { onSheet() {}, onDay() {}, onBook() {} };
const mount = (p: Partial<RegsScreenProps> = {}) =>
  render(<WaterFrame palette={LIGHT} face="regulations">
           <RegsScreen page={vedder1010 as unknown as RegsPage} palette={LIGHT} actions={NOOP} onDate={() => {}}
                       gear={<Text>gear</Text>} methods={<Text>methods</Text>} licence={<Text>licence</Text>} {...p} />
         </WaterFrame>);

describe("the sun card (T382)", () => {
  it("leads the page, above Keep, the whole line in a card", () => {
    mount({ sun: "Sunrise 08:00 PT · sunset 17:42 PT" });
    expect([...screen.getByTestId("regs-content").children].map((e) => e.getAttribute("data-testid")).slice(0, 2))
      .toEqual([null, "keep-tab"]);   // the card's inset, then Keep
    const card = screen.getByTestId("regs-sun");
    expect(card.textContent).toBe("Sunrise 08:00 PT · sunset 17:42 PT");
    expect(getComputedStyle(card).backgroundColor).not.toBe("");
    expect(screen.getByTestId("regs-content").firstElementChild!.contains(card)).toBe(true);
  });
  it("no line, no card", () => {
    mount({ sun: null });
    expect(screen.queryByTestId("regs-sun")).toBeNull();
  });
});
