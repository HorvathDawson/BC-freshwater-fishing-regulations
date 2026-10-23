/**
 * T45 — the layout class: one answer to "how much room is there", from the window's width and the breakpoint tokens.
 * The hook itself is mounted in layout.render.test.tsx.
 */
import { describe, expect, it } from "vitest";
import { LAYOUT_BREAKPOINTS, layoutClassFor } from "./index";

describe("layoutClassFor (T45)", () => {
  it("phone below the tablet breakpoint, tablet below the desktop one, desktop from it", () => {
    expect([320, 390, 844, 899, 900, 1024, 1279, 1280, 1440, 2560].map((w) => `${w} ${layoutClassFor(w)}`)).toEqual([
      "320 phone", "390 phone", "844 phone", "899 phone", "900 tablet", "1024 tablet", "1279 tablet",
      "1280 desktop", "1440 desktop", "2560 desktop",
    ]);
  });

  it("the breakpoints are the tokens (900 and 1280, the ticket's)", () => {
    expect(LAYOUT_BREAKPOINTS).toEqual({ tablet: 900, desktop: 1280 });
  });

  it("no width (not measured yet, a test with no provider) is a phone: the design, never a guess at a wide screen", () => {
    expect([null, undefined, 0, Number.NaN].map((w) => layoutClassFor(w))).toEqual(["phone", "phone", "phone", "phone"]);
  });
});
