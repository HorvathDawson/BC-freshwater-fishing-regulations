/**
 * T82 O5 — THE SHEET'S HEIGHT vs the prototype (regs v38), measured by `app/tools/regs-heights.mjs --sheets` over the
 * 192 golden sheets of vedder_1010, klwa_1010 and bc1_501 at 390 × 844 (handoff/PLAN/app-integration/tickets/T82-regs-walk-leftovers.md, As built). The
 * phone sheet is already sized to its content; what made a wrapping title's sheet taller than its words was the title's
 * line (19 px on TYPE.screen's 27 px line). Pinned here:
 *   - the title: 19 px on a 23 px line, in BOTH title rows — the phone sheet's and the wide dialog's (mutation: spreading
 *     TYPE.screen alone again, 27 px, in either branch fails that branch's "the title" case; adversarial review: the
 *     first cut rendered only the phone sheet, so reverting the dialog's title passed);
 *   - the cap: the body at most 0.68 of the window, so the tallest sheet on an 844 px phone stays under the prototype's
 *     cap, 88 % of its 844 px phone (mutation: 0.82, the T37 ticket's figure, fails "under the prototype's cap").
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { Text } from "react-native";
import { SHEET_BODY_CAP, SHEET_TITLE, Sheet } from "./Sheet";
import { LIGHT } from "./theme";

afterEach(cleanup);

/** the tallest chrome measured on a regs sheet (handle + a three-line title + the sub line + the foot), 192 sheets: rule:zp:conduct::conduct.r6 */
const TALLEST_CHROME = 158;
const PHONE_H = 844, PROTO_CAP = 0.88 * PHONE_H;

describe("sheet height (T82 O5)", () => {
  // both title rows: the phone bottom sheet and the wide dialog (T45) — each branch draws its own title Text
  it.each([["the phone sheet", "phone", false], ["the wide dialog", "desktop", true]] as const)(
    "the title: the screen face at 19 px on a 23 px line, as drawn — %s", (_, layout, dialog) => {
    expect([SHEET_TITLE.fontSize, SHEET_TITLE.lineHeight]).toEqual([19, 23]);
    render(<Sheet open onClose={() => {}} title="Do not release fish in a harmful manner" palette={LIGHT} scroll layout={layout} dialog={dialog}><Text>body</Text></Sheet>);
    const t = (screen.getByRole("heading", { name: "Do not release fish in a harmful manner" }) as HTMLElement).style;   // RNW: inline
    expect([t.fontSize, t.lineHeight]).toEqual(["19px", "23px"]);
  });
  it("under the prototype's cap: body cap 0.68 of the window + the tallest chrome < 88 % of an 844 px phone", () => {
    expect(SHEET_BODY_CAP).toBe(0.68);
    expect(SHEET_BODY_CAP * PHONE_H + TALLEST_CHROME).toBeLessThan(PROTO_CAP);   // 731.9 < 742.7
  });
});
