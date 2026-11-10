/** (T551) The B.C. words for an instant: UTC−7 all year, whatever the runtime's tz data says (2026a turns back to −8 on 1 Nov). */
import { describe, expect, it } from "vitest";
import { bcDayWords, bcFields, bcStampWords, bcWeekdayAbbr, bcIsoDayWords, nextBcMidnight } from "./day";

describe("B.C. words (T551)", () => {
  it("2026-11-09T19:00Z is 12:00 PT, as the app's clock says (tz data would say 11:00 PST)", () => {
    const t = Date.parse("2026-11-09T19:00:00Z");
    expect(bcStampWords(t)).toBe("Nov 9, 12:00 p.m. PT");
    expect([bcDayWords(t), bcDayWords(t, true), bcWeekdayAbbr(t)]).toEqual(["Nov 9", "Mon, Nov 9", "Mon"]);
  });
  it("the day boundary is B.C. midnight: 06:59Z is still Nov 9, 07:00Z is Nov 10", () => {
    expect(bcFields(Date.parse("2026-11-10T06:59:00Z"))).toMatchObject({ month: 11, day: 9, hour: 23, minute: 59 });
    expect(bcFields(Date.parse("2026-11-10T07:00:00Z"))).toMatchObject({ month: 11, day: 10, hour: 0 });
    expect(nextBcMidnight(Date.parse("2026-11-09T19:00:00Z"))).toBe(Date.parse("2026-11-10T07:00:00Z"));
  });
  it("a calendar day is said as itself", () => {
    expect(bcIsoDayWords("2026-11-09")).toBe("Mon, Nov 9");
    expect(bcIsoDayWords("2027-01-02")).toBe("Sat, Jan 2");
  });
});
