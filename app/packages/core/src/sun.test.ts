import { describe, expect, it } from "vitest";
import { bcNow, sunLine, sunTimes } from "./index";

/** minutes past midnight on the B.C. clock (PDT −7 / PST −8 given) */
const at = (ms: number | null, off: number) => {
  const d = new Date(ms! + off * 3_600_000);
  return d.getUTCHours() * 60 + d.getUTCMinutes();
};

describe("sunrise and sunset (owner 2026-11-04)", () => {
  it("Vancouver at the solstices, within two minutes of the almanac", () => {
    // NRC: 21 Jun 05:07 / 21:21 PDT; 21 Dec 08:05 / 16:16 PST
    const jun = sunTimes(49.2827, -123.1207, 2026, 6, 21), dec = sunTimes(49.2827, -123.1207, 2026, 12, 21);
    expect(Math.abs(at(jun.rise, -7) - (5 * 60 + 7))).toBeLessThanOrEqual(2);
    expect(Math.abs(at(jun.set, -7) - (21 * 60 + 21))).toBeLessThanOrEqual(2);
    expect(Math.abs(at(dec.rise, -8) - (8 * 60 + 5))).toBeLessThanOrEqual(2);
    expect(Math.abs(at(dec.set, -8) - (16 * 60 + 16))).toBeLessThanOrEqual(2);
  });

  it("east is earlier: Cranbrook's sun rises ~30 min before Vancouver's, said on the B.C. clock", () => {
    const v = sunTimes(49.28, -123.12, 2026, 11, 4), c = sunTimes(49.51, -115.77, 2026, 11, 4);
    expect((v.rise! - c.rise!) / 60_000).toBeGreaterThan(25);
    expect(sunLine(49.2827, -123.1207, { y: 2026, m: 11, d: 4 })).toMatch(/^Sunrise 0[67]:\d\d PST · sunset 16:\d\d PST$/);
  });
});

describe("the regulations' clock is B.C.'s", () => {
  it("23:30 in Edmonton is still the day before on the B.C. clock; 00:30 UTC is the evening before in B.C.", () => {
    // 2026-11-05 06:30 UTC = 23:30 MST on the 4th in Edmonton = 22:30 PST on the 4th in B.C.
    const b = bcNow(Date.UTC(2026, 10, 5, 6, 30));
    expect([b.getFullYear(), b.getMonth() + 1, b.getDate(), b.getHours(), b.getMinutes()]).toEqual([2026, 11, 4, 22, 30]);
    const c = bcNow(Date.UTC(2026, 6, 1, 0, 30));          // 17:30 PDT on 30 June
    expect([c.getMonth() + 1, c.getDate(), c.getHours()]).toEqual([6, 30, 17]);
  });
});
