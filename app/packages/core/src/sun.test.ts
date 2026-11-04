import { describe, expect, it } from "vitest";
import { bcClockTime, bcNow, bcOffsetHours, bcToday, sunLine, sunTimes } from "./index";

/** minutes past midnight at a fixed UTC offset */
const at = (ms: number | null, off: number) => {
  const d = new Date(ms! + off * 3_600_000);
  return d.getUTCHours() * 60 + d.getUTCMinutes();
};

describe("sunrise and sunset (owner 2026-11-04)", () => {
  it("Vancouver at the solstices, within two minutes of the almanac (UTC times, so no clock in the way)", () => {
    // NRC (old clock): 21 Jun 05:07 / 21:21 PDT (UTC−7); 21 Dec 08:05 / 16:16 PST (UTC−8)
    const jun = sunTimes(49.2827, -123.1207, 2026, 6, 21), dec = sunTimes(49.2827, -123.1207, 2026, 12, 21);
    expect(Math.abs(at(jun.rise, -7) - (5 * 60 + 7))).toBeLessThanOrEqual(2);
    expect(Math.abs(at(jun.set, -7) - (21 * 60 + 21))).toBeLessThanOrEqual(2);
    expect(Math.abs(at(dec.rise, -8) - (8 * 60 + 5))).toBeLessThanOrEqual(2);
    expect(Math.abs(at(dec.set, -8) - (16 * 60 + 16))).toBeLessThanOrEqual(2);
  });

  it("said on B.C.'s clock: UTC−7 in winter too, so the December sunrise reads 09:05 PT", () => {
    expect(sunLine(49.2827, -123.1207, { y: 2026, m: 12, d: 21 })).toMatch(/^Sunrise 09:0\d PT · sunset 17:1\d PT$/);
    const v = sunTimes(49.28, -123.12, 2026, 11, 4), c = sunTimes(49.51, -115.77, 2026, 11, 4);
    expect((v.rise! - c.rise!) / 60_000).toBeGreaterThan(25);          // Cranbrook is ~30 min east
  });
});

describe("the regulations' clock is B.C.'s, written in the app (not the device's zone data)", () => {
  it("UTC−7 all year from 8 Mar 2026; the old Pacific clock before", () => {
    expect(bcOffsetHours(Date.UTC(2026, 10, 4, 12))).toBe(-7);        // after 1 Nov 2026: no fall back
    expect(bcOffsetHours(Date.UTC(2027, 0, 15))).toBe(-7);
    expect(bcOffsetHours(Date.UTC(2026, 2, 8, 9, 59))).toBe(-8);      // a minute before the last spring-forward
    expect(bcOffsetHours(Date.UTC(2025, 6, 1))).toBe(-7);             // summer 2025: PDT
    expect(bcOffsetHours(Date.UTC(2025, 10, 2, 9))).toBe(-8);         // 2 Nov 2025 02:00 PDT: fell back
    expect(bcOffsetHours(Date.UTC(2025, 10, 2, 8, 59))).toBe(-7);
  });

  it("23:30 in Edmonton in winter is 23:30 in B.C. too now; 00:30 UTC is the evening before", () => {
    const b = bcNow(Date.UTC(2026, 10, 5, 6, 30));
    expect([b.getFullYear(), b.getMonth() + 1, b.getDate(), b.getHours(), b.getMinutes()]).toEqual([2026, 11, 4, 23, 30]);
    const c = bcNow(Date.UTC(2026, 6, 1, 0, 30));
    expect([c.getMonth() + 1, c.getDate(), c.getHours()]).toEqual([6, 30, 17]);
    expect(bcClockTime(Date.UTC(2026, 11, 21, 16, 5))).toBe("09:05 PT");
    const t = bcToday(Date.UTC(2026, 10, 5, 6, 30));                  // (T382) the same day, at its midnight
    expect([t.getFullYear(), t.getMonth() + 1, t.getDate(), t.getHours(), t.getMinutes()]).toEqual([2026, 11, 4, 0, 0]);
  });
});
