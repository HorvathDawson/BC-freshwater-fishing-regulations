import { describe, expect, it } from "vitest";
import { WEEKDAYS } from "./calendar.generated";
import { bcClockTime, bcNow, bcOffsetHours, bcToday, daylightAt, daylightClosed, daylightOfMoment, namesEveryGameFish, sunLine, sunTimes, timedLineText,
         type CaveatItemVM, type Moment } from "./index";

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
    // (T878) the line under the day pill and its spoken form, whole
    expect(sunLine(49.2827, -123.1207, { y: 2026, m: 12, d: 21 })).toEqual({ text: "9:05–17:16", aria: "Sunrise 9:05, sunset 17:16 PT", rise: "9:05", set: "17:16" });
    // a summer evening: the leading 0 comes off a one-digit hour only, never off a two-digit one; no "PT" drawn (B.C.-only)
    expect(sunLine(49.2827, -123.1207, { y: 2026, m: 6, d: 21 })).toEqual({ text: "5:06–21:21", aria: "Sunrise 5:06, sunset 21:21 PT", rise: "5:06", set: "21:21" });
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

describe("(T898) a daylight-only closure's times in its rule line: the date line's sun, offset by the rule's own minutes", () => {
  const MISSION = { lat: 49.133, lon: -122.31 }, NOV19 = { y: 2026, m: 11, d: 19 }, JUN21 = { y: 2026, m: 6, d: 21 };
  const rule = (after: number, before: number): CaveatItemVM => ({ what: [{ t: "text", s: "No fishing for any game fish" }],
    where: `${after} min after sunset–${before} min before sunrise`, rule: null, fish: null, book: [], daylight: { after, before } });
  it("Mission, Nov 19: 1 h after sunset to 1 h before sunrise, each time after its part, beside the date line", () => {
    expect(sunLine(MISSION.lat, MISSION.lon, NOV19)!.text).toBe("8:25–17:24");
    expect(timedLineText(rule(60, 60), daylightClosed(MISSION.lat, MISSION.lon, NOV19, 60, 60)))
      .toBe("Daylight only: closed from 1 h after sunset (18:24) to 1 h before sunrise (7:25)");
    expect(timedLineText(rule(60, 60), daylightAt(MISSION.lat, MISSION.lon, NOV19).closed(60, 60)))
      .toBe("Daylight only: closed from 1 h after sunset (18:24) to 1 h before sunrise (7:25)");
    // (T473) the open hours under the verdict, on the same clock as the date line (owner 2026-11-19)
    expect(daylightAt(MISSION.lat, MISSION.lon, NOV19).open(60, 60)).toBe("7:25–18:24");
  });
  it("each end takes its own offset (30 min after sunset, 45 before sunrise); a summer evening keeps its two-digit hour", () => {
    expect(timedLineText(rule(30, 45), daylightClosed(MISSION.lat, MISSION.lon, NOV19, 30, 45)))
      .toBe("Daylight only: closed from 30 min after sunset (17:54) to 45 min before sunrise (7:40)");
    expect(sunLine(MISSION.lat, MISSION.lon, JUN21)!.text).toBe("5:04–21:17");
    expect(timedLineText(rule(60, 60), daylightClosed(MISSION.lat, MISSION.lon, JUN21, 60, 60)))
      .toBe("Daylight only: closed from 1 h after sunset (22:17) to 1 h before sunrise (4:04)");
    expect(daylightAt(MISSION.lat, MISSION.lon, JUN21).open(60, 60)).toBe("4:04–22:17");
  });
  it("no times known: the rule's words alone", () => {
    expect(timedLineText(rule(60, 60), null)).toBe("Daylight only: closed from 1 h after sunset to 1 h before sunrise");
    expect(timedLineText(rule(60, 60))).toBe("Daylight only: closed from 1 h after sunset to 1 h before sunrise");
  });
});

describe("(T898) the offsets come from the moment's stored window, never its words", () => {
  const ALL = WEEKDAYS;   // the one calendar spec (gate 4): never a hand-typed weekday list
  const m = (hours: Moment["hours"], weekdays: Moment["weekdays"] = ALL): Moment => ({ weekdays, hours });
  const sun = (solar: "sunrise" | "sunset", offset_min: number) => ({ solar, offset_min });
  it("sunset + N to sunrise − M, every day, inside the window: { after: N, before: M }", () => {
    expect(daylightOfMoment(m({ start: sun("sunset", 60), end: sun("sunrise", -60), in: true }))).toEqual({ after: 60, before: 60 });
    expect(daylightOfMoment(m({ start: sun("sunset", 30), end: sun("sunrise", -45), in: true }))).toEqual({ after: 30, before: 45 });
  });
  it("any other window is not one: outside it, some weekdays, a clock end, an end on the sun itself, no hours", () => {
    expect(daylightOfMoment(m({ start: sun("sunset", 60), end: sun("sunrise", -60), in: false }))).toBeNull();
    expect(daylightOfMoment(m({ start: sun("sunset", 60), end: sun("sunrise", -60), in: true }, ["Saturday", "Sunday"]))).toBeNull();
    expect(daylightOfMoment(m({ start: { at: "21:00", offset_min: 0 }, end: sun("sunrise", -60), in: true }))).toBeNull();
    expect(daylightOfMoment(m({ start: sun("sunset", 0), end: sun("sunrise", -60), in: true }))).toBeNull();
    expect(daylightOfMoment(m({ start: sun("sunrise", -60), end: sun("sunset", 60), in: true }))).toBeNull();
    expect(daylightOfMoment(m(null))).toBeNull();
    expect(daylightOfMoment(null)).toBeNull();
  });
});

describe("(T898) only a rule that names every game fish reads \"Daylight only\" (the old `^no fishing` word test is gone)", () => {
  const GAME = ["RB", "ST", "CT", "CRA"];
  it("every game fish, none excepted: yes; some fish, or an exception: no; no game fish in the book: no", () => {
    expect(namesEveryGameFish({ species: ["RB", "ST", "CT", "CRA", "KO"], speciesExcept: [] }, GAME)).toBe(true);
    expect(namesEveryGameFish({ species: ["ST"], speciesExcept: [] }, GAME)).toBe(false);                 // "No fishing for steelhead"
    expect(namesEveryGameFish({ species: ["RB", "ST", "CT", "CRA"], speciesExcept: ["ST"] }, GAME)).toBe(false);
    expect(namesEveryGameFish({ species: ["RB"], speciesExcept: [] }, [])).toBe(false);
  });
});
