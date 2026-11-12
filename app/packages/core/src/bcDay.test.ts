/**
 * (owner 2026-11-04: "spots should use the same date/time as everything else") Every day and hour an instant is said in
 * is B.C.'s, whatever zone the device keeps: run with the process in Halifax (UTC−4/−3), the helpers still say B.C.'s.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { bcHour, bcMidnight, leadUpDays, localDate, localDateTime, setVisitHour, shiftVisitDay, visitSteps, wallIso,
         bcNow } from "./index";

let was: string | undefined;
beforeAll(() => { was = process.env.TZ; process.env.TZ = "America/Halifax"; });
afterAll(() => { if (was === undefined) delete process.env.TZ; else process.env.TZ = was; });

// 2026-11-05 05:30 UTC = 22:30 on the 4th in B.C. (UTC−7) = 01:30 on the 5th in Halifax
const T = Date.UTC(2026, 10, 5, 5, 30);

describe("the B.C. day of an instant, on a device in another zone", () => {
  it("the date, the hour and the minute are B.C.'s", () => {
    expect(new Date(T).getDate()).toBe(5);                         // the device says the 5th — not what we say
    expect([localDate(T), bcHour(T), localDateTime(T)]).toEqual(["2026-11-04", 22, "2026-11-04 at 22:30"]);
  });
  it("a visit's day steps, hours and lead-up days are B.C.'s", () => {
    const now = T + 3_600_000;
    expect(visitSteps(T, now)).toMatchObject({ dayOffset: 0, hour: 22, hourLabel: "22:00", dayText: "Wed Nov 04 2026" });
    expect(localDate(shiftVisitDay(T, -1, now))).toBe("2026-11-03");
    expect(localDateTime(setVisitHour(T, 7, now))).toBe("2026-11-04 at 07:00");
    expect(leadUpDays(T, 2)).toEqual(["2026-11-02", "2026-11-03"]);
    expect(bcMidnight("2026-11-04")).toBe(Date.UTC(2026, 10, 4, 7));
  });
  it("a wall-clock date is read as it is; `localDate` of its time would move it twice (MUTATION PIN)", () => {
    const wall = bcNow(Date.UTC(2026, 10, 5, 7, 30));               // B.C. 00:30 on the 5th: fields Nov 5 00:30
    expect(wallIso(wall)).toBe("2026-11-05");
    // read as an instant, Halifax's 00:30 on the 5th is B.C.'s 20:30 on the 4th: a day off
    expect(localDate(wall.getTime())).toBe("2026-11-04");
  });
});

/**
 * (T368 Q6, owner 2026-11-12) "today" is B.C.'s day: one helper (`bcToday`/`localDate`, clock.ts), whatever zone the phone
 * keeps. 2026-11-12 07:30 UTC is 00:30 on the 12th in B.C. (UTC−7, permanent): a phone in Mountain (Edmonton, UTC−7 until the
 * 8th, same wall clock here) or in UTC must both call it the 12th, and 06:30 UTC (23:30 on the 11th in B.C.) the 11th.
 */
describe("B.C.'s today from a phone in Mountain or UTC, around B.C. midnight", () => {
  const day = (ms: number) => { const b = bcNow(ms); return wallIso(b); };
  for (const zone of ["America/Edmonton", "UTC"]) {
    it(zone, () => {
      const was = process.env.TZ; process.env.TZ = zone;
      try {
        const after = Date.UTC(2026, 10, 12, 7, 30), before = Date.UTC(2026, 10, 12, 6, 30);
        expect([day(before), day(after)]).toEqual(["2026-11-11", "2026-11-12"]);
        expect([localDate(before), localDate(after)]).toEqual(["2026-11-11", "2026-11-12"]);
        expect(bcMidnight("2026-11-12")).toBe(Date.UTC(2026, 10, 12, 7));
      } finally { if (was === undefined) delete process.env.TZ; else process.env.TZ = was; }
    });
  }
});
