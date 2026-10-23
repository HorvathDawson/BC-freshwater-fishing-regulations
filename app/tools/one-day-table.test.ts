/**
 * T125 item 8 (app 9, app 11) — ONE MONTH TABLE, ONE WEEKDAY TABLE, ONE "IS THIS A DAY".
 *
 * The months live in `flow.ts` (`MONTHS`, `monthAbbr`) and the weekdays beside them (`WEEKDAY_NAMES`, `weekdayAbbr`,
 * Sunday first for `Date.getDay()`); the regulations' calendar (`calendar.generated.ts`, from the pipeline) names the
 * same months and the same seven days, Monday first. The day picker, a spot's coming days, the change view and a
 * lead-up day each typed `["Sun", "Mon", …]` or `["Jan", "Feb", …]` again; and a day string was tested with three
 * copies of one regex, one of which also asked `Date.parse`. `isIsoDay` (day.ts) is the one test.
 *
 * MUTATIONS: paste a month or weekday list into any app source file, or a `\d{4}-\d{2}-\d{2}` regex outside day.ts,
 * and "are spelled once" fails.
 */
import { readdirSync, readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { DAY_PICKER, isIsoDay, MONTHS, monthAbbr, WEEKDAY_NAMES, weekdayAbbr } from "@app/core";
import { MONTHS as CAL_MONTHS, WEEKDAYS as CAL_WEEKDAYS } from "../packages/core/src/calendar.generated";

const APP = new URL("..", import.meta.url).pathname.replace(/\/$/, "");

describe("the day tables", () => {
  it("are the calendar's", () => {
    expect(MONTHS.map((_, i) => monthAbbr(i))).toEqual(CAL_MONTHS);
    expect([...WEEKDAY_NAMES.slice(1), WEEKDAY_NAMES[0]]).toEqual(CAL_WEEKDAYS);
    expect(WEEKDAY_NAMES.map((_, i) => weekdayAbbr(i))).toEqual(["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]);
    expect(DAY_PICKER.weekdays[6]).toEqual(["Sat", "Saturday"]);
    expect(DAY_PICKER.monthsShort[8]).toBe("Sep");
  });

  it("say which strings are a day", () => {
    expect(["2026-10-23", "2024-02-29"].map(isIsoDay)).toEqual([true, true]);
    expect(["2026-10-23T09:00:00Z", "2026-1-3", "", null, undefined, "2026-13-45"].map(isIsoDay))
      .toEqual([false, false, false, false, false, false]);
  });

  it("are spelled once", () => {
    const LISTS = /"(Jan|January)", ?"(Feb|February)"|"(Sun|Sunday)", ?"(Mon|Monday)"|"Mon", ?"Tue"/;
    const ISO = /\\d\{4\}-\\d\{2\}-\\d\{2\}/;
    const walk = (dir: string): string[] => readdirSync(dir, { withFileTypes: true }).flatMap((e) => {
      const p = `${dir}/${e.name}`;
      if (e.isDirectory()) return ["node_modules", "dist", "testing"].includes(e.name) ? [] : walk(p);
      return /\.(ts|tsx)$/.test(e.name) && !/\.test\.tsx?$/.test(e.name) ? [p] : [];
    });
    const files = ["packages", "apps"].flatMap((d) => { try { return walk(`${APP}/${d}`); } catch { return []; } });
    const rel = (f: string) => f.slice(APP.length + 1);
    expect(files.filter((f) => LISTS.test(readFileSync(f, "utf8"))).map(rel).sort())
      .toEqual(["packages/core/src/calendar.generated.ts", "packages/core/src/flow.ts"]);
    expect(files.filter((f) => ISO.test(readFileSync(f, "utf8"))).map(rel)).toEqual(["packages/core/src/day.ts"]);
    // the scans can fail: each pattern catches the copy it exists for
    expect(LISTS.test('const W = ["Sun", "Mon", "Tue"];') && ISO.test("const D = /^\\d{4}-\\d{2}-\\d{2}$/;")).toBe(true);
  });
});
