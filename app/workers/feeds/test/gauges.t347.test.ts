// T347 (owner 2026-11-02): the Forecast Centre issues on working days only, so a run is current to the next working
// day's issue plus the grace — the port of pipeline/gauges/feed/change.py `due_by` / `bc_holidays`, held to the same
// cases as test_forecast_change.py and to the holiday list the pipeline emits for the app (gauge-policy.generated.ts).
import { describe, expect, it } from "vitest";
import { FORECAST_HOLIDAYS } from "../../../packages/core/src/gauge-policy.generated";
import { bcHolidays, dueBy, stale } from "../src/gauges/change";

const at = (s: string) => Date.parse(`${s}-08:00`);

describe("T347: a forecast run is current to the next working day's issue", () => {
  it("a weekday run lasts 36 h, as before", () => {
    const now = Date.parse("2026-10-22T22:00:00Z");                    // Thu 14:00 PST
    expect([stale({ issuedAt: "2026-10-21T01:59:00" }, now), stale({ issuedAt: "2026-10-21T02:00:00" }, now),
            stale({ issuedAt: null }, now)]).toEqual([true, false, false]);
  });

  it("Friday's run holds the weekend and Monday's expected issue; a missed Monday is stale", () => {
    const fri = { issuedAt: "2026-10-23T09:00:00" };
    expect(["2026-10-24T12:00:00", "2026-10-25T20:00:00", "2026-10-26T08:00:00", "2026-10-26T20:59:00", "2026-10-26T21:01:00"]
      .map((t) => stale(fri, at(t)))).toEqual([false, false, false, false, true]);
    expect(dueBy(fri.issuedAt)).toBe(at("2026-10-26T21:00:00"));
  });

  it("a holiday Monday (Thanksgiving 2026-10-12) holds Friday's run to Tuesday evening", () => {
    const fri = { issuedAt: "2026-10-09T09:00:00" };
    expect([stale(fri, at("2026-10-13T20:59:00")), stale(fri, at("2026-10-13T21:01:00"))]).toEqual([false, true]);
  });

  it("the holidays are the pipeline's, every emitted year", () => {
    const years = [...new Set(FORECAST_HOLIDAYS.map((d) => +d.slice(0, 4)))];
    expect(years.flatMap((y) => [...bcHolidays(y)].sort())).toEqual(FORECAST_HOLIDAYS);
  });
});
