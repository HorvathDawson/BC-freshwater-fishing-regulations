import { describe, expect, it } from "vitest";
import {
  ALERT_STALE_MS, alertLine, alertNotification, evaluateAlert, newestReading, parseAlertValue, runLine,
  type AlertRuleState, type GaugeAlertRule,
} from "./alerts";

const T0 = Date.parse("2026-11-05T12:00:00-07:00");
const iso = (minutes: number) => new Date(T0 + minutes * 60_000).toISOString();
const rule = (o: Partial<GaugeAlertRule> = {}): GaugeAlertRule => ({
  id: "r1", station: "08MH001", stationName: "Chilliwack River at Vedder Crossing", quantity: "discharge",
  direction: "above", value: 50, water: { item: "w1", name: "Chilliwack River" }, createdAt: T0, ...o,
});

/** Feed a rule a sequence of readings (one per 15 min), each checked when it is read; the fires, in order. */
function walk(r: GaugeAlertRule, values: (number | null)[]): boolean[] {
  let st: AlertRuleState | null = null;
  return values.map((v, i) => {
    const e = evaluateAlert(r, st, v === null ? null : { at: iso(i * 15), value: v }, T0 + i * 15 * 60_000);
    st = e.state;
    return e.fire;
  });
}

describe("gauge alert evaluation (T234)", () => {
  it("rising past an 'above' line fires once, and not again while it stays above", () => {
    expect(walk(rule(), [40, 45, 52, 60, 55, 51])).toEqual([false, false, true, false, false, false]);
  });

  it("falling past a 'below' line fires once", () => {
    expect(walk(rule({ direction: "below", value: 10 }), [14, 12, 9, 8, 9.5])).toEqual([false, false, true, false, false]);
  });

  it("fires again only after coming back inside by the margin and crossing again (hysteresis)", () => {
    // 50 ± 2 %: 49.5 is back over the line but inside the margin (49), so 51 is not a new crossing; 48 re-arms
    expect(walk(rule(), [40, 52, 49.5, 51, 48, 53])).toEqual([false, true, false, false, false, true]);
  });

  it("the first reading only sets the side: a rule set while already past does not ring at once", () => {
    expect(walk(rule(), [60, 62, 40, 55])).toEqual([false, false, false, true]);
  });

  it("missing data changes nothing and fires nothing", () => {
    expect(walk(rule(), [40, null, null, 55])).toEqual([false, false, false, true]);
    const e = evaluateAlert(rule(), { past: false }, null, T0);
    expect(e).toEqual({ state: { past: false }, fire: false, skipped: "missing" });
  });

  it("a stale reading is not evaluated: no fire, the state kept", () => {
    const old = { at: iso(0), value: 99 };
    const e = evaluateAlert(rule(), { past: false, at: iso(-15), value: 40 }, old, T0 + ALERT_STALE_MS + 60_000);
    expect(e.fire).toBe(false);
    expect(e.skipped).toBe("stale");
    expect(e.state).toEqual({ past: false, at: iso(-15), value: 40 });
  });

  it("the same reading read twice (the feed has not moved) fires once", () => {
    const r = rule();
    const a = evaluateAlert(r, { past: false, at: iso(0), value: 40 }, { at: iso(15), value: 60 }, T0 + 15 * 60_000);
    const b = evaluateAlert(r, a.state, { at: iso(15), value: 60 }, T0 + 30 * 60_000);
    expect([a.fire, b.fire]).toEqual([true, false]);
  });

  it("a level rule reads the level column of the feed's rows", () => {
    const rows = [[iso(0), 1.2, 30], [iso(30), 1.4, null], [iso(15), 1.3, 31]] as const;
    expect(newestReading(rows, "level")).toEqual({ at: iso(30), value: 1.4 });
    expect(newestReading(rows, "discharge")).toEqual({ at: iso(15), value: 31 });
    expect(newestReading(undefined, "level")).toBeNull();
    expect(newestReading([[iso(0), null, null]], "level")).toBeNull();
  });
});

describe("alert words (T234)", () => {
  it("a rule and its notification, whole", () => {
    expect(alertLine(rule())).toBe("Flow rises above 50 m³/s");
    expect(alertLine(rule({ quantity: "level", direction: "below", value: 1.25 }))).toBe("Level falls below 1.25 m");
    expect(alertNotification(rule(), { at: "2026-11-05T14:00:00-07:00", value: 52.34 })).toEqual({
      title: "Chilliwack River: flow above 50 m³/s",
      body: "Chilliwack River at Vedder Crossing read 52.3 m³/s at 14:00 PT.",
    });
    expect(alertNotification(rule({ water: null }), { at: "2026-11-05T14:00:00-07:00", value: 52 }).title)
      .toBe("Chilliwack River at Vedder Crossing: flow above 50 m³/s");
  });

  it("a typed value", () => {
    expect([parseAlertValue("12.5"), parseAlertValue(" 3 "), parseAlertValue("12,5"), parseAlertValue(".5"),
            parseAlertValue("abc"), parseAlertValue(""), parseAlertValue("1e3")]).toEqual([12.5, 3, 12.5, 0.5, null, null, null]);
  });

  it("a background run's line", () => {
    expect(runLine({ at: Date.parse("2026-11-05T14:05:00-07:00"), ms: 1234, result: "1 alert" }))
      .toBe("2026-11-05 at 14:05 PT · 1.2 s · 1 alert");
  });
});
