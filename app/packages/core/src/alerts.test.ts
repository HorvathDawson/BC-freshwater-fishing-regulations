import { describe, expect, it } from "vitest";
import {
  ALERT_STALE_MS, alertCardLine, alertContextLine, alertLine, alertNotification, alertSummary, alertValueOk,
  cadenceAllows, dayAgoReading, evaluateAlert, evaluateAlerts, migrateAlertRule, newestReading, normalRangeOf,
  parseAlertValue, runLine, WATER_DAY_MS, type AlertInput, type AlertRuleState, type GaugeAlertRule,
} from "./alerts";

const T0 = Date.parse("2026-11-05T12:00:00-07:00");
const H = 3_600_000;
const iso = (minutes: number) => new Date(T0 + minutes * 60_000).toISOString();
const rule = (o: Partial<GaugeAlertRule> = {}): GaugeAlertRule => ({
  id: "r1", station: "08MH001", stationName: "Chilliwack River at Vedder Crossing", quantity: "discharge",
  kind: "above", value: 50, gapH: null, on: true, water: { item: "w1", name: "Chilliwack River" }, createdAt: T0, ...o,
});
const at = (i: number, v: number | null): AlertInput | null => (v === null ? null : { now: { at: iso(i * 15), value: v } });

/** Feed a rule a sequence of readings (one per 15 min), each checked when it is read; the crossings, in order. */
function walk(r: GaugeAlertRule, values: (number | null)[]): boolean[] {
  let st: AlertRuleState | null = null;
  return values.map((v, i) => {
    const e = evaluateAlert(r, st, at(i, v), T0 + i * 15 * 60_000);
    st = e.state;
    return e.due;
  });
}

describe("gauge alert evaluation: above / below (T234)", () => {
  it("rising past an 'above' line is due once, and not again while it stays above", () => {
    expect(walk(rule(), [40, 45, 52, 60, 55, 51])).toEqual([false, false, true, false, false, false]);
  });

  it("falling past a 'below' line is due once", () => {
    expect(walk(rule({ kind: "below", value: 10 }), [14, 12, 9, 8, 9.5])).toEqual([false, false, true, false, false]);
  });

  it("due again only after coming back inside by the margin and crossing again (hysteresis)", () => {
    // 50 ± 2 %: 49.5 is back over the line but inside the margin (49), so 51 is not a new crossing; 48 re-arms
    expect(walk(rule(), [40, 52, 49.5, 51, 48, 53])).toEqual([false, true, false, false, false, true]);
  });

  it("the first reading only sets the side: an alert set while already past does not ring at once", () => {
    expect(walk(rule(), [60, 62, 40, 55])).toEqual([false, false, false, true]);
  });

  it("missing data changes nothing and fires nothing", () => {
    expect(walk(rule(), [40, null, null, 55])).toEqual([false, false, false, true]);
    expect(evaluateAlert(rule(), { past: false }, null, T0)).toEqual({ state: { past: false }, due: false, skipped: "missing" });
  });

  it("a stale reading is not evaluated: nothing due, the state kept", () => {
    const e = evaluateAlert(rule(), { past: false, at: iso(-15), value: 40 }, { now: { at: iso(0), value: 99 } },
                            T0 + ALERT_STALE_MS + 60_000);
    expect(e).toEqual({ state: { past: false, at: iso(-15), value: 40 }, due: false, skipped: "stale" });
  });

  it("the same reading read twice (the feed has not moved) is due once", () => {
    const a = evaluateAlert(rule(), { past: false, at: iso(0), value: 40 }, at(1, 60), T0 + 15 * 60_000);
    const b = evaluateAlert(rule(), a.state, at(1, 60), T0 + 30 * 60_000);
    expect([a.due, b.due]).toEqual([true, false]);
  });

  it("a level alert reads the level column of the feed's rows", () => {
    const rows = [[iso(0), 1.2, 30], [iso(30), 1.4, null], [iso(15), 1.3, 31]] as const;
    expect(newestReading(rows, "level")).toEqual({ at: iso(30), value: 1.4 });
    expect(newestReading(rows, "discharge")).toEqual({ at: iso(15), value: 31 });
    expect(newestReading(undefined, "level")).toBeNull();
    expect(newestReading([[iso(0), null, null]], "level")).toBeNull();
  });
});

describe("gauge alert evaluation: ±% within 24 h (T234 v1.1)", () => {
  const rise = rule({ kind: "rise", value: 50 });
  const fall = rule({ kind: "fall", value: 30 });
  const day = (then: number, now: number, i = 0): AlertInput =>
    ({ now: { at: iso(i * 15), value: now }, dayAgo: { at: iso(i * 15 - 24 * 60), value: then } });

  it("rises by Z %: due when the change reaches Z, once per crossing", () => {
    let s = evaluateAlert(rise, null, day(20, 25), T0).state;                         // +25 %: sets the side
    const a = evaluateAlert(rise, s, day(20, 31, 1), T0 + 15 * 60_000);                // +55 %: due
    s = a.state;
    const b = evaluateAlert(rise, s, day(20, 34, 2), T0 + 30 * 60_000);                // +70 %: still past, quiet
    expect([a.due, b.due]).toEqual([true, false]);
    expect(a.state.change).toBeCloseTo(55);
  });

  it("falls by Z %: a fall is positive for 'fall'; a rise never fires it", () => {
    const s = evaluateAlert(fall, null, day(100, 95), T0).state;
    expect(evaluateAlert(fall, s, day(100, 65, 1), T0 + 15 * 60_000).due).toBe(true);    // −35 %
    expect(evaluateAlert(fall, s, day(100, 200, 1), T0 + 15 * 60_000).due).toBe(false);  // +100 %
  });

  it("re-arms only once the change is 2 points back under the line", () => {
    let s = evaluateAlert(rise, null, day(20, 21), T0).state;
    const seq = [31, 30.7, 31, 30, 31];   // +55 due; +53.5, +55, +50 (still ≥ 50), +55: all still past, quiet
    const due = seq.map((v, i) => { const e = evaluateAlert(rise, s, day(20, v, i + 1), T0 + (i + 1) * 15 * 60_000); s = e.state; return e.due; });
    expect(due).toEqual([true, false, false, false, false]);
    const back = evaluateAlert(rise, s, day(20, 29.5, 9), T0 + 9 * 15 * 60_000);       // +47.5: back by 2.5 points
    const again = evaluateAlert(rise, back.state, day(20, 31, 10), T0 + 10 * 15 * 60_000);
    expect([back.due, again.due]).toEqual([false, true]);
  });

  it("no reading 24 h earlier: no fire, no error, the state kept", () => {
    const e = evaluateAlert(rise, { past: false }, { now: { at: iso(0), value: 99 }, dayAgo: null }, T0);
    expect(e).toEqual({ state: { past: false }, due: false, skipped: "no-day-ago" });
    expect(evaluateAlert(rise, { past: false }, day(0, 5), T0).skipped).toBe("no-day-ago");   // a zero base: no %
  });

  it("dayAgoReading: the row nearest 24 h before the newest, within 2 h; none outside", () => {
    const now = { at: iso(0), value: 10 };
    const rows = [[iso(-24 * 60 - 150), null, 1], [iso(-24 * 60 - 30), null, 2], [iso(-24 * 60 + 60), null, 3],
                  [iso(0), null, 10]] as const;
    expect(dayAgoReading(rows, "discharge", now)).toEqual({ at: iso(-24 * 60 - 30), value: 2 });
    expect(dayAgoReading([[iso(-20 * 60), null, 5], [iso(0), null, 10]], "discharge", now)).toBeNull();
    expect(dayAgoReading(rows, "level", now)).toBeNull();
    expect(dayAgoReading(rows, "discharge", null)).toBeNull();
  });
});

describe("cadence (T234 v1.1)", () => {
  const a = rule({ id: "a" });
  const b = rule({ id: "b", quantity: "level", value: 2 });
  const other = rule({ id: "c", water: { item: "w2", name: "Vedder" } });
  const crossing: Record<string, AlertRuleState> = { a: { past: false }, b: { past: false }, c: { past: false } };
  const past = (r: GaugeAlertRule, i: number): AlertInput => ({ now: { at: iso(i), value: r.quantity === "level" ? 3 : 60 } });

  it("default: at most one notification per water per 24 h ACROSS its alerts; another water is not held", () => {
    const r = evaluateAlerts([a, b, other], crossing, (x) => past(x, 0), {}, T0);
    expect(r.fire.map((f) => f.rule.id)).toEqual(["a", "c"]);
    expect([r.dropped, r.states.b!.past]).toEqual([1, true]);
    expect(r.waterFired).toEqual({ w1: T0, w2: T0 });
  });

  it("a crossing the cadence blocks is dropped, never told later (owner 2026-11-05); a new crossing after the day is", () => {
    const first = evaluateAlerts([a, b], crossing, (x) => past(x, 0), {}, T0);
    const later = evaluateAlerts([a, b], first.states, (x) => past(x, 60), first.waterFired, T0 + 6 * H);
    expect(later.fire).toEqual([]);
    // still past after the day: nothing (no queue; it was never back inside)
    const day = evaluateAlerts([a, b], later.states, (x) => past(x, 24 * 60 + 15), later.waterFired, T0 + WATER_DAY_MS + 15 * 60_000);
    expect(day.fire).toEqual([]);
    // rise and fall inside the day: nothing then, nothing after
    const inside = evaluateAlerts([b], first.states, () => ({ now: { at: iso(120), value: 1 } }), first.waterFired, T0 + 2 * H);
    const after = evaluateAlerts([b], inside.states, () => ({ now: { at: iso(25 * 60), value: 1.5 } }), inside.waterFired, T0 + 25 * H);
    expect(after.fire.length).toBe(0);
    // back inside, then past again once the day allows: told
    const again = evaluateAlerts([b], after.states, () => ({ now: { at: iso(26 * 60), value: 3 } }), after.waterFired, T0 + 26 * H);
    expect(again.fire.map((f) => f.rule.id)).toEqual(["b"]);
  });

  it("a per-alert gap is the alert's own: a 1 h alert fires again after an hour and a re-crossing", () => {
    const quick = rule({ id: "q", gapH: 1 });
    let st: Record<string, AlertRuleState> = { q: { past: false } };
    let w: Record<string, number> = { w1: T0 - 60_000 };          // the water was just told by another alert
    const seq: [number, number][] = [[0, 60], [15, 40], [30, 60], [75, 40], [90, 60]];
    const fired = seq.map(([m, v]) => {
      const r = evaluateAlerts([quick], st, () => ({ now: { at: iso(m), value: v } }), w, T0 + m * 60_000);
      st = r.states; w = r.waterFired;
      return r.fire.length;
    });
    // fires at 0 (its own gap, not the water's day); the re-crossing at 30 is dropped (gap 1 h); 75 back inside;
    // 90 crosses again with the hour passed
    expect(fired).toEqual([1, 0, 0, 0, 1]);
  });

  it("hysteresis holds under any gap: hovering on the line does not fire at every gap", () => {
    const quick = rule({ id: "q", gapH: 1 });
    let st: Record<string, AlertRuleState> = { q: { past: false } };
    let w: Record<string, number> = {};
    let n = 0;
    for (let i = 0; i < 12; i++) {
      const v = i % 2 ? 50.5 : 49.6;                               // wobbles across 50, never 2 % back inside
      const r = evaluateAlerts([quick], st, () => ({ now: { at: iso(i * 60), value: v } }), w, T0 + i * H);
      st = r.states; w = r.waterFired; n += r.fire.length;
    }
    expect(n).toBe(1);
  });

  it("an alert switched off is not evaluated", () => {
    const r = evaluateAlerts([rule({ on: false })], { r1: { past: false } }, () => past(a, 0), {}, T0);
    expect([r.fire.length, "r1" in r.states]).toEqual([0, false]);
  });

  it("cadenceAllows", () => {
    expect(cadenceAllows({ gapH: null }, null, null, T0)).toBe(true);
    expect(cadenceAllows({ gapH: null }, null, T0 - 23 * H, T0)).toBe(false);
    expect(cadenceAllows({ gapH: null }, null, T0 - 24 * H, T0)).toBe(true);
    expect(cadenceAllows({ gapH: 3 }, T0 - 2 * H, T0 - 60_000, T0)).toBe(false);
    expect(cadenceAllows({ gapH: 3 }, T0 - 3 * H, T0 - 60_000, T0)).toBe(true);
  });
});

describe("v1 alerts migrate (T234 v1.1)", () => {
  it("a v1 rule keeps everything: its direction is its kind, the default cadence, on", () => {
    const v1 = { id: "x", station: "08MH001", stationName: "Vedder", quantity: "level", direction: "below", value: 1.2,
                 water: { item: "w1", name: "Chilliwack River" }, createdAt: 5 };
    expect(migrateAlertRule(v1)).toEqual({ id: "x", station: "08MH001", stationName: "Vedder", quantity: "level",
      kind: "below", value: 1.2, gapH: null, on: true, water: { item: "w1", name: "Chilliwack River" }, createdAt: 5 });
  });
  it("a v1.1 rule reads back as itself; a broken one is null", () => {
    const r = rule({ kind: "rise", gapH: 6, on: false });
    expect(migrateAlertRule(JSON.parse(JSON.stringify(r)))).toEqual(r);
    expect(migrateAlertRule({ id: "x", station: "s", value: 1, quantity: "discharge" })).toBeNull();
    expect(migrateAlertRule(null)).toBeNull();
  });
});

describe("alert words (T234)", () => {
  it("each kind's line, the cadence and the card line, whole", () => {
    expect(alertLine(rule())).toBe("Flow above 50 m³/s");
    expect(alertLine(rule({ quantity: "level", kind: "below", value: 1.25 }))).toBe("Level below 1.25 m");
    expect(alertLine(rule({ kind: "rise", value: 50 }))).toBe("Flow rises 50 % or more within 24 h");
    expect(alertLine(rule({ kind: "fall", value: 30 }))).toBe("Flow falls 30 % or more within 24 h");
    expect(alertCardLine(rule(), null)).toBe("Flow above 50 m³/s · at most once a day for this water · not sent yet");
    expect(alertCardLine(rule({ gapH: 3 }), { past: true, firedAt: Date.parse("2026-11-05T14:05:00-07:00") }))
      .toBe("Flow above 50 m³/s · at most once every 3 h · last sent 2026-11-05 at 14:05 PT");
    expect(alertCardLine(rule({ on: false }), null)).toBe("Flow above 50 m³/s · at most once a day for this water · off");
  });

  it("the editor's summary line, whole", () => {
    expect(alertSummary(rule())).toBe("You get a notification when the flow at Chilliwack River at Vedder Crossing rises "
      + "above 50 m³/s, at most once a day for Chilliwack River. Not again until it has dropped back under the line.");
    expect(alertSummary(rule({ kind: "fall", value: 30, gapH: 48, quantity: "level" }))).toBe("You get a notification "
      + "when the level at Chilliwack River at Vedder Crossing falls by 30 % or more within 24 hours, at most once every "
      + "2 days. Not again until the day's change has eased off.");
  });

  it("the context beside the value", () => {
    expect(alertContextLine("discharge", 31.24, normalRangeOf([5, 12, 18, 25, 40]))).toBe("Now 31.2 m³/s · normal today 12–25 m³/s");
    expect(alertContextLine("level", null, null)).toBe("No reading now · no normal range for today");
  });

  it("notifications, whole", () => {
    expect(alertNotification(rule(), { now: { at: "2026-11-05T14:00:00-07:00", value: 52.34 } })).toEqual({
      title: "Chilliwack River: flow above 50 m³/s",
      body: "Chilliwack River at Vedder Crossing read 52.3 m³/s at 14:00 PT.",
    });
    expect(alertNotification(rule({ water: null }), { now: { at: "2026-11-05T14:00:00-07:00", value: 52 } }).title)
      .toBe("Chilliwack River at Vedder Crossing: flow above 50 m³/s");
    expect(alertNotification(rule({ kind: "rise", value: 50 }), { now: { at: "2026-11-05T14:00:00-07:00", value: 31 },
                                                                   dayAgo: { at: "2026-11-04T14:00:00-07:00", value: 20 } }))
      .toEqual({ title: "Chilliwack River: flow up 55 % in a day",
                 body: "Chilliwack River at Vedder Crossing read 31 m³/s at 14:00 PT, from 20 m³/s a day earlier." });
  });

  it("a typed value, and what each kind accepts", () => {
    expect([parseAlertValue("12.5"), parseAlertValue(" 3 "), parseAlertValue("12,5"), parseAlertValue(".5"),
            parseAlertValue("abc"), parseAlertValue(""), parseAlertValue("1e3")]).toEqual([12.5, 3, 12.5, 0.5, null, null, null]);
    expect([alertValueOk("rise", 0), alertValueOk("rise", 150), alertValueOk("fall", 100), alertValueOk("above", -1),
            alertValueOk("below", null)]).toEqual([false, true, false, true, false]);
  });

  it("a background run's line", () => {
    expect(runLine({ at: Date.parse("2026-11-05T14:05:00-07:00"), ms: 1234, result: "1 alert" }))
      .toBe("2026-11-05 at 14:05 PT · 1.2 s · 1 alert");
  });
});
