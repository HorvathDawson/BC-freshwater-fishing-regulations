/**
 * (T234, T527) Gauge alerts as pure functions: a crossing above / below a value or a percentile's line for the day,
 * fired once, re-armed past the hysteresis margin; the date-moving percentile line; the migration of saved alerts; the
 * finished words (AGENTS 33). Mutation-style cases pin each boundary from both sides, so a flipped comparison, a lost
 * margin or a day off by one fails a named case.
 */
import { describe, expect, it } from "vitest";
import {
  ALERT_STALE_MS, alertContextLine, alertLine, alertListLine, alertNotification, alertSummary, evaluateAlert,
  evaluateAlerts, findGauges, hasRecord, migrateAlertRule, newestReading, normalRangeOf, parseAlertValue,
  percentileByDay, percentileOk, rearmMargin, refreshAlertLines, readingDay, runLine, thresholdAt, NOTIFY_WORDS,
  type AlertReading, type AlertRuleState, type AlertThreshold, type GaugeAlertRule,
} from "./alerts";
import { levelName } from "./standing";
import { calendarDayOfYear } from "./day";
import { ordinal, percentileInBand, valueAtPercentile, type Band } from "./flow";

const T0 = Date.parse("2026-11-05T12:00:00-07:00");
const iso = (minutes: number) => new Date(T0 + minutes * 60_000).toISOString();
const value = (v: number): AlertThreshold => ({ kind: "value", value: v });
const rule = (o: Partial<GaugeAlertRule> = {}): GaugeAlertRule => ({
  id: "r1", station: "08MH001", stationName: "Chilliwack River at Vedder Crossing", quantity: "discharge",
  direction: "above", threshold: value(50), on: true, water: { item: "w1", name: "Chilliwack River" }, createdAt: T0, ...o,
});
const at = (i: number, v: number | null): AlertReading | null => (v === null ? null : { at: iso(i * 15), value: v });

/** Feed a rule a sequence of readings (one per 15 min), each checked when it is read; the crossings, in order. */
function walk(r: GaugeAlertRule, values: (number | null)[]): boolean[] {
  let st: AlertRuleState | null = null;
  return values.map((v, i) => {
    const e = evaluateAlert(r, st, at(i, v), T0 + i * 15 * 60_000);
    st = e.state;
    return e.due;
  });
}

describe("crossing a value (T527: edge-triggered, like a price alert)", () => {
  it("crossing above fires once, and not again while it stays above", () => {
    expect(walk(rule(), [40, 45, 52, 60, 55, 51])).toEqual([false, false, true, false, false, false]);
  });

  it("crossing below fires once", () => {
    expect(walk(rule({ direction: "below", threshold: value(10) }), [14, 12, 9, 8, 9.5])).toEqual([false, false, true, false, false]);
  });

  it("the line itself is not past it: above needs > the line, below needs < the line (mutation: >= / <=)", () => {
    expect(walk(rule(), [40, 50, 50.01])).toEqual([false, false, true]);
    expect(walk(rule({ direction: "below", threshold: value(10) }), [12, 10, 9.99])).toEqual([false, false, true]);
  });

  it("re-arms only once back across by the margin (hysteresis), then a new crossing fires", () => {
    // 50 ± 2 % = 1: 49.5 is back under the line but inside the margin, so 51 is no new crossing; 49 re-arms
    expect(walk(rule(), [40, 52, 49.5, 51, 49, 53])).toEqual([false, true, false, false, false, true]);
  });

  it("the margin's edge re-arms; just inside it does not (mutation: < vs <=)", () => {
    expect(walk(rule(), [40, 52, 49, 51])).toEqual([false, true, false, true]);        // 49 = 50 − 1: re-armed
    expect(walk(rule(), [40, 52, 49.01, 51])).toEqual([false, true, false, false]);    // 49.01: still inside
  });

  it("below re-arms only above the line by the margin (the margin is on the other side)", () => {
    const r = rule({ direction: "below", threshold: value(10) });     // margin 0.2
    expect(walk(r, [12, 9, 10.1, 9, 10.2, 9])).toEqual([false, true, false, false, false, true]);
  });

  it("a noisy reading wobbling on the line rings once, not on every wobble", () => {
    expect(walk(rule(), [49.8, 50.2, 49.7, 50.3, 49.6, 50.4, 49.9, 50.1]).filter(Boolean)).toHaveLength(1);
  });

  it("the margin is 2 % of the line, never under the quantity's floor", () => {
    expect(rearmMargin("discharge", 50)).toBe(1);
    expect(rearmMargin("discharge", 1)).toBe(0.05);   // 2 % = 0.02, the floor wins
    expect(rearmMargin("level", 0)).toBe(0.01);
    expect(rearmMargin("discharge", -100)).toBe(2);
  });

  it("the first reading only sets the side: an alert saved while already past does not ring at once", () => {
    expect(walk(rule(), [60, 62, 40, 55])).toEqual([false, false, false, true]);
  });

  it("missing data changes nothing and fires nothing", () => {
    expect(walk(rule(), [40, null, null, 55])).toEqual([false, false, false, true]);
    expect(evaluateAlert(rule(), { past: false }, null, T0))
      .toEqual({ state: { past: false, skipped: "missing", skippedSince: T0 }, due: false, line: null, skipped: "missing" });
    expect(evaluateAlert(rule(), { past: false }, { at: iso(0), value: NaN }, T0).skipped).toBe("missing");
  });

  it("a stale reading is not evaluated: nothing due, the state kept; just inside the limit is", () => {
    const st = { past: false, at: iso(-15), value: 40 };
    const late = evaluateAlert(rule(), st, { at: iso(0), value: 99 }, T0 + ALERT_STALE_MS + 60_000);
    expect(late).toEqual({ state: { ...st, skipped: "stale", skippedSince: T0 + ALERT_STALE_MS + 60_000 }, due: false,
                           line: null, skipped: "stale" });
    expect(evaluateAlert(rule(), st, { at: iso(0), value: 99 }, T0 + ALERT_STALE_MS).due).toBe(true);
    expect(evaluateAlert(rule(), st, { at: "not a time", value: 99 }, T0).skipped).toBe("stale");
  });

  it("the same reading read twice (the feed has not moved) is due once", () => {
    const a = evaluateAlert(rule(), { past: false, at: iso(0), value: 40 }, at(1, 60), T0 + 15 * 60_000);
    const b = evaluateAlert(rule(), a.state, at(1, 60), T0 + 30 * 60_000);
    expect([a.due, b.due]).toEqual([true, false]);
    expect(a.line).toBe(50);
  });
});

/** An envelope that is the same every day except a step on one day, so a day off by one shows. */
const BAND: Band = [10, 20, 40, 60, 80];
const yearOf = (f: (day: number) => Band | null): (Band | null)[] => Array.from({ length: 366 }, (_, i) => f(i + 1));

describe("the value at a percentile (flow.ts, one formula with the chart's ranking)", () => {
  it("is the knot itself at p10 / p25 / p50 / p75 / p90", () => {
    expect([0.1, 0.25, 0.5, 0.75, 0.9].map((p) => valueAtPercentile(BAND, p))).toEqual([10, 20, 40, 60, 80]);
  });

  it("is linear between the knots", () => {
    expect(valueAtPercentile(BAND, 0.3)).toBeCloseTo(24);       // 20 + (0.05 / 0.25) × 20
    expect(valueAtPercentile(BAND, 0.85)).toBeCloseTo(73.333, 3);
  });

  it("runs percentileInBand backwards: every whole percentile inside the outer knots round-trips", () => {
    // at p10 / p90 exactly the ranking says "the bottom / top tenth" (0.05 / 0.95): the knot is the edge of the record
    expect([percentileInBand(valueAtPercentile(BAND, 0.1), BAND), percentileInBand(valueAtPercentile(BAND, 0.9), BAND)]).toEqual([0.05, 0.95]);
    for (let p = 11; p <= 89; p++) expect(percentileInBand(valueAtPercentile(BAND, p / 100), BAND)).toBeCloseTo(p / 100, 9);
  });

  it("is null outside p10..p90 (the record holds nothing past its outer knots) and with no envelope", () => {
    expect([valueAtPercentile(BAND, 0.09), valueAtPercentile(BAND, 0.91), valueAtPercentile(null, 0.5),
            valueAtPercentile(BAND, NaN)]).toEqual([null, null, null, null]);
  });
});

describe("a percentile for the day: the line moves with the date (T527)", () => {
  // a river that rises through the year: day d's envelope is BAND × d / 100
  const rising = yearOf((d) => BAND.map((v) => (v * d) / 100) as unknown as Band);
  const p75 = (byDay = percentileByDay(rising, 75)): AlertThreshold => ({ kind: "percentile", pct: 75, byDay });

  it("one line per day of the year, from that day's envelope", () => {
    const line = percentileByDay(rising, 75);
    expect(line).toHaveLength(366);
    expect(line[0]).toBeCloseTo(0.6);          // Jan 1: 60 × 1 / 100
    expect(line[312]).toBeCloseTo(187.8);      // day 313 (Nov 9 2026): 60 × 313 / 100
    expect(percentileByDay(rising, 50)[312]).toBeCloseTo(125.2);
  });

  it("a day with no envelope has no line; a year with none has no record", () => {
    const gappy = yearOf((d) => (d === 200 ? null : BAND));
    expect(percentileByDay(gappy, 50)[199]).toBeNull();
    expect(percentileByDay(gappy, 50)[198]).toBe(40);
    expect(hasRecord(yearOf(() => null))).toBe(false);
    expect(hasRecord(gappy)).toBe(true);
    expect(hasRecord(null)).toBe(false);
  });

  it("the line is the reading's own day (as the chart's band), never the day before or after (mutation: ±1)", () => {
    const steps = yearOf((d) => [d, d, d, d, d] as unknown as Band);    // the line on day d is d
    const t: AlertThreshold = { kind: "percentile", pct: 50, byDay: percentileByDay(steps, 50) };
    expect(thresholdAt(t, "2026-11-09T07:00:00-08:00")).toBe(313);
    expect(calendarDayOfYear("2026-11-09T07:00:00-08:00")).toBe(313);
    expect(thresholdAt(t, "2026-01-01T00:00:00-08:00")).toBe(1);
    expect(thresholdAt(t, "2024-12-31T23:30:00-08:00")).toBe(366);     // a leap year's last day
    expect(thresholdAt(value(7), "2026-11-09T07:00:00-08:00")).toBe(7);
  });

  /** A reading at noon PST on day `d` of 2026, checked a minute later. */
  const day = (d: number, v: number): AlertReading =>
    ({ at: new Date(Date.UTC(2026, 0, d, 20)).toISOString().slice(0, 10) + "T12:00:00-08:00", value: v });
  function checker(r: GaugeAlertRule) {
    let st: AlertRuleState | null = null;
    return (rd: AlertReading) => {
      const e = evaluateAlert(r, st, rd, Date.parse(rd.at) + 60_000);
      st = e.state;
      return [e.due, e.line === null ? null : Math.round(e.line * 100) / 100];
    };
  }

  it("crossing above a rising season's 8 in 10 days lower, re-armed when the line moves up past the margin", () => {
    const check = checker(rule({ threshold: p75() }));     // the line on day d is 0.6 d
    expect(check(day(311, 180))).toEqual([false, 186.6]);   // under the line: sets the side
    expect(check(day(312, 188))).toEqual([true, 187.2]);    // crossed above that day's 8 in 10 days lower
    expect(check(day(313, 188))).toEqual([false, 187.8]);   // still above: not again
    expect(check(day(316, 188))).toEqual([false, 189.6]);   // under the line now, but inside the margin (185.81): not re-armed
    expect(check(day(317, 199))).toEqual([false, 190.2]);   // so this is no new crossing
    expect(check(day(330, 188))).toEqual([false, 198]);     // 188 ≤ 198 − 3.96: re-armed
    expect(check(day(331, 199))).toEqual([true, 198.6]);    // a new crossing
  });

  it("a steady reading crosses when the line moves down past it with the date (the threshold is the day's)", () => {
    const falling = yearOf((d) => BAND.map((v) => (v * (400 - d)) / 100) as unknown as Band);   // p50 line: 0.4 (400 − d)
    const check = checker(rule({ threshold: { kind: "percentile", pct: 50, byDay: percentileByDay(falling, 50) } }));
    expect(check(day(140, 100))).toEqual([false, 104]);
    expect(check(day(149, 100))).toEqual([false, 100.4]);
    expect(check(day(150, 100))).toEqual([false, 100]);     // on the line is not past it
    expect(check(day(151, 100))).toEqual([true, 99.6]);     // the same 100, a lower line: crossed above
  });

  it("a day with no line skips the check and keeps the state (no record that day)", () => {
    const gappy = yearOf((d) => (d === 313 ? null : BAND));
    const r = rule({ threshold: { kind: "percentile", pct: 50, byDay: percentileByDay(gappy, 50) } });
    const st = { past: false, at: "2026-11-08T12:00:00-08:00", value: 30 };
    const e = evaluateAlert(r, st, { at: "2026-11-09T12:00:00-08:00", value: 99 }, Date.parse("2026-11-09T12:10:00-08:00"));
    expect(e).toEqual({ state: { ...st, skipped: "no-record", skippedSince: Date.parse("2026-11-09T12:10:00-08:00") },
                        due: false, line: null, skipped: "no-record" });
  });

  it("presets and custom: whole numbers 10..90 only", () => {
    expect([10, 25, 50, 75, 90, 33].map(percentileOk)).toEqual([true, true, true, true, true, true]);
    expect([9, 91, 50.5, null, NaN].map((p) => percentileOk(p as number | null))).toEqual([false, false, false, false, false]);
  });
});

describe("the line follows each day and the record in hand (T563)", () => {
  // a record whose p75 on day d is d (so Nov 4 = 308, Dec 31 = 365), and a newer record that is 10 higher every day
  const older = yearOf((d) => [d, d, d, d, d] as unknown as Band);
  const newer = yearOf((d) => [d + 10, d + 10, d + 10, d + 10, d + 10] as unknown as Band);
  const saved = (): GaugeAlertRule => rule({ threshold: { kind: "percentile", pct: 75, byDay: percentileByDay(older, 75) } });
  const NOV4 = "2026-11-04T12:00:00-08:00", DEC31 = "2026-12-31T12:00:00-08:00";

  it("saved on Nov 4 at p75, it fires on Dec 31 when the reading crosses Dec 31's line, not when it crosses Nov 4's", () => {
    const r = saved();
    const state = { past: false, at: "2026-12-30T12:00:00-08:00", value: 300 };
    const t = Date.parse(DEC31) + 60_000;
    const nov4Line = thresholdAt(r.threshold, NOV4);
    expect(nov4Line).toBe(308);
    const crossNov4 = evaluateAlert(r, state, { at: DEC31, value: 320 }, t);     // past Nov 4's 308, under Dec 31's 365
    expect(crossNov4).toMatchObject({ due: false, line: 365 });
    expect(evaluateAlert(r, state, { at: DEC31, value: 370 }, t)).toMatchObject({ due: true, line: 365 });
  });

  it("a newer record applies by itself: the lines are worked out again from it, the percentile kept", () => {
    const out = refreshAlertLines([saved()], () => newer);
    expect(out.changed).toBe(true);
    const t = out.rules[0]!.threshold;
    expect(t).toMatchObject({ kind: "percentile", pct: 75 });
    expect(thresholdAt(t, DEC31)).toBe(375);
    expect(thresholdAt(t, NOV4)).toBe(318);
  });

  it("the same record changes nothing (the same rules back, no write)", () => {
    const rules = [saved()];
    const out = refreshAlertLines(rules, () => older);
    expect(out).toEqual({ rules, changed: false, unread: [] });
    expect(out.rules).toBe(rules);
  });

  it("a record that cannot be read keeps the old line and says which alert; a value alert is never touched", () => {
    const v = rule({ id: "v" });
    const out = refreshAlertLines([saved(), v], () => null);
    expect(out.changed).toBe(false);
    expect(out.unread).toEqual(["r1"]);
    expect(out.rules[0]).toEqual(saved());
  });
});

describe("every alert, one check", () => {
  it("fires each crossed alert with the reading and the line; stamps when it fired; skips an alert switched off", () => {
    const a = rule();
    const b = rule({ id: "r2", direction: "below", threshold: value(45) });
    const off = rule({ id: "r3", on: false });
    const states = { r1: { past: false, at: iso(0), value: 40 }, r2: { past: false, at: iso(0), value: 48 } };
    const reading = { at: iso(15), value: 55 };
    const e = evaluateAlerts([a, b, off], states, () => reading, T0 + 15 * 60_000);
    expect(e.fire).toEqual([{ rule: a, reading, line: 50 }]);
    expect(e.states.r1).toEqual({ past: true, at: iso(15), value: 55, firedAt: T0 + 15 * 60_000 });
    expect(e.states.r2).toEqual({ past: false, at: iso(15), value: 55 });
    expect("r3" in e.states).toBe(false);
  });

  it("collects why alerts were skipped", () => {
    const e = evaluateAlerts([rule(), rule({ id: "r2" })], {}, (r) => (r.id === "r1" ? null : { at: iso(0), value: 1 }),
                             T0 + ALERT_STALE_MS * 2);
    expect(e.skipped).toEqual(["missing", "stale"]);
  });
});

describe("readings from the feed", () => {
  it("the newest reading of the quantity, skipping gaps", () => {
    const rows: [string, number | null, number | null][] = [[iso(0), 1.1, 30], [iso(30), 1.2, null], [iso(15), 1.0, 31]];
    expect(newestReading(rows, "discharge")).toEqual({ at: iso(15), value: 31 });
    expect(newestReading(rows, "level")).toEqual({ at: iso(30), value: 1.2 });
    expect(newestReading(undefined, "level")).toBeNull();
  });

  it("a typed value", () => {
    expect(["12.5", "12,5", " 3 ", "-0.4", ".5", "lots", "", "1e3"].map(parseAlertValue)).toEqual([12.5, 12.5, 3, -0.4, 0.5, null, null, null]);
  });
});

describe("saved alerts from before T527: migrated explicitly, refused and said, never dropped quietly", () => {
  const base = { id: "a", station: "08MH001", stationName: "Vedder", quantity: "discharge", water: { item: "w1", name: "Chilliwack River" }, createdAt: 1 };

  it("a v1 alert (direction, value) is a crossing of that value", () => {
    expect(migrateAlertRule({ ...base, direction: "below", value: 12 })).toEqual({ rule: {
      id: "a", station: "08MH001", stationName: "Vedder", quantity: "discharge", direction: "below", threshold: value(12),
      on: true, water: { item: "w1", name: "Chilliwack River" }, createdAt: 1 } });
  });

  it("a v1.1 above / below alert keeps its switch; its cadence is gone (once per crossing replaces it)", () => {
    const m = migrateAlertRule({ ...base, kind: "above", value: 30, gapH: 6, on: false });
    expect(m).toEqual({ rule: expect.objectContaining({ direction: "above", threshold: value(30), on: false }) });
    expect("gapH" in (m as { rule: object }).rule).toBe(false);
  });

  it("a v1.1 ±% alert is refused with its words (it has no line to cross)", () => {
    expect(migrateAlertRule({ ...base, kind: "rise", value: 50, gapH: null })).toEqual({ refused: {
      line: "Vedder · Flow rises 50 % or more within 24 h", why: NOTIFY_WORDS.refused.change } });
    expect(migrateAlertRule({ ...base, quantity: "level", kind: "fall", value: 20 })).toEqual({ refused: {
      line: "Vedder · Level falls 20 % or more within 24 h", why: NOTIFY_WORDS.refused.change } });
  });

  it("the current shape reads as is, a percentile with its 366-day line", () => {
    const t: AlertThreshold = { kind: "percentile", pct: 75, byDay: percentileByDay(yearOf(() => BAND), 75) };
    const r = rule({ threshold: t });
    expect(migrateAlertRule(JSON.parse(JSON.stringify(r)))).toEqual({ rule: r });
  });

  it("anything unreadable is refused as such: no id, no quantity, a broken line, a percentile out of range", () => {
    const bad = [null, 3, { ...base, id: 1, kind: "above", value: 1 }, { ...base, quantity: "temp", kind: "above", value: 1 },
                 { ...base, direction: "above", threshold: { kind: "percentile", pct: 75, byDay: [1, 2] } },
                 { ...base, direction: "above", threshold: { kind: "percentile", pct: 95, byDay: Array(366).fill(1) } },
                 { ...base, direction: "sideways", threshold: value(3) }, { ...base, kind: "above" }];
    for (const b of bad)
      expect(migrateAlertRule(b)).toEqual({ refused: { line: NOTIFY_WORDS.refused.unreadableLine, why: NOTIFY_WORDS.refused.unreadable } });
  });
});

describe("the words (whole strings, AGENTS 33)", () => {
  const pct = (p: number, d: GaugeAlertRule["direction"] = "above"): GaugeAlertRule =>
    rule({ direction: d, threshold: { kind: "percentile", pct: p, byDay: percentileByDay(yearOf(() => BAND), p) } });

  it("ordinals and percentile names", () => {
    expect([1, 2, 3, 4, 10, 11, 12, 13, 21, 22, 23, 25, 75, 90].map(ordinal))
      .toEqual(["1st", "2nd", "3rd", "4th", "10th", "11th", "12th", "13th", "21st", "22nd", "23rd", "25th", "75th", "90th"]);
    expect([levelName(50), levelName(75), levelName(10)]).toEqual(["the median", "the level higher than 8 days in 10", "the level lower than 9 days in 10"]);
    expect(readingDay("2026-11-09T07:00:00-08:00")).toBe("Nov 9");
  });

  it("an alert's line and its list line", () => {
    expect(alertLine(rule())).toBe("Flow crosses above 50 m³/s");
    expect(alertLine(rule({ quantity: "level", direction: "below", threshold: value(1.25) }))).toBe("Level crosses below 1.25 m");
    expect(alertLine(pct(75))).toBe("Flow crosses above the level higher than 8 days in 10 for the day");
    expect(alertLine(pct(50, "below"))).toBe("Flow crosses below the median for the day");
    expect(alertListLine(rule(), { past: true, firedAt: Date.parse("2026-11-05T14:05:00-07:00") }))
      .toBe("Chilliwack River at Vedder Crossing · Flow crosses above 50 m³/s · last fired 2026-11-05 at 14:05 PT");
    expect(alertListLine(rule(), null)).toBe("Chilliwack River at Vedder Crossing · Flow crosses above 50 m³/s · not fired yet");
    expect(alertListLine(rule({ on: false }), { past: true, firedAt: 1 })).toBe("Chilliwack River at Vedder Crossing · Flow crosses above 50 m³/s · off");
  });

  it("the notification: the gauge, the line, the reading and its time; a percentile names the reading's day", () => {
    const cow = { ...pct(75), stationName: "Cowichan R. at Lake Cowichan", water: { item: "w", name: "Cowichan River" } };
    expect(alertNotification(cow, { at: "2026-11-09T14:00:00-07:00", value: 41.2 })).toEqual({
      title: "Cowichan River: flow alert",
      body: "Cowichan R. at Lake Cowichan crossed above the level higher than 8 days in 10 for Nov 9 — 41.2 m³/s at 14:00 PT.",
    });
    expect(alertNotification(rule({ direction: "below", threshold: value(12), water: null, quantity: "level" }),
                             { at: "2026-11-09T06:30:00-07:00", value: 11.94 })).toEqual({
      title: "Chilliwack River at Vedder Crossing: level alert",
      body: "Chilliwack River at Vedder Crossing crossed below 12 m — 11.9 m at 06:30 PT.",
    });
  });

  it("the editor's one plain line: what happens, when it is ready again, a moving line, already past", () => {
    const v = rule({ stationName: "Vedder Crossing" });
    expect(alertSummary(v)).toBe("You get one notification each time the flow at Vedder Crossing crosses above 50 m³/s. "
      + "It is ready again once the flow is back below the line.");
    expect(alertSummary(v, { value: 62, at: iso(0) })).toBe("You get one notification each time the flow at Vedder Crossing "
      + "crosses above 50 m³/s. It is ready again once the flow is back below the line. It is above that now (62 m³/s), so "
      + "the first notification comes after it crosses back and then above again.");
    expect(alertSummary(v, { value: 40, at: iso(0) })).toBe(alertSummary(v));
    const p = { ...pct(50, "below"), stationName: "Vedder Crossing" };
    expect(alertSummary(p, { value: 50, at: "2026-11-09T07:00:00-08:00" })).toBe("You get one notification each time the flow "
      + "at Vedder Crossing crosses below the median for the day. The line is worked out for each day from the gauge's record, so it moves with the date: 40 m³/s for Nov 9. "
      + "It is ready again once the flow is back above the line.");
    expect(alertSummary(p)).toBe("You get one notification each time the flow at Vedder Crossing crosses below the median "
      + "for the day. The line is worked out for each day from the gauge's record, so it moves with the date. It is ready again once the flow is back above the line.");
  });

  it("the editor's context line and the normal range", () => {
    expect(alertContextLine("discharge", 31.2, { lo: 12, hi: 25 })).toBe("Now 31.2 m³/s · normal today 12–25 m³/s");
    expect(alertContextLine("level", null, null)).toBe("No reading now · no normal range for today");
    expect(normalRangeOf(BAND)).toEqual({ lo: 20, hi: 60 });
    expect(normalRangeOf(null)).toBeNull();
  });

  it("a run's line", () => {
    expect(runLine({ at: Date.parse("2026-11-05T14:05:00-07:00"), ms: 1234, result: "1 alert" })).toBe("2026-11-05 at 14:05 PT · 1.2 s · 1 alert");
  });
});

describe("finding a gauge (Add an alert, step 1)", () => {
  const pts = [{ station: "08HA011", name: "COWICHAN RIVER NEAR DUNCAN" }, { station: "08HA002", name: "COWICHAN RIVER AT LAKE COWICHAN" },
               { station: "08MH001", name: "CHILLIWACK RIVER AT VEDDER CROSSING" }, { station: "08HA003", name: "KOKSILAH RIVER AT COWICHAN STATION" }];

  it("every word typed, in the name or the number; names starting with it first", () => {
    expect(findGauges(pts, "cowichan", 8).map((p) => p.station)).toEqual(["08HA002", "08HA011", "08HA003"]);
    expect(findGauges(pts, "cowichan lake", 8).map((p) => p.station)).toEqual(["08HA002"]);
    expect(findGauges(pts, "08mh", 8).map((p) => p.station)).toEqual(["08MH001"]);
    expect(findGauges(pts, "  ", 8)).toEqual([]);
    expect(findGauges(pts, "river", 2)).toHaveLength(2);
  });
});
