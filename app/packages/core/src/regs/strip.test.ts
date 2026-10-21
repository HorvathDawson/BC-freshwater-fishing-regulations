/**
 * strip.ts over hand-built inputs (no store, no golden). The 134-state parity against the golden keep.strip is
 * app/conformance/src/regs/strip.parity.test.ts.
 */
import { describe, expect, it } from "vitest";
import type { CardModel, HydRow } from "./card";
import type { DisplayFrame, RegsInputs, RuleView, Statics } from "./inputs";
import type { Day } from "./model";
import type { Settled } from "./settle";
import {
  BOOK_DAYS, bookIndex, closedOnBookDay, daySegCount, displayOnBookDay, legendOf, nextOpen, runOf, spanWords, stripOf,
  stripRuns, troutLine, waterSegs,
} from "./strip";

const D = (month: number, day: number): Day => ({ month, day });
const OPEN = { status: "base", closing: [], strip: 0 } as unknown as DisplayFrame;
const FLY = { status: "own", closing: [], strip: 4 | 16 } as unknown as DisplayFrame;
const SHUT = { status: "closed", closing: [], strip: 1 } as unknown as DisplayFrame;
const TIDAL = { status: "tidal", tidal: true, note: "", see: [], licence: "" } as DisplayFrame;
const STRIP_LABELS = [{ bit: 0, label: "open" }, { bit: 1, label: "closed" }, { bit: 2, label: "release only" },
  { bit: 4, label: "fly fishing only" }, { bit: 8, label: "flies only" }, { bit: 16, label: "bait ban" }];

/** A year of leap-calendar frames: Jun 1–30 (leap days 153–182) closed, Jul 1–31 fly-only + bait ban, else open. */
function year(over: (leapDay: number) => DisplayFrame | null = (d) => (d >= 153 && d <= 182 ? SHUT : d >= 183 && d <= 213 ? FLY : OPEN)) {
  return Array.from({ length: 366 }, (_, i) => over(i + 1));
}
function inputs(o: { day?: Day; byDay?: (DisplayFrame | null)[]; moments?: boolean; starts?: number[]; weekday?: string; tidal?: boolean } = {}): RegsInputs {
  return {
    day: o.day ?? D(5, 15), weekday: o.weekday ?? "Friday", key: { tidal: !!o.tidal },
    statics: { strip: STRIP_LABELS } as unknown as Statics,
    year: { segStarts: o.starts ?? [1], moments: o.moments ? [{}, {}] : null, displayByDay: o.byDay ?? year() },
  } as unknown as RegsInputs;
}
const settled = (status: DisplayFrame | null, inpart: { family: string; now: boolean }[] = []): Settled =>
  ({ status, inpart: inpart.map((x) => ({ now: x.now, rule: { export: { family: x.family } } as unknown as RuleView })) }) as unknown as Settled;
const row = (kind: HydRow["kind"], members: string[], yr: [string, number][] = [["release", 365]], pool = false): HydRow =>
  ({ kind, members, pool: pool ? ({} as RuleView) : null, year: yr }) as unknown as HydRow;
const card = (rows: HydRow[] = []): CardModel => ({ rows }) as unknown as CardModel;
const texts = (inp: RegsInputs, s: Settled, c = card(), today = D(10, 10)) => stripOf(inp, s, c, today)!.line.map((l) => l.text);

describe("the book's year", () => {
  it("BOOK_DAYS is the 365 days with no Feb 29; bookIndex is its index (Feb 29 reads as Feb 28)", () => {
    expect([BOOK_DAYS.length, BOOK_DAYS[0], BOOK_DAYS[58], BOOK_DAYS[59], BOOK_DAYS[364]])
      .toEqual([365, D(1, 1), D(2, 28), D(3, 1), D(12, 31)]);
    expect([bookIndex(D(10, 10)), bookIndex(D(2, 29)), bookIndex(D(3, 1)), bookIndex(D(1, 1))]).toEqual([282, 58, 59, 0]);
  });

  it("displayOnBookDay drops the leap calendar's Feb 29 (index 59); closedOnBookDay reads its status", () => {
    const byDay = year((d) => (d === 60 ? SHUT : d === 61 ? FLY : OPEN));
    const inp = inputs({ byDay });
    expect([displayOnBookDay(inp, 58), displayOnBookDay(inp, 59)]).toEqual([OPEN, FLY]);
    expect([closedOnBookDay(inputs(), 151), closedOnBookDay(inputs(), 150)]).toEqual([true, false]);
  });

  it("spanWords over book indices (prototype span)", () => {
    expect([spanWords(151, 180), spanWords(184, 318), spanWords(151, 151)]).toEqual(["Jun 1–30", "Jul 4–Nov 15", "Jun 1"]);
  });
});

describe("runOf (page v36, verbatim) over a stored run-length strip", () => {
  it("all year: {all:true}", () => {
    expect(runOf([["keep", 365]], D(4, 1))).toEqual({ now: "keep", all: true });
  });
  it("a run, the day after it and what comes then; a run over New Year is one run", () => {
    expect(runOf([["release", 90], ["keep", 275]], D(2, 1)))
      .toEqual({ now: "release", all: false, from: D(1, 1), until: D(3, 31), next: D(4, 1), then: "keep" });
    expect(runOf([["release", 31], ["keep", 303], ["release", 31]], D(1, 5)))
      .toEqual({ now: "release", all: false, from: D(12, 1), until: D(1, 31), next: D(2, 1), then: "keep" });
  });
});

describe("the water's runs", () => {
  it("nextOpen: the next day the stored frame does not call closed (wrapping); null when closed all year", () => {
    expect(nextOpen(inputs(), D(6, 15))).toEqual(D(7, 1));
    expect(nextOpen(inputs({ byDay: year((d) => (d <= 31 || d >= 335 ? SHUT : OPEN)) }), D(12, 20))).toEqual(D(2, 1));
    expect(nextOpen(inputs({ byDay: year(() => SHUT) }), D(6, 15))).toBeNull();
    // a year on: the day itself is the next open day when every other day is closed (page k < 366)
    expect(nextOpen(inputs({ byDay: year((d) => (d === 167 ? OPEN : SHUT)) }), D(6, 15))).toEqual(D(6, 15));
  });
  it("waterSegs: closed / keep runs of the stored frames", () => {
    expect(waterSegs(inputs())).toEqual([["keep", 151], ["closed", 30], ["keep", 184]]);
  });
  it("stripRuns: the stored strip bits run-length; no frame or a tidal frame is null", () => {
    expect(stripRuns(inputs())).toEqual([{ bits: 0, days: 151 }, { bits: 1, days: 30 }, { bits: 20, days: 31 }, { bits: 0, days: 153 }]);
    expect(stripRuns(inputs({ byDay: year((d) => (d < 10 ? null : TIDAL)) }))).toEqual([{ bits: null, days: 365 }]);
  });
  it("legendOf: a closed run adds 1 only; others add 0 and each restriction bit; ascending; the data's labels", () => {
    expect(legendOf(inputs(), [{ bits: 20 }, { bits: 1 | 4 }, { bits: 2 }])).toEqual([
      { bit: 0, mark: "✓", label: "open" }, { bit: 1, mark: "✕", label: "closed" }, { bit: 2, mark: null, label: "release only" },
      { bit: 4, mark: null, label: "fly fishing only" }, { bit: 16, mark: null, label: "bait ban" }]);
    expect(legendOf(inputs(), [{ bits: null }])).toEqual([{ bit: 0, mark: "✓", label: "open" }]);
    expect(legendOf(inputs(), [{ bits: 1 | 16 }])).toEqual([{ bit: 1, mark: "✕", label: "closed" }]);
  });
  it("daySegCount: segments sharing the day's start (page daySegs length)", () => {
    expect([daySegCount([1, 1, 100, 200, 200, 200], D(1, 5)), daySegCount([1, 1, 100, 200, 200, 200], D(4, 9)),
      daySegCount([1, 1, 100, 200, 200, 200], D(12, 1))]).toEqual([2, 1, 3]);
  });
});

describe("stripOf: the status line", () => {
  it("open on another day; a closure within 21 days is the whole closed run of the strip (round 20i)", () => {
    expect(texts(inputs(), settled(OPEN))).toEqual(["Open", "on May 15", "Closed Jun 1–30"]);
  });
  it("closed: 'Closed' + 'Opens <the next open day>'; 'today' when the day is today", () => {
    const inp = inputs({ day: D(6, 15) });
    const v = stripOf(inp, settled(SHUT), card(), D(6, 15))!;
    expect([v.status, v.line]).toEqual(["closed",
      [{ kind: "status", text: "Closed" }, { kind: "when", text: "today" }, { kind: "soonOpen", text: "Opens Jul 1" }]]);
  });
  it("'Closed from' stays when no closed run starts after the day (the rewrite finds none)", () => {
    // a closure over New Year seen from Dec 20: the strip's closed run starts on Jan 1, before the day
    const byDay = year((d) => (d <= 5 ? SHUT : OPEN));
    expect(texts(inputs({ byDay, day: D(12, 20) }), settled(OPEN))).toEqual(["Open", "on Dec 20", "Closed from Jan 1"]);
  });
  it("the closure is looked for 21 days ahead, no further", () => {
    // closed from Jun 1 (book 151): May 11 is 21 days before it, May 10 is 22
    expect(texts(inputs({ day: D(5, 11) }), settled(OPEN))).toEqual(["Open", "on May 11", "Closed Jun 1–30"]);
    expect(texts(inputs({ day: D(5, 10) }), settled(OPEN))).toEqual(["Open", "on May 10"]);
  });
  it("'Open, except some parts': a retention rule of an undrawn part in force today", () => {
    expect(texts(inputs({ day: D(10, 10) }), settled(OPEN, [{ family: "retention", now: true }]))).toEqual(["Open, except some parts", "today"]);
    expect(texts(inputs({ day: D(10, 10) }), settled(OPEN, [{ family: "retention", now: false }, { family: "gear", now: true }])))
      .toEqual(["Open", "today"]);
  });
  it("the weekday only when the key has moments and the day has more than one segment", () => {
    expect(texts(inputs({ day: D(10, 12), weekday: "Monday", moments: true, starts: [1, 1] }), settled(OPEN))).toEqual(["Open", "on Oct 12 (Mon)"]);
    expect(texts(inputs({ day: D(10, 12), weekday: "Monday", moments: true, starts: [1] }), settled(OPEN))).toEqual(["Open", "on Oct 12"]);
    expect(texts(inputs({ day: D(10, 12), weekday: "Monday", moments: false, starts: [1, 1] }), settled(OPEN))).toEqual(["Open", "on Oct 12"]);
  });
  it("tidal water or no display frame: no strip", () => {
    expect(stripOf(inputs({ tidal: true }), settled(OPEN), card(), D(10, 10))).toBeNull();
    expect(stripOf(inputs(), settled(TIDAL), card(), D(10, 10))).toBeNull();
    expect(stripOf(inputs(), settled(null), card(), D(10, 10))).toBeNull();
  });
  it("runs, marker, months and legend", () => {
    const v = stripOf(inputs({ day: D(10, 10) }), settled(OPEN), card(), D(10, 10))!;
    expect([v.runs.map((r) => r.days), v.todayAt, v.months.join(""), v.legend.map((l) => l.bit)])
      .toEqual([[151, 30, 31, 153], 282 / 365, "JFMAMJJASOND", [0, 1, 4, 16]]);
  });
});

describe("the trout line: a stored release/closed trout row, worded over its STORED year strip", () => {
  const today = D(10, 10), open = inputs({ day: today, byDay: year(() => OPEN) });
  it("release only until the stored run ends; closed all year", () => {
    const rel = card([row("release", ["RB", "KO"], [["release", 90], ["keep", 275]])]);
    expect(troutLine(rel, D(2, 1))).toBe("Trout release only until Mar 31");
    expect(texts(open, settled(OPEN), card([row("closed", ["CT"], [["closed", 365]])]), today)).toEqual(["Open", "today", "Trout closed all year"]);
  });
  it("never beside a trout keep row, never a pooled row, never a char-only row", () => {
    const rel = row("release", ["RB"]);
    expect(troutLine(card([row("keep", ["GB"]), rel]), today)).toBeNull();
    expect(troutLine(card([row("release", ["RB"], undefined, true)]), today)).toBeNull();
    expect(troutLine(card([row("release", ["EB", "DV", "LT"])]), today)).toBeNull();
    expect(troutLine(card([row("nolimit", ["RB"]), rel]), today)).toBe("Trout release only all year");
  });
  it("not when a closure is within 21 days (the closure speaks first)", () => {
    expect(texts(inputs({ day: D(5, 15) }), settled(OPEN), card([row("release", ["RB"])]))).toEqual(["Open", "on May 15", "Closed Jun 1–30"]);
  });
});
