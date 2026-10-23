/**
 * T52 — the Change view through the ONE colouring pathway and the panel's VM, whole.
 *
 * Runs are the feed's of 2026-10-22T22:28:51Z (`pipeline/tests/fixtures/gauge_feed/forecast_change/`); today is Thu
 * 22 Oct 2026.
 */
import { describe, expect, it } from "vitest";
import { answerFrom, buildStandingClasses, sectionStanding, type PanelMember, type SectionId, type StationId } from "@app/data";
import { codedChange, type RunChange, type SourceChange } from "@app/core";
import { classMarks, pushedStandings, standingValues } from "./conditionsColour";
import { changeChartModel, changeSpark, changeView, donorView, flowHorizon, horizonChips, pickHorizon } from "./panel";
import { CHANGE_RAMP_BASE, pushedRoute, rampPosition, toRamp } from "./routeColour";

const st = (s: string) => s as StationId;
const member = (station: string, areaKm2: number, role: "up" | "down" = "up"): PanelMember =>
  ({ station: st(station), role, areaKm2, years: { discharge: 40, level: 40 }, regulated: false, sameRiver: true });
const MEMBERS = new Map([[1, [member("A", 100), member("B", 400, "down")]]]);
const CLASSES = buildStandingClasses("r-test", [{ sid: 5, panel: 1, areaKm2: 120 }, { sid: 7, panel: 1, areaKm2: 900 }],
                                     MEMBERS, new Map(), new Map());
/** A expects 3x, B 1.5x; C (not in the panel) nothing. Today's percentiles beside, to prove Change does not read them. */
const IDX = { stations: {
  A: { percentile: 0.2, discharge: 0.2, parameter: "discharge" as const, change: { discharge: codedChange(3) } },
  B: { percentile: 0.6, discharge: 0.6, parameter: "discharge" as const, change: { discharge: codedChange(1.5) } },
} } as never;
const QUIET = { stations: {
  A: { percentile: 0.2, discharge: 0.2, parameter: "discharge" as const },
  B: { percentile: 0.6, discharge: 0.6, parameter: "discharge" as const },
} } as never;

describe("the Change chip", () => {
  it("is offered after +10, and live under Flow", () => {
    expect(horizonChips("change", "discharge")).toEqual([
      { day: 0, chip: "Now", label: "Conditions now", on: false, note: null, needs: null },
      { day: 1, chip: "+1d", label: "Forecast 1 day ahead", on: false, note: null, needs: null },
      { day: 3, chip: "+3d", label: "Forecast 3 days ahead", on: false, note: null, needs: null },
      { day: 5, chip: "+5d", label: "Forecast 5 days ahead", on: false, note: null, needs: null },
      { day: 10, chip: "+10d", label: "Forecast 10 days ahead", on: false, note: null, needs: null },
      { day: "change", chip: "Change", label: "Expected change over the next 10 days, showing", on: true,
        note: null, needs: null }]);
  });

  /*
   * T130 (owner 2026-10-22: the Change chip "disappeared"): under Level or Temperature it was DROPPED, and the Show
   * choice persists, so the chip was simply gone. Now it stays, greyed, saying "Flow only", and a tap moves to Flow.
   */
  it("stays under Level and Temperature, greyed: \"Flow only\", and a tap switches to Flow", () => {
    for (const q of ["level", "temperature"] as const) {
      const chips = horizonChips("change", q);
      expect(chips.map((c) => c.chip)).toEqual(["Now", "+1d", "+3d", "+5d", "+10d", "Change"]);
      expect(chips[5]).toEqual({
        day: "change", chip: "Change", on: false, note: "Flow only", needs: "discharge",
        label: "Expected change over the next 10 days. Flow only: a tap shows Flow" });
      // the chosen day is answered as Now, so Now is the one lit
      expect(chips.filter((c) => c.on).map((c) => c.day)).toEqual([0]);
    }
    expect(horizonChips(3, "level").filter((c) => c.on).map((c) => c.day)).toEqual([3]);
  });

  it("picks: Change under Level moves to Flow and Change; anything else moves no quantity", () => {
    expect([pickHorizon("change", "level"), pickHorizon("change", "temperature"), pickHorizon("change", "discharge"),
            pickHorizon(3, "level"), pickHorizon(0, "temperature")]).toEqual([
      { horizon: "change", show: "discharge" }, { horizon: "change", show: "discharge" },
      { horizon: "change", show: null }, { horizon: 3, show: null }, { horizon: 0, show: null }]);
  });

  it("answers as Now outside Flow, the choice kept for when Flow returns", () => {
    expect([flowHorizon("change", "discharge"), flowHorizon("change", "level"), flowHorizon(5, "level")])
      .toEqual(["change", 0, 5]);
  });
});

describe("the Change values, through the one engine's standing channel", () => {
  it("live in their own domain (200-300) of the same channel; a refusal there is nothing, never steady", () => {
    expect([toRamp(0.5, "change"), toRamp(0.75, "change"), toRamp(-0.01, "change"), toRamp(0.5, 3), toRamp(-0.01, 0)])
      .toEqual([250, 275, NaN, 50, -1]);
    expect(CHANGE_RAMP_BASE).toBe(200);
    expect([rampPosition(250), rampPosition(50), rampPosition(-1)]).toEqual([0.5, 0.5, -0.01]);
  });

  it("each class's value is the section's own expected change through `sectionStanding`, offset", () => {
    const v = standingValues(CLASSES, IDX, "discharge", "change");
    for (const s of [5, 7]) {
      const want = sectionStanding({ areaKm2: s === 7 ? 900 : 120, members: MEMBERS.get(1)! }, undefined, IDX,
                                   "discharge", "change")!;
      expect(v.section(CLASSES.classOf(s)!)).toBe(200 + want * 100);
    }
    // between 1.5x and 3x: the panel's weighted geometric mean, on the change scale
    const mid = v.section(CLASSES.classOf(5)!);
    expect([mid > 200 + codedChange(1.5) * 100, mid < 200 + codedChange(3) * 100]).toEqual([true, true]);
    // Level has no change at all: nothing, not the dashed "no baseline"
    const lv = standingValues(CLASSES, IDX, "level", "change");
    expect([0, 1].map((k) => lv.section(k)).every(Number.isNaN)).toBe(true);
    // out of season (no station carries a change): nothing either
    const off = standingValues(CLASSES, QUIET, "discharge", "change");
    expect([0, 1].map((k) => off.section(k)).every(Number.isNaN)).toBe(true);
  });

  it("the legend's marks sit on the change scale, 0-1", () => {
    const v = { section: (k: number) => [275, NaN][k] ?? NaN, basins: {} };
    // one class reads 275 (a quarter of the way from steady to the top), the other nothing: one mark, at 0.75
    expect(classMarks(CLASSES, v, [5, 7].map((s) => s as SectionId))).toEqual([0.75]);
  });

  it("the pushed path and the route map carry the same offset values", () => {
    const one = new Map([[5 as SectionId, 0.75]]);
    expect(pushedStandings(one, new Map([["08MF", 0.25]]), "change")).toEqual({
      sections: { 5: { standing: 275 } }, basin: { "08MF": { standing: 225 } } });
    expect(pushedRoute(one, "change")).toEqual({ sections: { 5: { standing: 275 } } });
    expect(pushedRoute(new Map([[5 as SectionId, -0.01]]), "change"))
      .toEqual({ sections: { 5: { standing: NaN } } });
  });
});

/** 08HA003's CLEVER run: 23x on Mon 26 Oct, back down by Tue. */
const SPIKE: RunChange = {
  dir: "rise", base: 0.467,
  peak: { day: "2026-10-26", ratio: 22.7944, value: 10.645, percentile: 0.7717, points: 0.6717, back: "2026-10-27" },
  trough: { day: "2026-10-22", ratio: 1, value: 0.467, percentile: 0.1, points: 0, back: null },
  days: [["2026-10-22", 0.467, 0.467], ["2026-10-23", 0.467, 0.467], ["2026-10-24", 0.467, 0.467],
         ["2026-10-25", 0.467, 10.617], ["2026-10-26", 1.391, 10.645], ["2026-10-27", 0.469, 1.253],
         ["2026-10-28", 0.468, 0.469], ["2026-10-29", 0.468, 2.151], ["2026-10-30", 0.642, 2.152],
         ["2026-10-31", 0.47, 0.615]],
};
/** Its ELF run: steady over the window. */
const ELF: RunChange = { ...SPIKE, dir: "steady", base: 0.456,
  peak: { day: "2026-10-22", ratio: 1, value: 0.456, back: null },
  trough: { day: "2026-11-01", ratio: 0.9135, value: 0.4166, back: null },
  days: [["2026-10-22", 0.456, 0.456], ["2026-11-01", 0.4166, 0.4166]] };
const TODAY = new Date(2026, 9, 22, 15, 0);
const ATTRIBUTION = "Forecast data provided by the BC River Forecast Centre, Province of British Columbia, and used "
  + "under the Province's copyright terms (https://www2.gov.bc.ca/gov/content/home/copyright). Users should use the "
  + "information on this website with caution and at their own risk.";

describe("the Change block for the charted gauge", () => {
  it("names each model in the publisher's order, with its line, its days and the attribution", () => {
    const vm = changeView([{ model: "ELF", change: ELF }, { model: "COFFEE", change: null },
                           { model: "CLEVER", change: SPIKE }], TODAY, ATTRIBUTION, { width: 90, height: 40 });
    expect(vm.runs.map((r) => r.line)).toEqual([
      "CLEVER: rises to about 25× today's flow on Mon (high for the time of year), back down by Tue.",
      "ELF: little change expected."]);
    expect(vm.none).toBeNull();
    expect(vm.runs[1]!.spark).toBeNull();          // steady: no day worth marking
    expect(vm.attribution).toBe(ATTRIBUTION);
    expect(vm.runs[0]!.spark).toEqual({
      path: "M0,40 L10,40 L20,40 L30,0 L40,0 L50,27.4 L60,39.9 L70,20.5 L80,20.5 L90,36.5",
      mark: { x: 40, y: 0, label: "Peak Mon" },
      ends: [{ x: 0, label: "today" }, { x: 90, label: "Sat 31 Oct" }],
      handovers: [],
      label: "Each day's highest forecast flow against today's, from today to Sat 31 Oct",
    });
  });

  it("says so when no run is running for the gauge — and claims no attribution for a forecast it does not show", () => {
    expect(changeView([{ model: "CLEVER", change: null }], TODAY, ATTRIBUTION)).toEqual({
      runs: [], caveat: null, note: null, none: "No forecast is running for this gauge now.", attribution: null });
  });

  // T144: the publisher's station change decides which models speak; the block only words it.
  const JOINED: SourceChange = {
    models: ["CLEVER", "COFFEE"], lowWater: false, reach: { CLEVER: "2026-10-31", COFFEE: "2026-10-26" },
    dir: "rise", base: 1,
    peak: { day: "2026-10-25", ratio: 31.6876, value: 31.6876, percentile: 0.8327, points: 0.7327, back: "2026-10-27" },
    trough: { day: "2026-10-22", ratio: 1, value: 1, percentile: 0.1, points: 0, back: null },
    days: [["2026-10-22", 1, 1], ["2026-10-24", 3.3417, 3.3417], ["2026-10-25", 6.6458, 31.6876],
           ["2026-10-27", 1.0043, 2.6831], ["2026-10-31", 1.0064, 1.3169]],
  };
  const COFFEE: RunChange = { ...SPIKE, base: 0.6,
    peak: { day: "2026-10-25", ratio: 44.1667, value: 26.5, percentile: 0.95, points: 0.85, back: null } };

  it("leads with CLEVER and COFFEE together, then each one's own line — and leaves ELF out", () => {
    const vm = changeView([{ model: "ELF", change: { ...ELF, dir: "fall" } }, { model: "COFFEE", change: COFFEE },
                           { model: "CLEVER", change: SPIKE }], TODAY, ATTRIBUTION, { width: 40, height: 40 }, JOINED);
    expect(vm).toEqual({
      runs: [
        { model: "CLEVER and COFFEE together", part: false,
          line: "CLEVER and COFFEE together: rises to about 30× today's flow on Sun (high for the time of year), "
            + "back down by Tue.",
          // (T157) the line breaks after COFFEE's last day drawn (Sun 25) and a tick stands in the gap
          spark: { path: "M0,40 L10,26 L20,0 M30,28.6 L40,36.8", mark: { x: 20, y: 0, label: "Peak Sun" },
                   ends: [{ x: 0, label: "today" }, { x: 40, label: "Sat 31 Oct" }],
                   handovers: [{ x: 25, model: "COFFEE" }],
                   label: "Each day's highest forecast flow against today's, from today to Sat 31 Oct; COFFEE's "
                     + "forecast ends Mon" } },
        { model: "CLEVER", part: true, spark: null,
          line: "CLEVER: rises to about 25× today's flow on Mon (high for the time of year), back down by Tue." },
        { model: "COFFEE", part: true, spark: null,
          line: "COFFEE: rises to about 45× today's flow by Sun (very high for the time of year) and stays up." },
      ],
      caveat: null,
      note: "COFFEE runs to Mon; after that, CLEVER alone.",
      none: null,
      attribution: ATTRIBUTION,
    });
  });

  /*
   * T157: the joined line is CLEVER and COFFEE together to COFFEE's fifth day and CLEVER alone after it, so it can drop
   * the day after with neither model forecasting a fall. Every day of 08HD027's shape (T144 review: COFFEE 11x and
   * still rising on its last day, CLEVER 1.06x): the drawing breaks there and marks it, never draws the cliff.
   * Mutations: the break dropped (every day `L`) -> this and the joined block test fail; `handovers` empty -> both fail;
   * a one-model line broken at its own reach -> "one model alone" below fails.
   */
  it("draws the COFFEE to CLEVER handover as a gap with a tick, not a fall", () => {
    const handover: SourceChange = {
      models: ["CLEVER", "COFFEE"], lowWater: false, reach: { CLEVER: "2026-10-31", COFFEE: "2026-10-26" },
      dir: "rise", base: 1,
      peak: { day: "2026-10-26", ratio: 3.32, value: 3.32, back: null, until: "2026-10-26" },
      trough: { day: "2026-10-22", ratio: 1, value: 1, back: null },
      days: [["2026-10-22", 1, 1], ["2026-10-23", 1, 1.4], ["2026-10-24", 1.2, 2.1], ["2026-10-25", 1.9, 2.9],
             ["2026-10-26", 2.5, 3.32], ["2026-10-27", 1.02, 1.06], ["2026-10-28", 1.01, 1.04],
             ["2026-10-29", 1, 1.03], ["2026-10-30", 1, 1.02], ["2026-10-31", 1, 1.01]],
    };
    expect(changeSpark(handover, "2026-10-22", 90, 40)).toEqual({
      path: "M0,40 L10,28.8 L20,15.3 L30,4.5 L40,0 M50,38.1 L60,38.7 L70,39 L80,39.3 L90,39.7",
      mark: { x: 40, y: 0, label: "Peak Mon" },
      ends: [{ x: 0, label: "today" }, { x: 90, label: "Sat 31 Oct" }],
      handovers: [{ x: 45, model: "COFFEE" }],
      label: "Each day's highest forecast flow against today's, from today to Sat 31 Oct; COFFEE's forecast ends Mon",
    });
  });

  it("one model alone is one line, with its days", () => {
    const alone: SourceChange = { ...JOINED, models: ["CLEVER"], reach: { CLEVER: "2026-10-31" } };
    const vm = changeView([{ model: "CLEVER", change: SPIKE }, { model: "ELF", change: ELF }], TODAY, ATTRIBUTION,
                          { width: 40, height: 40 }, alone);
    expect([vm.runs.map((r) => [r.model, r.line, r.part, r.spark?.mark.label, r.spark?.handovers, r.spark?.path]),
            vm.caveat, vm.note]).toEqual([
      [["CLEVER", "CLEVER: rises to about 30× today's flow on Sun (high for the time of year), back down by Tue.",
        false, "Peak Sun", [], "M0,40 L10,26 L20,0 L30,28.6 L40,36.8"]], null, null]);
  });

  it("ELF alone carries the low-water caveat", () => {
    const low: SourceChange = { ...ELF, dir: "fall", base: 1, models: ["ELF"], lowWater: true,
      reach: { ELF: "2026-11-01" },
      trough: { day: "2026-11-01", ratio: 0.45, value: 0.45, back: null },
      days: [["2026-10-22", 1, 1], ["2026-11-01", 0.45, 0.45]] };
    const vm = changeView([{ model: "ELF", change: ELF }], TODAY, ATTRIBUTION, { width: 40, height: 40 }, low);
    expect([vm.runs.map((r) => r.line), vm.caveat, vm.note]).toEqual([
      ["ELF: falls to about 45% of today's flow by Sun 1 Nov, still falling on the last day shown."],
      "ELF is a low-water model: it carries the river's recent fall forward and has no rain or snowmelt in it, so it "
        + "cannot see a rise coming.",
      null]);
  });

  it("marks a falling run's lowest day", () => {
    const fall: RunChange = { ...ELF, dir: "fall", trough: { ...ELF.trough, day: "2026-11-01", ratio: 0.45 },
      days: [["2026-10-22", 0.456, 0.456], ["2026-11-01", 0.2, 0.2]] };
    expect(changeSpark(fall, "2026-10-22", 100, 40)!.mark).toEqual({ x: 100, y: 40, label: "Lowest Sun 1 Nov" });
  });
});

describe("the donor panel under Change", () => {
  const PANEL = { areaKm2: 120, members: MEMBERS.get(1)! };
  const view = (idx: never) => {
    return donorView(answerFrom(PANEL, idx, "discharge", "change"),
                     { day: TODAY, horizon: "change", from: null, size: { width: 300, height: 200 }, selected: null,
                       pickable: false, quantity: "discharge" });
  };

  it("heads and says the section's expected change in core's words", () => {
    const vm = view(IDX);
    if (!vm.ok) throw new Error("expected an answer");
    expect([vm.heading, vm.headline, vm.headlineSuffix, vm.sentence]).toEqual([
      "EXPECTED CHANGE · NEXT 10 DAYS", "A big rise expected", null,
      "At its highest, about 2× today's flow — fairly confident, forecast from 2 gauges nearby."]);
    expect(vm.rows.map((r) => r.reads)).toEqual([
      "Expects about 3× today's flow at its highest.", "Expects about 50% more than today's flow at its highest."]);
    expect(vm.horizons.at(-1)).toEqual({ day: "change", chip: "Change",
                                         label: "Expected change over the next 10 days, showing", on: true,
                                         note: null, needs: null });
  });

  /*
   * T157 (T144 left it on the charted gauge only): the spot's own Change says the low-water caveat where a counted gauge's
   * change is ELF's alone. Mutations: `lowWater` not carried onto the row (`answerFrom`) -> the first two expectations
   * fail; `all` always false -> the second fails; the note said under every horizon -> the last fails.
   */
  it("says where the change comes from ELF alone, for the spot's own answer (T157)", () => {
    const low = (s: Record<string, true>) => ({ stations: {
      A: { percentile: 0.2, discharge: 0.2, parameter: "discharge", change: { discharge: codedChange(3), ...(s.A ? { lowWater: true } : {}) } },
      B: { percentile: 0.6, discharge: 0.6, parameter: "discharge", change: { discharge: codedChange(1.5), ...(s.B ? { lowWater: true } : {}) } },
    } }) as never;
    const said = (idx: never) => { const vm = view(idx); if (!vm.ok) throw new Error("expected an answer"); return vm.lowWater; };
    const CAVEAT = "ELF is a low-water model: it carries the river's recent fall forward and has no rain or snowmelt in "
      + "it, so it cannot see a rise coming.";
    expect(said(low({ B: true }))).toBe(`Part of this change comes from ELF alone (B). ${CAVEAT}`);
    expect(said(low({ A: true, B: true }))).toBe(`This change comes from ELF alone (A and B). ${CAVEAT}`);
    expect(said(IDX)).toBeNull();
    const now = donorView(answerFrom(PANEL, low({ A: true, B: true }), "discharge", 0),
                          { day: TODAY, horizon: 0, from: null, size: { width: 300, height: 200 }, selected: null,
                            pickable: false, quantity: "discharge" });
    expect(now.ok && now.lowWater).toBeNull();
  });

  it("out of season: the honest refusal, the chips kept to step back, never steady", () => {
    expect(view(QUIET)).toEqual({
      ok: false, heading: "NO ESTIMATE",
      reason: "No forecast is running for the gauges here now. The River Forecast Centre's models are seasonal, so out "
        + "of season there is no expected change to show.",
      horizons: horizonChips("change", "discharge"),
      silent: "No forecast running: A (last report not known) · B (last report not known)" });
  });
});

describe("the chart's run under Change (T144 review: owner \"no elf\")", () => {
  const joined = { models: ["CLEVER", "COFFEE"] } as unknown as SourceChange;
  it("is the change's own lead model unless the reader picked one; freshest elsewhere and on an older feed", () => {
    expect([changeChartModel(null, "change", joined), changeChartModel("ELF", "change", joined),
            changeChartModel(null, 3, joined), changeChartModel(null, "change", null),
            changeChartModel(null, "change", { models: ["ELF"] } as unknown as SourceChange)])
      .toEqual(["CLEVER", "ELF", undefined, undefined, "ELF"]);
  });
});
