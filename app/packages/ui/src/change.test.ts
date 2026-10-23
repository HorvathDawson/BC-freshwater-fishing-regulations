/**
 * T52 — the Change view through the ONE colouring pathway and the panel's VM, whole.
 *
 * Runs are the feed's of 2026-10-22T22:28:51Z (`pipeline/tests/fixtures/gauge_feed/forecast_change/`); today is Thu
 * 22 Oct 2026.
 */
import { describe, expect, it } from "vitest";
import { answerFrom, buildStandingClasses, sectionStanding, type PanelMember, type SectionId, type StationId } from "@app/data";
import { codedChange, type RunChange } from "@app/core";
import { classMarks, pushedStandings, standingValues } from "./conditionsColour";
import { changeSpark, changeView, donorView, flowHorizon, horizonChips } from "./panel";
import { CHANGE_RAMP_BASE, pushedRoute, rampPosition, toRamp } from "./routeColour";

const st = (s: string) => s as StationId;
const member = (station: string, areaKm2: number, role: "up" | "down" = "up"): PanelMember =>
  ({ station: st(station), role, areaKm2, years: 40, regulated: false, sameRiver: true });
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
  it("is offered after +10, under Flow only", () => {
    expect(horizonChips("change", "discharge").map((c) => [c.chip, c.label, c.on])).toEqual([
      ["Now", "Conditions now", false], ["+1d", "Forecast 1 day ahead", false], ["+3d", "Forecast 3 days ahead", false],
      ["+5d", "Forecast 5 days ahead", false], ["+10d", "Forecast 10 days ahead", false],
      ["Change", "Expected change over the next 10 days, showing", true]]);
    expect(horizonChips(0, "level").map((c) => c.chip)).toEqual(["Now", "+1d", "+3d", "+5d", "+10d"]);
    expect(horizonChips(0, "temperature").map((c) => c.chip)).toEqual(["Now", "+1d", "+3d", "+5d", "+10d"]);
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
      stream: { 5: { standing: 275 } }, lake: { 5: { standing: 275 } }, wetland_flowing: { 5: { standing: 275 } },
      basin: { "08MF": { standing: 225 } } });
    expect(pushedRoute(one, "change")).toEqual(pushedRoute(new Map([[5 as SectionId, 0.75]]), "change"));
    expect(Object.values(pushedRoute(one, "change")).map((l) => l[5]?.standing)).toEqual([275, 275]);
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
      label: "Each day's highest forecast flow against today's, from today to Sat 31 Oct",
    });
  });

  it("says so when no run is running for the gauge — and claims no attribution for a forecast it does not show", () => {
    expect(changeView([{ model: "CLEVER", change: null }], TODAY, ATTRIBUTION)).toEqual({
      runs: [], none: "No forecast is running for this gauge now.", attribution: null });
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
                                         label: "Expected change over the next 10 days, showing", on: true });
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
