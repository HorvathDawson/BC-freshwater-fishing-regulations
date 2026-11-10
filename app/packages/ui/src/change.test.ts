/**
 * T52 — the Change view through the ONE colouring pathway and the panel's VM, whole.
 *
 * Runs are the feed's of 2026-10-22T22:28:51Z (`pipeline/tests/fixtures/gauge_feed/forecast_change/`); today is Thu
 * 22 Oct 2026.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { answerFrom, buildStandingClasses, sectionStanding, type PanelMember, type SectionId, type StationId } from "@app/data";
import { HORIZONS, codedChange, horizonDateChip, type RunChange, type SourceChange } from "@app/core";
import { classMarks, standingValues } from "./conditionsColour";
import { changeChartModel, changeView, donorView, forecastDrawn, flowHorizon, horizonChips, NO_MODEL_PICK, pickHorizon, pickModel,
         realtimeOnly } from "./panel";
import { CHANGE_RAMP_BASE, rampPosition, routeLabels, routeValues, toRamp } from "./routeColour";

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
  // (T367) the day chips are dates: a fixed clock (Tuesday 2026-11-03)
  const NOW = new Date(2026, 10, 3, 9);
  beforeEach(() => { vi.useFakeTimers({ toFake: ["Date"] }); vi.setSystemTime(NOW); });
  afterEach(() => { vi.useRealTimers(); });

  it("is offered after +10, and live under Flow", () => {
    const chips = horizonChips("change", "discharge");
    expect(chips.slice(0, 2).concat(chips.slice(-2))).toEqual([
      { day: 0, chip: "Now", label: "Conditions now", on: false, note: null, needs: null },
      { day: 1, chip: "Wed 4", label: "Forecast 1 day ahead", on: false, note: null, needs: null },
      { day: 10, chip: "Fri 13", label: "Forecast 10 days ahead", on: false, note: null, needs: null },
      { day: "change", chip: "Change", label: "Expected change over the next 10 days, showing", on: true,
        note: null, needs: null }]);
    expect(chips.map((c) => c.day)).toEqual(HORIZONS);
  });

  /*
   * T130 (owner 2026-10-22: the Change chip "disappeared"): under Level or Temperature it was DROPPED, and the Show
   * choice persists, so the chip was simply gone. Now it stays, greyed, saying "Flow only", and a tap moves to Flow.
   */
  it("stays under Level and Temperature, greyed: \"Flow only\", and a tap switches to Flow", () => {
    for (const q of ["level", "temperature"] as const) {
      const chips = horizonChips("change", q);
      expect(chips.map((c) => c.chip)).toEqual(HORIZONS.map((d) => horizonDateChip(d, NOW)));
      expect(chips.at(-1)).toEqual({
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

  it("the route map carries the same offset values", () => {
    const labels = routeLabels([5 as SectionId]);
    expect(routeValues(labels, new Map([[5 as SectionId, 0.75]]), "change")!.section(0)).toBe(275);
    expect(routeValues(labels, new Map([[5 as SectionId, -0.01]]), "change")!.section(0)).toBeNaN();
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
  it("names each model in the publisher's order, with its line, the attribution, and (T367) one chart", () => {
    const vm = changeView([{ model: "ELF", change: ELF }, { model: "COFFEE", change: null },
                           { model: "CLEVER", change: SPIKE }], TODAY, ATTRIBUTION, { width: 90, height: 40 });
    expect(vm.runs.map((r) => r.line)).toEqual([
      "CLEVER: rises to about 25× today's flow on Mon (higher than 8 days in 10, above normal for the time of year), back down by Tue.",
      "ELF: little change expected."]);
    expect([vm.none, vm.attribution, vm.models, vm.runs.length]).toEqual([null, ATTRIBUTION, null, 2]);
    // a feed from before T144: the chart is the first run's, alone (no station change to join models under)
    expect([vm.chart?.series.map((x) => [x.model, x.bold]), vm.chart?.mark?.label, vm.chart?.days.map((d) => d.label)])
      .toEqual([[["CLEVER", true]], "Peak Mon", ["today", "Mon", "Sat 31 Oct"]]);
  });

  it("says so when no run is running for the gauge — and claims no attribution for a forecast it does not show", () => {
    expect(changeView([{ model: "CLEVER", change: null }], TODAY, ATTRIBUTION)).toEqual({
      runs: [], caveat: null, note: null, none: "No forecast is running for this gauge now.", attribution: null,
      models: null, chart: null });
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

  it("(T367) leads with the station's change, unnamed; the models behind it are one note and thin lines on its chart — ELF left out", () => {
    const vm = changeView([{ model: "ELF", change: { ...ELF, dir: "fall" } }, { model: "COFFEE", change: COFFEE },
                           { model: "CLEVER", change: SPIKE }], TODAY, ATTRIBUTION, { width: 40, height: 40 }, JOINED);
    expect([vm.runs, vm.models, vm.caveat, vm.note, vm.none, vm.attribution]).toEqual([
      [{ model: "CLEVER and COFFEE together", part: false,
         line: "Rises to about 30× today's flow on Sun (higher than 8 days in 10, above normal for the time of year), back down by Tue." }],
      "Models: CLEVER rises to 23× (Mon) · COFFEE rises to 44× (Sun)",
      null, "COFFEE runs to Mon; after that, CLEVER alone.", null, ATTRIBUTION]);
    // one chart: the station's line bold first, each model thin; 1× always a gridline; the peak marked
    const c = vm.chart!;
    expect([c.series.map((x) => [x.model, x.bold]), c.grid.some((g) => g.label === "1.0×"), c.mark?.label])
      .toEqual([[["CLEVER and COFFEE together", true], ["CLEVER", false], ["COFFEE", false]], true, "Peak Sun"]);
  });

  /*
   * T157: the joined line is CLEVER and COFFEE together to COFFEE's fifth day and CLEVER alone after it, so it can drop
   * the day after with neither model forecasting a fall. Every day of 08HD027's shape (T144 review: COFFEE 11x and
   * still rising on its last day, CLEVER 1.06x): the drawing breaks there and marks it, never draws the cliff.
   * Mutations: the break dropped (every day `L`) -> this and the joined block test fail; `handovers` empty -> both fail;
   * a one-model line broken at its own reach -> "one model alone" below fails.
   */
  it("one model alone is one line, named, with its chart", () => {
    const alone: SourceChange = { ...JOINED, models: ["CLEVER"], reach: { CLEVER: "2026-10-31" } };
    const vm = changeView([{ model: "CLEVER", change: SPIKE }, { model: "ELF", change: ELF }], TODAY, ATTRIBUTION,
                          { width: 40, height: 40 }, alone);
    expect([vm.runs.map((r) => [r.model, r.line, r.part]), vm.models, vm.chart?.series.map((x) => x.model),
            vm.chart?.mark?.label, vm.caveat, vm.note]).toEqual([
      [["CLEVER", "CLEVER: rises to about 30× today's flow on Sun (higher than 8 days in 10, above normal for the time of year), back down by Tue.",
        false]], null, ["CLEVER"], "Peak Sun", null, null]);
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

  it("T380: carries each member's percentile at its extreme through the same pathway, beside today's answer", () => {
    const at = { stations: {
      A: { percentile: 0.2, discharge: 0.2, parameter: "discharge" as const, change: { discharge: codedChange(3), percentile: 0.9 } },
      B: { percentile: 0.6, discharge: 0.6, parameter: "discharge" as const, change: { discharge: codedChange(1.5), percentile: 0.8 } },
    } } as never;
    const a = answerFrom(PANEL, at, "discharge", "change");
    const now = answerFrom(PANEL, at, "discharge", 0);
    if (!now.answer.ok || !a.changeEnds) throw new Error("expected both ends");
    expect(a.changeEnds.from).toBe(now.answer.value.percentile);          // the Now view's own answer, not a second one
    expect(a.changeEnds.to).toBeGreaterThan(0.8);
    expect(a.changeEnds.to).toBeLessThan(0.9);
    const vm = view(at);
    if (!vm.ok) throw new Error("expected an answer");
    expect(vm.sentence).toMatch(/^At its highest, from (?:lower|higher) than [^()]+ today to about (?:lower|higher) than [^()]+ \(about 2× today's flow\) — /);
    // MUTATION PIN: no member carrying its extreme's percentile → no ends; a Now question never carries them
    expect(answerFrom(PANEL, IDX, "discharge", "change").changeEnds).toBeUndefined();
    expect(answerFrom(PANEL, at, "discharge", 0).changeEnds).toBeUndefined();
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

  /*
   * T157 review: a reserve member that STOOD BACK (core `answering`: bringing it in made the answer too uncertain) reads
   * a change and weighs nothing — it is not part of "this change". Mutations (each run): `answerFrom` setting `lowWater`
   * on a stood-back member (no `!back`) -> the first expectation fails (R1/R2 carry it); `lowWaterNote` on
   * `donorCounted` instead of `donorVotes` -> the second fails ("Part of" for a change that is all ELF's); both -> fails.
   */
  it("never names a reserve gauge that stood back from the answer (T157 review)", () => {
    const trib = (station: string) => ({ station, role: "up", areaKm2: 100, years: { discharge: 40, level: 40 },
                                         regulated: false, sameRiver: false });
    const DEEP = { areaKm2: 100, members: ["A", "B", "C", "D", "R1", "R2"].map(trib) } as never;
    const ch = (r: number, lowWater = false) => ({ change: { discharge: codedChange(r), ...(lowWater ? { lowWater: true } : {}) } });
    const idx = (o: { core: boolean; reserve: boolean }) => ({ stations: {
      B: ch(1.1, o.core), C: ch(1.15, o.core), D: ch(1.2, o.core), R1: ch(0.05, o.reserve), R2: ch(40, o.reserve) } }) as never;
    const said = (i: never) => {
      const a = answerFrom(DEEP, i, "discharge", "change");
      const vm = donorView(a, { day: TODAY, horizon: "change", from: null, size: { width: 300, height: 200 },
                                selected: null, pickable: false, quantity: "discharge" });
      if (!vm.ok) throw new Error("expected an answer");
      return [a.rows.filter((r) => r.weight === 0 && r.percentile !== null).map((r) => [r.station, r.lowWater ?? null]),
              vm.lowWater];
    };
    const CAVEAT = "ELF is a low-water model: it carries the river's recent fall forward and has no rain or snowmelt in "
      + "it, so it cannot see a rise coming.";
    // the two reserve gauges stood back: the core's change is not ELF's, and nothing is said
    expect(said(idx({ core: false, reserve: true }))).toEqual([[["R1", null], ["R2", null]], null]);
    // the core's change IS all ELF's: "This change", whatever the stood-back reserve is
    expect(said(idx({ core: true, reserve: false }))).toEqual([[["R1", null], ["R2", null]],
      `This change comes from ELF alone (B, C and D). ${CAVEAT}`]);
  });

  it("out of season: the honest refusal, the chips kept to step back, never steady", () => {
    expect(view(QUIET)).toEqual({
      ok: false, heading: "NO ESTIMATE",
      reason: "No forecast is running for the gauges here now. The River Forecast Centre's models are seasonal, so out "
        + "of season there is no expected change to show.",
      horizons: horizonChips("change", "discharge"),
      silent: "No forecast running: A (last report not known) · B (last report not known)", map: null });
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

describe("(T398) forecastDrawn — ELF is opt-in, a forecast can always be switched off", () => {
  const elf = [{ model: "ELF" }], both = [{ model: "CLEVER" }, { model: "ELF" }];
  it("untouched: drawn unless ELF is the only model, none while the runs are unknown", () => {
    expect([forecastDrawn(null, elf), forecastDrawn(null, both), forecastDrawn(null, []), forecastDrawn(null, null)])
      .toEqual([false, true, true, false]);
  });
  it("the reader's switch wins either way", () => {
    expect([forecastDrawn(true, elf), forecastDrawn(false, both), forecastDrawn(false, elf)]).toEqual([true, false, false]);
  });
});

describe("(T377) pickModel — several models drawn at once, each legend entry a switch", () => {
  it("with none drawn, a tap draws that one alone", () => {
    expect(pickModel({ model: "CLEVER", also: ["GEM"], on: false }, "ELF", null, false)).toEqual({ model: "ELF", also: [], on: true });
  });
  it("a tap on one not drawn adds it beside the chosen one, which is pinned (the default never shifts under it)", () => {
    expect(pickModel(NO_MODEL_PICK, "GEM", "CLEVER", true)).toEqual({ model: "CLEVER", also: ["GEM"], on: true });
    expect(pickModel({ model: "CLEVER", also: ["GEM"], on: true }, "ELF", "CLEVER", true))
      .toEqual({ model: "CLEVER", also: ["GEM", "ELF"], on: true });
  });
  it("a tap on one drawn beside the chosen takes it off", () => {
    expect(pickModel({ model: "CLEVER", also: ["GEM", "ELF"], on: true }, "GEM", "CLEVER", true))
      .toEqual({ model: "CLEVER", also: ["ELF"], on: true });
  });
  it("a tap on the chosen one: the next drawn takes its place; the last one off is Real-time only", () => {
    expect(pickModel({ model: "CLEVER", also: ["GEM", "ELF"], on: true }, "CLEVER", "CLEVER", true))
      .toEqual({ model: "GEM", also: ["ELF"], on: true });
    expect(pickModel({ model: null, also: [], on: null }, "CLEVER", "CLEVER", true)).toEqual({ model: null, also: [], on: false });
    expect(forecastDrawn(pickModel(NO_MODEL_PICK, "CLEVER", "CLEVER", true).on, [{ model: "CLEVER" }])).toBe(false);
  });
  it("Real-time only clears every model and keeps the choice for when one is drawn again", () => {
    expect(realtimeOnly({ model: "GEM", also: ["ELF"], on: true })).toEqual({ model: "GEM", also: [], on: false });
  });
});
