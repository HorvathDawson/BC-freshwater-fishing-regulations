/**
 * The working behind an estimate, mounted.
 *
 * The failure mode worth testing is not "it renders a list". It is the app claiming more
 * than it knows: a bare percentile where the measurement says ±12 points at best, a
 * confident label built on one distant gauge, or a blank where a reader is owed a reason.
 * Every case below is one of those.
 *
 * WHOLE STRINGS ONLY (AGENTS 33, P12a). The wording is core's and pinned there; these
 * assert that the finished line is what lands on screen — `getByText` with a string is an
 * exact, whole-text match, so a sentence that renders broken cannot pass.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { donorReason, type NoEstimate } from "@app/core";
import { donorView, type DonorRow, type PanelAnswer } from "@app/ui";
import { DonorPanel } from "./DonorPanel";
import { LIGHT } from "./theme";

afterEach(cleanup);

// TYPED AS THE REAL ROW on purpose: a fixture that is not the shape it stands in for is
// not a fixture.
const row = (station: string, over: Partial<DonorRow> = {}): DonorRow => ({
  station: station as never, role: "up", percentile: 0.12,
  weight: 0.6, areaRatio: 2.4, areaKm2: 120, trust: "close", years: 40,
  regulated: false, sameRiver: true,
  factors: { share: 0.42, role: 1, record: 1 }, ...over,
});

const answer = (over = {}): PanelAnswer => ({
  answer: { ok: true, value: { percentile: 0.15, plusMinus: 12, trust: "close",
                               spread: 6, donors: 2, ...over } },
  rows: [row("08MH001"), row("08MH024", { weight: 0.4, role: "down" })],
  areaKm2: 50, routesReady: true,
});

/** The chosen day. Fixed, so the range's "for early September" never depends on the clock. */
const DAY = new Date(2026, 8, 4, 12);

type Props = Omit<Parameters<typeof DonorPanel>[0], "palette" | "day">;
const draw = (p: Props) => render(<DonorPanel palette={LIGHT} day={DAY} {...p} />);

const text = () => document.body.textContent ?? "";

describe("the donor panel", () => {
  it("draws the view model it is given, every line of it", () => {
    // THE VM IS DRAWN, NOT RE-DECIDED: every string `donorView` finished is on screen.
    const v = answer({ spread: 44 });
    const vm = donorView(v, { day: DAY, horizon: 0, from: null,
                              size: { width: 360, height: 210 }, selected: null,
                              pickable: false });
    if (!vm.ok) throw new Error("expected an estimate");
    draw({ value: v });
    for (const s of [vm.heading, vm.headline, vm.sentence, vm.range, vm.disagree!,
                     vm.explainer])
      expect(screen.getByText(s)).toBeTruthy();
    for (const r of vm.rows) {
      // Two rows may finish the same sentence (same factors), so all of them, not one.
      for (const s of [r.weightLine, r.facts, r.because!])
        expect(screen.getAllByText(s).length).toBeGreaterThan(0);
      expect(screen.getByLabelText(r.label)).toBeTruthy();
    }
  });

  it("shows a RANGE and never a bare percentile", () => {
    // ±11.7 points at the median even for a near-identical donor; one number would lie.
    draw({ value: answer() });
    expect(screen.getByText("3rd–27th percentile for early September · this spot drains "
                            + "50 km²")).toBeTruthy();
  });

  it("says the range for the CHOSEN day plus the horizon, not the clock's", () => {
    render(<DonorPanel palette={LIGHT} value={answer()} day={new Date(2026, 5, 18, 12)}
                       horizon={5} />);
    expect(screen.getByText("3rd–27th percentile for late June · this spot drains 50 km²"))
      .toBeTruthy();
  });

  it("names the season of the day it is given, counting a forecast from that day", () => {
    // A saved spot draws this panel months later; the season is the chosen day's, never today's.
    render(<DonorPanel palette={LIGHT} value={answer()} day={new Date(2026, 5, 9)} horizon={3} />);
    expect(screen.getByText(/percentile for/).textContent).toBe("3rd–27th percentile for mid June · this spot drains 50 km²");
  });

  it("names every gauge that voted, with where it is and how far off in size", () => {
    draw({ value: answer() });
    expect(screen.getByText("08MH001")).toBeTruthy();
    expect(screen.getByText("08MH024")).toBeTruthy();
    expect(screen.getByText("upstream · drains 120 km² · 2.4× apart")).toBeTruthy();
    expect(screen.getByText("downstream · drains 120 km² · 2.4× apart")).toBeTruthy();
  });

  it("says a station is quiet rather than dropping it or printing a zero", () => {
    const v = answer();
    draw({ value: { ...v, rows: [row("08MH001", { percentile: null })] } });
    expect(screen.getByText("not counted today")).toBeTruthy();
    expect(screen.getByText("Not reporting today.")).toBeTruthy();
  });

  it("warns in words when the gauges disagree", () => {
    draw({ value: answer({ spread: 44 }) });
    expect(screen.getByText("These gauges disagree by 44 points. That is a catchment doing "
      + "more than one thing, and the range above is the honest answer rather than their "
      + "average.")).toBeTruthy();
  });

  it("stays quiet about disagreement when there is none", () => {
    draw({ value: answer({ spread: 4 }) });
    expect(text()).not.toContain("disagree");
  });

  const REASONS: NoEstimate[] = ["no-station", "no-record", "regulated", "too-uncertain",
                                 "offline"];
  it.each(REASONS)("explains a refusal (%s) instead of showing a blank", (why) => {
    draw({ value: { answer: { ok: false, why }, rows: [], areaKm2: null, routesReady: true } });
    expect(screen.getByText("NO ESTIMATE")).toBeTruthy();
    expect(screen.getByText(donorReason(why))).toBeTruthy();
  });

  it("counts one gauge as one gauge", () => {
    // "1 gauges" is the kind of thing that survives review and then ships.
    draw({ value: { ...answer({ donors: 1 }), rows: [row("08MH001")] } });
    expect(screen.getByText("Lower than 8 days in 10 at this time of year — fairly "
                            + "confident, from one gauge nearby.")).toBeTruthy();
  });

  it("says what each gauge contributes, as a number and not only as a bar", () => {
    draw({ value: answer() });
    expect(screen.getByText("60% of this estimate")).toBeTruthy();
    expect(screen.getByText("40% of this estimate")).toBeTruthy();
  });

  it("never rounds a real contribution down to nothing", () => {
    const v = answer();
    draw({ value: { ...v, rows: [row("08MH001", { weight: 0.003 })] } });
    expect(screen.getByText("<1% of this estimate")).toBeTruthy();
  });

  it("explains the contribution in the model's own terms", () => {
    const v = answer();
    draw({ value: { ...v, rows: [
      row("08MH001", { factors: { share: 0.42, role: 0.85, record: 0.5 }, years: 10 }),
    ] } });
    expect(screen.getByText("Counts this much because it is 42% as informative as a gauge "
      + "on this very water; it sits downstream, so it carries extra water; it has only 10 "
      + "years of record, so it counts 50%.")).toBeTruthy();
  });

  it("leads with a sentence, not a percentile", () => {
    draw({ value: answer({ percentile: 0.15 }) });
    expect(screen.getByText("Low for the time of year")).toBeTruthy();
    expect(screen.getByText("Lower than 8 days in 10 at this time of year — fairly "
                            + "confident, from 2 gauges nearby.")).toBeTruthy();
  });

  it("says a forecast is a forecast, in the headline and the sentence", () => {
    draw({ value: answer(), horizon: 3 });
    expect(screen.getByText("FORECAST · 3 DAYS AHEAD")).toBeTruthy();
    expect(screen.getByText("by then")).toBeTruthy();
    expect(screen.getByText("Lower than 8 days in 10 at this time of year — fairly "
                            + "confident, forecast from 2 gauges nearby.")).toBeTruthy();
  });

  it("sounds less certain when the interval is wide", () => {
    draw({ value: answer({ plusMinus: 22 }) });
    expect(screen.getByText("Lower than 8 days in 10 at this time of year — very roughly, "
                            + "from 2 gauges nearby.")).toBeTruthy();
  });

  it("says how far each gauge is in reaches when it knows the route", () => {
    const v = answer();
    draw({ value: { ...v, rows: [
      row("08MH001", { route: { station: "08MH001" as never, name: "VEDDER", lat: 49,
                                lon: -122, role: "up", path: ["a", "b", "c"] as never } }),
      row("08MH024", { route: { station: "08MH024" as never, name: null, lat: 49,
                                lon: -122, role: "up", path: ["a"] as never } }),
    ] } });
    expect(screen.getByText("upstream · drains 120 km² · 2.4× apart · 2 reaches away"))
      .toBeTruthy();
    expect(screen.getByText("upstream · drains 120 km² · 2.4× apart · on this reach"))
      .toBeTruthy();
  });

  it("draws no map when the caller has no tiles, and still lists the gauges", () => {
    draw({ value: answer() });
    expect(screen.getByText("08MH001")).toBeTruthy();
    expect(text()).not.toContain("could not be placed");
  });

  it("says a gauge is quiet in the key, never that it contributed nothing", () => {
    const v = answer();
    draw({ value: { ...v, rows: [row("08MH001", { percentile: null, weight: 0 })] } });
    expect(screen.getByText("not counted today")).toBeTruthy();
    expect(screen.queryByText("0% of this estimate")).toBeNull();
  });

  it("draws no map until the routes are in", () => {
    // A MapLibre map reads its opening camera ONCE; mounted early it stays on the tap alone.
    const v = answer();
    const tiles = { atlas: "x", basemap: "y" } as never;
    draw({ theme: "light", at: tiles, from: { lat: 49, lon: -122 },
           value: { ...v, routesReady: false } });
    expect(screen.queryByText("you are here")).toBeNull();
    // ...and the answer itself is on screen the whole time.
    expect(screen.getByText("08MH001")).toBeTruthy();
  });

  it("says what each gauge itself reads, in the same words as the headline", () => {
    draw({ value: { ...answer(), rows: [row("08MH001", { percentile: 0.2 })] } });
    expect(screen.getByText("Reads lower than 8 days in 10 at this time of year."))
      .toBeTruthy();
  });

  const DAM = "This water is controlled by a dam. The reading is real, but it follows a "
    + "release schedule as much as the weather — a low river here can mean the gate is shut "
    + "rather than that the country is dry.";

  it("says when the water is dam-controlled, above the working and not only in it", () => {
    draw({ value: { ...answer(), rows: [row("08PA004", { regulated: true })] } });
    expect(screen.getByText(DAM)).toBeTruthy();
    expect(screen.getByText("upstream · drains 120 km² · 2.4× apart · dam-controlled"))
      .toBeTruthy();
  });

  it("says nothing about dams when no donor is on regulated water", () => {
    draw({ value: answer() });
    expect(screen.queryByText(DAM)).toBeNull();
  });

  it("does not warn about a dam on a gauge that is not reporting", () => {
    draw({ value: { ...answer(), rows: [row("08PA004", { regulated: true, percentile: null })] } });
    expect(screen.queryByText(DAM)).toBeNull();
  });

  const SHORT = (n: number) => "Most of this rests on a gauge with only " + n + " years of "
    + "record. That is few enough that \"normal for this week\" is itself a rough idea, "
    + "which is part of why the range above is wide.";

  it("warns when most of the answer rests on a short record", () => {
    draw({ value: { ...answer(), rows: [
      row("08A", { years: 6, weight: 0.8, factors: { share: 0.4, role: 1, record: 0.3 } }),
      row("08B", { years: 60, weight: 0.2 }),
    ] } });
    expect(screen.getByText(SHORT(6))).toBeTruthy();
  });

  it("does not warn when the short-record gauge is a minor voice", () => {
    draw({ value: { ...answer(), rows: [
      row("08A", { years: 6, weight: 0.2 }),
      row("08B", { years: 60, weight: 0.8 }),
    ] } });
    expect(screen.queryByText(SHORT(6))).toBeNull();
  });

  it("gives the record length in the row's reason, so the discount is checkable", () => {
    draw({ value: { ...answer(), rows: [
      row("08A", { years: 6, factors: { share: 0.4, role: 1, record: 0.3 } }),
    ] } });
    expect(screen.getByText("Counts this much because it is 40% as informative as a gauge "
      + "on this very water; it has only 6 years of record, so it counts 30%.")).toBeTruthy();
  });

  it("offers the same horizons the map does, beside the number they change", () => {
    draw({ value: answer(), horizon: 3, onHorizon: () => {} });
    for (const t of ["Now", "+1d", "+3d", "+5d"]) expect(screen.getByText(t)).toBeTruthy();
    expect(screen.getByLabelText("Forecast 3 days ahead, showing")).toBeTruthy();
  });

  it("hands back the horizon that was tapped", () => {
    const onHorizon = vi.fn();
    draw({ value: answer(), horizon: 0, onHorizon });
    fireEvent.click(screen.getByLabelText("Forecast 5 days ahead"));
    expect(onHorizon).toHaveBeenCalledWith(5);
  });

  it("offers no horizons where the caller does not own one", () => {
    draw({ value: answer() });
    expect(screen.queryByText("+3d")).toBeNull();
  });

  it("keeps the heading one line at every horizon", () => {
    const HEAD = { 0: "ESTIMATE FOR THIS SPOT", 1: "FORECAST · 1 DAY AHEAD",
                   3: "FORECAST · 3 DAYS AHEAD", 5: "FORECAST · 5 DAYS AHEAD" } as const;
    for (const h of [0, 1, 3, 5] as const) {
      cleanup();
      draw({ value: answer(), horizon: h, onHorizon: () => {} });
      expect(screen.getByText(HEAD[h])).toBeTruthy();
    }
  });

  it("describes each row for a screen reader, not only for the eye", () => {
    draw({ value: answer() });
    expect(screen.getByLabelText("08MH001, upstream, 2.4× apart, 60 per cent of the answer"))
      .toBeTruthy();
  });

  it("tells a screen reader which row's chart is showing", () => {
    draw({ value: answer(), selected: "08MH024" as never, onSelect: () => {} });
    expect(screen.getByText("TAP A GAUGE TO SEE ITS CHART")).toBeTruthy();
    expect(screen.getByLabelText("08MH024, downstream, 2.4× apart, 40 per cent of the "
                                 + "answer. Showing this gauge's chart")).toBeTruthy();
    expect(screen.getByLabelText("08MH001, upstream, 2.4× apart, 60 per cent of the "
                                 + "answer. Show this gauge's chart")).toBeTruthy();
  });
});
