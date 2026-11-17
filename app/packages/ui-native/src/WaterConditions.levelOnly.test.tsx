/**
 * (T817, owner 2026-11-16 on Harrison Lake: "opening harrison lake then going to conditions should default to level only
 * because there is no other data … if only level data exists it should show level data. just mention at top it is level
 * data") A WATER'S CONDITIONS VIEW, FIRST VIEW: where no gauge that speaks for it measures flow, it opens on Level with
 * core's line at the top and the switch offers only Level; a flow-lead water still opens on Flow (T728) with no line.
 *
 * The real `WaterConditions` and `ConditionsPanel` over the fixture source, with who speaks and what each gauge measures
 * replaced: a gauged lake whose own station has a level record only (Harrison Lake's shape), and a panel with both.
 *
 * MUTATION: make `WaterConditions` read `own ?? opening` again (no `conditionsOpening`) -> the level-only case opens on
 * Flow, the card says "Reports water level only today, not flow. Show Level to see it." and offers Flow / Level.
 *
 * (T817, owner 2026-11-16 on the Chehalis: "when you click level on chehalis then you can never go back to flow because
 * it moves to harrison level gauge") FLOW -> LEVEL -> FLOW keeps the gauge and the switch: a quantity is a view of the one
 * lead (chosen in Flow, `leadOver`), and the switch offers the WATER's quantities. MUTATION: drop `offer` from
 * `ConditionsPanel`'s `cardQuantity(...)` -> after Level the switch is gone and Flow cannot be picked again.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { LEVEL_ONLY_WATER } from "@app/core";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId, Panel, SectionId, WaterSource } from "@app/data";
import { clearAnswers, clearPanelCache } from "@app/ui";
import { WaterConditions } from "./WaterViews";
import { LIGHT } from "./theme";

afterEach(cleanup);
beforeEach(() => { clearPanelCache(); clearAnswers(); });

const WATER = "gnis:8634" as ItemId;
const base = makeFixtureSource();
const SEC = (await base.water(WATER))!.sections[0]! as SectionId;
const AT = { lat: 49.31, lon: -121.77 };

/** The fixture with its speaker replaced: a lake station, or a panel of one gauge; each station's quantities given. */
function speaking(kind: "lake" | "panel", measures: readonly ("discharge" | "level")[],
                  now: Record<string, unknown>): WaterSource {
  const panel: Panel = { areaKm2: 50, members: [{ station: "G1" as never, role: "up", areaKm2: 60,
    years: { discharge: measures.includes("discharge") ? 40 : 0, level: measures.includes("level") ? 40 : 0 },
    regulated: false, sameRiver: true }] };
  return {
    ...base,
    panelsFor: async () => new Map(kind === "panel" ? [[SEC, panel]] : []),
    panelRoutes: async () => (kind === "panel"
      ? [{ station: "G1", name: "Gauge Creek near Town", ...AT, role: "up", path: [SEC] }] : []),
    lakeStationsFor: async () => new Map(kind === "lake" ? [[SEC, "G1"]] : []),
    gaugePoints: async () => [{ station: "G1" as never, name: "Lake Near Town", ...AT, mag: null }],
    gaugeForSection: async () => null,
    traceToGauge: async () => [],
    gaugeSeries: async () => null,
    gaugeParameters: async () => measures,
    gaugeNow: async () => ({ fetchedAt: Date.now() - 5 * 60_000, value: now }),
  } as unknown as WaterSource;
}

const LEVEL_NOW = { discharge: null, level: 10.3, percentile: 0.9, standing: "above", at: "2026-09-04T06:00:00Z",
                    parameter: "level" };
const BOTH_NOW = { discharge: 12.5, level: 1.2, percentile: 0.15, standing: "below", at: "2026-09-04T06:00:00Z",
                   parameter: "discharge" };

/** The first view: the line at the top, the card's figure and unit, and the quantity switch's choices (none: no switch). */
async function firstView(source: WaterSource) {
  render(<WaterConditions source={source} item={WATER} tapSid={SEC} palette={LIGHT} opening="discharge" from={AT} />);
  await waitFor(() => expect(screen.queryByTestId("gauge-card")).not.toBeNull());
  await waitFor(() => expect(screen.queryByTestId("reading-figure") ?? screen.queryByTestId("reading-other-quantity"))
    .not.toBeNull());
  const strip = screen.queryByTestId("chart-quantity");
  return {
    note: screen.queryByTestId("conditions-note")?.textContent ?? null,
    figure: screen.queryByTestId("reading-figure")?.textContent ?? screen.getByTestId("reading-other-quantity").textContent,
    switch: strip ? [...strip.querySelectorAll("[role=radio]")]
      .map((r) => `${r.textContent}${r.getAttribute("aria-checked") === "true" ? "*" : ""}`).join(" ") : "none",
    leadNote: screen.queryByTestId("lead-note")?.textContent ?? null,
  };
}

describe("a water's Conditions view opens on what is measured there (T817)", () => {
  it("LEVEL ONLY (a lake gauge with a level record alone): opens on Level, says so at the top, offers only Level", async () => {
    expect(await firstView(speaking("lake", ["level"], LEVEL_NOW))).toEqual({
      note: LEVEL_ONLY_WATER, figure: "10.30", switch: "none", leadNote: null });
    expect(screen.getByText("m")).toBeTruthy();
    expect(LEVEL_ONLY_WATER).toBe("Level only: no flow is measured here.");
  });

  it("a panel whose gauges have a level record only: the same", async () => {
    expect(await firstView(speaking("panel", ["level"], LEVEL_NOW))).toEqual({
      note: LEVEL_ONLY_WATER, figure: "10.30", switch: "none", leadNote: null });
  });

  it("FLOW LEADS where flow is measured (T728 unchanged): opens on Flow, no line, the switch offers Flow and Level", async () => {
    expect(await firstView(speaking("panel", ["discharge", "level"], BOTH_NOW))).toEqual({
      note: null, figure: "12.5", switch: "Flow* Level", leadNote: null });
    expect(screen.getByText("m³/s")).toBeTruthy();
  });
});

/*
 * THE CHEHALIS' SHAPE on 2026-11-16: its own gauge (both quantities) not reporting, the Harrison River gauge below it (level
 * only) reporting. No gauge answers in flow today, so the lead is chosen on level (the Harrison River gauge, said so) — in
 * Flow and in Level alike.
 */
function chehalis(): { source: WaterSource; feed: never } {
  const member = (station: string) => ({ station: station as never, role: "down" as const, areaKm2: 400,
    years: { discharge: station === "OWN" ? 40 : 0, level: 40 }, regulated: false, sameRiver: station === "OWN" });
  const panel: Panel = { areaKm2: 380, members: [member("OWN"), member("BELOW")] };
  const names: Record<string, string> = { OWN: "Chehalis River Near Town", BELOW: "Harrison River at Town" };
  const source = {
    ...base,
    panelsFor: async () => new Map([[SEC, panel]]),
    panelRoutes: async () => ["OWN", "BELOW"].map((st, i) => ({ station: st, name: names[st], lat: 49.3 - i / 10, lon: -121.9,
                                                               role: "down", path: [SEC] })),
    lakeStationsFor: async () => new Map(),
    gaugePoints: async () => [],
    gaugeForSection: async () => null,
    traceToGauge: async () => [],
    gaugeSeries: async () => null,
    gaugeParameters: async (st: string) => (st === "OWN" ? ["discharge", "level"] : ["level"]),
    gaugeNow: async (st: string) => (st === "BELOW"
      ? { fetchedAt: Date.now() - 5 * 60_000, value: { discharge: null, level: 7.03, percentile: 0.7, standing: "normal",
                                                     at: "2026-09-04T06:00:00Z", parameter: "level" } }
      : null),
  } as unknown as WaterSource;
  const feed = { index: async () => ({ listed: ["OWN", "BELOW"], stations: {
    BELOW: { percentile: 0.7, parameter: "level" as const, level: 0.7, observedAt: "2026-09-04T06:00:00Z" } } }) } as never;
  return { source, feed };
}

/** The card as the reader sees it: the gauge's name, the switch's choices (chosen *), what it shows. */
const card = () => {
  const strip = screen.queryByTestId("chart-quantity");
  return [screen.getByTestId("gauge-title").textContent,
          strip ? [...strip.querySelectorAll("[role=radio]")]
            .map((r) => `${r.textContent}${r.getAttribute("aria-checked") === "true" ? "*" : ""}`).join(" ") : "no switch",
          screen.queryByTestId("reading-figure")?.textContent ?? screen.queryByTestId("reading-other-quantity")?.textContent];
};
const pickQuantity = (name: string) => act(() => {
  fireEvent.click([...screen.getByTestId("chart-quantity").querySelectorAll("[role=radio]")].find((r) => r.textContent === name)!);
});

describe("Flow -> Level -> Flow keeps the gauge and the switch (T817)", () => {
  it("the Chehalis' shape: the same lead gauge throughout, Flow always one tap away", async () => {
    const { source, feed } = chehalis();
    render(<WaterConditions source={source} item={WATER} tapSid={SEC} palette={LIGHT} opening="discharge" from={AT}
                            feed={feed} />);
    await waitFor(() => expect(screen.queryByTestId("reading-other-quantity")).not.toBeNull());
    const flow = card();
    await pickQuantity("Level");
    await waitFor(() => expect(screen.queryByTestId("reading-figure")).not.toBeNull());
    const level = card();
    await pickQuantity("Flow");
    await waitFor(() => expect(screen.queryByTestId("reading-other-quantity")).not.toBeNull());
    expect([flow, level, card()]).toEqual([
      ["Harrison River at Town", "Flow* Level", "Reports water level only today, not flow. Show Level to see it."],
      ["Harrison River at Town", "Flow Level*", "7.03"],
      ["Harrison River at Town", "Flow* Level", "Reports water level only today, not flow. Show Level to see it."],
    ]);
    expect(screen.queryByTestId("conditions-note")).toBeNull();      // flow is measured here: not a level-only water
  });

  it("Harrison Lake's shape (level only): one gauge, on Level, and no Flow to lose", async () => {
    render(<WaterConditions source={speaking("lake", ["level"], LEVEL_NOW)} item={WATER} tapSid={SEC} palette={LIGHT}
                            opening="discharge" from={AT} />);
    await waitFor(() => expect(screen.queryByTestId("reading-figure")).not.toBeNull());
    expect(card()).toEqual(["Lake Near Town", "no switch", "10.30"]);
  });
});
