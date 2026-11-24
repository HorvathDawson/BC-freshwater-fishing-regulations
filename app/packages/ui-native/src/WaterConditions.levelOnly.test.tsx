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
import { CONDITIONS_WORDS, LEVEL_ONLY_WATER, NO_RECORD_LINE } from "@app/core";
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

/*
 * (T767 / T728, T369 one load path) THE CARD'S PICK IS THE REACH'S: the shell renders one `WaterConditions` for whatever
 * the frame shows (no remount between reaches), so a pick kept as bare state carried Level into the next reach. The same
 * reach opens the same whatever was open before. MUTATION: make `own` a bare `useState` again -> the next reach, and the
 * first again, open on "Flow Level*".
 */
describe("a Flow / Level pick stays on its reach (T767)", () => {
  it("Level picked on one reach: the next reach, and the first again, open on Flow", async () => {
    const sections = (await base.water(WATER))!.sections as readonly SectionId[];
    const [A, B] = [sections[0]!, sections[1]!];
    const panel: Panel = { areaKm2: 50, members: [{ station: "G1" as never, role: "up", areaKm2: 60,
      years: { discharge: 40, level: 40 }, regulated: false, sameRiver: true }] };
    const source = {
      ...speaking("panel", ["discharge", "level"], BOTH_NOW),
      panelsFor: async (asked: readonly SectionId[]) => new Map(asked.map((s) => [s, panel])),
      panelRoutes: async (s: SectionId) => [{ station: "G1", name: "Gauge Creek near Town", ...AT, role: "up", path: [s] }],
    } as unknown as WaterSource;
    const view = (sid: SectionId) => (
      <WaterConditions source={source} item={WATER} tapSid={sid} palette={LIGHT} opening="discharge" from={AT} />);
    const settle = () => act(async () => { await new Promise((r) => setTimeout(r, 50)); });
    const { rerender } = render(view(A));
    await waitFor(() => expect(card()[1]).toBe("Flow* Level"));
    await pickQuantity("Level");
    await waitFor(() => expect(card()[1]).toBe("Flow Level*"));
    rerender(view(B));
    await settle();
    await waitFor(() => expect(screen.queryByTestId("chart-quantity")).not.toBeNull());
    const next = card()[1];
    rerender(view(A));
    await settle();
    await waitFor(() => expect(screen.queryByTestId("chart-quantity")).not.toBeNull());
    expect([next, card()[1]]).toEqual(["Flow* Level", "Flow* Level"]);   // each reach opens as it opens
  });
});

/*
 * (T1043, owner 2026-11-23: "i still dont know why 0.8.6 capilano at canyon gauge doesnt have level like website does")
 * CAPILANO AT CANYON'S SHAPE: a flow band, a live level on every row and too short a level record for a band. T830 offered
 * that level on the card from the publisher's `reports`; T817's water choices (`offer`, the bundle's bands alone) then
 * decided the switch on a water's view, so it never showed there. MUTATION: drop `|| available.includes(q)` from
 * `cardQuantity` -> "no switch" and Level cannot be reached.
 */
describe("a live level with no band is offered on the water's view (T1043)", () => {
  it("Capilano's shape: Flow and Level, Level drawn unranked with Python's line, Flow one tap back", async () => {
    const source = speaking("panel", ["discharge"], { ...BOTH_NOW, level: 0.592, reports: ["discharge", "level"] });
    render(<WaterConditions source={source} item={WATER} tapSid={SEC} palette={LIGHT} opening="discharge" from={AT} />);
    await waitFor(() => expect(screen.queryByTestId("reading-figure")).not.toBeNull());
    await waitFor(() => expect(screen.queryByTestId("chart-quantity")).not.toBeNull());
    const flow = card();
    await pickQuantity("Level");
    await waitFor(() => expect(screen.queryByTestId("reading-no-record")).not.toBeNull());
    const level = [...card(), screen.getByTestId("reading-no-record").textContent, screen.queryByTestId("reading-dot")];
    await pickQuantity("Flow");
    await waitFor(() => expect(screen.queryByTestId("reading-no-record")).toBeNull());
    expect([flow, level, card()]).toEqual([
      ["G1", "Flow* Level", "12.5"],
      ["G1", "Flow Level*", "0.59", NO_RECORD_LINE, null],
      ["G1", "Flow* Level", "12.5"],
    ]);
  });

  // (review of #77) GUN CREEK (08ME030), production 2026-11-23: a level band only, a live flow (7.8 m³/s) and level
  // (4.271 m) reported. Its water is NOT "level only": the note and the switch agree, opening on Flow (unranked).
  // MUTATION: `useSpeakerMeasures` without the readings' `reports` -> "Level only: no flow is measured here." over "Flow Level*";
// the lead note without its `reports` check -> "No gauge here reports flow today, …" under a flow of 7.8 m³/s.
  it("Gun Creek's shape (a level band, a live flow): no level-only note, Flow and Level, the flow unranked", async () => {
    const now = { discharge: 7.8, level: 4.271, percentile: 0.75, standing: "above", at: "2026-09-04T06:00:00Z",
                  parameter: "level", reports: ["discharge", "level"] };
    expect(await firstView(speaking("panel", ["level"], now))).toEqual({
      note: null, figure: "7.8", switch: "Flow* Level", leadNote: null });
    expect(screen.getByTestId("reading-no-record").textContent).toBe(NO_RECORD_LINE);
  });

  it("Gun Creek through the feed (its lead chosen on level): no \"No gauge here reports flow today\" over its live flow", async () => {
    const now = { discharge: 7.8, level: 4.271, percentile: 0.75, standing: "above", at: "2026-09-04T06:00:00Z",
                  parameter: "level", reports: ["discharge", "level"] };
    const feed = { index: async () => ({ listed: ["G1"], stations: {
      G1: { percentile: 0.75, parameter: "level" as const, level: 0.75, reports: ["discharge", "level"],
            observedAt: "2026-09-04T06:00:00Z" } } }) } as never;
    render(<WaterConditions source={speaking("panel", ["level"], now)} item={WATER} tapSid={SEC} palette={LIGHT}
                            opening="discharge" from={AT} feed={feed} />);
    await waitFor(() => expect(screen.queryByTestId("reading-figure")).not.toBeNull());
    await waitFor(() => expect(screen.queryByTestId("estimate-ranked-on")).not.toBeNull());   // the estimate is on level
    expect([screen.queryByTestId("conditions-note"), screen.queryByTestId("lead-note"), card()[1], card()[2]])
      .toEqual([null, null, "Flow* Level", "7.8"]);
  });

  // (review of #77) 08HD006 (Salmon River near Sayward), production 2026-11-23: bands in both, the live level 2.058 m off
  // the record's rating (T550: the publisher leaves it out of `reports`; the record is near 7 m). Level is offered (the
  // band), but the live level is not today's reading: no "2.06 m stage" under Flow, and under Level the card says the
  // station reports flow only today. MUTATION: `stationReading` keeps a figure missing from `reports` -> "2.06" shows.
  it("08HD006's shape (an other-datum live level): never shown as today's reading", async () => {
    const now = { discharge: 77, level: 2.058, percentile: 0.57, standing: "normal", at: "2026-09-04T06:00:00Z",
                  parameter: "discharge", reports: ["discharge"] };
    render(<WaterConditions source={speaking("panel", ["discharge", "level"], now)} item={WATER} tapSid={SEC} palette={LIGHT}
                            opening="discharge" from={AT} />);
    await waitFor(() => expect(screen.queryByTestId("reading-figure")).not.toBeNull());
    await waitFor(() => expect(screen.queryByTestId("chart-quantity")).not.toBeNull());
    const flow = [...card(), screen.getByTestId("gauge-card").textContent!.includes("2.06")];
    await pickQuantity("Level");
    await waitFor(() => expect(screen.queryByTestId("reading-other-quantity")).not.toBeNull());
    expect([flow, [...card(), screen.getByTestId("gauge-card").textContent!.includes("2.06")]]).toEqual([
      ["G1", "Flow* Level", "77", false],
      ["G1", "Flow Level*", "Reports flow only today, not water level. Show Flow to see it.", false],
    ]);
  });

  // (T1047, 0.9.0) UNDER LEVEL 08HD006's LIVE LINE IS DRAWN WITHOUT THE RECORD: its band (about 6.5 to 8 m) is on another
  // zero than the live 2.06 m, so no band, no earlier years, and no "short record" reason in the key; under Flow the band
  // stays. MUTATION: `ConditionsPanel` passes `lineOnly` false -> "normal range" in the Level key.
  it("08HD006 under Level: the live line alone, the other-zero note in the key; Flow keeps its band", async () => {
    const now = { discharge: 77, level: 2.058, percentile: 0.57, standing: "normal", at: "2026-09-04T06:00:00Z",
                  parameter: "discharge", reports: ["discharge"] };
    const at = ["2026-09-04T03:00:00Z", "2026-09-04T04:00:00Z", "2026-09-04T05:00:00Z", "2026-09-04T06:00:00Z"];
    const seriesOf = (p: "discharge" | "level") => ({ fetchedAt: Date.now() - 60_000, value: {
      step: "1h", from: "feed", parameter: p, at,
      values: p === "level" ? [1.9, 2.0, 2.05, 2.058] : [60, 66, 72, 77],
      band: at.map(() => (p === "level" ? [6, 6.5, 7, 7.5, 8] : [20, 40, 60, 90, 140])),
      now: { index: 3, value: p === "level" ? 2.058 : 77 }, forecast: null, forecasts: [], priorYears: [] } });
    const source = { ...speaking("panel", ["discharge", "level"], now),
                     gaugeSeries: async (_st: string, _span: string, p: "discharge" | "level") => seriesOf(p ?? "discharge") } as unknown as WaterSource;
    render(<WaterConditions source={source} item={WATER} tapSid={SEC} palette={LIGHT} opening="discharge" from={AT} />);
    await waitFor(() => expect(screen.queryByTestId("chart-legend")).not.toBeNull());
    const flowKey = screen.getByTestId("chart-legend").textContent;
    await pickQuantity("Level");
    await waitFor(() => expect(screen.queryByTestId("reading-other-quantity")).not.toBeNull());
    await waitFor(() => expect(screen.getByTestId("chart-unit").textContent).toBe("m"));
    const levelKey = screen.getByTestId("chart-legend").textContent;
    expect([flowKey!.includes("10th–90th"), levelKey!.includes("10th–90th") || levelKey!.includes("short record"), levelKey!.includes("reading"),
            screen.getByTestId("chart-record-apart").textContent])
      .toEqual([true, false, true, CONDITIONS_WORDS.key.recordApart]);
    expect(CONDITIONS_WORDS.key.recordApart).toBe(
      "This gauge's level is measured from a different zero than its record, so there is no normal range to compare it with.");
  });
});
