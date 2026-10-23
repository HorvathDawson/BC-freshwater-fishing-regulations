/**
 * Owner 2026-10-21: "when clicking between the gauges in the condition panel it flickers a millisecond before showing
 * the chart on first load (maybe get all the data when the page opens rather than one gauge at a time when clicked)".
 *
 *   - every donor's series is asked when the panel opens — one request per donor per span, the other span after;
 *   - switching to a gauge already in hand draws its chart in the same render as the click: no loading frame;
 *   - a gauge whose series is still on its way keeps the last chart up, dimmed, with a small note — never a spinner.
 *
 * Whole strings only (AGENTS 33).
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { chartLabel, CONDITIONS_WORDS, readingHeading } from "@app/core";
import type { Panel, RegsSource, SectionId } from "@app/data";
import { makeFixtureSource } from "@app/data/fixture";
import { clearAnswers, clearPanelCache } from "@app/ui";
import { ConditionsPanel } from "./ConditionsPanel";
import { LIGHT } from "./theme";

afterEach(cleanup);
beforeEach(() => { clearPanelCache(); clearAnswers(); });

const SEC = 7 as SectionId;
const DAY = new Date(2026, 8, 4, 12);
const PANEL: Panel = { areaKm2: 50, members: [
  { station: "LEAD" as never, role: "up", areaKm2: 60, years: { discharge: 40, level: 40 }, regulated: false, sameRiver: true },
  { station: "DAM" as never, role: "down", areaKm2: 55, years: { discharge: 6, level: 6 }, regulated: true, sameRiver: true },
] };
const ROUTES = [
  { station: "LEAD", name: "Lead Creek near Town", lat: 49, lon: -122, role: "up", path: [SEC] },
  { station: "DAM", name: "Dam Creek below the Dam", lat: 49.1, lon: -122.1, role: "down", path: [SEC] },
];
const FIXTURE = makeFixtureSource();
const feed = { index: async () => ({ stations: {
  LEAD: { percentile: 0.15, parameter: "discharge" as const, discharge: 0.15 },
  DAM: { percentile: 0.8, parameter: "discharge" as const, discharge: 0.8 },
} }) };

/** A source that logs every series it is asked for; `hold` stations never answer. */
function sourceLogging(asked: string[], hold: readonly string[] = []): RegsSource {
  return {
    panelsFor: async () => new Map([[SEC, PANEL]]),
    panelRoutes: async () => ROUTES,
    lakeStationsFor: async () => new Map(),
    gaugeForSection: async () => null,
    traceToGauge: async () => [],
    gaugeSeries: (st: string, span: "72h" | "year", p?: never, m?: string) => {
      asked.push(`${st}:${span}:${p ?? "auto"}`);
      return hold.includes(st) ? new Promise(() => {}) : FIXTURE.gaugeSeries("08MH001" as never, span, p, m);
    },
    gaugeParameters: async () => ["discharge", "level"],
    gaugeNow: async () => ({ fetchedAt: Date.now() - 5 * 60_000,
                             value: { discharge: 12.5, level: 1.2, percentile: 0.15, standing: "below",
                                      at: "2026-09-04T06:00:00Z" } }),
  } as unknown as RegsSource;
}

async function draw(src: RegsSource) {
  render(<GestureHandlerRootView>
    <ConditionsPanel source={src} section={SEC} palette={LIGHT} feed={feed} scroll={false} day={DAY}
                     parameter="discharge" />
  </GestureHandlerRootView>);
  await waitFor(() => expect(screen.getByLabelText(chartLabel("72h"))).toBeTruthy());
}

const LOADING_NOTE = CONDITIONS_WORDS.loadingRecord;

describe("every donor's chart is asked when the panel opens", () => {
  it("one request per donor per span — this span first, the other after", async () => {
    const asked: string[] = [];
    await draw(sourceLogging(asked));
    await waitFor(() => expect(asked).toHaveLength(4));
    expect([...asked.slice(0, 2)].sort()).toEqual(["DAM:72h:discharge", "LEAD:72h:discharge"]);
    expect([...asked.slice(2)].sort()).toEqual(["DAM:year:discharge", "LEAD:year:discharge"]);
    // switching gauges and spans asks for nothing more: it is all in hand
    fireEvent.click(screen.getByRole("radio", { name: "Chart DAM, Dam Creek below the Dam, 20% of this estimate" }));
    fireEvent.click(screen.getByRole("radio", { name: CONDITIONS_WORDS.span.year }));
    await waitFor(() => expect(screen.getByLabelText(chartLabel("year"))).toBeTruthy());
    expect(asked).toHaveLength(4);
  });
});

describe("switching gauges", () => {
  it("draws a prefetched gauge's chart and reading in the same render as the click — no loading frame", async () => {
    const asked: string[] = [];
    await draw(sourceLogging(asked));
    await waitFor(() => expect(asked).toHaveLength(4));
    fireEvent.click(screen.getByRole("radio", { name: "Chart DAM, Dam Creek below the Dam, 20% of this estimate" }));
    // NO await between the click and these: anything that needed a promise to settle is not on screen yet.
    expect(screen.getByRole("radio", { name: "Showing DAM, Dam Creek below the Dam, 20% of this estimate" })
      .getAttribute("aria-checked")).toBe("true");
    expect(screen.getByText(readingHeading("Dam Creek below the Dam"))).toBeTruthy();
    expect(screen.getByLabelText(chartLabel("72h"))).toBeTruthy();
    expect(screen.queryByText(LOADING_NOTE)).toBeNull();
    expect(screen.queryByRole("progressbar")).toBeNull();
  });

  it("keeps the last chart up, dimmed with a small note, while a gauge not yet in hand loads", async () => {
    const asked: string[] = [];
    await draw(sourceLogging(asked, ["DAM"]));
    fireEvent.click(screen.getByRole("radio", { name: "Chart DAM, Dam Creek below the Dam, 20% of this estimate" }));
    await waitFor(() => expect(screen.getByText(readingHeading("Dam Creek below the Dam"))).toBeTruthy());
    expect(screen.getByLabelText(chartLabel("72h"))).toBeTruthy();       // the last chart, still drawn
    expect(screen.getByText(LOADING_NOTE)).toBeTruthy();                 // the inline note
    expect(screen.queryByRole("progressbar")).toBeNull();                // never the spinner in its place
  });
});
