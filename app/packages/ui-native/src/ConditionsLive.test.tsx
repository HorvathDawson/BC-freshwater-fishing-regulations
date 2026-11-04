/**
 * (owner 2026-10-29) THE OPEN PAGE FOLLOWS THE FEEDS: a gauge feed the update service takes (`onFeedChange`, through
 * `useSync`) redraws a MOUNTED Conditions view in place — the new reading replaces the old one with no reload and no
 * loading frame between them.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import type { Panel, SectionId, UpdateService, WaterSource } from "@app/data";
import { clearAnswers, clearPanelCache, resetFeedVersions, useSync } from "@app/ui";
import { ConditionsPanel } from "./ConditionsPanel";
import { LIGHT } from "./theme";

afterEach(cleanup);
beforeEach(() => { clearPanelCache(); clearAnswers(); resetFeedVersions(); });

const SEC = 7 as SectionId;
const PANEL: Panel = { areaKm2: 50, members: [
  { station: "LEAD" as never, role: "up", areaKm2: 60, years: { discharge: 40, level: 40 }, regulated: false,
    sameRiver: true },
] };

describe("live feed → the open Conditions view", () => {
  it("a gauge feed the service took redraws the mounted reading in place", async () => {
    let discharge = 12.5;
    const source = {
      panelsFor: async () => new Map([[SEC, PANEL]]),
      panelRoutes: async () => [{ station: "LEAD", name: "Lead Creek near Town", lat: 49, lon: -122, role: "up", path: [SEC] }],
      lakeStationsFor: async () => new Map(),
      gaugeForSection: async () => null,
      traceToGauge: async () => [],
      gaugeSeries: async () => null,
      gaugeParameters: async () => ["discharge", "level"],
      gaugeNow: async () => ({ fetchedAt: Date.now() - 60_000,
                               value: { discharge, level: 1.2, percentile: 0.15, standing: "below", at: "2026-09-04T06:00:00Z" } }),
    } as unknown as WaterSource;
    const feed = { index: async () => ({ stations: { LEAD: { percentile: 0.15, parameter: "discharge" as const, discharge: 0.15 } } }) };
    // the update service as useSync reads it: no snapshot, and the feed-change event the test fires
    let moved: ((id: "gauges" | "dfo" | "inseason") => void) | null = null;
    const updates = { subscribe: () => () => {}, snapshot: () => null,
                      onFeedChange: (on: typeof moved) => { moved = on; return () => { moved = null; }; },
                      check: async () => {} } as unknown as UpdateService;
    function Page() {
      useSync({ release: null, updates });
      return <ConditionsPanel source={source} section={SEC} palette={LIGHT} feed={feed} scroll={false}
                              day={new Date(2026, 8, 4, 12)} />;
    }
    render(<Page />);
    await waitFor(() => expect(screen.getByText("12.5")).toBeTruthy());

    discharge = 30.1;
    act(() => { moved!("gauges"); });
    // no loading frame: the reading stays drawn while the new one is asked
    expect(screen.getByTestId("gauge-title").textContent).toBe("Lead Creek near Town");
    await waitFor(() => expect(screen.getByText("30.1")).toBeTruthy());
    expect(screen.queryByText("12.5")).toBeNull();
  });
});
