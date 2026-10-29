/**
 * (T168) THE CONDITIONS PANEL, WIRED: `useConditionsView` reads each station's water (`WaterSource.gaugeWaters`), and the
 * count line, the gauge switch and the donor rows all group from that one answer. A panel with its own gauge and a
 * silent donor; a gauged lake read at its own station.
 *
 * Mutation (run 2026-10-24, failed both tests and was reverted): `useGaugeSides` answering no waters instead of asking
 * `source.gaugeWaters` — every station a donor, the count line and the groups wrong.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { Panel, WaterSource, SectionId } from "@app/data";
import { clearPanelCache } from "@app/ui";
import { ConditionsPanel } from "./ConditionsPanel";
import { LIGHT } from "./theme";

afterEach(cleanup);
beforeEach(clearPanelCache);

const SEC = 7 as SectionId;
const member = (station: string) => ({ station: station as never, role: "up" as const, areaKm2: 60,
                                       years: { discharge: 40, level: 40 }, regulated: false, sameRiver: true });
const PANEL: Panel = { areaKm2: 50, members: [member("OWN"), member("FAR")] };

const source = (o: { panel: Panel | null; lake?: string }) => ({
  panelsFor: async () => new Map(o.panel ? [[SEC, o.panel]] : []),
  panelRoutes: async () => (o.panel ? o.panel.members : []).map((m) => ({
    station: m.station, name: null, lat: 49, lon: -122, role: "up", path: [SEC] })),
  lakeStationsFor: async () => new Map(o.lake ? [[SEC, o.lake]] : []),
  gaugePoints: async () => (o.lake ? [{ station: o.lake, name: "A LAKE AT ITS OUTLET", lon: -122, lat: 49, mag: 1 }] : []),
  gaugeWaters: async () => ({ waters: ["gnis:1"], gauges: new Map([
    ["OWN", { item: "gnis:1", section: 7 }], ["FAR", { item: "gnis:2", section: 9 }],
    ["LAKEG", { item: "gnis:1", section: 7 }]]) }),
  gaugeForSection: async () => null,
  traceToGauge: async () => [],
  gaugeSeries: async () => null,
  gaugeParameters: async () => ["discharge"],
  gaugeNow: async (st: string) => (st === "OWN" || st === "LAKEG"
    ? { fetchedAt: Date.now() - 5 * 60_000, value: { discharge: 12.5, level: null, percentile: 0.45, standing: "normal",
                                                     at: "2026-10-23T06:00:00Z" } } : null),
}) as unknown as WaterSource;

// OWN reads; FAR is not on ECCC's real-time list
const feed = { index: async () => ({ listed: ["OWN", "LAKEG"], stations: {
  OWN: { percentile: 0.45, parameter: "discharge" as const, discharge: 0.45 } } }) };

describe("(T168) the conditions panel, grouped", () => {
  it("a panel: the count line, the switch and the rows group from one answer", async () => {
    render(<ConditionsPanel source={source({ panel: PANEL })} section={SEC} palette={LIGHT} feed={feed}
                            scroll={false} day={new Date(2026, 9, 23, 12)} />);
    await waitFor(() => expect(screen.getByTestId("gauge-count")).toBeTruthy());
    expect(screen.getByTestId("gauge-count").textContent).toBe("1 gauge on this water · 1 not reporting");
    // the switch: OWN on this water (the chart's gauge), FAR folded under the donors
    expect(screen.getByTestId("switch-groups-own").textContent).toBe("OWN100%");
    const fold = screen.getByTestId("switch-groups-donor-fold");
    expect(fold.getAttribute("aria-label")).toBe("1 gauge from other waters not reporting. Show them");
    expect(screen.queryByRole("radio", { name: /FAR/ })).toBeNull();
    fireEvent.click(fold);
    expect(screen.getByRole("radio", { name: /FAR/ })).toBeTruthy();
    // the rows: the same grouping
    expect(screen.getByTestId("donor-groups-own").textContent?.startsWith("ON THIS WATEROWN")).toBe(true);
    expect(screen.getByTestId("donor-groups-donor-fold").getAttribute("aria-label"))
      .toBe("1 gauge from other waters not reporting. Show them");
  });

  it("a gauged lake: its own station is on this water", async () => {
    render(<ConditionsPanel source={source({ panel: null, lake: "LAKEG" })} section={SEC} palette={LIGHT} feed={feed}
                            scroll={false} day={new Date(2026, 9, 23, 12)} />);
    await waitFor(() => expect(screen.getByTestId("gauge-count")).toBeTruthy());
    expect(screen.getByTestId("gauge-count").textContent).toBe("1 gauge on this water");
  });
});
