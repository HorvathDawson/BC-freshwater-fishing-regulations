/**
 * T568 — THE ROUTE MAP HAS ONE PLACE IN THE SHEET: over the chart, whether the estimate answers or refuses. It sat under
 * the chart when the estimate answered and nowhere when it did not, so the Flow / Level toggle moved the map across the
 * chart (owner 2026-11-09: "the whole map jumps position from above the chart to below it").
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import type { MapProps } from "@app/map";
import type { DonorRow, PanelAnswer } from "@app/ui";
import { LIGHT } from "./theme";

vi.mock("@app/map", async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  Map: (_p: MapProps) => <div data-testid="route-map" />,
}));
const { DonorPanel } = await import("./DonorPanel");

afterEach(cleanup);

const row = (station: string, over: Partial<DonorRow> = {}): DonorRow => ({
  station: station as never, role: "up", percentile: 0.12, weight: 0.6, areaRatio: 2.4, areaKm2: 120, trust: "close",
  years: 40, regulated: false, sameRiver: true, factors: { share: 0.42, role: 1, record: 1 },
  route: { station: station as never, name: station, lat: 49.2, lon: -121.5, role: "up", path: [`${station}:1`] as never },
  ...over });
const DAY = new Date(2026, 8, 4, 12);
const chart = <div data-testid="chart" />;
const draw = (value: PanelAnswer) => render(
  <DonorPanel palette={LIGHT} day={DAY} value={value} at={{ atlas: "a", basemap: "b" }} theme="light" chart={chart} />);
const before = (a: Element, b: Element) => (a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING) !== 0;

describe("the route map's place", () => {
  it("is over the chart when the estimate answers", () => {
    draw({ answer: { ok: true, value: { percentile: 0.15, plusMinus: 12, trust: "close", spread: 6, donors: 1 } },
           rows: [row("08MH001")], areaKm2: 50, routesReady: true });
    expect(before(screen.getByTestId("route-map"), screen.getByTestId("chart"))).toBe(true);
  });

  it("is over the chart when the estimate refuses, as well", () => {
    draw({ answer: { ok: false, why: "level-only" }, rows: [row("08MH001", { percentile: null, weight: 0, otherOnly: "level" })],
           areaKm2: 50, routesReady: true });
    expect(before(screen.getByTestId("route-map"), screen.getByTestId("chart"))).toBe(true);
  });
});
