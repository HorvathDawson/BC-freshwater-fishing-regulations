/**
 * A SILENT MEMBER IS NOT DRAWN, AND THE COLOURS STILL MATCH (owner 2026-10-22). Mounted with the route map's renderer
 * stubbed (MapLibre needs WebGL) and recording what it is asked to draw, so the table's dots and the map's pins can be
 * compared as colours. A silent member sits BETWEEN two shown ones in the weight order here on purpose: had the tone
 * followed the shown row, the gauge after it would be drawn in one colour in the table and another on the map — or,
 * worse, in a colour that is not the one its gauge-switch chip wears.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import type { MapProps } from "@app/map";
import type { DonorRow, PanelAnswer } from "@app/ui";
import { LIGHT } from "./theme";

const seen: MapProps[] = [];
vi.mock("@app/map", async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  Map: (p: MapProps) => { seen.push(p); return null; },
}));
const { DonorPanel, donorTone } = await import("./DonorPanel");

afterEach(() => { cleanup(); seen.length = 0; });

const row = (station: string, lat: number, over: Partial<DonorRow> = {}): DonorRow => ({
  station: station as never, role: "up", percentile: 0.12, weight: 0.6, areaRatio: 2.4, areaKm2: 120,
  trust: "close", years: 40, regulated: false, sameRiver: true, factors: { share: 0.42, role: 1, record: 1 },
  route: { station: station as never, name: null, lat, lon: -121.5, role: "up", path: [lat] as never }, ...over,
});

const VALUE: PanelAnswer = {
  answer: { ok: true, value: { percentile: 0.15, plusMinus: 12, trust: "close", spread: 6, donors: 2 } },
  rows: [row("08A", 49.2), row("08S", 49.3, { percentile: null, weight: 0 }), row("08C", 49.0, { weight: 0.4 })],
  areaKm2: 50, routesReady: true,
};

/** jsdom reports a colour as `rgb(r, g, b)`; the tokens are hex. */
const rgb = (hex: string) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
};

describe("the gauge table and its route map, with a silent member", () => {
  it("draws neither a row nor a pin for it, and each shown row is its pin's colour", () => {
    render(<DonorPanel palette={LIGHT} value={VALUE} day={new Date(2026, 8, 4, 12)} at={{ atlas: "a", basemap: "b" }}
                       theme="light" />);
    expect(screen.queryByText("08S")).toBeNull();
    expect(screen.getByText("Not reporting: 08S (last report not known)")).toBeTruthy();
    const pins = seen.at(-1)!.trace!.pins.filter((p) => p.kind === "gauge");
    // Whole: two gauge pins, the silent one's latitude nowhere, each in its member's tone (index 0 and 2).
    expect(pins.map((p) => [p.lat, p.tone])).toEqual([[49.2, LIGHT.donor[0]], [49.0, LIGHT.donor[2]]]);
    expect(seen.at(-1)!.trace!.routes.map((r) => [r.path, r.tone]))
      .toEqual([[[49.2], LIGHT.donor[0]], [[49.0], LIGHT.donor[2]]]);
    // TABLE == MAP: each row's dot is the tone of the pin at its gauge, and the switch chip's (`donorTone` by index).
    const dots = ["08A", "08C"].map((s) => (screen.getByTestId(`donor-dot-${s}`) as HTMLElement).style.backgroundColor);
    expect(dots).toEqual(pins.map((p) => rgb(p.tone)));
    expect(dots).toEqual([0, 2].map((i) => rgb(donorTone(LIGHT, i, false))));
  });
});
