/**
 * THE ROUTE MAP, mounted (owner 2026-10-21): what it hands the map, and what its key says.
 *
 * The renderer is stubbed — MapLibre needs WebGL — and the stub RECORDS its props, because
 * the claims under test are claims about what the map is asked to draw: each route with its
 * role and its gauge's tone, the spot as a pin and the gauges as dots with their shares, and
 * a fit that is sent on a press and at no other time.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { MapProps } from "@app/map";
import { donorView, type DonorRow, type PanelAnswer } from "@app/ui";
import { LIGHT } from "./theme";

const seen: MapProps[] = [];
vi.mock("@app/map", async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  Map: (p: MapProps) => { seen.push(p); return null; },
}));
const { DonorMap, routeMapHeight } = await import("./DonorMap");

afterEach(() => { cleanup(); seen.length = 0; });

const row = (station: string, over: Partial<DonorRow> = {}): DonorRow => ({
  station: station as never, role: "up", percentile: 0.12,
  weight: 0.6, areaRatio: 2.4, areaKm2: 120, trust: "close", years: 40,
  regulated: false, sameRiver: true,
  factors: { share: 0.42, role: 1, record: 1 }, ...over,
});
const route = (station: string, lat: number, role: "up" | "down", path: number[]) => ({
  station: station as never, name: null, lat, lon: -121.5, role, path: path as never });

const VALUE: PanelAnswer = {
  answer: { ok: true, value: { percentile: 0.15, plusMinus: 12, trust: "close",
                               spread: 6, donors: 2 } },
  // 08MH024 reports with nothing to rank (the evening before): a quiet, grey pin. 08MH099 is SILENT — no report at
  // all — so it is not drawn, though placed (owner 2026-10-22: hidden as rows and pins).
  rows: [row("08MH001", { route: route("08MH001", 49.2, "up", [1, 2]) }),
         row("08MH024", { weight: 0.4, role: "down", percentile: null,
                          observedAt: new Date(2026, 8, 3, 20).toISOString(),
                          route: route("08MH024", 49.0, "down", [1, 3, 4]) }),
         row("08MH099", { weight: 0, percentile: null, route: route("08MH099", 50.5, "up", [9]) })],
  areaKm2: 50, routesReady: true,
};
const FROM = { lat: 49.1, lon: -122 };

function vmOf() {
  const vm = donorView(VALUE, { day: new Date(2026, 8, 4), horizon: 0, from: FROM,
                                size: { width: 360, height: 220 }, selected: null,
                                pickable: false });
  if (!vm.ok || !vm.map) throw new Error("expected a map");
  return vm.map;
}

/** The panel's own tone rule: the accent for the spot, grey for a quiet gauge. */
const toneOf = (i: number | null) =>
  i === null ? LIGHT.accent : i === 1 ? LIGHT.quiet : LIGHT.donor[i % LIGHT.donor.length]!;

const draw = (larger = false, onLarger = () => {}) =>
  render(<DonorMap vm={vmOf()} at={{ atlas: "a", basemap: "b" }} theme="light"
                   palette={LIGHT} toneOf={toneOf} height={routeMapHeight(360, larger)}
                   larger={larger} onLarger={onLarger} />);

describe("<DonorMap>", () => {
  it("asks the map for each route, with its role and its gauge's tone", () => {
    draw();
    const t = seen.at(-1)!.trace!;
    expect(t.routes).toEqual([
      { role: "up", tone: LIGHT.donor[0], path: [1, 2], from: { lon: -122, lat: 49.1 },
        to: { lon: -121.5, lat: 49.2 } },
      { role: "down", tone: LIGHT.quiet, path: [1, 3, 4], from: { lon: -122, lat: 49.1 },
        to: { lon: -121.5, lat: 49.0 } },
    ]);
    expect(t.key).toBe(`up:${LIGHT.donor[0]}:1,2|down:${LIGHT.quiet}:1,3,4`);
  });

  it("marks the spot as a pin and each gauge as a dot labelled with its share", () => {
    draw();
    expect(seen.at(-1)!.trace!.pins).toEqual([
      { lat: 49.2, lon: -121.5, kind: "gauge", tone: LIGHT.donor[0], label: "60%", rank: 0 },
      { lat: 49.0, lon: -121.5, kind: "gauge", tone: LIGHT.quiet, label: "not reporting",
        rank: 1 },
      { lat: 49.1, lon: -122, kind: "here", tone: LIGHT.accent, label: "you are here",
        rank: -1 },
    ]);
    // The old DOM teardrops are gone: one kind of mark per thing, drawn by the overlay.
    expect(seen.at(-1)!.pins).toBeUndefined();
  });

  it("keys upstream and downstream apart, and names every gauge with its share", () => {
    draw();
    for (const t of ["you are here", "gauge downstream: your water flows on to it",
                     "gauge upstream: its water flows down to you", "08MH001 · 60%",
                     "08MH024 · not reporting"])
      expect(screen.getByText(t)).toBeTruthy();
  });

  it("can be zoomed: its zoom buttons are on, though it is bare", () => {
    draw();
    expect(seen.at(-1)!.bare).toBe(true);
    expect(seen.at(-1)!.zoomControls).toBe(true);
  });

  it("sends the camera back to everything only when Fit is pressed", () => {
    draw();
    expect(seen.at(-1)!.fit).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Fit the map to you and the gauges" }));
    expect(seen.at(-1)!.fit).toEqual({ key: "fit:1", bbox: [-122, 49, -121.5, 49.2] });
    fireEvent.click(screen.getByRole("button", { name: "Fit the map to you and the gauges" }));
    expect(seen.at(-1)!.fit!.key).toBe("fit:2");
  });

  it("offers a larger map, and says when it is showing one", () => {
    const onLarger = vi.fn();
    draw(false, onLarger);
    fireEvent.click(screen.getByRole("button", { name: "Larger map" }));
    expect(onLarger).toHaveBeenCalledWith(true);
    cleanup();
    draw(true, onLarger);
    fireEvent.click(screen.getByRole("button", { name: "Smaller map" }));
    expect(onLarger).toHaveBeenLastCalledWith(false);
  });

  it("is about square when larger, and never a letterbox", () => {
    expect([routeMapHeight(358, false), routeMapHeight(358, true)]).toEqual([220, 380]);
    expect([routeMapHeight(640, false), routeMapHeight(640, true)]).toEqual([340, 608]);
    expect([routeMapHeight(1000, false), routeMapHeight(1000, true)]).toEqual([340, 620]);
  });
});
