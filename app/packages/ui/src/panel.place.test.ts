/**
 * T175 — THE WATER IS IN THE ROUTE MAP'S FRAME. Opened without a tap (from search, the list, a link), the route map
 * framed the gauges alone: Nursery Creek's one donor is the Chilliwack at Vedder Crossing, 18 km down the valley, so it
 * opened at z12.5 on the gauge and the creek was off the map. The water's own box (the release's `waterBox`) now stands
 * for the place when no tap does.
 */
import { describe, expect, it } from "vitest";
import { panelCamera } from "@app/core";
import type { Panel } from "@app/data";
import { answerFrom, donorView, type PanelAnswer } from "./panel";

const PANEL: Panel = { areaKm2: 50, members: [
  { station: "08MH001" as never, role: "down", areaKm2: 60, years: { discharge: 40, level: 40 }, regulated: false,
    sameRiver: true },
] };
const INDEX = { stations: {
  "08MH001": { percentile: 0.2, parameter: "discharge" as const, discharge: 0.2, observedAt: "2026-10-20T20:30:00-08:00" },
} };
/** The gauge, and Nursery Creek's box (`part_geo`, gnis:24108). */
const GAUGE = { lon: -121.9675, lat: 49.0974 };
const CREEK: readonly [number, number, number, number] = [-121.7205, 49.0628, -121.7165, 49.0700];
const answer = (): PanelAnswer => {
  const a = answerFrom(PANEL, INDEX, "discharge", 0);
  return { ...a, routesReady: true, rows: a.rows.map((r) => ({ ...r, route: {
    station: r.station, name: "CHILLIWACK RIVER AT VEDDER CROSSING", lon: GAUGE.lon, lat: GAUGE.lat, role: "down" as const,
    path: [302079, 1235781] as never } })) } as PanelAnswer;
};
const SIZE = { width: 486, height: 292 };
const view = (o: { from?: { lat: number; lon: number } | null; place?: typeof CREEK | null }) =>
  donorView(answer(), { day: new Date("2026-10-21T05:00:00Z"), horizon: 0, from: o.from ?? null, size: SIZE,
                        selected: null, pickable: true, place: o.place ?? null });

describe("the route map frames the water when no tap says where the reader is", () => {
  it("no tap, no place: the lone gauge, as before (the creek is not in the frame)", () => {
    const vm = view({});
    if (!vm.ok || !vm.map) throw new Error("expected a map");
    expect(vm.map.camera).toEqual(panelCamera([GAUGE], null, SIZE));
    expect(vm.map.camera.zoom).toBe(12.5);
  });

  it("no tap, the water's box: the gauge AND the creek, and Fit returns to both", () => {
    const vm = view({ place: CREEK });
    if (!vm.ok || !vm.map) throw new Error("expected a map");
    expect(vm.map.camera).toEqual(panelCamera(
      [GAUGE, { lat: CREEK[1], lon: CREEK[0] }, { lat: CREEK[3], lon: CREEK[2] }], null, SIZE));
    expect(vm.map.bbox).toEqual([GAUGE.lon, CREEK[1], CREEK[2], GAUGE.lat]);
    // no "you are here" is invented for a box
    expect(vm.map.pins.map((p) => p.kind)).toEqual(["gauge"]);
  });

  it("a tap is the place: the box is ignored", () => {
    const at = { lat: 49.066, lon: -121.719 };
    const vm = view({ from: at, place: CREEK });
    if (!vm.ok || !vm.map) throw new Error("expected a map");
    expect(vm.map.camera).toEqual(panelCamera([GAUGE], at, SIZE));
    expect(vm.map.pins.map((p) => p.kind)).toEqual(["gauge", "here"]);
  });
});
