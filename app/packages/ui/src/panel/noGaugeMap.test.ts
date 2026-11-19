/**
 * (T881, owner 2026-11-18, on Babine Lake — Main Body → Conditions: "Lakes or other things without a meaningful map here
 * don't need to include gauge map..") A ROUTE MAP DRAWS A GAUGE OR IS NOT DRAWN. The Babine sheet said "No gauge on this
 * water is close enough…" and then drew a map holding "you are here" alone over the lake. Every way a map ends with no
 * gauge on it is a null map; one located gauge (or a gauged lake's own station, `ownGauge.test.ts`) keeps it.
 *
 * MUTATION: drop `&& drawsAGauge` from `map` in `donorView` -> the first three cases draw a map of "you are here" alone.
 */
import { describe, expect, it } from "vitest";
import type { Panel } from "@app/data";
import { answerFrom, donorView, type PanelAnswer } from "../panel";

const DAY = new Date("2026-10-21T05:00:00Z");
const HERE = { lat: 55.0, lon: -126.0 };
const GAUGE = { lat: 55.2, lon: -126.3 };
const opts = { day: DAY, horizon: 0 as const, from: HERE, size: { width: 358, height: 380 }, selected: null,
               pickable: true };
const PANEL: Panel = { areaKm2: 50, members: [
  { station: "08EB001" as never, role: "down", areaKm2: 60, years: { discharge: 40, level: 40 }, regulated: false,
    sameRiver: true },
] };
const READS = { listed: ["08EB001"], stations: {
  "08EB001": { percentile: 0.2, parameter: "discharge" as const, discharge: 0.2, observedAt: "2026-10-20T20:30:00-08:00" },
} };
/** The panel's one member routed, at `at` (null: a station with no known place). */
const routed = (a: PanelAnswer, at: { lat: number; lon: number } | null): PanelAnswer => ({
  ...a, routesReady: true, rows: a.rows.map((r) => ({ ...r, route: {
    station: r.station, name: "Babine River at outlet of Nilkitkwa Lake", lat: at?.lat ?? null, lon: at?.lon ?? null,
    role: "down" as const, path: (at ? [1, 2] : []) as never } })) }) as PanelAnswer;

describe("the route map is drawn only when it draws a gauge (T881)", () => {
  it("no gauge near enough (Babine Lake — Main Body): no map, only the refusal", () => {
    const vm = donorView({ answer: { ok: false, why: "no-station" }, rows: [], areaKm2: null, routesReady: true }, opts);
    expect([vm.ok, vm.ok ? null : vm.reason, vm.map])
      .toEqual([false, "No gauge on this water is close enough in size to speak for it.", null]);
  });

  it("a refusal whose every member is not reporting: none is framed or pinned, so no map", () => {
    const quiet = routed(answerFrom(PANEL, { listed: ["08EB001"], stations: {} }, "discharge", 0), GAUGE);
    expect(quiet.answer.ok).toBe(false);
    expect(donorView(quiet, opts).map).toBeNull();
  });

  it("an answer whose gauges have no known place: no map (the rows still name them)", () => {
    const vm = donorView(routed(answerFrom(PANEL, READS, "discharge", 0), null), opts);
    if (!vm.ok) throw new Error("expected an estimate");
    expect([vm.map, vm.rows.map((r) => r.station)]).toEqual([null, ["08EB001"]]);
  });

  it("one located gauge: the map, with the gauge and you", () => {
    const vm = donorView(routed(answerFrom(PANEL, READS, "discharge", 0), GAUGE), opts);
    expect(vm.map?.pins.map((p) => p.kind)).toEqual(["gauge", "here"]);
  });
});
