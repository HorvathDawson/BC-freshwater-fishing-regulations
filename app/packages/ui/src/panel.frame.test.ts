/**
 * T568 — THE GAUGE MAP KEEPS ONE FRAME PER WATER. Owner 2026-11-09: tapping Level / Flow moved the map. The camera used to
 * fit only the members that speak in the quantity asked (a member silent in one quantity left the frame, so the map
 * re-zoomed and remounted), and a refusal had no map at all. Both quantities now fit the same members, and a refusal
 * keeps the map, each pin saying why it does not count.
 *
 * MUTATION PINS: camera fitted to `shown` again -> "the camera and the key are equal for Flow and Level"; the refusal
 * returning `map: null` -> "a refusal keeps the map".
 */
import { describe, expect, it } from "vitest";
import type { DonorRow, Panel, PanelAnswer } from "@app/data";
import { answerFrom, donorView } from "./panel";

const member = (station: string, role: "up" | "down") =>
  ({ station: station as never, role, areaKm2: 60, years: { discharge: 40, level: 40 }, regulated: false, sameRiver: true });
const PANEL: Panel = { areaKm2: 50, members: [member("MISSION", "down"), member("HANSARD", "up")] };
const LAT: Record<string, number> = { MISSION: 49.13, HANSARD: 54.1 };
const LON: Record<string, number> = { MISSION: -122.3, HANSARD: -122.0 };

// A DAY AHEAD: Mission has a flow forecast and none ranked in level; Hansard the reverse (the owner's phone, T567).
const INDEX = {
  listed: ["MISSION", "HANSARD"],
  stations: {
    MISSION: { percentile: 0.66, parameter: "level" as const, level: 1.548, observedAt: "2026-11-09T20:00:00-08:00",
               ahead: { "3": { percentile: 0.7, discharge: 0.7 } } },
    HANSARD: { percentile: 0.27, parameter: "discharge" as const, discharge: 197, level: 2.18,
               observedAt: "2026-11-09T20:00:00-08:00", ahead: { "3": { percentile: 0.4, level: 0.4 } } },
  },
};
const DAY = new Date("2026-11-10T20:00:00Z");

function routed(a: PanelAnswer): PanelAnswer {
  return { ...a, routesReady: true, rows: a.rows.map((r): DonorRow => ({ ...r, route: {
    station: r.station, name: r.station, lat: LAT[r.station]!, lon: LON[r.station]!, role: r.role,
    path: [`${r.station}:1`] as never } })) };
}
const opts = (horizon: 0 | 3) => ({ day: DAY, horizon, from: null, size: { width: 360, height: 220 },
                                    selected: null, pickable: false, zone: "America/Vancouver" });
const vmOf = (q: "discharge" | "level", h: 0 | 3, panel = PANEL) =>
  donorView(routed(answerFrom(panel, INDEX as never, q, h)), opts(h));

describe("the gauge map's frame", () => {
  it("the camera and the key are equal for Flow and Level on a mixed panel, today and a day ahead", () => {
    for (const h of [0, 3] as const) {
      const flow = vmOf("discharge", h), level = vmOf("level", h);
      expect(flow.map, `flow +${h}`).not.toBeNull();
      expect(level.map, `level +${h}`).not.toBeNull();
      expect({ camera: level.map!.camera, key: level.map!.key, bbox: level.map!.bbox })
        .toEqual({ camera: flow.map!.camera, key: flow.map!.key, bbox: flow.map!.bbox });
    }
  });

  it("the day ahead: each quantity answers from the member that has a forecast in it, the frame is the same", () => {
    const flow = vmOf("discharge", 3), level = vmOf("level", 3);
    if (!flow.ok || !level.ok) throw new Error("expected both to answer");
    expect(flow.map!.pins.map((p) => p.title)).not.toEqual(level.map!.pins.map((p) => p.title));
    expect(flow.map!.key).toBe(level.map!.key);
  });

  it("a refusal keeps the map, with the frame the answering quantity has", () => {
    // Mission alone: a flow forecast three days ahead, no ranked level forecast -> Level refuses, Flow answers.
    const alone: Panel = { areaKm2: 50, members: [member("MISSION", "down")] };
    const flow = vmOf("discharge", 3, alone), level = vmOf("level", 3, alone);
    expect(flow.ok).toBe(true);
    expect(level.ok).toBe(false);
    if (level.ok) return;
    expect(level.map).not.toBeNull();
    expect(level.map!.key).toBe(flow.map!.key);
    expect(level.map!.pins.map((p) => [p.row, p.kind])).toEqual([[0, "gauge"]]);
  });
});
