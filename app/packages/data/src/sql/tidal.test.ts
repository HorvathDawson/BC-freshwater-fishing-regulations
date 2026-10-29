/**
 * T78 — the release's `gauge.tidal` reaches the donor's route, which is where core's `tidalNote` reads it.
 *
 * An older release (or bundle) has no `tidal` column at all; its rows read as not flagged, never as an error — the
 * route map must not go dark because a release predates a disclosure.
 */
import { describe, expect, it } from "vitest";
import { gaugePlaces } from "./queries";
import { placesOf, routesOf } from "./shared";
import type { SectionId, StationId } from "../index";

const s = (n: number) => n as unknown as SectionId;
const st = (id: string) => id as StationId;

describe("a tidal gauge's flag on its route (T78)", () => {
  it("is carried where the release says 1, and absent where it says 0 or has no column", async () => {
    const places = placesOf([
      { station: "08MH035", name: "PITT RIVER NEAR PORT COQUITLAM", sid: 1, lon: -122.6, lat: 49.3, tidal: 1 },
      { station: "08MH001", name: "CHILLIWACK RIVER AT VEDDER CROSSING", sid: 2, lon: -122, lat: 49.1, tidal: 0 },
      { station: "08MF005", name: "FRASER RIVER AT HOPE", sid: 3, lon: -121.4, lat: 49.4 },
    ]);
    const routes = await routesOf(s(1),
      [{ station: st("08MH035"), role: "up" }, { station: st("08MH001"), role: "up" },
       { station: st("08MF005"), role: "up" }],
      places, async () => null);
    expect(routes).toEqual([
      { station: "08MH035", name: "PITT RIVER NEAR PORT COQUITLAM", lon: -122.6, lat: 49.3, role: "up", path: [s(1)],
        tidal: true },
      { station: "08MH001", name: "CHILLIWACK RIVER AT VEDDER CROSSING", lon: -122, lat: 49.1, role: "up", path: [] },
      { station: "08MF005", name: "FRASER RIVER AT HOPE", lon: -121.4, lat: 49.4, role: "up", path: [] },
    ]);
  });

  it("carries a lake's backwater (T160) where the release says 1, beside the tide and never as it", async () => {
    const places = placesOf([
      { station: "08HB074", name: "CRUICKSHANK RIVER NEAR THE MOUTH", sid: 1, lon: -125.2, lat: 49.7, tidal: 0,
        backwater: 1 },
      { station: "08MH035", name: "PITT RIVER NEAR PORT COQUITLAM", sid: 2, lon: -122.6, lat: 49.3, tidal: 1,
        backwater: 0 },
      { station: "08MF005", name: "FRASER RIVER AT HOPE", sid: 3, lon: -121.4, lat: 49.4, tidal: 0 },
    ]);
    const routes = await routesOf(s(1),
      [{ station: st("08HB074"), role: "up" }, { station: st("08MH035"), role: "up" },
       { station: st("08MF005"), role: "up" }],
      places, async () => null);
    expect(routes).toEqual([
      { station: "08HB074", name: "CRUICKSHANK RIVER NEAR THE MOUTH", lon: -125.2, lat: 49.7, role: "up",
        path: [s(1)], backwater: true },
      { station: "08MH035", name: "PITT RIVER NEAR PORT COQUITLAM", lon: -122.6, lat: 49.3, role: "up", path: [],
        tidal: true },
      { station: "08MF005", name: "FRASER RIVER AT HOPE", lon: -121.4, lat: 49.4, role: "up", path: [] },
    ]);
  });

  it("is asked of the gauge table without naming the column, so an older release still answers", () => {
    expect(gaugePlaces(2)).toBe("SELECT * FROM gauge WHERE station IN (?,?)");
  });
});

describe("a gauge on another water joins through the confluence (owner 2026-10-28)", () => {
  // 1 -> 2 -> 3 -> 4 is the spot's water flowing on; 10 -> 11 -> 3 is the gauge's river meeting it at 3.
  const next = new Map<number, number>([[1, 2], [2, 3], [3, 4], [10, 11], [11, 3]]);
  const down = async (f: SectionId) => (next.get(f as unknown as number) ?? null) as SectionId | null;
  const places = () => placesOf([
    { station: "08MH006", name: "NORTH ALOUETTE RIVER AT 232ND STREET", sid: 10, lon: -122.58, lat: 49.24 },
    { station: "08XX000", name: "SOMEWHERE ELSE", sid: 99, lon: -122, lat: 49 },
  ]);

  it("walks the spot down to the confluence, then the gauge's river back up to the gauge", async () => {
    const [r] = await routesOf(s(1), [{ station: st("08MH006"), role: "up" }], places(), down);
    expect(r!.path).toEqual([s(1), s(2), s(3), s(11), s(10)]);
    expect(r!.join).toBe(2);
  });

  it("leaves a gauge no walk reaches undrawn, and a direct route without a join", async () => {
    const out = await routesOf(s(1), [{ station: st("08XX000"), role: "up" }], places(), down);
    expect(out[0]!.path).toEqual([]);
    expect(out[0]!.join).toBeUndefined();
    const direct = await routesOf(s(11), [{ station: st("08MH006"), role: "up" }], places(), down);
    expect(direct[0]!.path).toEqual([s(11), s(10)]);
    expect(direct[0]!.join).toBeUndefined();
  });
});

describe("T252: a route's distance along the river, and where its waters meet", () => {
  // the upper Pitt (1) flows into Pitt Lake (2), out to the lower Pitt (3, 4); the Alouette (11) joins at 3 below its
  // gauge's reach (10). Lengths in metres; the lake's is its through-line.
  const next = new Map<number, number>([[1, 2], [2, 3], [3, 4], [10, 11], [11, 3]]);
  const len = new Map<number, number>([[1, 8000], [2, 25000], [3, 4000], [10, 2000], [11, 6000]]);
  const name = new Map<number, string>([[1, "Pitt River"], [2, "Pitt Lake"], [3, "Pitt River"], [4, "Pitt River"],
                                        [10, "North Alouette River"], [11, "Alouette River"]]);
  const down = async (f: SectionId) => (next.get(f as unknown as number) ?? null) as SectionId | null;
  const facts = { lenOf: async (x: SectionId) => len.get(x as unknown as number) ?? null,
                  nameOf: async (x: SectionId) => {
                    const n = name.get(x as unknown as number);
                    return n ? { name: n, kind: n.endsWith("Lake") ? "lake" : "stream" } : null;
                  } };
  const places = () => placesOf([
    { station: "08MH006", name: "North Alouette River at 232nd Street", sid: 10, lon: -122.58, lat: 49.24 },
    { station: "08MH035", name: "Pitt River near Port Coquitlam", sid: 4, lon: -122.7, lat: 49.24 },
    { station: "08ZZ001", name: "Upper", sid: 1, lon: -122.6, lat: 49.7 },
  ]);

  it("sums half the spot, the reaches to the confluence and the gauge's water below its gauge", async () => {
    const [r] = await routesOf(s(1), [{ station: st("08MH006"), role: "up" }], places(), down, facts);
    expect(r!.path).toEqual([s(1), s(2), s(3), s(11), s(10)]);
    // 4,000 (half the spot) + 25,000 (the lake) + 6,000 (the Alouette below its gauge); the confluence reach counts 0
    expect(r!.alongM).toBe(35000);
    expect(r!.joinSide).toBe("gauge");
    expect(r!.joinWater).toBe("Alouette River");
  });

  it("counts a downstream gauge's own reach, and not an upstream one's", async () => {
    const [d] = await routesOf(s(1), [{ station: st("08MH035"), role: "down" }], places(), down, facts);
    expect(d!.path).toEqual([s(1), s(2), s(3), s(4)]);
    expect(d!.alongM).toBeUndefined();               // 4's length is unknown here: never a sum with a hole
    const [u] = await routesOf(s(3), [{ station: st("08ZZ001"), role: "up" }], places(), down, facts);
    expect(u!.path).toEqual([s(3), s(2), s(1)]);
    expect(u!.alongM).toBe(2000 + 25000);             // half of 3, the lake; the gauge stands at 1's lower end
  });

  it("marks the spot's side when this water is the tributary", async () => {
    // the spot on the Alouette (11), its gauge up the Pitt (1): the Alouette enters at 3
    const [r] = await routesOf(s(11), [{ station: st("08ZZ001"), role: "up" }], places(), down, facts);
    expect(r!.path).toEqual([s(11), s(3), s(2), s(1)]);
    expect(r!.join).toBe(1);
    expect(r!.joinSide).toBe("spot");
    expect(r!.joinWater).toBe("Alouette River");
    expect(r!.alongM).toBe(3000 + 25000);
  });

  it("says nothing of distance on a release with no lengths", async () => {
    const [r] = await routesOf(s(1), [{ station: st("08MH006"), role: "up" }], places(), down);
    expect(r!.alongM).toBeUndefined();
    expect(r!.joinWater).toBeUndefined();
  });
});
