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

  it("is asked of the gauge table without naming the column, so an older release still answers", () => {
    expect(gaugePlaces(2)).toBe("SELECT * FROM gauge WHERE station IN (?,?)");
  });
});
