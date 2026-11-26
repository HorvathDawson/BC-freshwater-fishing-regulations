/**
 * (T1163 G5 / G6) "USE MY LOCATION" ON A SPOT'S PARKING: what the press sets and what it says.
 *   - a good fix sets the pin and says nothing;
 *   - a rough fix (accuracy worse than `ROUGH_FIX_M`) sets the pin and says so (`roughFixLine`), as the spot path does;
 *   - a fix outside B.C. sets nothing (`MY_LOCATION.far`);
 *   - a refusal sets nothing and says `MY_LOCATION.denied`, which `needsSettings` knows: the parking block draws "Open
 *     Settings" under it (the browser's draws nothing). It is the environment's, never reported;
 *   - anything else sets nothing, is reported, and says `SPOT_WORDS.parkingNoFix`.
 */
import { MY_LOCATION, nearBC, reportFailure, roughFixLine, SPOT_WORDS, type LatLon } from "@app/core";
import type { Locate } from "./mapRail";
import { refusedLine } from "./myLocation";

export interface ParkingHere {
  /** the point to save as the parking, or null: nothing is set */
  parking: LatLon | null;
  /** the line under the parking row, or null */
  notice: string | null;
}

export async function parkingHere(locate: Locate): Promise<ParkingHere> {
  try {
    const f = await locate.here();
    // (T864) parking far outside B.C. would send directions across the continent: said, nothing set
    if (!nearBC(f)) return { parking: null, notice: MY_LOCATION.far };
    return { parking: { lat: f.lat, lon: f.lon }, notice: roughFixLine(f.accuracy) };
  } catch (e) {
    reportFailure(e);
    return { parking: null, notice: refusedLine(e) === MY_LOCATION.denied ? MY_LOCATION.denied : SPOT_WORDS.parkingNoFix };
  }
}
