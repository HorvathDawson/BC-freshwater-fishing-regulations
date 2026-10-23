/**
 * The gauges themselves, as GeoJSON, labelled with what they are reading.
 *
 * A DOT ALONE SAYS "SOMETHING IS HERE", which is not the answer the Conditions view exists
 * to give. The label carries the reading and where it sits against normal — "15.7 m³/s ·
 * p4th" — because that pairing is the whole claim: a discharge without a percentile means
 * nothing to a person who does not know the river, and a percentile without a discharge is
 * a statistic with no substance.
 *
 * Stations with no reading today are DROPPED rather than drawn bare. A dot with no number
 * invites the reader to assume the map failed rather than that the station is quiet.
 */
import { changeDotLabel, changeRatio, zoomForMagnitude } from "@app/core";
import { percentileLabel } from "@app/core";
import { reading, type GaugeFeed, type Horizon } from "@app/data";
import { toRamp } from "./routeColour";

export interface GaugePoint {
  station: string;
  name: string;
  lon: number;
  lat: number;
  /** FWA stream magnitude at the station's node. Null when the node never got one. */
  mag?: number | null;
}

type Index = Awaited<ReturnType<GaugeFeed["index"]>>;

/** Which question the dots are answering. Three different sets of stations, not one. */
export type GaugeQuantity = "flow" | "level" | "temperature";

/** "15.7 m³/s · p4th", or "16.3 °C" in the temperature view. Null when nothing to draw. */
export function gaugeLabel(now: { discharge: number | null; level: number | null;
                                  parameter?: string } | undefined,
                           percentile: number | null,
                           temperatureC?: number | null): string | null {
  /*
   * TEMPERATURE REPLACES THE LABEL RATHER THAN JOINING IT.
   *
   * When the reader has asked about temperature, "15.7 m³/s · p4th · 19.8 °C" buries the
   * one number they are deciding on in two they did not ask for. And the degrees stand
   * alone honestly in a way the others cannot: a discharge means nothing without its
   * record, but 20 °C is the threshold this province closes rivers at.
   */
  if (temperatureC !== null && temperatureC !== undefined)
    return `${temperatureC.toFixed(1)} °C`;
  const parts: string[] = [];
  if (now) {
    const level = now.parameter === "level";
    const v = level ? now.level : now.discharge;
    if (v !== null && v !== undefined)
      parts.push(level ? `${v.toFixed(2)} m` : `${v} m³/s`);
  }
  // THE SAME LABEL THE SHEET USES. This rounded while the chip kept a decimal below 1%,
  // so a dot could read "p1st" beside a sheet saying "p0.4th" about that very station.
  if (percentile !== null) parts.push(percentileLabel(percentile));
  return parts.length ? parts.join(" · ") : null;
}

/**
 * GeoJSON for the map's `gauges` source, or null when there is nothing to draw.
 *
 * `showTemperature` swaps which quantity every dot is about. It is a different SET of
 * stations as well as a different number — 274 of the 439 publish a temperature and 361
 * publish a discharge, and they are not the same 274 — so the view genuinely redraws
 * rather than recolouring.
 */
export function gaugeGeoJSON(points: readonly GaugePoint[], index: Index,
                             quantity: GaugeQuantity = "flow", horizon: Horizon = 0): string | null {
  if (!index) return null;
  const features = [];
  for (const p of points) {
    const row = index.stations[p.station];
    if (!row) continue;                     // not transmitting: draw nothing at all
    const tC = quantity === "temperature" ? row.temperatureC ?? null : null;
    /*
     * A STATION THAT CANNOT ANSWER THIS QUESTION IS NOT DRAWN.
     *
     * These are three different rosters, not three colourings of one: 361 stations publish
     * a discharge percentile, 419 a level one and 274 a temperature, and they are not
     * nested sets. Drawing a station's flow number under a temperature heading — or a
     * bare dot where it has nothing to say — both read as the map having failed.
     */
    /*
     * THE DOT FOLLOWS THE HORIZON (T126), like the river under it: +3d is the station's forecast percentile for that
     * day, Change its expected change — read by `reading`, the ONE function the panels read a station with, so a dot
     * and its river cannot answer different days. (It always showed now, so under +5 a dot said today beside a river
     * saying Friday.) A day no run reaches has no dot, as a river there has no colour. Temperature has no forecast and
     * stays on now (the horizon control is not offered under it).
     *
     * `reading` also holds the station-default rule this used to repeat: `percentile` stands in only for the quantity
     * `parameter` names (discharge on a row from before `parameter`), never for the other.
     */
    const pct = quantity === "temperature" ? row.percentile ?? null
      : reading(row, quantity === "level" ? "level" : "discharge", horizon);
    if (quantity === "temperature" ? tC === null : pct === null) continue;
    // under Change the label is the station's own ratio (the coded number the colour rides is clamped)
    const label = quantity !== "temperature" && horizon === "change" && pct !== null
      ? changeDotLabel(row.change?.ratio ?? changeRatio(pct)) : gaugeLabel(undefined, pct, tC);
    if (!label) continue;                   // quiet station: a bare dot would read as a bug
    features.push({
      type: "Feature" as const,
      geometry: { type: "Point" as const, coordinates: [p.lon, p.lat] },
      // `minz` IS THE LADDER, precomputed here rather than expressed in the style.
      //
      // A tile feature carries the zoom it appears at because tippecanoe stamped it; a
      // GeoJSON source carries nothing, so without this every station in the province drew
      // at every zoom and a creek gauge floated over country where its creek had vanished
      // four zooms earlier. Stamping the number here keeps the style's filter to a single
      // comparison, and keeps the ladder itself in ONE place (`@app/core/ladder`) that a
      // test holds equal to the pipeline's.
      properties: { station: p.station, name: p.name, label,
                    // The value the DOT is painted with, in the standing channel's units (`toRamp`: a
                    // percentile x 100, or Change's own 200-300 domain) — the river's own scale, so a dot
                    // and the river under it cannot be coloured differently.
                    ramp: pct === null ? null : toRamp(pct, quantity === "temperature" ? 0 : horizon),
                    ...(tC !== null ? { temperatureC: tC,
                                        temperatureBand: row.temperatureBand ?? null } : {}),
                    minz: zoomForMagnitude(p.mag ?? null) },
    });
  }
  return features.length
    ? JSON.stringify({ type: "FeatureCollection", features })
    : null;
}
