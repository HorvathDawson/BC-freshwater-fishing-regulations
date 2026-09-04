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
import { zoomForMagnitude } from "@app/core";
import type { GaugeFeed } from "@app/data";

export interface GaugePoint {
  station: string;
  name: string;
  lon: number;
  lat: number;
  /** FWA stream magnitude at the station's node. Null when the node never got one. */
  mag?: number | null;
}

type Index = Awaited<ReturnType<GaugeFeed["index"]>>;

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
  if (percentile !== null) parts.push(`p${ordinal(Math.round(percentile * 100))}`);
  return parts.length ? parts.join(" · ") : null;
}

/** 1st, 2nd, 3rd, 4th — the form a person reads a percentile in. */
function ordinal(n: number): string {
  const v = Math.max(1, Math.min(99, n));
  if (v % 100 >= 11 && v % 100 <= 13) return `${v}th`;
  return `${v}${["th", "st", "nd", "rd"][v % 10] ?? "th"}`;
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
                             showTemperature = false): string | null {
  if (!index) return null;
  const features = [];
  for (const p of points) {
    const row = index.stations[p.station];
    if (!row) continue;                     // not transmitting: draw nothing at all
    const tC = showTemperature ? row.temperatureC ?? null : null;
    // In the temperature view a station with no sensor is not drawn at all, rather than
    // drawn with its flow number under a temperature heading.
    if (showTemperature && tC === null) continue;
    const label = gaugeLabel(undefined, row.percentile ?? null, tC);
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
                    percentile: row.percentile,
                    ...(tC !== null ? { temperatureC: tC,
                                        temperatureBand: row.temperatureBand ?? null } : {}),
                    minz: zoomForMagnitude(p.mag ?? null) },
    });
  }
  return features.length
    ? JSON.stringify({ type: "FeatureCollection", features })
    : null;
}
