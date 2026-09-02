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
import type { GaugeFeed } from "@app/data";

export interface GaugePoint {
  station: string;
  name: string;
  lon: number;
  lat: number;
}

type Index = Awaited<ReturnType<GaugeFeed["index"]>>;

/** "15.7 m³/s · p4th", or null when there is nothing worth drawing. */
export function gaugeLabel(now: { discharge: number | null; level: number | null;
                                  parameter?: string } | undefined,
                           percentile: number | null): string | null {
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

/** GeoJSON for the map's `gauges` source, or null when there is nothing to draw. */
export function gaugeGeoJSON(points: readonly GaugePoint[], index: Index): string | null {
  if (!index) return null;
  const features = [];
  for (const p of points) {
    const row = index.stations[p.station];
    if (!row) continue;                     // not transmitting: draw nothing at all
    const label = gaugeLabel(undefined, row.percentile ?? null);
    if (!label) continue;                   // quiet station: a bare dot would read as a bug
    features.push({
      type: "Feature" as const,
      geometry: { type: "Point" as const, coordinates: [p.lon, p.lat] },
      properties: { station: p.station, name: p.name, label,
                    percentile: row.percentile },
    });
  }
  return features.length
    ? JSON.stringify({ type: "FeatureCollection", features })
    : null;
}
