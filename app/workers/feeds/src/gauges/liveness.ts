// T225 port of pipeline/gauges/liveness.py: which stations the feed asks (ECCC's real-time list this run, else the
// roster's snapshot) and how far the roster has drifted from that list.
import { dictRows } from "./py";

export const STATION_LIST = "https://dd.weather.gc.ca/today/hydrometric/doc/hydrometric_StationList.csv";

/** A roster row: the only fields the feed reads (`bc_hydrometric_stations.json` rows carry more). */
export interface RosterRow {
  station: string;
  realtime?: boolean | null;
  [k: string]: unknown;
}

export function speaksNow(s: RosterRow, feed: Set<string> | null = null): boolean {
  if (feed !== null) return feed.has(s.station);
  return Boolean(s.realtime);
}

export function listedStations(text: string | null): Set<string> | null {
  if (!text) return null;
  const out = new Set<string>();
  for (const r of dictRows(text)) {
    if ((r["Prov/Terr"] || "").trim() === "BC") out.add((r.ID || "").trim());
  }
  out.delete("");
  return out.size ? out : null;
}

export function stationsToAsk(roster: RosterRow[], listed: Set<string> | null): [string[], "eccc" | "roster"] {
  if (listed !== null) return [[...listed].sort(), "eccc"];
  return [roster.filter((s) => speaksNow(s)).map((s) => s.station).sort(), "roster"];
}

/** liveness.roster_drift. `unknown` (T847): listed now, and no roster row at all — the only owner task; a known
 *  station joining or leaving the list is followed by the feed with no refresh. */
export interface RosterDrift {
  joined: string[];
  left: string[];
  unknown: string[];
}

export function rosterDrift(roster: RosterRow[], listed: Set<string> | null): RosterDrift | null {
  if (listed === null) return null;
  const was = new Set(roster.filter((s) => speaksNow(s)).map((s) => s.station));
  const held = new Set(roster.map((s) => s.station));
  return {
    joined: [...listed].filter((s) => !was.has(s)).sort(),
    left: [...was].filter((s) => !listed.has(s)).sort(),
    unknown: [...listed].filter((s) => !held.has(s)).sort(),
  };
}
