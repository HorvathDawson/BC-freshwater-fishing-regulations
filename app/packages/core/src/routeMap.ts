/**
 * The words ON and UNDER the route map — how the gauges reach the place being read.
 *
 * Owner 2026-10-21: upstream and downstream gauges drawn apart, with a key that says which
 * is which; each gauge's share of the estimate on the map itself; a map you can zoom, pan
 * and fit back. The shares are `donorShare` — the panel's own words for the same number —
 * so the map and the rows under it cannot disagree about what a gauge contributed.
 */
import { CONDITIONS_WORDS, donorShare, donorState, type DonorAsk, type DonorFacts, type DonorReading }
  from "./conditions";

export const ROUTE_MAP_WORDS = {
  /** The key's two line samples. Role is where the GAUGE sits (`PanelRoute.role`). */
  down: "gauge downstream: your water flows on to it",
  up: "gauge upstream: its water flows down to you",
  /** Beside a gauge that is not reporting — it counts for nothing today. */
  quiet: "not reporting",
  /** Beside a gauge that reports but whose reading cannot be ranked — the row says the same ("Reporting, but …"). */
  unranked: "reporting, not ranked",
  /** Beside a gauge ECCC does not list as real-time (the feed index's `listed`) — it never reports live. */
  notRealtime: "not a real-time gauge",
  /**
   * Beside a gauge no forecast reaches at the horizon asked about — the row says the same ("No forecast …", T123).
   * It said "not reporting" here, which the row contradicted.
   */
  noForecast: "no forecast",
  /** (T157) Beside a gauge whose forecast that day is ranked in flow only, asked Level — the sheet's refusal says it. */
  flowOnly: "flow forecast only",
  /** (T157) Beside a gauge whose runs forecast the day with no record to rank it against. */
  noRecord: "no record to compare with",
  fit: "Fit",
  fitLabel: "Fit the map to you and the gauges",
  larger: "Larger map",
  smaller: "Smaller map",
} as const;

/** What a gauge's dot says on the map: its share of the estimate, or its state. */
type MapFacts = Pick<DonorFacts, "weight"> & DonorReading;

/**
 * `o`: the instant and horizon the answer is for. A `switch` over `donorState` — the state the row's own reading line
 * (`donorReads`) says — so the dot and the row cannot read one gauge two ways (T123).
 */
export function donorMapLabel(r: MapFacts, o: DonorAsk = {}): string {
  const s = donorState(r, o);
  switch (s.kind) {
    case "reads": return donorShare(r);
    case "not-realtime": return ROUTE_MAP_WORDS.notRealtime;
    case "flow-only": return ROUTE_MAP_WORDS.flowOnly;
    case "forecast-unranked": return ROUTE_MAP_WORDS.noRecord;
    case "no-forecast": return ROUTE_MAP_WORDS.noForecast;
    case "unranked": return ROUTE_MAP_WORDS.unranked;
    case "quiet": return ROUTE_MAP_WORDS.quiet;
    default: { const never: never = s; return never; }
  }
}

/**
 * A choice in the chart's gauge switcher, for a screen reader: "Chart 08MH016, Chilliwack River at outlet of
 * Chilliwack Lake, 35% of this estimate" — "Showing …" for the one on the chart. A member that does not count says its
 * state in the words its dot and its row say (T157: "flow forecast only" on a flow-only day under Level, never "not
 * counted today").
 */
export function gaugeChoiceLabel(r: MapFacts & Pick<DonorFacts, "station" | "route">, on: boolean,
                                 o: DonorAsk = {}): string {
  const name = r.route?.name ? `, ${r.route.name}` : "";
  const said = donorMapLabel(r, o);
  const what = donorState(r, o).kind === "reads" ? `${said} of this estimate` : said;
  return `${on ? "Showing" : "Chart"} ${r.station}${name}, ${what}`;
}

/** A gauge's entry in the map's key: its station and the same words its dot carries. */
export function donorMapKey(r: MapFacts & Pick<DonorFacts, "station">, o: DonorAsk = {}): string {
  return `${r.station} · ${donorMapLabel(r, o)}`;
}

/** The spot's own label on the map — the same words as its key entry. */
export const hereMapLabel = (): string => CONDITIONS_WORDS.youAreHere;

/**
 * The key's line entries for the roles actually drawn — downstream first (the water you are
 * standing in goes there), and nothing for a role no route has: a key entry with no line on
 * the map is a promise the map does not keep.
 */
export function routeLegend(roles: readonly ("up" | "down")[]):
    { role: "up" | "down"; label: string }[] {
  const has = new Set(roles);
  return (["down", "up"] as const).filter((r) => has.has(r))
    .map((role) => ({ role, label: ROUTE_MAP_WORDS[role] }));
}

/**
 * THE WATER A STATION IS ON, from ECCC's station name: "NORTH ALOUETTE RIVER AT 232ND STREET, MAPLE RIDGE" is on the
 * "North Alouette River". Null when the name carries no such split (then the sentence names the station alone).
 */
export function stationWater(name: string | null | undefined): string | null {
  if (!name) return null;
  const head = name.split(/\s+(?:AT|NEAR|BELOW|ABOVE|DOWNSTREAM OF|UPSTREAM OF|AT THE)\s+/i)[0]!.trim();
  if (!head || head.length === name.trim().length) return null;
  return head.toLowerCase().replace(/\b[a-z]/g, (c) => c.toUpperCase());
}

/** What the route to a gauge is, as `donorRelation` reads it. */
export interface DonorRelationFacts {
  station: string;
  name: string | null;
  role: "up" | "down";
  /** Reaches in its path (spot first); 0: not drawable. */
  reaches: number;
  /** `PanelRoute.join` present: the gauge is on another water that this one flows to meet. */
  joined: boolean;
  /** Straight-line distance from here to the gauge, km; null where either end is not known. */
  km: number | null;
  /** "45× apart" (`sizeApart`): how far the gauge's drainage is from this water's. */
  apart: string;
}

/**
 * ONE PLAIN LINE per gauge estimating from another water (owner 2026-10-28): how it relates to the water being read,
 * and that its reading is scaled by drainage. Three cases, said as they are: it joins this water below here; it is on
 * this water's own line; or no path joins them and it stands in for this water by similar country.
 */
export function donorRelation(r: DonorRelationFacts): string {
  const water = stationWater(r.name);
  const where = water ? `${r.station} is on ${water}` : r.station;
  const far = r.km === null ? "" : ` (the gauge is about ${r.km < 10 ? r.km.toFixed(1) : Math.round(r.km)} km away)`;
  const scaled = `scaled by drainage (${r.apart})`;
  if (r.joined)
    return `${where}, which joins this water below here${far}; ${scaled}.`;
  if (r.reaches > 1)
    return `${where}, ${r.reaches - 1} ${r.reaches === 2 ? "reach" : "reaches"} ${r.role === "up" ? "upstream" : "downstream"} on this water's line; ${scaled}.`;
  if (r.reaches === 1) return `${where}, on this reach; ${scaled}.`;
  return `${where}${r.km === null ? "" : `, about ${r.km < 10 ? r.km.toFixed(1) : Math.round(r.km)} km away`}: no mapped path joins it to this water, so it stands in for it by similar country; ${scaled}.`;
}
