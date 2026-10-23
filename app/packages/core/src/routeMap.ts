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
