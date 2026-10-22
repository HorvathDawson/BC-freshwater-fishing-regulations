/**
 * The words ON and UNDER the route map — how the gauges reach the place being read.
 *
 * Owner 2026-10-21: upstream and downstream gauges drawn apart, with a key that says which
 * is which; each gauge's share of the estimate on the map itself; a map you can zoom, pan
 * and fit back. The shares are `donorShare` — the panel's own words for the same number —
 * so the map and the rows under it cannot disagree about what a gauge contributed.
 */
import { CONDITIONS_WORDS, donorShare, donorUnranked, type DonorFacts, type Horizon } from "./conditions";

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
  fit: "Fit",
  fitLabel: "Fit the map to you and the gauges",
  larger: "Larger map",
  smaller: "Smaller map",
} as const;

/** What a gauge's dot says on the map: its share of the estimate, or that it is quiet. */
type MapFacts = Pick<DonorFacts, "weight" | "percentile" | "realtime" | "observedAt">;

/** `o`: the instant and horizon the answer is for, so a gauge that reports but cannot be ranked is said as that. */
export function donorMapLabel(r: MapFacts, o: { ref?: number; horizon?: Horizon } = {}): string {
  if (r.percentile !== null) return donorShare(r);
  if (r.realtime === false) return ROUTE_MAP_WORDS.notRealtime;
  return donorUnranked(r, o) ? ROUTE_MAP_WORDS.unranked : ROUTE_MAP_WORDS.quiet;
}

/** A gauge's entry in the map's key: its station and the same words its dot carries. */
export function donorMapKey(r: MapFacts & Pick<DonorFacts, "station">,
                            o: { ref?: number; horizon?: Horizon } = {}): string {
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
