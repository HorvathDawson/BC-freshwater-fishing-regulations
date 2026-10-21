/**
 * THE ROUTE MAP'S COLOURING (the Conditions sheet's `DonorMap` → `MiniMap`): only the reaches between the place being
 * read and its gauges, each coloured by its own standing — through the SAME engine as the whole map (owner 2026-10-21:
 * "it should be a single pathway ideally"; engine polish).
 *
 *   per-tile (web)   `standing`, the engine's standing channel with the route as its classes: a chain section's class is
 *                    its place on the chain, every other section has none (so the engine leaves it unlabelled — the
 *                    route's filter is the labelling itself). A horizon or quantity change is new `values` over the SAME
 *                    labels: the engine relabels the loaded route sections, feature-state only.
 *   otherwise        `data`: the same values pushed (the native map, which has no engine yet — colour-latency README,
 *                    "Native").
 *
 * The values are `usePanelStandings` over the chain (`sectionStanding`, the one arithmetic) in the ramp's units, so the
 * two renderers cannot colour a reach differently, and neither can differ from the whole map.
 */
import type { SectionId } from "@app/data";

/** The style's ramp is a percentage; every percentile in the app is 0–1. Converted HERE, where data meets the style. */
export const toRamp = (p: number): number => p * 100;

/** The route as the engine's classes: a section's index on the chain, null off it. */
export interface RouteLabels {
  readonly chain: readonly SectionId[];
  classOf(section: number): number | null;
}

/** The route's values as the engine's standing channel takes them: per chain index; the route draws no basins. */
export interface RouteValues {
  section(k: number): number;
  readonly basins: Readonly<Record<string, number>>;
}

export interface RouteColourVM {
  /** Per-tile maps (`MapProps.standing`). Null on a pushed-data map. */
  standing: { labels: RouteLabels; values: RouteValues | null } | null;
  /** Pushed-data maps (`MapProps.data`): the chain's standings on streams and lakes. Empty on a per-tile map. */
  data: Record<string, Record<string, Record<string, unknown>>>;
}

/** The chain as classes. A new object only for a new chain: a horizon change keeps the labels, so nothing is cleared. */
export function routeLabels(chain: readonly SectionId[]): RouteLabels {
  const at = new Map<number, number>();
  chain.forEach((s, k) => { if (!at.has(s)) at.set(s, k); });
  return { chain, classOf: (s) => at.get(s) ?? null };
}

/** Each chain index's standing in ramp units; NaN where nothing speaks (the engine clears it). Null before any answer. */
export function routeValues(labels: RouteLabels, standings: ReadonlyMap<SectionId, number>): RouteValues | null {
  if (!standings.size) return null;
  return {
    section: (k) => {
      const s = labels.chain[k];
      const p = s === undefined ? undefined : standings.get(s);
      return p === undefined ? NaN : toRamp(p);
    },
    basins: {},
  };
}

/** The pushed path's data: every chain section's standing, on streams and lakes (a lake section is a section). */
export function pushedRoute(standings: ReadonlyMap<SectionId, number>): RouteColourVM["data"] {
  const values = Object.fromEntries([...standings].map(([s, p]) => [s, { standing: toRamp(p) }]));
  return { stream: values, lake: values };
}
