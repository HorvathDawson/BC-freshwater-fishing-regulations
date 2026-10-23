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
import type { Horizon, SectionId } from "@app/data";

/**
 * WHERE THE CHANGE SCALE STARTS in the `standing` channel's units (T52). The Change view's values ride the SAME channel
 * and the SAME colour mode as every other horizon — a horizon switch stays a relabel, never a paint call — in a domain
 * of their own: the percentile ramp is 0–100 (and -1, "no baseline"), the change ramp 200–300
 * (`colorModes.*.standing` stops at 200 + 100 × `codedChange(ratio)`). Nothing is ever coloured between the two.
 */
export const CHANGE_RAMP_BASE = 200;

/**
 * The style's ramp is a percentage; every percentile in the app is 0–1. Converted HERE, where data meets the style.
 *
 * Under Change a value is moved into the change domain, and a refusal (the -0.01 sentinel: no donor has a forecast
 * running) is NOTHING — NaN, which the engine leaves unlabelled — never the "no baseline" dash and never steady.
 */
export const toRamp = (p: number, horizon: Horizon = 0): number =>
  horizon === "change" ? (p >= 0 ? CHANGE_RAMP_BASE + p * 100 : NaN) : p * 100;

/** A channel value's place on its own legend, 0–1 (either scale); negative for a sentinel. */
export const rampPosition = (v: number): number =>
  (v >= CHANGE_RAMP_BASE ? v - CHANGE_RAMP_BASE : v) / 100;

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
export function routeValues(labels: RouteLabels, standings: ReadonlyMap<SectionId, number>,
                            horizon: Horizon = 0): RouteValues | null {
  if (!standings.size) return null;
  return {
    section: (k) => {
      const s = labels.chain[k];
      const p = s === undefined ? undefined : standings.get(s);
      return p === undefined ? NaN : toRamp(p, horizon);
    },
    basins: {},
  };
}

/** The pushed path's data: every chain section's standing, on streams and lakes (a lake section is a section). */
export function pushedRoute(standings: ReadonlyMap<SectionId, number>, horizon: Horizon = 0): RouteColourVM["data"] {
  const values = Object.fromEntries([...standings].map(([s, p]) => [s, { standing: toRamp(p, horizon) }]));
  return { stream: values, lake: values };
}
