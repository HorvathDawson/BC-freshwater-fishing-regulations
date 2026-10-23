/**
 * HOW THE CONDITIONS STANDINGS REACH THE MAP, per renderer — the other half of `mapModes`' `perTile` choice, as
 * `statusFeed` is for the day's status (owner 2026-10-21: "we need to do the same painting speed up for the conditions
 * map too").
 *
 *   per-tile (web)   `standing`: the release's classes (each section's, fixed for the release) and each class's
 *                    standing for the live feed at the chosen horizon. The map labels each tile's sections with their
 *                    class's value as the tile arrives — no wait for `idle`, no React round trip. A feed refresh or a
 *                    horizon change is one new `values` object: one engine call on the map, which relabels the
 *                    sections of the tiles it holds (feature-state only; no paint change, no tile reload).
 *   otherwise        `data`: every VISIBLE section's standing, pushed through `setData` after the map reports what is
 *                    on screen — the pre-existing path, which N2's native map reads in the `standing` mode.
 *
 * Both compute through the same arithmetic (`sectionStanding`, `basinStandings` in @app/data), so the two renderers
 * cannot colour a section differently; and both only on a PROVEN PAIR (the caller's `paired`).
 */
import { useMemo, useRef } from "react";
import { basinStandings, classStanding, type Horizon, type Parameter, type RegsSource, type SectionId,
         type StandingClasses } from "@app/data";
import { useAsync, usePoll } from "./async";
import { useBasinStandings, usePanelStandings, type PanelFeed } from "./panel";
import { rampPosition, toRamp } from "./routeColour";

export { toRamp };

/** The standing channel's values as the map takes them: per class and per basin, in the ramp's units. */
export interface StandingValuesVM {
  /**
   * A class's value: percent, -1 the "no baseline" sentinel, NaN where nothing speaks. COMPUTED WHEN FIRST ASKED and
   * kept: a horizon change computes the classes of the sections the map has loaded (hundreds), not all 18.5k.
   */
  section(classIndex: number): number;
  readonly basins: Readonly<Record<string, number>>;
}

/** What the Conditions map is handed: the colouring per renderer, and the legend's marks. */
export interface ConditionsColourVM {
  /** Per-tile maps: the classes and their values (`MapProps.standing`). Null off a proven pair or before the classes. */
  standing: { labels: StandingClasses; values: StandingValuesVM | null } | null;
  /** Pushed-data maps: the visible sections' and basins' standings (`MapProps.data` on Conditions). */
  data: Record<string, Record<string, Record<string, unknown>>>;
  /** The readings on screen, as positions on the legend's scale (0–1), sorted. Sentinels excluded: a state, not a point. */
  marks: readonly number[];
}

/** A source's identity as a cache key: a new source (a release swap) is a new set of classes. */
const sources = new WeakMap<object, number>();
let nextSource = 0;
export const sourceKey = (s: object): number => {
  let k = sources.get(s);
  if (k === undefined) sources.set(s, (k = ++nextSource));
  return k;
};

/** How often the map re-asks the feed: the feed's own cache life, so a published refresh lands within one. */
export const FEED_POLL_MS = 5 * 60_000;

/** The values for one feed index, quantity and horizon — one class's standing each, and the basins'. */
export function standingValues(c: StandingClasses, idx: Parameters<typeof classStanding>[2], quantity: Parameter,
                               horizon: Horizon): StandingValuesVM {
  const memo = new Float64Array(c.classes.length), done = new Uint8Array(c.classes.length);
  const section = (k: number): number => {
    if (!(k >= 0 && k < memo.length)) return NaN;
    if (!done[k]) {
      const p = classStanding(c, k, idx, quantity, horizon);
      memo[k] = Number.isFinite(p) ? toRamp(p, horizon) : NaN;
      done[k] = 1;
    }
    return memo[k]!;
  };
  const basins: Record<string, number> = {};
  for (const [b, p] of basinStandings(c.basins, idx, quantity, horizon)) basins[b] = toRamp(p, horizon);
  return { section, basins };
}

/** The legend's marks from the visible sections, through the classes: no fetch, no panel read. */
export function classMarks(c: StandingClasses, v: StandingValuesVM, visible: readonly SectionId[]): number[] {
  const out: number[] = [];
  for (const s of visible) {
    const k = c.classOf(s);
    const p = k === null ? NaN : v.section(k);
    if (p >= 0) out.push(rampPosition(p));
  }
  return out.sort((a, b) => a - b);
}

/** The pushed path's per-feature data: every visible section's standing on streams AND lakes, and the field. */
export function pushedStandings(standings: ReadonlyMap<SectionId, number>,
                                basins: ReadonlyMap<string, number>, horizon: Horizon = 0): ConditionsColourVM["data"] {
  return {
    // feature-state is per SOURCE-layer: `sections` is @app/map's `SECTION_DATA`, which the map fans out to every
    // per-section layer (`perSectionLayers()`, T124) — the stream lines, the lake polygons (`lake`, `lake_flowing`) and
    // the marsh a stream flows through (`wetland_flowing`, the `wetland` source-layer), so no list here can miss one
    sections: Object.fromEntries([...standings].map(([section, p]) => [section, { standing: toRamp(p, horizon) }])),
    basin: Object.fromEntries([...basins].map(([id, p]) => [id, { standing: toRamp(p, horizon) }])),
  };
}

export function useConditionsColour(i: {
  source: RegsSource; feed: PanelFeed | undefined;
  /** `coloursEachTile(Platform.OS)`: the renderer labels tiles itself. */
  perTile: boolean;
  /** The proven pair (P11). Anything less colours nothing. */
  paired: boolean;
  onConditions: boolean;
  quantity: Parameter;
  horizon: Horizon;
  /** The zoomed-out field is drawn (the pushed path asks for the basins only then). */
  fieldOn: boolean;
  /** What the map reports on screen — the pushed path's question, and the legend's marks on both. */
  visible: readonly SectionId[];
  /** (the sync icon) bumped by a refresh the reader asked for: the feed is re-asked now, not at the next poll */
  epoch?: number;
}): ConditionsColourVM {
  const { source, feed, perTile, paired, onConditions, quantity, horizon } = i;
  // THE PUSHED PATH (native): the viewport, through the panels — asked only where it is the path
  const pushed = !perTile && paired;
  const standings = usePanelStandings(source, feed, pushed ? i.visible : [], quantity, horizon);
  const basins = useBasinStandings(source, feed, pushed && i.fieldOn, quantity, horizon);
  // THE PER-TILE PATH (web): the release's classes, once — kept across tabs, so leaving Conditions withdraws nothing
  const classes = useAsync(() => source.standingClasses(), `standingclasses:${sourceKey(source)}`,
                           perTile && paired && onConditions);
  const labels = perTile && paired && classes.state === "ready" ? classes.value : null;
  // the feed, re-asked on its own schedule: a refresh that brings a new index is new values, and nothing else
  const tick = usePoll(FEED_POLL_MS, labels !== null && onConditions && feed !== undefined);
  // ONE TABLE PER (index, quantity, horizon), kept while the index and the classes last: the same question again is the
  // same object, so the map does nothing at all, and going back to a horizon already seen recomputes nothing
  const memo = useRef<{ labels: StandingClasses; idx: unknown; byKey: Map<string, StandingValuesVM> } | null>(null);
  const values = useAsync(async (): Promise<{ labels: StandingClasses; v: StandingValuesVM } | null> => {
    if (!labels || !feed) return null;
    const idx = await feed.index();
    if (!memo.current || memo.current.labels !== labels || memo.current.idx !== idx)
      memo.current = { labels, idx, byKey: new Map() };
    const key = `${quantity}:${horizon}`;
    let v = memo.current.byKey.get(key);
    if (!v) memo.current.byKey.set(key, v = standingValues(labels, idx, quantity, horizon));
    return { labels, v };
  }, `classstandings:${labels?.release ?? ""}:${quantity}:${horizon}:${tick}:${i.epoch ?? 0}`,
     labels !== null && onConditions && feed !== undefined,
     // A REFRESH, NOT A NEW QUESTION: the last table stays on the map until the next one is ready. Without it every
     // horizon change painted the whole map `missing` for a frame (the loading state) and then repainted it.
     `classstandings:${labels?.release ?? ""}`);
  // values computed from THESE labels only: a value table from another release's classes would colour wrong water
  const v = values.state === "ready" && values.value?.labels === labels ? values.value.v : null;
  const standing = useMemo(() => (labels ? { labels, values: v } : null), [labels, v]);
  const data = useMemo(() => (pushed ? pushedStandings(standings, basins, horizon) : {}),
                       [pushed, standings, basins, horizon]);
  const marks = useMemo(
    () => (labels && v ? classMarks(labels, v, i.visible)
      : [...standings.values()].filter((p) => p >= 0).sort((a, b) => a - b)),
    [labels, v, i.visible, standings]);
  return { standing, data, marks };
}
