/**
 * HOW THE CONDITIONS STANDINGS REACH THE MAP (owner 2026-10-21: "we need to do the same painting speed up for the
 * conditions map too"), as `statusFeed` is for the day's status. Both renderers, web and native, colour per tile
 * (T74): `standing` is the release's classes (each section's, fixed for the release) and each class's standing for the
 * live feed at the chosen horizon. The map labels each tile's sections with their class's value as the tile arrives —
 * no wait for `idle`, no React round trip. A feed refresh or a horizon change is one new `values` object: one engine
 * call on the map, which relabels the sections of the tiles it holds (feature-state only; no paint change, no tile
 * reload). The arithmetic is the panel's (`classStanding`, `basinStandings` in @app/data); only on a PROVEN PAIR (the
 * caller's `paired`).
 */
import { useMemo, useRef } from "react";
import { basinStandings, classStanding, type Horizon, type Parameter, type WaterSource, type SectionId, FEED_TTL_MS,
         type StandingClasses } from "@app/data";
import { useAsync, usePoll } from "./async";
import type { PanelFeed } from "./panel";
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

/** What the Conditions map is handed: the colouring, and the legend's marks. */
export interface ConditionsColourVM {
  /** The classes and their values (`MapProps.standing`). Null off a proven pair or before the classes. */
  standing: { labels: StandingClasses; values: StandingValuesVM | null } | null;
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
export const FEED_POLL_MS = FEED_TTL_MS;

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

export function useConditionsColour(i: {
  source: WaterSource; feed: PanelFeed | undefined;
  /** The proven pair (P11). Anything less colours nothing. */
  paired: boolean;
  onConditions: boolean;
  quantity: Parameter;
  horizon: Horizon;
  /** What the map reports on screen — the legend's marks. */
  visible: readonly SectionId[];
  /** (the sync icon) bumped by a refresh the reader asked for: the feed is re-asked now, not at the next poll */
  epoch?: number;
}): ConditionsColourVM {
  const { source, feed, paired, onConditions, quantity, horizon } = i;
  // the release's classes, once — kept across tabs, so leaving Conditions withdraws nothing
  const classes = useAsync(() => source.standingClasses(), `standingclasses:${sourceKey(source)}`,
                           paired && onConditions);
  const labels = paired && classes.state === "ready" ? classes.value : null;
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
  const marks = useMemo(() => (labels && v ? classMarks(labels, v, i.visible) : []), [labels, v, i.visible]);
  return { standing, marks };
}
