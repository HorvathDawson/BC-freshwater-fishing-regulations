/**
 * ONE WATER, AND FINDING ONE: its name and sections, the sections its Conditions map holds, its box, which water a reach
 * belongs to, and search over names, places and pointers.
 */
import { useMemo } from "react";
import type { ItemId, NameHit, PlaceHit, PointerHit, WaterSource, SectionId, Water } from "@app/data";
import { buildSearchIndex, queryIndex, type AreaAsk, type IndexWater, type SearchIndex } from "@app/core";
import { useAsync, useDebounced, type Async } from "../async";

/**
 * One water — its name and its sections. What the water sheet titles itself with.
 *
 * No regulations: they are not integrated (see `regulations.ts` in @app/core). The sheet
 * renders a placeholder where they will go.
 */
export function useWater(source: WaterSource, item: ItemId | null): Async<Water | null> {
  return useAsync(
    () => (item ? source.water(item) : Promise.resolve(null)),
    `water-item:${item}`,
    item !== null,
  );
}

/**
 * (T175) THE SECTIONS A WATER'S CONDITIONS MAP HOLDS AT EVERY ZOOM: the named water's own (`water.sections`), or — an
 * unnamed reach — the tapped section alone. What `MapProps.waterLine` keeps from the tiles; never a geometry.
 */
export function waterLineSections(water: Water | null, item: ItemId | null,
                                  tapSid: SectionId | null): readonly SectionId[] {
  if (item !== null && water?.item === item) return water.sections;
  return tapSid !== null ? [tapSid] : [];
}

/** `waterLineSections` for a water read from the source (the named water's sections once read; the tap meanwhile). */
export function useWaterLine(source: WaterSource, item: ItemId | null, tapSid: SectionId | null): readonly SectionId[] {
  const w = useWater(source, item);
  const water = w.state === "ready" ? w.value ?? null : null;
  return useMemo(() => waterLineSections(water, item, tapSid), [water, item, tapSid]);
}

/** (T175) A water's box from the release (`WaterSource.waterBox`): where its Conditions map frames it without a tap. */
export function useWaterBox(source: WaterSource, item: ItemId | null):
    readonly [number, number, number, number] | null {
  const b = useAsync(() => (item ? source.waterBox(item) : Promise.resolve(null)), `water-box:${item}`, item !== null);
  return b.state === "ready" ? b.value ?? null : null;
}

export interface SearchResults {
  waters: readonly NameHit[];
  places: readonly PlaceHit[];
  /** Group names that only point ("Tee Pee Lakes: see Friday Lake, …"), each leading to the waters it names. */
  pointers: readonly PointerHit[];
  /** (T247) the management unit or region the query named ("2-8", "region 2"): the waters are that area's */
  area?: AreaAsk | null;
  /** (T247) the waters were found through a typo-tolerant match: nothing matched as typed */
  fuzzy?: boolean;
  /** (T247) the (debounced, trimmed) query these answer, so a screen can tell "answered, nothing found" from "not answered yet" */
  query?: string;
}

/**
 * (T247) THE SEARCH INDEX, once per source: read the searchable set from the source (`WaterSource.searchIndex`) and build
 * it (`buildSearchIndex`); every keystroke after is an in-memory question (`queryIndex`). A failed read is not cached, so
 * the next keystroke asks again, and `useSearch` falls back to the source's SQL searches meanwhile.
 */
export type WaterIndex = SearchIndex<IndexWater, PlaceHit>;
const INDEXES = new WeakMap<WaterSource, Promise<WaterIndex>>();
export function searchIndexOf(source: WaterSource): Promise<WaterIndex> {
  let p = INDEXES.get(source);
  if (!p) {
    p = Promise.resolve().then(() => source.searchIndex()).then((d) => buildSearchIndex(d));
    INDEXES.set(source, p);
    p.catch(() => { if (INDEXES.get(source) === p) INDEXES.delete(source); });
  }
  return p;
}
/** Start building the index before the reader types (search opens): the first keystroke then finds it ready. */
export function warmSearchIndex(source: WaterSource): void {
  searchIndexOf(source).catch(() => {});
}

/** One index answer as the rows draw it: `NameHit`s (the alias it was found by, where each name came from, the area). */
function indexHits(ix: WaterIndex, q: string, limit: number, placeLimit: number): Omit<SearchResults, "pointers"> {
  const a = queryIndex(ix, q, limit, placeLimit);
  return {
    waters: a.waters.map((w): NameHit => ({
      item: w.item as ItemId, name: w.name, kind: w.kind, matchedAs: w.matchedAs, matchedSource: w.matchedSource,
      nameSource: w.nameSource ?? null, pieces: w.pieces, size: w.size, areaHa: w.areaHa, near: w.near,
      regions: w.regions, mus: w.mus })),
    places: a.places, area: a.area, fuzzy: a.fuzzy,
  };
}

/** How many group names that only point a search shows: a few, above the waters (`resultGroups`). */
export const POINTER_LIMIT = 3;

/**
 * HOW LONG THE SEARCH BOX WAITS after the last keystroke before it asks (owner decision 2026-10-20).
 *
 * (T247) Search is now the in-memory index (`searchIndexOf`): a keystroke costs about 1–8 ms on a laptop (measured in
 * core/searchIndex.test.ts), so the wait could shrink — the owner's 200 ms stays until the owner says otherwise.
 *
 * Before T247 search matched a substring anywhere in a name, a full scan of ~20k names + aliases: ~5 ms p95 on the laptop, so
 * ~27–54 ms on a phone (A5 in tools/release-perf.test.ts). That was accepted on the condition that the box does not
 * query on every keystroke. 200 ms is past the gap between letters of a fast typist (~100–180 ms on a phone keyboard),
 * so a word typed in one go is ONE query; and under the ~250 ms where a reader starts to wait for results. Pinned with
 * fake timers in search.debounce.render.test.tsx (a burst of keystrokes -> exactly one search).
 */
export const SEARCH_DEBOUNCE_MS = 200;

/** Names and aliases, plus the towns you can ask "what is near here" about. Debounced: see `SEARCH_DEBOUNCE_MS`. */
export function useSearch(
  source: WaterSource,
  query: string,
  limit = 40,
  debounceMs = SEARCH_DEBOUNCE_MS,
): Async<SearchResults> {
  const q = useDebounced(query.trim(), debounceMs);
  return useAsync(
    async () => {
      if (q.length < 2) return { waters: [], places: [], pointers: [], query: q };
      const indexed = await searchIndexOf(source).then((ix) => indexHits(ix, q, limit, 6), () => null);
      if (indexed) return { ...indexed, pointers: await source.searchPointers(q, POINTER_LIMIT), query: q };
      // the index could not be read: the source's own searches (exact and substring only) still answer
      const [waters, places, pointers] = await Promise.all([
        source.searchNames(q, limit), source.searchPlaces(q, 6), source.searchPointers(q, POINTER_LIMIT)]);
      return { waters, places, pointers, query: q };
    },
    `search:${q}:${limit}`,
  );
}

/**
 * Which water a reach belongs to, by name.
 *
 * One indexed read, so a screen can put a TITLE on itself without listing the whole water.
 * The Conditions screen once had no title at all because the only thing that knew a water's
 * name also read everything else about it.
 */
export function useWaterName(
  source: WaterSource, section: SectionId | null,
): Async<{ item: ItemId; name: string; kind: string } | null> {
  return useAsync(
    () => (section ? source.waterFor(section) : Promise.resolve(null)),
    `water:${section ?? ""}`,
    section !== null,
  );
}
