/**
 * ONE WATER, AND FINDING ONE: its name and sections, the sections its Conditions map holds, its box, which water a reach
 * belongs to, and search over names, places and pointers.
 */
import { useMemo } from "react";
import type { ItemId, NameHit, PlaceHit, PointerHit, WaterSource, SectionId, Water } from "@app/data";
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
}

/** How many group names that only point a search shows: a few, above the waters (`resultGroups`). */
export const POINTER_LIMIT = 3;

/**
 * HOW LONG THE SEARCH BOX WAITS after the last keystroke before it asks (owner decision 2026-10-20).
 *
 * Search matches a substring anywhere in a name, a full scan of ~20k names + aliases: ~5 ms p95 on the laptop, so
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
      if (q.length < 2) return { waters: [], places: [], pointers: [] };
      const [waters, places, pointers] = await Promise.all([
        source.searchNames(q, limit),
        source.searchPlaces(q, 6),
        source.searchPointers(q, POINTER_LIMIT),
      ]);
      return { waters, places, pointers };
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
