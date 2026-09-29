/**
 * WHAT THE MAP SHOWS WHILE YOU SEARCH — as data, so every surface shows the same thing.
 *
 * Riffle's search pane keeps a map pinned under the results: as you type it opens on the
 * best match and lights it; pick a town and it opens on the town and lights every water
 * within 25 km of it. That behaviour is a decision about WHICH water and WHICH box, and a
 * decision belongs in a hook (rule 25) — the phone draws it with a `MiniMap`, the desktop
 * will draw it with its own, and both must light the same water.
 *
 * `focusFor` is the whole decision, pure, so it is tested without a renderer. The hooks
 * below only fetch what it needs.
 */
import { useMemo } from "react";
import {
  fixOf, normalise, townBox, type Bbox, type Extent, type LatLon,
} from "@app/core";
import type { ItemId, NameHit, NearHit, PlaceHit, RegsSource, SectionId } from "@app/data";
import { useAsync, type Async } from "./async";

/** What a map should do for the current search. Handed straight to `<Map fit highlight>`. */
export interface MapFocus {
  /**
   * Identity of the subject. The map moves when THIS changes and not otherwise — a
   * re-render with the same water must not yank a camera the reader has since panned.
   */
  key: string;
  /** Where to open. Null when the bundle cannot place the water — then only light it. */
  bbox: Bbox | null;
  /** Every section to draw as selected: the WHOLE water, not a point on it. */
  highlight: readonly SectionId[];
  /**
   * Whether the map should widen or tighten `bbox` against the geometry it actually loads.
   * True for a water (the box is evidence, not an outline); false for a town, whose box is
   * already exactly what is wanted.
   */
  refine: boolean;
  /** A point to mark — the town itself. */
  marker: LatLon | null;
  /** The caption under the map, composed here so every surface says the same thing. */
  caption: string;
}

/** A water, resolved enough to draw: its sections and where the bundle puts it. */
export interface Located {
  item: ItemId;
  name: string;
  sections: readonly SectionId[];
  extent: Extent | null;
}

/** A town, and what is near it. */
export interface TownView {
  place: PlaceHit;
  near: readonly NearHit[];
  /** The sections of every water in `near`, so the map can light them all. */
  sections: readonly SectionId[];
}

export const NEAR_KM = 25;

/**
 * THE DECISION. A picked town wins over the water (it is a narrower question the reader
 * asked after typing); otherwise the water is shown; otherwise nothing, and the map stays
 * where the reader left it. Which water is "the" water — the best match for a query, or
 * the one the reader chose — is the caller's to say; see the two hooks below.
 */
export function focusFor(input: {
  town: TownView | null;
  best: Located | null;
}): MapFocus | null {
  const { town, best } = input;
  if (town) {
    const far = town.near.reduce((m, n) => Math.max(m, n.km), 0);
    const p = town.place;
    return {
      key: `place:${p.place}`,
      bbox: townBox(p, far),
      highlight: town.sections,
      refine: false,
      marker: { lat: p.lat, lon: p.lon },
      caption: town.near.length
        ? `${town.near.length} ${town.near.length === 1 ? "water" : "waters"} within ` +
          `${NEAR_KM} km of ${p.name}`
        : `No named water within ${NEAR_KM} km of ${p.name}`,
    };
  }
  if (best) {
    return {
      key: `item:${best.item}`,
      bbox: best.extent?.bbox ?? null,
      highlight: best.sections,
      refine: true,
      marker: null,
      caption: best.extent ? best.name : `${best.name} — the map cannot place it yet`,
    };
  }
  return null;
}

/** The best match's sections and extent. Idle (loading forever) when there is none. */
export function useLocated(source: RegsSource, item: ItemId | null): Async<Located | null> {
  return useAsync(
    async () => {
      if (!item) return null;
      const [water, fix] = await Promise.all([source.water(item), source.locate(item)]);
      if (!water) return null;
      return { item, name: water.name, sections: water.sections, extent: fixOf(fix) };
    },
    `located:${item}`,
    item !== null,
  );
}

/** What is near a town, with every nearby water's sections for the highlight. */
export function useTown(source: RegsSource, place: PlaceHit | null): Async<TownView | null> {
  return useAsync(
    async () => {
      if (!place) return null;
      const near = await source.watersNear(place.place);
      const waters = await Promise.all(near.map((n) => source.water(n.item)));
      return {
        place, near,
        sections: waters.flatMap((w) => (w ? w.sections : [])),
      };
    },
    `town:${place?.place ?? ""}`,
    place !== null,
  );
}

/**
 * The map's half of the search screen: which water or town to open on and light.
 *
 * `hits` is the ranked list the screen is already showing (from `useSearch`), so the map
 * and the first row can never disagree about which water is "best".
 */
export function useSearchFocus(source: RegsSource, query: string,
                               hits: readonly NameHit[], place: PlaceHit | null):
    MapFocus | null {
  const bestItem = query.trim().length >= 2 && !place ? hits[0]?.item ?? null : null;
  const located = useLocated(source, bestItem);
  const town = useTown(source, place);
  const best = located.state === "ready" ? located.value : null;
  const tv = town.state === "ready" ? town.value : null;
  return useMemo(
    () => focusFor({ town: place && tv?.place.place === place.place ? tv : null,
                     // Only the located water that IS the current best — a slow answer
                     // for the previous keystroke must not light the wrong river.
                     best: best && best.item === bestItem ? best : null }),
    [place, tv, best, bestItem]);
}

/**
 * The main map's focus for a water the reader CHOSE — from a search row, or from the list
 * of what is near a town. Same decision, same box and highlight as the search map drew, so
 * the water they picked is the water the map shows when they come back to it.
 */
export function usePickFocus(source: RegsSource, item: ItemId | null): MapFocus | null {
  const located = useLocated(source, item);
  const best = located.state === "ready" ? located.value : null;
  return useMemo(() => focusFor({ town: null,
                                  best: best && best.item === item ? best : null }),
                 [best, item]);
}

/**
 * A TOWN TYPED IN FULL is a town. "Smithers" names no water, and "Chilliwack" names a city
 * and no water by exactly that name — so the map opens on the town and lights what is
 * near it, as riffle does, without making the reader find and tap the town's row first.
 *
 * Only on an EXACT town name, and only when no water carries that exact name: someone who
 * typed "Fraser Lake" wants the lake, not the village of the same name, and someone still
 * typing "Chilli" has not asked for anything yet.
 */
export function townToPreview(query: string, waters: readonly { name: string }[],
                              places: readonly PlaceHit[]): PlaceHit | null {
  const q = normalise(query);
  if (q.length < 2) return null;
  const town = places.find((p) => normalise(p.name) === q);
  if (!town) return null;
  return waters.some((w) => normalise(w.name) === q) ? null : town;
}
