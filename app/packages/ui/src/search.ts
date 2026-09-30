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
  duplicateNames, fixOf, normalise, rankNear, regulationsFor, townBox, waterStatus,
  type Bbox, type Extent, type LatLon, type WaterStatus,
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
  /**
   * A town to keep marked while a WATER is shown — the reader opened "water near Smithers"
   * and is now looking at one of them. The water is the subject; the pin says where they
   * were asking from.
   */
  pin?: PlaceHit | null;
}): MapFocus | null {
  const { town, best, pin = null } = input;
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
      marker: pin ? { lat: pin.lat, lon: pin.lon } : null,
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
      // The source answers nearest first; the list reads MOST WORTH LISTING first —
      // importance against distance, decided in core (`rankNear`).
      const near = rankNear(await source.watersNear(place.place));
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

/* ────────────────────────────────────────────────────────────────────────────────────────
 * LOOK, THEN GO.
 *
 * A result row answers "where is that?" and nothing else: tapping it frames the water on
 * the pinned map and lights it, or frames the town and marks it — and the reader stays on
 * the list, free to tap the next row and compare. LEAVING is a separate, deliberate act:
 * the eye button on the row. For a water the eye opens its page, framed and lit on the main
 * map; for a town it opens the list of what is near it (which is still this screen — a town
 * has no page of its own).
 *
 * The two used to be one tap, so every look at a row was a trip away from the results and
 * back. Kept pure here so "a tap never leaves" is a test, not a hope.
 * ──────────────────────────────────────────────────────────────────────────────────────── */

/** Something in the results the reader can point at: a water, or a town. */
export type SearchTarget =
  | { kind: "water"; item: ItemId; name: string }
  | { kind: "place"; place: PlaceHit };

/** The key a target's row shares with the `MapFocus` it produces — how a row knows it is lit. */
export function targetKey(t: SearchTarget): string {
  return t.kind === "water" ? `item:${t.item}` : `place:${t.place.place}`;
}

/** What the reader has in hand on the search screen. */
export interface SearchPick {
  /**
   * The SELECTED row: the one tap chose it, the pinned map frames and lights it, and it alone
   * carries the go button. Nothing navigates by selecting.
   */
  selected: SearchTarget | null;
  /** The town whose "water near" list is open — the eye on a town. */
  town: PlaceHit | null;
}

export const NO_PICK: SearchPick = { selected: null, town: null };

/** Is this row the selected one — the one that shows the go button? */
export function isSelected(pick: SearchPick, t: SearchTarget): boolean {
  return pick.selected !== null && targetKey(pick.selected) === targetKey(t);
}

export type SearchAction =
  /** The query changed: a selection among the old results means nothing any more. */
  | { t: "typed" }
  /** A tap (or Enter) on a row: select it. */
  | { t: "select"; target: SearchTarget }
  /** The go button, which only the selected row carries. */
  | { t: "go"; target: SearchTarget }
  /** "All results" from a town's list. */
  | { t: "back" };

/**
 * One step of the search screen: SELECT, THEN GO.
 *
 * The first tap on a row selects it — the pinned map frames and lights it — and never
 * navigates; tapping it again keeps it selected; tapping another row moves the selection.
 * Only the selected row carries the go button, and only `go` on the SELECTED row acts: a go
 * aimed at any other row (a stale press, a second pointer) selects that row instead. `leave`
 * is the water to open, set only by going on a water; going on a town stays and opens the
 * town's list, with every water near it lit.
 */
export function searchStep(s: SearchPick, a: SearchAction):
    { pick: SearchPick; leave: ItemId | null } {
  switch (a.t) {
    case "typed":
    case "back":
      return { pick: NO_PICK, leave: null };
    case "select":
      return { pick: { ...s, selected: a.target }, leave: null };
    case "go":
      if (!isSelected(s, a.target)) return { pick: { ...s, selected: a.target }, leave: null };
      return a.target.kind === "water"
        ? { pick: s, leave: a.target.item }
        : { pick: { selected: null, town: a.target.place }, leave: null };
  }
}

/**
 * WHAT THE PINNED MAP IS ABOUT, in order: the row the reader tapped; else the town whose
 * list is open; else a town typed in full; else the best match for the typing. A water
 * looked at from inside a town's list keeps that town pinned.
 */
export function subjectOf(pick: SearchPick, auto: { best: ItemId | null;
                                                    typedTown: PlaceHit | null }):
    { item: ItemId | null; place: PlaceHit | null; pin: PlaceHit | null } {
  const p = pick.selected;
  if (p?.kind === "water") return { item: p.item, place: null, pin: pick.town };
  if (p?.kind === "place") return { item: null, place: p.place, pin: null };
  if (pick.town) return { item: null, place: pick.town, pin: null };
  if (auto.typedTown) return { item: null, place: auto.typedTown, pin: null };
  return { item: auto.best, place: null, pin: null };
}

/** One group of results, in the order they are drawn. */
export type ResultGroup =
  | { kind: "places"; title: string; note: string; places: readonly PlaceHit[] }
  | { kind: "waters"; title: string;
      waters: readonly { hit: NameHit; dup: boolean }[] };

/**
 * PLACES FIRST, AND APART. A town is not a water — it is a question about the ground
 * around it — so it never sits in the same list as the rivers, and when the results hold
 * one it comes before them: a reader who typed a town's name meant the town. Each water
 * carries whether its name is shared with another in the list, which is what earns it a
 * "near Fernie".
 */
export function resultGroups(r: { waters: readonly NameHit[]; places: readonly PlaceHit[] }):
    ResultGroup[] {
  const out: ResultGroup[] = [];
  if (r.places.length)
    out.push({ kind: "places", title: r.places.length === 1 ? "Place" : "Places",
               note: `water within ${NEAR_KM} km`, places: r.places });
  if (r.waters.length) {
    const dups = duplicateNames(r.waters);
    out.push({ kind: "waters",
               title: `${r.waters.length} named ${r.waters.length === 1 ? "water" : "waters"}`,
               waters: r.waters.map((hit) => ({ hit, dup: dups.has(normalise(hit.name)) })) });
  }
  return out;
}

/**
 * The search screen's map and its town list, from what the reader has in hand.
 *
 * `results` is the ranked list the screen is already showing, so the map and the first row
 * can never disagree about which water is "best". ONE town fetch serves both the open list
 * and the map: inside a town's list the subject is that town or a water near it.
 */
export function useSearchView(source: RegsSource, query: string,
                              results: { waters: readonly NameHit[];
                                         places: readonly PlaceHit[] },
                              pick: SearchPick):
    { focus: MapFocus | null; town: Async<TownView | null> } {
  const typed = query.trim().length >= 2;
  const subject = subjectOf(pick, {
    best: typed ? results.waters[0]?.item ?? null : null,
    typedTown: typed ? townToPreview(query, results.waters, results.places) : null,
  });
  const located = useLocated(source, subject.item);
  const townPlace = pick.town ?? subject.place;
  const town = useTown(source, townPlace);
  const best = located.state === "ready" ? located.value : null;
  const tv = town.state === "ready" ? town.value : null;
  const focus = useMemo(
    () => focusFor({
      town: subject.place && tv?.place.place === subject.place.place ? tv : null,
      // Only the located water that IS the current subject — a slow answer for the
      // previous keystroke must not light the wrong river.
      best: best && best.item === subject.item ? best : null,
      pin: subject.pin,
    }),
    [subject.place, subject.item, subject.pin, tv, best]);
  // A town list reads only ITS town's answer, never a stale one for another.
  const stale = pick.town !== null && tv !== null && tv.place.place !== pick.town.place;
  const mine: Async<TownView | null> = stale ? { state: "loading", value: null, error: null }
                                             : town;
  return { focus, town: mine };
}

/**
 * THE STATUS A RESULT ROW WEARS — a water in the search results or in the list of water near a
 * town. The same answer the map will colour its line by: `waterStatus` over the water's
 * regulations (`regulationsFor`), both in @app/core. Null today, because the app reads no
 * regulations yet — the row then draws no status at all, never a guessed one.
 */
export function rowStatus(item: ItemId): WaterStatus | null {
  return waterStatus(regulationsFor(item));
}
