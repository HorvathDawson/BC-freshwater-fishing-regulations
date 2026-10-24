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
import { useMemo, useRef } from "react";
import {
  NEAR_KM, SEARCH_BAR, duplicateNames, extentOf, searchKey, rankNear, seeLine, townBox, waterCaption, waterStatusOn,
  watersNear,
  type Bbox, type Extent, type LatLon, type StatusIndex, type WaterStatus,
} from "@app/core";
import type { ItemId, NameHit, NearHit, PlaceHit, PointerHit, WaterSource, SectionId } from "@app/data";
import { useAsync, type Async } from "./async";
import { useSearch, type SearchResults } from "./hooks";

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
   * True only for a water placed by evidence (gauges, towns — `fixOf`); false for a water placed by its own
   * outline (`extentOf`, every water in a release) and for a town, whose boxes are already exactly what is wanted.
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
      caption: watersNear(town.near.length, p.name),
    };
  }
  if (best) {
    return {
      key: `item:${best.item}`,
      bbox: best.extent?.bbox ?? null,
      highlight: best.sections,
      // the water's own outline is already its shape; only evidence (gauges, towns) is refined against the tiles
      refine: best.extent?.basis !== "outline",
      marker: pin ? { lat: pin.lat, lon: pin.lon } : null,
      caption: waterCaption(best.name, best.extent !== null),
    };
  }
  return null;
}

/** The best match's sections and extent. Idle (loading forever) when there is none. */
export function useLocated(source: WaterSource, item: ItemId | null): Async<Located | null> {
  return useAsync(
    async () => {
      if (!item) return null;
      // THE WATER'S OWN SHAPE (its parts' boxes) places it; the gauge-and-town evidence is asked only where the
      // source holds no geometry — it used to be all there was, and a lake with neither (Kooryet) went unplaced.
      const [water, box] = await Promise.all([source.water(item), source.waterBox(item)]);
      if (!water) return null;
      const extent = extentOf(box, box ? null : await source.locate(item));
      return { item, name: water.name, sections: water.sections, extent };
    },
    `located:${item}`,
    item !== null,
  );
}

/** What is near a town, with every nearby water's sections for the highlight. */
export function useTown(source: WaterSource, place: PlaceHit | null): Async<TownView | null> {
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
export function usePickFocus(source: WaterSource, item: ItemId | null): MapFocus | null {
  const located = useLocated(source, item);
  const best = located.state === "ready" ? located.value : null;
  return useMemo(() => focusFor({ town: null,
                                  best: best && best.item === item ? best : null }),
                 [best, item]);
}

/* ────────────────────────────────────────────────────────────────────────────────────────
 * LOOK, THEN GO.
 *
 * A result row answers "where is that?" and nothing else: (T48) tapping a WATER frames it on the main map behind the
 * results sheet and lights it; tapping a PLACE shows it at once — the map flies to it, its pin drops, the waters near it
 * light and its "Water near" list opens (owner 2026-10-21: no eye for places). The reader stays in search, free to tap
 * the next row and compare. LEAVING is a separate, deliberate act: the selected water row's open action, which opens
 * the water screen through `openWater` (the one opener; back returns to these results as they were).
 *
 * Kept pure here so "a tap never leaves" is a test, not a hope.
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
  /** The town whose "water near" list is open — a place row tapped (T48). */
  town: PlaceHit | null;
}

export const NO_PICK: SearchPick = { selected: null, town: null };

/**
 * WHERE THE READER WAS IN SEARCH: what they typed and what they had selected. Held by the Shell, not the sheet, so a
 * water opened from the results and back returns to the same query, the same results and the same selected row, with
 * the main map on the same water.
 */
export interface SearchMemory { q: string; pick: SearchPick }
export const NO_SEARCH: SearchMemory = { q: "", pick: NO_PICK };

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
 * The first tap on a water row selects it — the main map frames and lights it — and never navigates; tapping it again
 * keeps it selected; tapping another row moves the selection. Only the selected water row carries the open action, and
 * only `go` on the SELECTED row acts: a go aimed at any other water (a stale press, a second pointer) selects it instead.
 * `leave` is the water to open, set only by going on a water. (T48) A PLACE needs no second step: selecting it (or
 * going on it) opens the town's list at once, with every water near it lit.
 */
export function searchStep(s: SearchPick, a: SearchAction):
    { pick: SearchPick; leave: ItemId | null } {
  switch (a.t) {
    case "typed":
    case "back":
      return { pick: NO_PICK, leave: null };
    case "select":
      if (a.target.kind === "place") return { pick: { selected: null, town: a.target.place }, leave: null };
      return { pick: { ...s, selected: a.target }, leave: null };
    case "go":
      if (a.target.kind === "place") return { pick: { selected: null, town: a.target.place }, leave: null };
      if (!isSelected(s, a.target)) return { pick: { ...s, selected: a.target }, leave: null };
      return { pick: s, leave: a.target.item };
  }
}

/**
 * WHAT THE PINNED MAP IS ABOUT, in order: the row the reader tapped; else the town whose
 * list is open. A water looked at from inside a town's list keeps that town pinned. (T48) The main map behind a full
 * sheet follows only what was PICKED, never the typing: moving it under the typing is unseen work (T81 / Q17 dropped
 * the "follow the best match" branch, which nothing asked for).
 */
export function subjectOf(pick: SearchPick):
    { item: ItemId | null; place: PlaceHit | null; pin: PlaceHit | null } {
  const p = pick.selected;
  if (p?.kind === "water") return { item: p.item, place: null, pin: pick.town };
  if (p?.kind === "place") return { item: null, place: p.place, pin: null };
  if (pick.town) return { item: null, place: pick.town, pin: null };
  return { item: null, place: null, pin: null };
}

/** One group of results, in the order they are drawn. */
export type ResultGroup =
  | { kind: "places"; title: string; note: string; places: readonly PlaceHit[] }
  /** Group names that only point, each with its line (`seeLine`) and the waters it opens. */
  | { kind: "see"; title: string; pointers: readonly { hit: PointerHit; line: string }[] }
  | { kind: "waters"; title: string;
      waters: readonly { hit: NameHit; dup: boolean }[] };

/**
 * PLACES FIRST, AND APART. A town is not a water — it is a question about the ground
 * around it — so it never sits in the same list as the rivers, and when the results hold
 * one it comes before them: a reader who typed a town's name meant the town. Each water
 * carries whether its name is shared with another in the list, which is what earns it a
 * "near Fernie".
 */
export function resultGroups(r: { waters: readonly NameHit[]; places: readonly PlaceHit[];
                                  pointers?: readonly PointerHit[] }):
    ResultGroup[] {
  const out: ResultGroup[] = [];
  if (r.places.length)
    out.push({ kind: "places", title: r.places.length === 1 ? "Place" : "Places",
               note: `water within ${NEAR_KM} km`, places: r.places });
  // A GROUP NAME THAT ONLY POINTS (owner 2026-10-22) comes before the waters: "Tee Pee Lakes" typed is a question the
  // book answers with "see Friday, Saturday and Sunday Lakes", and the three Tepee polygons below it are those waters.
  if (r.pointers?.length)
    out.push({ kind: "see", title: SEARCH_BAR.seeHead,
               pointers: r.pointers.map((hit) => ({ hit, line: seeLine(hit) })) });
  if (r.waters.length) {
    const dups = duplicateNames(r.waters);
    out.push({ kind: "waters",
               title: `${r.waters.length} named ${r.waters.length === 1 ? "water" : "waters"}`,
               waters: r.waters.map((hit) => ({ hit, dup: dups.has(searchKey(hit.name)) })) });
  }
  return out;
}

/**
 * The search screen's map and its town list, from what the reader has picked. ONE town fetch serves both the open
 * list and the map: inside a town's list the subject is that town or a water near it.
 */
export function useSearchView(source: WaterSource, pick: SearchPick):
    { focus: MapFocus | null; town: Async<TownView | null> } {
  const subject = subjectOf(pick);
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
 * (T48) HOW HIGH THE RESULTS SHEET STANDS over the main map: FULL while the reader is choosing (nothing picked — the
 * keyboard up, the map dimmed under it), HALF while they look (a water framed, a town's list open — the map clear above,
 * the list below). Typing again forgets the pick (`searchStep` "typed"), so the sheet rises again.
 */
export type SearchSheetSize = "full" | "half";
export function searchSheet(pick: SearchPick): SearchSheetSize {
  return pick.selected !== null || pick.town !== null ? "half" : "full";
}

/** Everything search draws, from what the reader typed and picked: the rows' answers, and what the main map shows. */
export interface SearchSession {
  /** The results the rows draw (the last answer held while the next loads, so neither the list nor the map flashes). */
  results: SearchResults;
  /** At least two letters typed. */
  typed: boolean;
  /** Typed, the first answer not in yet: the spinner. */
  busy: boolean;
  /** The search could not read the source. */
  failed: boolean;
  /** Typed, answered, nothing found. */
  none: boolean;
  /** What the main map frames, lights and pins (`focusFor`), or null: the map stays where the reader left it. */
  focus: MapFocus | null;
  /** The open town's list. */
  town: Async<TownView | null>;
  size: SearchSheetSize;
}

/**
 * (T48) ONE SEARCH SESSION, read by the Shell: the sheet's rows and the main map behind it come from this one value, so
 * the list and the map can never disagree about which water is lit. `open` false asks nothing (search is closed).
 */
export function useSearchSession(source: WaterSource, memory: SearchMemory, open: boolean): SearchSession {
  const { q, pick } = memory;
  const hits = useSearch(source, open ? q : "");
  const last = useRef<SearchResults>({ waters: [], places: [], pointers: [] });
  if (hits.state === "ready") last.current = hits.value;
  const typed = open && q.trim().length >= 2;
  const results = typed ? last.current : EMPTY_RESULTS;
  const { focus, town } = useSearchView(source, open ? pick : NO_PICK);
  return {
    results, typed, town, focus,
    busy: typed && hits.state === "loading" && !last.current.waters.length && !last.current.places.length
      && !last.current.pointers.length,
    failed: typed && hits.state === "failed",
    none: typed && hits.state === "ready" && !results.waters.length && !results.places.length
      && !results.pointers.length,
    size: searchSheet(pick),
  };
}

const EMPTY_RESULTS: SearchResults = { waters: [], places: [], pointers: [] };

/**
 * THE STATUS A RESULT ROW WEARS — a water in the search results or in the list of water near a
 * town, on `on` (today, by default). The same answer the map colours its line by: the status
 * index, read through `waterStatusOn` in @app/core. Null when there is no index (not loaded, or
 * refused for another atlas) — the row then draws no status at all, never a guessed one.
 */
export function rowStatus(item: ItemId, index: StatusIndex | null,
                          on: Date = new Date()): WaterStatus | null {
  return waterStatusOn(index, item, on);
}
