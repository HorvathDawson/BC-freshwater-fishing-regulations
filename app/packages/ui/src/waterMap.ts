/**
 * THE WATER'S MAP VIEW (P15), headless: the parts of the water in mouth-first order, the part you are on, ▼ / ▲ over the
 * release's marks graph, the boundary marks, the mouth's exit, the camera's box and the section ids to light — all from
 * the release (`@app/data/regs` `loadWaterGroup`: `part_geo`, `part_mark`, `part_section`, `water_exit`), stepped by
 * `@app/core` waterNav. NAVIGATION, not a regulation: nothing here decides what is open or kept.
 *
 *   waterMapStep   the pure reducer: ▼, ▲ (a fork either way opens the chooser), choose, tap a part, tap a mark, ⤢
 *   waterMapView   the pure view model a component draws (every string finished here, AGENTS rule 33)
 *   useWaterMap    the store → group read (one keyed read per table per item), the start part, the state
 *
 * THE START PART is the regulations view's: the frame's part, else the part `targetFor` picks for the tapped section
 * (`useRegsTarget`, the same read the Regulations view makes) — so the map opens on the part the page was showing.
 * AND THE STEPPED PART IS THE FRAME'S (one water screen): every move to another part is handed to the caller
 * (`onPart`), which makes it the frame's part (the route's `part` action), so the Regulations and Conditions views of
 * the water are on the part the map is on — the map never holds a part the rest of the screen does not.
 *
 * ACROSS WATERS: ▲ on the top of a water that goes on from another (the Vedder, from the Chilliwack), or ▼ on the
 * mouth part of one that goes on as another, is not a step in this group — the view model says which water and part
 * (`up.cross` / `down.cross`), and the caller opens it (`crossAction`, Map in front) IN PLACE of this one: one system.
 *
 * NO GEOMETRY: the tiles draw the water; the release says which sections are which part (lit by feature-state through
 * the map adapter's one `highlight` call: `selected` on this part, `water` on the rest) and each part's box.
 */
import { useMemo, useRef, useState } from "react";
import {
  reportOnce, crossLabel, crossDown, crossUp, entryNames, entryPieces, exitLabel, groupBounds, mergeNav, itemNote, markA11y, markLabel, navSections, partAt, partBounds,
  partName, partOfSection, partRowAria, partWhere, stepsDown, stepsUp, TIDAL_SHEET_WORDS, WATER_MAP_WORDS, wholeAria, wholeLabel, type Bbox,
  type NavCross, type NavExit, type WaterGroup,
} from "@app/core";
import type { ItemId, SectionId } from "@app/data";
import { loadRawWaterGroup, loadTidalWater, type GroupStore, type RegsStore, type TidalWater } from "@app/data/regs";
import { useAsync, type Async } from "./async";
import { useFeedVersion } from "./feedVersion";
import { useFrameView } from "./frameView";
import { openWater } from "./openWater";
import { salmonKeyOf, type SalmonLoader } from "./salmonKey";
import type { RouteAction } from "./route";

// ── the state and its step ───────────────────────────────────────────────────────────────────────────────────────────

export interface WaterMapState {
  /** The part you are on: an index into the group's mouth-first `parts`. */
  readonly at: number;
  /** ⤢: the camera frames the whole water rather than the part. */
  readonly whole: boolean;
  /** The boundary mark that is open (its index in the group's `marks`), or null. */
  readonly mark: number | null;
  /** ▲ or ▼ at a fork: the chooser is open, for that way; null when it is not. */
  readonly choosing: "up" | "down" | null;
  /** (T150) The list of the water's parts under the map is open. Closed at the start and after a pick. */
  readonly list: boolean;
}

export type WaterMapAction =
  | { type: "down" }
  | { type: "up" }
  | { type: "choose"; to: number }
  | { type: "closeChooser" }
  | { type: "part"; ix: number }
  /** A tap on the map: the section id; a section of another water changes nothing here. */
  | { type: "section"; sid: number }
  | { type: "mark"; ix: number | null }
  | { type: "whole" }
  /** (T150) Open or close the list of parts. */
  | { type: "list" };

export const waterMapStart = (at: number, whole = false): WaterMapState => ({ at, whole, mark: null, choosing: null, list: false });

const go = (at: number): WaterMapState => waterMapStart(at);
const ways = (g: WaterGroup, at: number, dir: "up" | "down") => (dir === "up" ? stepsUp(g, at) : stepsDown(g, at));

/** One step of the water map. Pure: an end, a stray index or a tap off the water leaves the state as it was. */
export function waterMapStep(g: WaterGroup, s: WaterMapState, a: WaterMapAction): WaterMapState {
  const ok = (i: number) => Number.isInteger(i) && i >= 0 && i < g.parts.length;
  switch (a.type) {
    case "down":
    case "up": {
      const to = ways(g, s.at, a.type);
      if (!to.length) return s;
      // a fork (a side channel, a lake split, two parts below one): the reader picks which way, the main stem first
      return to.length > 1 ? { ...s, choosing: a.type, mark: null } : go(to[0]!);
    }
    case "choose": return s.choosing && ways(g, s.at, s.choosing).includes(a.to) ? go(a.to) : s;
    case "closeChooser": return s.choosing ? { ...s, choosing: null } : s;
    case "part": return ok(a.ix) ? go(a.ix) : s;
    case "section": {
      const i = partOfSection(g, a.sid);
      return i < 0 ? s : go(i);
    }
    case "mark":
      if (a.ix === null) return s.mark === null ? s : { ...s, mark: null };
      if (!g.marks[a.ix]) return s;
      return { ...s, mark: s.mark === a.ix ? null : a.ix, choosing: null };        // a second tap closes it
    case "whole": return { ...s, whole: !s.whole, mark: null, choosing: null };
    // a water of one part has no list to open
    case "list": return g.parts.length > 1 ? { ...s, list: !s.list, mark: null, choosing: null } : s;
  }
}

// ── the view model ────────────────────────────────────────────────────────────────────────────────────────────────────

/** A part as the controls name it. */
export interface WaterMapPartVM {
  readonly ix: number;
  readonly item: string;
  readonly partIx: number;
  /** Its short name where its water has several parts, else its water's name (the prototype's `partName`). */
  readonly name: string;
  /** Its water's name where that water has several parts in view, else "". */
  readonly note: string;
}

/** A boundary mark the map draws (only the marks the release places: a split lake's item boundaries have none). */
export interface WaterMapMarkVM {
  /** Its index in the group's marks: what a tap hands back. */
  readonly ix: number;
  readonly lon: number;
  readonly lat: number;
  /** Shown beside the ring at the current part's ends and on the open mark; else null (a ring alone). */
  readonly label: string | null;
  readonly a11y: string;
  readonly on: boolean;
}

export interface WaterMapStepVM {
  readonly label: string;
  readonly aria: string;
  readonly disabled: boolean;
  readonly fork: boolean;
  readonly cross: { readonly item: string; readonly part: number } | null;
}

export interface WaterMapVM {
  readonly parts: readonly WaterMapPartVM[];
  readonly current: WaterMapPartVM;
  /** "Part 2 of 4 · Chilliwack River". */
  readonly where: string;
  /** ▼ / ▲: the label, its words for a screen reader (`aria`), whether it is an end, whether it is a fork, and — where it
   *  steps into ANOTHER water (`water_continue`) — that water and its part, for the caller to open (`crossAction`). */
  readonly down: WaterMapStepVM;
  readonly up: WaterMapStepVM;
  /** ▲ or ▼ at a fork: which way, the main stem first. */
  readonly chooser: { readonly title: string; readonly close: string; readonly options: readonly WaterMapPartVM[] } | null;
  readonly marks: readonly WaterMapMarkVM[];
  /** The open mark: its words, the parts above it and the part below it. */
  readonly pop: {
    readonly title: string; readonly close: string;
    readonly upLabel: string; readonly up: readonly WaterMapPartVM[];
    readonly downLabel: string; readonly down: WaterMapPartVM;
  } | null;
  /** At the lowest part: the water its mouth flows into, opened at the part the mouth meets — or, `tidal` (T345), only
   *  said: the mouth meets tidal water, which no water map holds, so there is nothing to open. */
  readonly exit: { readonly label: string; readonly item: string; readonly part: number | null;
                   readonly tidal?: true } | null;
  /** (T214 item 4) The water's mouth flows into another water, at ANY part: the bar keeps a place for the line at every part, so it
   *  appearing at the mouth part (and going again one step up) never moves the steps or "Show regulations". */
  readonly exitHeld: boolean;
  readonly whole: { readonly label: string; readonly aria: string; readonly on: boolean };
  /** The camera: this part's box, or the whole water's under ⤢; `fitKey` changes when the camera should move. */
  readonly bounds: Bbox | null;
  readonly fitKey: string;
  /** Feature-state `selected`: exactly this part's `part_section` sids. */
  readonly selected: readonly number[];
  /** Feature-state `water`: the rest of the water's sids. */
  readonly water: readonly number[];
  /** "Show regulations for this part". */
  readonly regs: { readonly label: string; readonly item: string; readonly partIx: number };
  /**
   * (T150) THE PARTS OF THIS WATER, by name, under the map — the header's Parts sheet moved here (owner 2026-10-23):
   * tiny parts are hard to tap and a screen reader cannot tap a map. Every part in mouth-first order (a split lake's
   * areas too); `here` on the part shown. A pick is `{type: "part", ix}`. Null for a water of one part.
   */
  readonly list: {
    readonly title: string;
    /** The toggle's words: "Show all 15 parts" while closed, "Hide the list of parts" while open. */
    readonly toggle: string;
    readonly open: boolean;
    readonly rows: readonly (WaterMapPartVM & { readonly aria: string; readonly here: boolean })[];
  } | null;
}

/**
 * (T443, owner 2026-11-06 on 0.6.7: "the boundary names at Landstrom Bar overlap") TWO MARKS THAT SAY THE SAME THING, A
 * FEW STEPS APART, ARE ONE LABEL. A short zone inside a part (the Fraser's "Within Landstrom Bar sign zone", 138 m long) has
 * a mark at each end with the same words, and both were labelled: two copies of "Landstrom Bar sign zone boundary" over
 * each other. Within `MARK_LABEL_ONE_M` of a labelled mark with the same words, a mark keeps its ring (and its own
 * words for a screen reader, `a11y`) and draws no label; the open mark's label is the one kept. The release holds 12 such
 * pairs in 9 waters (r-67b456b5e64b, every pair under 200 m: 5–197 m). (T517) Since the release names an end by its
 * place ("…@km156.4" / "…@km156.5": "Landstrom Bar sign zone boundary (km 156.4)" / "(km 156.5)"), the two ends say
 * different words: "the same thing" is the same end TOKEN without its place (`placeOf`), never the words.
 */
const MARK_LABEL_ONE_M = 250;

const metresApart = (a: { lon: number; lat: number }, b: { lon: number; lat: number }): number => {
  const k = Math.PI / 180;
  const x = (b.lon - a.lon) * k * Math.cos(((a.lat + b.lat) / 2) * k), y = (b.lat - a.lat) * k;
  return Math.hypot(x, y) * 6_371_000;
};

/** A mark's end token without the place the release says it at (`<token>@km…` / `@near:…`): the same end. */
const placeOf = (token: string): string => token.replace(/@.*$/, "");

/** The marks with every label of the same end (`same`: each mark's end token without its place) within
 *  `MARK_LABEL_ONE_M` of a kept one dropped (the open mark's kept first). */
function oneLabelPerPlace(marks: readonly WaterMapMarkVM[], same: ReadonlyMap<number, string>): WaterMapMarkVM[] {
  const kept: WaterMapMarkVM[] = [];
  const order = [...marks].sort((a, b) => Number(b.on) - Number(a.on));
  const drop = new Set<number>();
  for (const m of order) {
    if (m.label === null) continue;
    if (kept.some((k) => same.get(k.ix) === same.get(m.ix) && metresApart(k, m) < MARK_LABEL_ONE_M)) drop.add(m.ix);
    else kept.push(m);
  }
  return marks.map((m) => (drop.has(m.ix) ? { ...m, label: null } : m));
}

const partVM = (g: WaterGroup, i: number): WaterMapPartVM => {
  const p = g.parts[i]!;
  return { ix: i, item: p.item, partIx: p.partIx, name: partName(g, i), note: itemNote(g, i) };
};

/** The view model for state `s` (null for a group with no parts: nothing to show). */
export function waterMapView(g: WaterGroup, s: WaterMapState): WaterMapVM | null {
  if (!g.parts.length) return null;
  // the state's part is one the group holds (`waterMapStep` never leaves it, `waterMapStartIx` throws first): never clamped
  if (!Number.isInteger(s.at) || s.at < 0 || s.at >= g.parts.length)
    throw new Error(`water map: part ${s.at} of a group of ${g.parts.length}`);
  const at = s.at;
  const W = WATER_MAP_WORDS;
  const downs = stepsDown(g, at), ups = stepsUp(g, at);
  const step = (dir: "up" | "down", to: readonly number[], cross: NavCross | null): WaterMapStepVM => {
    const fwd = dir === "up" ? W.up : W.down, end = dir === "up" ? W.highest : W.lowest;
    const label = cross ? crossLabel(dir, cross) : to.length ? fwd : end;
    return { label, aria: label, disabled: !to.length && !cross, fork: to.length > 1,
             cross: cross ? { item: cross.item, part: cross.part } : null };
  };
  const choice = s.choosing === null ? [] : s.choosing === "up" ? ups : downs;
  const current = partVM(g, at);
  const { selected, water } = navSections(g, at);
  const open = s.mark === null ? null : g.marks[s.mark];
  if (open === undefined) throw new Error(`water map: mark ${s.mark} of ${g.marks.length}`);
  const oneWater = new Set(g.parts.map((p) => p.item)).size === 1;
  const marks: WaterMapMarkVM[] = [];
  g.marks.forEach((m, ix) => {
    if (m.lon === null || m.lat === null) return;            // a boundary with no place: in the graph, not drawn
    const named = m.down === at || m.up.includes(at) || ix === s.mark;
    marks.push({ ix, lon: m.lon, lat: m.lat, label: named ? markLabel(g, m) : null, a11y: markA11y(g, m),
                 on: ix === s.mark });
  });
  return {
    parts: g.parts.map((_, i) => partVM(g, i)),
    current,
    where: partWhere(g, at),
    down: step("down", downs, crossDown(g, at)),
    up: step("up", ups, crossUp(g, at)),
    chooser: choice.length > 1
      ? { title: s.choosing === "down" ? W.chooserDown : W.chooser, close: W.close,
          options: choice.map((i) => partVM(g, i)) } : null,
    marks: oneLabelPerPlace(marks, new Map(g.marks.flatMap((m, ix) => (m.token === null ? [] : [[ix, placeOf(m.token)] as const])))),
    // a mark with no named place (no end token) is titled "Boundary" — the words it has, nothing composed
    pop: open ? { title: markLabel(g, open) ?? W.boundary, close: W.close,
                  upLabel: W.upstreamSide, up: open.up.map((i) => partVM(g, i)),
                  downLabel: W.downstreamSide, down: partVM(g, open.down) } : null,
    exit: !downs.length && at === 0 && g.exit
      ? { label: exitLabel(g.exit), item: g.exit.item, part: g.exit.part, ...(g.exit.tidal ? { tidal: true as const } : {}) }
      : null,
    exitHeld: !!g.exit,
    whole: { label: wholeLabel(g, at, s.whole), aria: wholeAria(g, at, s.whole), on: s.whole },
    bounds: s.whole ? groupBounds(g) : partBounds(g, at),
    fitKey: s.whole ? `${g.parts[0]!.item}:whole` : `${current.item}:${g.parts[at]!.pieces.join("+")}`,
    selected, water,
    regs: { label: W.regs, item: current.item, partIx: current.partIx },
    list: g.parts.length > 1
      ? { title: W.parts, toggle: s.list ? W.partsHide : W.partsShow(g.parts.length), open: s.list,
          // a row names its water only where the list holds more than one (a split lake's areas): one river's name
          // under every row says nothing the title does not
          rows: g.parts.map((_, i) => ({ ...partVM(g, i), ...(oneWater ? { note: "" } : {}), aria: partRowAria(g, i),
                                         here: i === at })) }
      : null,
  };
}

// ── leaving the map: the route actions ──────────────────────────────────────────────────────────────────────────────────

/**
 * "Show regulations for this part": the Regulations view of the water on screen at that part. A split lake's other
 * closure area is the SAME water (owner 2026-10-22 late: a move within a water is no layer), so the frame moves there
 * exactly as a step does (`partAction`) — back leaves the water, it does not return to this map.
 */
export function showRegsActions(onScreen: ItemId, item: ItemId, partIx: number): readonly RouteAction[] {
  return [partAction(onScreen, item, partIx), { type: "view", view: "regs" }];
}

/**
 * "Flows into …": the receiving water, opened on top at the part the mouth meets (the one `openWater` path). `group`
 * (T58): its split-lake areas, when the opener read them — a lake already held in another area is gone back to.
 */
export function exitAction(item: ItemId, part: number | null, group?: readonly ItemId[]): RouteAction {
  return openWater({ item, part, view: "regs", group });
}

/** ▲ / ▼ into the water this one goes on from or as (`water_continue`): that water, its Map in front, on the part it
 *  meets this one. A river that changes its name is ONE system (owner 2026-10-22 late), so it REPLACES the water on
 *  screen (the route's `cross`) — to and fro never stacks, and one back leaves the system for where it was opened
 *  from. "Flows into" (`exitAction`) from a tributary stays a layer. */
export function crossAction(item: ItemId, part: number): RouteAction {
  return { type: "cross", item, part };
}

/**
 * A part stepped to on the map, as the frame's part (one water screen): this water's part, or — a split lake's other
 * closure area — that area, which then becomes the frame's water.
 */
export function partAction(onScreen: ItemId, item: ItemId, partIx: number): RouteAction {
  return item === onScreen ? { type: "part", part: partIx } : { type: "part", part: partIx, item };
}

// ── the hook ───────────────────────────────────────────────────────────────────────────────────────────────────────────

const ids = new WeakMap<object, number>();
let nextId = 0;
const idOf = (o: object): number => { let i = ids.get(o); if (i === undefined) ids.set(o, i = ++nextId); return i; };

/** The group of `item`, read once per (store, item, salmon key). (T228) `salmonKey`: each section's salmon reading
 *  (`@app/core` `salmonTimelineKey`, the live feed over the release) — pieces of a part join only where it reads them alike. */
export function useWaterGroup(store: GroupStore | null, item: ItemId | null,
                              salmonKey: ((sid: number) => string) | null = null): Async<WaterGroup | null> {
  const { raw, g } = useWaterGroups(store, item, salmonKey);
  return raw.state === "ready" ? { state: "ready", value: g, error: null } : { ...raw, value: null };
}

/** The group as the release cuts it (`raw`: one entry per part) and as the map joins it (`g`, the live salmon key's). */
function useWaterGroups(store: GroupStore | null, item: ItemId | null, salmonKey: ((sid: number) => string) | null) {
  const key = `water-group:${store ? idOf(store) : ""}:${item ?? ""}`;
  const read = useAsync(async () => (store && item ? { key, g: await loadRawWaterGroup(store, item) } : null), key,
    !!store && !!item);
  // (T444) THE ANSWER IS THIS WATER'S OR NONE: for the one render after the key changes `useAsync` still holds the last
  // key's answer, and the Chilliwack's group met the Vedder's part in `waterMapStartIx` ("part 0 of gnis:3062 is not in
  // its group"). An answer to another question is loading here; no water, no group.
  const raw: Async<WaterGroup | null> = !store || !item ? { state: "ready", value: null, error: null }
    : read.state !== "ready" ? read
      : read.value?.key === key ? { state: "ready", value: read.value.g, error: null }
        : { state: "loading", value: null, error: null };
  // the salmon grouping is joined in memory: a feed change re-groups without a read (or a loading state)
  const g = useMemo(() => (raw.value ? mergeNav(raw.value, salmonKey) : null), [raw.value, salmonKey]);
  return { raw, g };
}

/**
 * (T228) `salmonKeyOf` (`salmonKey.ts`, the live salmon key) as a hook, for `useWaterGroup` / `useWaterMap`: asked again, in place, when a DFO feed is taken.
 * Null until read, or where the release ships no salmon — nothing then joins.
 */
export function useSalmonKey(loader: SalmonLoader | null, store: Pick<RegsStore, "itemOrd" | "parts"> | null,
                             item: ItemId | null): ((sid: number) => string) | null {
  const key = `water-salmon:${loader ? idOf(loader) : ""}:${store ? idOf(store) : ""}:${item ?? ""}`;
  const dfo = useFeedVersion("dfo");   // (owner 2026-10-29) a DFO feed taken: compared again, in place
  const r = useAsync(async () => (loader && store && item ? salmonKeyOf(loader, store, item) : null),
    `${key}#${dfo}`, !!loader && !!store && !!item, key);
  return r.state === "ready" ? r.value : null;
}

/** The map's entries of a water, as the other views read them (`useMapEntries`). */
export interface MapEntries {
  /** each part → the name of the entry holding it (`entryNames`): the header's, the Parts sheet's, the place line's */
  readonly names: ReadonlyMap<number, string>;
  /** each part → the pieces of the entry holding it (`entryPieces`): a page's in-season coverage */
  readonly pieces: ReadonlyMap<number, readonly number[]>;
}

/**
 * (T517) THE PARTS OF `item` AS ITS MAP SHOWS THEM — the very group the map draws (`useWaterGroup` over the live
 * `useSalmonKey`): each part's entry name (the Regulations header, the Parts sheet, the Conditions view's place label)
 * and its entry's pieces (the page's in-season coverage), so no screen names or covers a part another way. Null until
 * the group is read (a failed read is the group's, reported by its `useAsync`): `headerOf` then says each part's own
 * name and the page covers its own part — the map's own entries before a salmon reading joins any.
 */
export function useMapEntries(loader: SalmonLoader | null, store: RegsStore | null, item: ItemId | null): MapEntries | null {
  const salmonKey = useSalmonKey(loader, store, item);
  const g = useWaterGroup(store, item, salmonKey);
  return useMemo(() => (g.value && item ? { names: entryNames(g.value, item), pieces: entryPieces(g.value, item) } : null),
                 [g.value, item]);
}

/**
 * Where the map starts in group `g`: the entry holding the frame's part of `item` (`partAt`: a joined entry holds every
 * piece), else — no part on the frame (a water opened whole, before its part is read) — the mouth, a documented rule
 * (waterMap.start.test.ts). A pure function of the frame and the group: the ONE way the map finds its place, whatever
 * way the frame was reached (owner 2026-11-03). A part the group does not hold THROWS: the map says it is unavailable and
 * the failure is reported (`useWaterMap`) — it never opens somewhere else.
 */
export function waterMapStartIx(g: WaterGroup, item: string, part: number | null): number {
  if (part === null) return 0;
  const i = partAt(g, item, part);
  if (i < 0) throw new Error(`water map: part ${part} of ${item} is not in its group`);
  return i;
}

/**
 * (T688, one load path) A WHOLLY TIDAL WATER'S MAP: it lists no part (T278), so `useWaterMap` has no view model and the
 * map had nothing to frame — it kept the camera of the water before and said "The parts of this water are not available",
 * a failure that was not one. The map frames the water itself (the union of its tidal pieces' boxes) and the bar says
 * what is true: a tidal water, with the Regulations view (the tidal card) one press away. The same whichever way the water
 * was opened (a search Open has no part, a tap or an address names the tidal piece).
 */
export interface WaterMapTidalVM {
  readonly name: string;
  readonly title: string;
  readonly body: string;
  readonly bounds: Bbox | null;
  readonly fitKey: string;
  readonly sids: readonly number[];
  readonly regs: { readonly label: string; readonly item: string; readonly partIx: number };
}

/** The tidal water's bar and frame (pure). */
export function waterMapTidalView(t: TidalWater): WaterMapTidalVM {
  return { name: t.name, title: WATER_MAP_WORDS.tidal, body: TIDAL_SHEET_WORDS.body[0], bounds: t.bounds,
           fitKey: `${t.item}:tidal`, sids: t.sids,
           regs: { label: WATER_MAP_WORDS.tidalRegs, item: t.item, partIx: t.partIx } };
}

/**
 * The water map's tidal case: asked only when `wanted` (the group is read and lists no part). Loading while it is read;
 * null: the water is not wholly tidal (so the map really has nothing to show, and says so).
 */
export function useTidalWaterMap(store: RegsStore | null, item: ItemId | null, wanted: boolean): Async<WaterMapTidalVM | null> {
  const key = `water-tidal:${store ? idOf(store) : ""}:${item ?? ""}`;
  const r = useAsync(async () => {
    const t = store && item ? await loadTidalWater(store, item) : null;
    return { key, vm: t ? waterMapTidalView(t) : null };
  }, key, wanted && !!store && !!item);
  if (!wanted || !store || !item) return { state: "ready", value: null, error: null };
  if (r.state !== "ready") return r.state === "failed" ? r : { state: "loading", value: null, error: null };
  return r.value.key === key ? { state: "ready", value: r.value.vm, error: null } : { state: "loading", value: null, error: null };
}

export interface WaterMap {
  readonly vm: WaterMapVM;
  readonly act: (a: WaterMapAction) => void;
  /** (T459) the sections of the water this one flows into, at the part its mouth meets (`useReceivingSections`) */
  readonly receiving: readonly number[];
}

/**
 * (T459, owner 2026-11-06: "all other rivers should be greyed out I think, except downstream maybe") THE WATER THIS ONE
 * FLOWS INTO, kept in colour (muted) while every other water on the water map is grey: the sections of the receiving
 * water's part where the mouth meets it (`g.exit`: `water_exit`, a part of its own map). Nothing for a mouth on tidal
 * water (no map lists it) or a water with no exit. A read that fails, or a part the receiving water's group does not
 * hold, is reported; the map then draws the receiving water grey like the rest — what it shows is still true.
 */
function useReceivingSections(store: RegsStore | null, exit: NavExit | null, salmon: SalmonLoader | null): readonly number[] {
  const part = exit && !exit.tidal ? exit.part : null;
  const item = part !== null ? exit!.item as ItemId : null;
  // the receiving water's entries as ITS map joins them: its own live salmon key (one structure, every screen)
  const salmonKey = useSalmonKey(salmon, store, item);
  const { g } = useWaterGroups(store, item, salmonKey);
  const key = `receiving:${store ? idOf(store) : ""}:${item ?? ""}:${part ?? ""}`;
  // a read that failed is reported by its own `useAsync` (the one place a hook's failure is reported): never twice
  if (!g || item === null) return NO_SIDS;
  const i = partAt(g, item, part!);
  if (i < 0) { reportOnce(key, new Error(`water map: the receiving water ${item} has no part ${part}`)); return NO_SIDS; }
  return navSections(g, i).selected;
}
const NO_SIDS: readonly number[] = [];

/**
 * The water map of `item` on the regs store: `part` the frame's part (null: the part the Regulations view opens for
 * `tapSid`, through the same `useRegsTarget`). Loading while the group or the start part is read; ready(null) for a
 * water with no parts. `onPart`: every step to another part, handed to the caller to make it the frame's part
 * (`partAction`) — the map then starts there again, so nothing it shows disagrees with the frame.
 *
 * A frame moved to a SIBLING of the group (a split lake's other closure area) keeps the group already read: it is the
 * same water, so the map does not go back to loading.
 */
export function useWaterMap(store: RegsStore | null, item: ItemId | null, part: number | null,
                            tapSid: SectionId | null,
                            onPart?: (item: ItemId, partIx: number) => void,
                            /** (T203, T369 D7) The frame's ⤢: framed on the whole water (on the route, never held here). */
                            whole = false,
                            /** (T228, T517) each section's LIVE salmon reading (`useSalmonKey`): parts of one run join
                             *  only where it reads them alike; null: nothing joins */
                            salmonKey: ((sid: number) => string) | null = null,
                            /** (T369 D7) ⤢ pressed: the caller toggles the frame's `whole` (the route's `whole`) */
                            onWhole?: () => void,
                            /** (T459, T517) the salmon slices, for the receiving water's own live key (its map's entries) */
                            salmon: SalmonLoader | null = null): Async<WaterMap | null> {
  const read = useRef<{ item: ItemId; g: WaterGroup } | null>(null);
  const inRead = !!item && !!read.current && read.current.g.parts.some((p) => p.item === item);
  const { raw: rawGroup, g: merged } = useWaterGroups(store, inRead ? read.current!.item : item, salmonKey);
  const group: Async<WaterGroup | null> = rawGroup.state === "ready" ? { state: "ready", value: merged, error: null }
    : { ...rawGroup, value: null };
  if (group.state === "ready" && group.value && item && !inRead) read.current = { item, g: group.value };
  // (T369) THE FRAME'S PART, from the one reading of a frame (`frameView.ts`): the part the Regulations view opens
  const fv = useFrameView(store, { item, part, tapSid, at: null });
  const startPart = fv.value?.part ?? null;
  const waiting = fv.state === "loading" && store !== null;
  const g = group.value;
  // (T228) a new salmon grouping re-indexes the pieces: the walk starts again on the frame's part. (T508) Keyed on the
  // GROUPING ITSELF (each entry's pieces and sections), never on the salmon reading's identity: a DFO feed taken
  // (`useFeedVersion("dfo")`) hands a new reading that most often groups the pieces exactly as before, and keyed on its
  // identity that reset an open mark, the chooser and the parts list under the reader's finger
  const grouping = useMemo(() => (g ? g.parts.map((p) => `${p.item}/${p.pieces.join("+")}/${p.sids.length}:${p.sids[0] ?? ""}`).join(",") : ""), [g]);
  const key = `${group.state}:${item ?? ""}:${startPart ?? ""}:${grouping}`;
  // THIS FRAME's visit: a new number each time the key changes, so what was held on an EARLIER visit to the same frame
  // (A → B → back to A by Show regulations and back) is never taken for this one's
  const visit = useRef({ key, n: 0 });
  if (visit.current.key !== key) visit.current = { key, n: visit.current.n + 1 };
  const at = visit.current.n;
  const [held, setHeld] = useState<{ at: number; s: WaterMapState } | null>(null);
  // the state as the last step left it, ahead of the render, so two taps before a render are two steps
  const latest = useRef<{ at: number; s: WaterMapState } | null>(null);
  // THE PLACE IS THE FRAME'S (owner 2026-11-03, one load path): the part comes from the frame alone
  // (`waterMapStartIx`); what is held here is only what the reader opened on THIS frame (a mark, the chooser, the list,
  // ⤢), dropped the moment the frame changes
  let startFailed: Error | null = null;
  let heldS: WaterMapState | null = null;
  // (T688) a group with no part (a wholly tidal water: every piece is tidal and never listed) has no part to start on —
  // not a failure: `useTidalWaterMap` frames it
  try { heldS = held && held.at === at ? held.s : g && item && g.parts.length ? waterMapStart(waterMapStartIx(g, item, startPart)) : null; }
  // (no silent fallbacks) a frame on a part its group does not hold: the map is FAILED (shown unavailable) and reported
  catch (e) { startFailed = e instanceof Error ? e : new Error(String(e)); reportOnce(`waterMapStart:${startFailed.message}`, e); }
  // (T369 D7) ⤢ is the FRAME's (`whole`), never held: a view switch, back and a reload keep it
  const s = useMemo(() => (heldS ? { ...heldS, whole } : null), [heldS, whole]);
  const vm = useMemo(() => (g && s ? waterMapView(g, s) : null), [g, s]);
  const receiving = useReceivingSections(store, g?.exit ?? null, salmon);
  // (T369 D3) no part is ADOPTED here any more: a tapped water opens at its part (the opener's `partOf`), and a frame
  // opened part-less reads its part from the same `targetFor` the Regulations view reads (`useFrameView`)
  // no store (no release, or not open yet: the caller says which) or no water: nothing to walk
  if (store === null || item === null) return { state: "ready", value: null, error: null };
  if (group.state === "failed") return group;
  if (startFailed) return { state: "failed", value: null, error: startFailed };
  if (group.state === "loading" || waiting) return { state: "loading", value: null, error: null };
  if (!g || !s || !vm) return { state: "ready", value: null, error: null };
  const act = (a: WaterMapAction) => {
    if (a.type === "whole") { onWhole?.(); return; }
    const from = latest.current && latest.current.at === at ? { ...latest.current.s, whole } : s;
    const next = waterMapStep(g, from, a);
    if (next === from) return;
    latest.current = { at, s: next };
    setHeld(latest.current);
    // every move to another part is the frame's (T345): the frame is the one place the map is on. A
    // part picked while the whole water is framed is a move too, onto the part the map already stood on (the frame's
    // ⤢ is dropped by it: a search Open frames the whole water at its first part, and the list's first row was a no-op)
    const a1 = g.parts[next.at]!;
    if (onPart && (next.at !== from.at || (from.whole && !next.whole))) onPart(a1.item as ItemId, a1.partIx);
  };
  return { state: "ready", value: { vm, act, receiving }, error: null };
}
