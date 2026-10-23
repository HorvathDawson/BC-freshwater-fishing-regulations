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
  crossLabel, crossDown, crossUp, exitLabel, groupBounds, itemNote, markA11y, markLabel, navSections, partAt, partBounds,
  partName, partOfSection, partWhere, stepsDown, stepsUp, WATER_MAP_WORDS, wholeAria, wholeLabel, type Bbox,
  type NavCross, type WaterGroup,
} from "@app/core";
import type { ItemId, SectionId } from "@app/data";
import { loadWaterGroup, type GroupStore, type RegsStore } from "@app/data/regs";
import { useAsync, type Async } from "./async";
import { useRegsTarget } from "./regs";
import { openWater } from "./openWater";
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
  | { type: "whole" };

export const waterMapStart = (at: number): WaterMapState => ({ at, whole: false, mark: null, choosing: null });

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
    case "choose": return ways(g, s.at, s.choosing ?? "up").includes(a.to) ? go(a.to) : s;
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
  /** At the lowest part: the water its mouth flows into, opened at the part the mouth meets. */
  readonly exit: { readonly label: string; readonly item: string; readonly part: number | null } | null;
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
}

const partVM = (g: WaterGroup, i: number): WaterMapPartVM => {
  const p = g.parts[i]!;
  return { ix: i, item: p.item, partIx: p.partIx, name: partName(g, i), note: itemNote(g, i) };
};

/** The view model for state `s` (null for a group with no parts: nothing to show). */
export function waterMapView(g: WaterGroup, s: WaterMapState): WaterMapVM | null {
  if (!g.parts.length) return null;
  const at = Math.min(Math.max(0, s.at), g.parts.length - 1);
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
  const open = s.mark !== null ? g.marks[s.mark] ?? null : null;
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
    marks,
    pop: open ? { title: markLabel(g, open), close: W.close,
                  upLabel: W.upstreamSide, up: open.up.map((i) => partVM(g, i)),
                  downLabel: W.downstreamSide, down: partVM(g, open.down) } : null,
    exit: !downs.length && at === 0 && g.exit
      ? { label: exitLabel(g.exit), item: g.exit.item, part: g.exit.part } : null,
    whole: { label: wholeLabel(g, at, s.whole), aria: wholeAria(g, at, s.whole), on: s.whole },
    bounds: s.whole ? groupBounds(g) : partBounds(g, at),
    fitKey: s.whole ? `${g.parts[0]!.item}:whole` : `${current.item}:${current.partIx}`,
    selected, water,
    regs: { label: W.regs, item: current.item, partIx: current.partIx },
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

/** The group of `item`, read once per (store, item). */
export function useWaterGroup(store: GroupStore | null, item: ItemId | null): Async<WaterGroup | null> {
  const key = `water-group:${store ? idOf(store) : ""}:${item ?? ""}`;
  return useAsync(async () => (store && item ? loadWaterGroup(store, item) : null), key, !!store && !!item);
}

/** Where the map starts in group `g`: the frame's part of `item`, else the regs view's target part, else the mouth. */
export function waterMapStartIx(g: WaterGroup, item: string, part: number | null): number {
  const i = part === null ? -1 : partAt(g, item, part);
  return i >= 0 ? i : 0;
}

export interface WaterMap {
  readonly vm: WaterMapVM;
  readonly act: (a: WaterMapAction) => void;
}

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
                            onPart?: (item: ItemId, partIx: number) => void): Async<WaterMap | null> {
  const read = useRef<{ item: ItemId; g: WaterGroup } | null>(null);
  const inRead = !!item && !!read.current && read.current.g.parts.some((p) => p.item === item);
  const group = useWaterGroup(store, inRead ? read.current!.item : item);
  if (group.state === "ready" && group.value && item && !inRead) read.current = { item, g: group.value };
  const target = useRegsTarget(part === null ? store : null, item, tapSid);
  const startPart = part ?? (target.value?.kind === "part" ? target.value.partIx : null);
  const waiting = part === null && target.state === "loading" && store !== null;
  const g = group.value;
  const key = `${group.state}:${item ?? ""}:${startPart ?? ""}`;
  const [held, setHeld] = useState<{ key: string; s: WaterMapState } | null>(null);
  // the state as the last step left it, ahead of the render, so two taps before a render are two steps
  const latest = useRef<{ key: string; s: WaterMapState } | null>(null);
  const s = held && held.key === key ? held.s : g && item ? waterMapStart(waterMapStartIx(g, item, startPart)) : null;
  const vm = useMemo(() => (g && s ? waterMapView(g, s) : null), [g, s]);
  // no store (no release, or not open yet: the caller says which) or no water: nothing to walk
  if (store === null || item === null) return { state: "ready", value: null, error: null };
  if (group.state === "failed") return group;
  if (group.state === "loading" || waiting) return { state: "loading", value: null, error: null };
  if (!g || !s || !vm) return { state: "ready", value: null, error: null };
  const act = (a: WaterMapAction) => {
    const from = latest.current && latest.current.key === key ? latest.current.s : s;
    const next = waterMapStep(g, from, a);
    if (next === from) return;
    latest.current = { key, s: next };
    setHeld(latest.current);
    if (onPart && next.at !== from.at) onPart(g.parts[next.at]!.item as ItemId, g.parts[next.at]!.partIx);
  };
  return { state: "ready", value: { vm, act }, error: null };
}
