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
  crossLabel, crossDown, crossUp, exitLabel, groupBounds, mergeNav, itemNote, markA11y, markLabel, navSections, partAt, partBounds,
  partName, partOfSection, partRowAria, partWhere, stepsDown, stepsUp, WATER_MAP_WORDS, wholeAria, wholeLabel, type Bbox,
  type NavCross, salmonTimelineKey, type WaterGroup,
} from "@app/core";
import type { ItemId, SectionId } from "@app/data";
import { loadRawWaterGroup, type GroupStore, type RegsStore } from "@app/data/regs";
import type { ReleaseLoader } from "@app/data/release";
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
  /** At the lowest part: the water its mouth flows into, opened at the part the mouth meets. */
  readonly exit: { readonly label: string; readonly item: string; readonly part: number | null } | null;
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
    marks,
    pop: open ? { title: markLabel(g, open), close: W.close,
                  upLabel: W.upstreamSide, up: open.up.map((i) => partVM(g, i)),
                  downLabel: W.downstreamSide, down: partVM(g, open.down) } : null,
    exit: !downs.length && at === 0 && g.exit
      ? { label: exitLabel(g.exit), item: g.exit.item, part: g.exit.part } : null,
    exitHeld: !!g.exit,
    whole: { label: wholeLabel(g, at, s.whole), aria: wholeAria(g, at, s.whole), on: s.whole },
    bounds: s.whole ? groupBounds(g) : partBounds(g, at),
    fitKey: s.whole ? `${g.parts[0]!.item}:whole` : `${current.item}:${current.partIx}${g.parts[at]!.stretch ? `:${g.parts[at]!.stretch}` : ""}`,
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
  const key = `water-group:${store ? idOf(store) : ""}:${item ?? ""}`;
  const raw = useAsync(async () => (store && item ? loadRawWaterGroup(store, item) : null), key, !!store && !!item);
  // the salmon grouping is joined in memory: a feed change re-groups without a read (or a loading state)
  const g = useMemo(() => (raw.value ? mergeNav(raw.value, salmonKey) : null), [raw.value, salmonKey]);
  return raw.state === "ready" ? { state: "ready", value: g, error: null } : raw;
}

/**
 * (T228) The salmon reading of water `item`'s sections, for `useWaterGroup`: the release's salmon slices of the regions
 * its parts' answers are in (as the Salmon card reads them), through the loader (live feed included). Null until read,
 * or where the release ships no salmon — the pieces then join as the part.
 */
export function useSalmonKey(loader: Pick<ReleaseLoader, "salmon" | "regionsOf"> | null,
                             store: Pick<RegsStore, "itemOrd" | "parts"> | null,
                             item: ItemId | null): ((sid: number) => string) | null {
  const key = `water-salmon:${loader ? idOf(loader) : ""}:${store ? idOf(store) : ""}:${item ?? ""}`;
  const r = useAsync(async () => {
    if (!loader || !store || !item) return null;
    const ord = await store.itemOrd(item);
    const akeys = ord === null ? [] : [...new Set((await store.parts(ord)).map((p) => p.akey))]
      .filter((a): a is number => a !== null);
    const regions = [...new Set((await Promise.all(akeys.map((a) => loader.regionsOf(a)))).flat())];
    const f = await loader.salmon(regions.length ? regions : undefined);
    // the ONE salmon comparison (`@app/core` `salmonTimelineKey`: the regulations' content, live feed included)
    return f ? (sid: number) => salmonTimelineKey(f, sid) : null;
  }, key, !!loader && !!store && !!item);
  return r.state === "ready" ? r.value : null;
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
                            onPart?: (item: ItemId, partIx: number) => void,
                            /** (T203) Start framed on the whole water (a search result's Map). */
                            wholeFirst = false,
                            /** (T228) each section's salmon reading (`useSalmonKey`): pieces of a part join only where
                             *  it reads them alike; null: joined as the part */
                            salmonKey: ((sid: number) => string) | null = null): Async<WaterMap | null> {
  const read = useRef<{ item: ItemId; g: WaterGroup } | null>(null);
  const inRead = !!item && !!read.current && read.current.g.parts.some((p) => p.item === item);
  const group = useWaterGroup(store, inRead ? read.current!.item : item, salmonKey);
  if (group.state === "ready" && group.value && item && !inRead) read.current = { item, g: group.value };
  const target = useRegsTarget(part === null ? store : null, item, tapSid);
  const startPart = part ?? (target.value?.kind === "part" ? target.value.partIx : null);
  const waiting = part === null && target.state === "loading" && store !== null;
  const g = group.value;
  // (T228) a new salmon grouping re-indexes the pieces: the walk starts again on the frame's part
  const key = `${group.state}:${item ?? ""}:${startPart ?? ""}:${wholeFirst ? "w" : ""}:${salmonKey ? idOf(salmonKey) : ""}`;
  const [held, setHeld] = useState<{ key: string; s: WaterMapState } | null>(null);
  // the state as the last step left it, ahead of the render, so two taps before a render are two steps
  const latest = useRef<{ key: string; s: WaterMapState } | null>(null);
  // (T204) the stretch a step landed on, so the frame's new part reopens there, not on the part's first stretch
  const stepped = useRef<{ item: string; partIx: number; at: number } | null>(null);
  const startAt = (gg: WaterGroup, it: string): number => {
    const st = stepped.current, p = st ? gg.parts[st.at] : undefined;
    return st && p && st.item === it && st.partIx === startPart && p.item === it && p.partIx === startPart
      ? st.at : waterMapStartIx(gg, it, startPart);
  };
  const s = held && held.key === key ? held.s : g && item ? waterMapStart(startAt(g, item), wholeFirst) : null;
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
    // (T204) a step between two stretches of ONE part is no new part for the frame: the map keeps it
    const a0 = g.parts[from.at]!, a1 = g.parts[next.at]!;
    if (onPart && next.at !== from.at && (a0.item !== a1.item || a0.partIx !== a1.partIx)) {
      stepped.current = { item: a1.item, partIx: a1.partIx, at: next.at };
      onPart(a1.item as ItemId, a1.partIx);
    }
  };
  return { state: "ready", value: { vm, act }, error: null };
}
