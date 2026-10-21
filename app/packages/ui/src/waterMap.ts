/**
 * THE WATER'S MAP VIEW (P15), headless: the parts of the water in mouth-first order, the part you are on, ▼ / ▲ over the
 * release's marks graph, the boundary marks, the mouth's exit, the camera's box and the section ids to light — all from
 * the release (`@app/data/regs` `loadWaterGroup`: `part_geo`, `part_mark`, `part_section`, `water_exit`), stepped by
 * `@app/core` waterNav. NAVIGATION, not a regulation: nothing here decides what is open or kept.
 *
 *   waterMapStep   the pure reducer: ▼, ▲ (a fork opens the chooser), choose, tap a part, tap a mark, ⤢ whole
 *   waterMapView   the pure view model a component draws (every string finished here, AGENTS rule 33)
 *   useWaterMap    the store → group read (one keyed read per table per item), the start part, the state
 *
 * THE START PART is the regulations view's: the frame's part, else the part `targetFor` picks for the tapped section
 * (`useRegsTarget`, the same read the Regulations view makes) — so the map opens on the part the page was showing.
 *
 * NO GEOMETRY: the tiles draw the water; the release says which sections are which part (lit by feature-state through
 * the map adapter's one `highlight` call: `selected` on this part, `water` on the rest) and each part's box.
 */
import { useMemo, useState } from "react";
import {
  exitLabel, groupBounds, itemNote, markA11y, markLabel, navSections, partAt, partBounds, partName, partOfSection,
  partWhere, stepDown, stepsUp, WATER_MAP_WORDS, wholeAria, wholeLabel, type Bbox, type WaterGroup,
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
  /** ▲ at a fork: the chooser is open. */
  readonly choosing: boolean;
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

export const waterMapStart = (at: number): WaterMapState => ({ at, whole: false, mark: null, choosing: false });

const go = (at: number): WaterMapState => waterMapStart(at);

/** One step of the water map. Pure: an end, a stray index or a tap off the water leaves the state as it was. */
export function waterMapStep(g: WaterGroup, s: WaterMapState, a: WaterMapAction): WaterMapState {
  const ok = (i: number) => Number.isInteger(i) && i >= 0 && i < g.parts.length;
  switch (a.type) {
    case "down": {
      const to = stepDown(g, s.at);
      return to === null ? s : go(to);
    }
    case "up": {
      const ups = stepsUp(g, s.at);
      if (!ups.length) return s;
      // a fork (a side channel, a lake split): the reader picks which way, the main stem first
      return ups.length > 1 ? { ...s, choosing: true, mark: null } : go(ups[0]!);
    }
    case "choose": return stepsUp(g, s.at).includes(a.to) ? go(a.to) : s;
    case "closeChooser": return s.choosing ? { ...s, choosing: false } : s;
    case "part": return ok(a.ix) ? go(a.ix) : s;
    case "section": {
      const i = partOfSection(g, a.sid);
      return i < 0 ? s : go(i);
    }
    case "mark":
      if (a.ix === null) return s.mark === null ? s : { ...s, mark: null };
      if (!g.marks[a.ix]) return s;
      return { ...s, mark: s.mark === a.ix ? null : a.ix, choosing: false };       // a second tap closes it
    case "whole": return { ...s, whole: !s.whole, mark: null, choosing: false };
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

export interface WaterMapVM {
  readonly parts: readonly WaterMapPartVM[];
  readonly current: WaterMapPartVM;
  /** "Part 2 of 4 · Chilliwack River". */
  readonly where: string;
  readonly down: { readonly label: string; readonly disabled: boolean };
  readonly up: { readonly label: string; readonly disabled: boolean; readonly fork: boolean };
  /** ▲ at a fork: which way, the main stem first. */
  readonly chooser: { readonly title: string; readonly close: string; readonly closeGlyph: string; readonly options: readonly WaterMapPartVM[] } | null;
  readonly marks: readonly WaterMapMarkVM[];
  /** The open mark: its words, the parts above it and the part below it. */
  readonly pop: {
    readonly title: string; readonly close: string; readonly closeGlyph: string;
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
  const down = stepDown(g, at), ups = stepsUp(g, at);
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
    down: { label: down === null ? W.lowest : W.down, disabled: down === null },
    up: { label: ups.length ? W.up : W.highest, disabled: !ups.length, fork: ups.length > 1 },
    chooser: s.choosing && ups.length > 1
      ? { title: W.chooser, close: W.close, closeGlyph: W.closeGlyph, options: ups.map((i) => partVM(g, i)) } : null,
    marks,
    pop: open ? { title: markLabel(g, open), close: W.close, closeGlyph: W.closeGlyph,
                  upLabel: W.upstreamSide, up: open.up.map((i) => partVM(g, i)),
                  downLabel: W.downstreamSide, down: partVM(g, open.down) } : null,
    exit: down === null && at === 0 && g.exit
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
 * "Show regulations for this part": the Regulations view of the water on screen at that part; a split lake's other
 * closure area is its own water, opened on top (back returns to this map).
 */
export function showRegsActions(onScreen: ItemId, item: ItemId, partIx: number): readonly RouteAction[] {
  return item === onScreen ? [{ type: "part", part: partIx }, { type: "view", view: "regs" }]
    : [openWater({ item, part: partIx, view: "regs" })];
}

/** "Flows into …": the receiving water, opened on top at the part the mouth meets (the one `openWater` path). */
export function exitAction(item: ItemId, part: number | null): RouteAction {
  return openWater({ item, part, view: "regs" });
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
 * water with no parts.
 */
export function useWaterMap(store: RegsStore | null, item: ItemId | null, part: number | null,
                            tapSid: SectionId | null): Async<WaterMap | null> {
  const group = useWaterGroup(store, item);
  const target = useRegsTarget(part === null ? store : null, item, tapSid);
  const startPart = part ?? (target.value?.kind === "part" ? target.value.partIx : null);
  const waiting = part === null && target.state === "loading" && store !== null;
  const g = group.value;
  const key = `${group.state}:${item ?? ""}:${startPart ?? ""}`;
  const [held, setHeld] = useState<{ key: string; s: WaterMapState } | null>(null);
  const s = held && held.key === key ? held.s : g && item ? waterMapStart(waterMapStartIx(g, item, startPart)) : null;
  const vm = useMemo(() => (g && s ? waterMapView(g, s) : null), [g, s]);
  // no store (no release, or not open yet: the caller says which) or no water: nothing to walk
  if (store === null || item === null) return { state: "ready", value: null, error: null };
  if (group.state === "failed") return group;
  if (group.state === "loading" || waiting) return { state: "loading", value: null, error: null };
  if (!g || !s || !vm) return { state: "ready", value: null, error: null };
  // from the state as it is when the step lands, so two taps before a render are two steps
  const act = (a: WaterMapAction) => setHeld((h) => ({ key, s: waterMapStep(g, h && h.key === key ? h.s : s, a) }));
  return { state: "ready", value: { vm, act }, error: null };
}
