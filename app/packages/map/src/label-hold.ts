/**
 * (T876) LABELS WAIT WHILE THE MAP ZOOMS, ON A PHONE (owner 2026-11-18 21:40: "We care number one about the streams and
 * colors and smoothness then labels can be a little behind etc.").
 *
 * On MapLibre Native every zoom crossing lays out and places the labels of every new tile on the map's render thread, and a
 * quick pinch crosses several (emulator bench, T876: the labels are half a pinch's render work). So while the camera's ZOOM
 * moves — a pinch and its carry, a double tap, a zoom button — the label layers below are not drawn, and they come back a
 * moment after the camera rests (`SETTLE_MS`). A pan that keeps its zoom keeps its labels. A FIT never holds them (the map
 * does not start the hold while its fit glides): a fit runs behind a held pane shown when the map is drawn whole, and
 * waiting for labels there would only make opening a water slower. (Not the binding's `userInteraction`: on Android it is
 * true for the app's own camera animations and false for a double tap.) The stream lines, their colours and the gauge dots
 * are never held.
 *
 * NEVER LEFT HIDDEN: a camera that reports no rest (a lost "did change") gives the labels back `LOST_END_MS` after its
 * last report (within `LOST_RESET_MS`: the clock is restarted at most that often), whatever happened.
 *
 * Platform-free (the state machine and which layers it holds); `Map.native.tsx` drives it from the binding's camera events
 * and draws the result by adopting the style's own label layers (`LabelHoldLayers`). The web map is unchanged: maplibre-gl
 * places labels off the main work and a pinch there is smooth.
 */

import { REGION_LABEL_SOURCE, TOWN_SOURCE } from "./look-a";
import { now as clockNow } from "@app/core";

/** The kinds of name and mark on the map: the per-kind lab switches (`lab.ts`) and what the hold takes. */
export type LabelGroup = "water" | "places" | "roads" | "areas" | "gauges" | "marks";
export const LABEL_GROUPS: readonly LabelGroup[] = ["water", "places", "roads", "areas", "gauges", "marks"];

type Layer = { id: string; type?: string; source?: string; layout?: Record<string, unknown> };

/** The basemap's names a reader steers by (towns, islands, the sea); the rest of its labels are roads, POIs, addresses. */
const BASEMAP_PLACES = new Set(["places_locality", "places_country", "places_region", "places_subplace",
                                "earth_label_islands", "water_label_ocean"]);
/** Our water names. */
const WATER_NAMES = new Set(["lake__label", "stream__label"]);
/** Marks drawn along or on the water: the selected gauge's route arrows, a stream's flow arrows, obstacles. */
const MARKS = new Set(["route-arrows", "stream__arrows", "obstacle"]);

/** The kind a style layer is, or null when it is not a label or mark this file knows (the route map's own overlay). */
export function labelGroup(l: Layer): LabelGroup | null {
  if (l.type !== "symbol") return null;
  if (MARKS.has(l.id)) return "marks";
  if (l.source === "gauges") return "gauges";
  if (l.source === "basemap") return BASEMAP_PLACES.has(l.id) ? "places" : "roads";
  if (WATER_NAMES.has(l.id)) return "water";
  if (l.source === "atlas" || l.source === "mus") return "areas";
  // (T1208, T1209) the towns are place names; "Region n" an area's (`look-a.ts`, their own GeoJSON sources)
  if (l.source === TOWN_SOURCE) return "places";
  if (l.source === REGION_LABEL_SOURCE) return "areas";
  return null;
}

/** What the hold takes while the zoom moves: every name and mark except the gauge pills (a reading, not a label). */
export const HELD_WHILE_ZOOMING: readonly LabelGroup[] = ["water", "places", "roads", "areas", "marks"];

/** The sources whose layers the native map can adopt by id (each is a source element `Map.native.tsx` mounts). */
export const HOLD_SOURCES = ["atlas", "basemap", "mus", "gauges"] as const;
export type HoldSource = (typeof HOLD_SOURCES)[number];

export interface HeldLayer { id: string; source: HoldSource; visibility: "visible" | "none" }

/** The style's layers the hold takes, by source, each with the visibility the style itself gives it (restored at rest). */
export function heldLayers(style: { layers: Layer[] }, groups: readonly LabelGroup[] = HELD_WHILE_ZOOMING):
    Record<HoldSource, HeldLayer[]> {
  const out = Object.fromEntries(HOLD_SOURCES.map((s) => [s, [] as HeldLayer[]])) as Record<HoldSource, HeldLayer[]>;
  for (const l of style.layers) {
    const g = labelGroup(l);
    if (!g || !groups.includes(g) || !(HOLD_SOURCES as readonly string[]).includes(l.source ?? "")) continue;
    out[l.source as HoldSource].push({ id: l.id, source: l.source as HoldSource,
                                      visibility: l.layout?.visibility === "none" ? "none" : "visible" });
  }
  return out;
}

/** After the camera rests, how long the labels wait (a quick run of pinches keeps them away between pinches). */
export const SETTLE_MS = 250;
/** With no camera report for this long, the labels come back even though no rest was reported. */
export const LOST_END_MS = 1500;
/** A zoom change smaller than this is a pan's rounding, not a zoom. */
export const ZOOM_EPS = 0.01;
/** The lost-end clock is restarted at most this often: a camera report comes every frame, a timer swap need not. */
export const LOST_RESET_MS = 100;

export interface HoldTimers {
  set(fn: () => void, ms: number): unknown;
  clear(handle: unknown): void;
  now(): number;
}
const REAL_TIMERS: HoldTimers = { set: (fn, ms) => setTimeout(fn, ms), clear: (h) => clearTimeout(h as never),
                                  now: clockNow };

/**
 * The hold's state, fed the camera's reports: `start` (the binding's "will change"), `moving` (each "is changing"),
 * `rest` (each "did change"), `reset` (off, or the map gone). `onChange(held)` is told each time the labels go or come back, never twice the same.
 */
export class LabelHold {
  private held = false;
  /** a move the hold was started for (`start` … `rest`): only then do camera frames count */
  private active = false;
  private from: number | null = null;
  private back: unknown = null;
  private lost: unknown = null;
  private lostAt = 0;
  constructor(private readonly onChange: (held: boolean) => void, private readonly timers: HoldTimers = REAL_TIMERS) {}

  isHeld(): boolean { return this.held; }

  /** A camera move starts from `zoom` (undefined: not reported; the next zoom reported is the start). The caller does not
   *  start the hold for a move it must not hold (a fit). */
  start(zoom: number | undefined): void {
    this.active = true;
    this.cancel("back");
    if (!this.held) this.from = zoom ?? null;
    this.armLost();
  }

  /** The camera is at `zoom`, moving. The labels go once it has left the zoom it started at. */
  moving(zoom: number | undefined): void {
    if (!this.active || zoom === undefined) return;
    this.cancel("back");
    if (this.from === null) this.from = zoom;
    if (!this.held && Math.abs(zoom - this.from) > ZOOM_EPS) this.set(true);
    this.armLost();
  }

  /** The camera rests. The labels come back after `SETTLE_MS` unless it moves again first. */
  rest(): void {
    this.active = false;
    this.from = null;
    this.cancel("lost");
    if (!this.held) return;
    this.cancel("back");
    this.back = this.timers.set(() => { this.back = null; this.release(); }, SETTLE_MS);
  }

  /** The hold is switched off (the lab) or the map is going away: no timer outlives it, and the labels are back. */
  reset(): void { this.cancel("back"); this.release(); }

  private release(): void {
    this.active = false;
    this.from = null;
    this.cancel("lost");
    this.set(false);
  }

  private armLost(): void {
    const now = this.timers.now();
    if (this.lost !== null && now - this.lostAt < LOST_RESET_MS) return;
    this.cancel("lost");
    this.lostAt = now;
    this.lost = this.timers.set(() => { this.lost = null; this.cancel("back"); this.release(); }, LOST_END_MS);
  }

  private cancel(which: "back" | "lost"): void {
    const h = this[which];
    if (h !== null) this.timers.clear(h);
    this[which] = null;
  }

  private set(held: boolean): void {
    if (held === this.held) return;
    this.held = held;
    this.onChange(held);
  }
}
