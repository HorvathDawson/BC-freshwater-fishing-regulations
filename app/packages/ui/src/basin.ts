/**
 * (T376) THE WATERSHED GROUP A CONDITIONS VIEW IS ABOUT — for T377's forecast-day list, which shows one group's standing
 * per day (`basinStandings` in @app/data: the group's id is its key). Two questions, and NEVER one answered for the other
 * (owner: no hidden fallbacks):
 *
 *   browse   (T417) the group with the LARGEST SHARE OF WHAT THE CONDITIONS MAP SHOWS, asked when the map comes to rest
 *            (`useViewBasin`, on the map colouring as `col.viewBasin`) — so a centre over the Strait next to the Lower
 *            Fraser still answers the Lower Fraser; null only when no group is on screen (open sea, outside B.C.)
 *   a water  the group the water's own place is in (`useWaterBasin`): where the reader tapped on it, or — opened without
 *            a tap — the water's own point (`WaterSource.waterPoint`). Never the map's centre.
 *
 * Both are the release's grid (`WaterSource.basinAt`, @app/data `basinGrid.ts`): the tiles' basin layer is not there to
 * ask above z7, which is where the Conditions map's rivers are. A group is `{ id, name }`, the name AS SOURCED
 * ("Chilliwack River"); a word around it is core's to add. A failure is an `Error` the screen shows (and reports,
 * `reportFailure`), never an empty answer.
 */
import { useCallback, useEffect, useRef, useState } from "react";
import { reportFailure } from "@app/core";
import type { Basin, ItemId, SectionId, WaterSource } from "@app/data";
import { useAsync, type Async } from "./async";
import type { LatLon } from "./route";

export type { Basin };

/** `source.basinAt`, or a thrown "cannot say" for a source that has no groups (the fixture) — never null. */
export function basinAt(source: WaterSource, at: LatLon): Promise<Basin | null> {
  if (!source.basinAt) return Promise.reject(new Error("basins: this source carries no watershed groups"));
  return source.basinAt(at);
}

/** What the map shows when it comes to rest: its centre, and (T417) its visible bounds once it has drawn them. */
export interface MapView extends LatLon {
  readonly bounds?: readonly [number, number, number, number];
}

/**
 * (T417) THE BOX A VIEW IS ASKED BY: the map's visible bounds; before the map has drawn any (the camera it starts on),
 * the start point itself — a box of one point, which IS the group under that point (`BasinGrid.mostIn`). The same
 * question either way, never a second way of answering it.
 */
function viewBox(v: MapView): readonly [number, number, number, number] {
  return v.bounds ?? [v.lon, v.lat, v.lon, v.lat];
}

/** `source.basinIn` over the view's box, or a thrown "cannot say" for a source that has no groups — never null. */
function basinIn(source: WaterSource, v: MapView): Promise<Basin | null> {
  if (!source.basinIn) return Promise.reject(new Error("basins: this source carries no watershed groups"));
  return source.basinIn(viewBox(v));
}

/** What a water's Conditions view is opened on: the water (null: an unnamed reach), the tapped section and the tap. */
export interface WaterBasinAsk {
  readonly item: ItemId | null;
  readonly tapSid: SectionId | null;
  /** Where on `tapSid` the tap landed (the route frame's `at`); null when not tapped there. */
  readonly at: LatLon | null;
}

/**
 * THE GROUP A WATER'S OWN PLACE IS IN: the tap on it, when the view was opened by one; else the water's own point
 * (`waterPoint`: the middle of its parts' line nearest the middle of its box). THROWS — never answers from somewhere
 * else — when neither can be had: a tapped reach whose tap point is not known, a water with no geometry.
 */
export async function waterBasin(source: WaterSource, w: WaterBasinAsk): Promise<Basin | null> {
  if (w.at) return basinAt(source, w.at);
  if (w.tapSid !== null) throw new Error("basins: the reach was opened without the point it was tapped at");
  if (w.item === null) throw new Error("basins: no water and no reach to find a watershed group for");
  if (!source.waterPoint) throw new Error("basins: this source has no water geometry to find a watershed group by");
  const p = await source.waterPoint(w.item);
  if (!p) throw new Error(`basins: ${w.item} has no geometry to find its watershed group by`);
  return basinAt(source, p);
}

/** `waterBasin` as a hook: ready with the group (or null: its place is in none), or failed with what went wrong. */
export function useWaterBasin(source: WaterSource, w: WaterBasinAsk, enabled = true): Async<Basin | null> {
  const key = `water-basin:${w.item ?? ""}:${w.tapSid ?? ""}:${w.at ? `${w.at.lon},${w.at.lat}` : ""}`;
  return useAsync(() => waterBasin(source, w), key, enabled);
}

/** The Conditions map's group (T417: the one most on screen), for the map colouring (`col.viewBasin`). */
export interface ViewBasinVM {
  /** The group most on screen; null: no group is on screen; undefined: not asked (off Conditions) or not answered yet. */
  viewBasin: Basin | null | undefined;
  /** Why the view's group could not be read (a release without groups, a source without them); null otherwise. */
  viewBasinError: Error | null;
  /** Hand it the camera each time the map comes to rest (`MapProps.onMoved`). Cheap: a ref, plus a lookup while asked. */
  noteView(at: MapView): void;
}

/**
 * THE GROUP WITH THE LARGEST SHARE OF THE MAP, while `enabled` (the Conditions map): asked on each `noteView` — which the
 * map calls once per gesture, when it comes to rest, never per frame; a pan, a search and a tap all end there, so all
 * three get the same answer for the same view (one load path) — and re-renders only when the group changes. Off
 * Conditions nothing is asked (no release file is opened for it) and the answer is withdrawn (`undefined`); back on, the
 * last view noted is asked at once. `initial` is where the camera starts, before the map first reports.
 */
export function useViewBasin(source: WaterSource, enabled: boolean, initial: MapView): ViewBasinVM {
  const at = useRef<MapView>(initial);
  const on = useRef(enabled);
  on.current = enabled;
  const seq = useRef(0);
  const [vm, setVm] = useState<{ basin: Basin | null | undefined; error: Error | null }>({ basin: undefined, error: null });
  const held = useRef(vm);
  const ask = useCallback((p: MapView) => {
    const n = ++seq.current;
    basinIn(source, p).then(
      (b) => {
        if (n !== seq.current) return;
        // compared HERE, not in an updater: React may render once even for an updater that returns the same state
        const was = held.current;
        if (was.error === null && (was.basin === b || (was.basin && b && was.basin.id === b.id))) return;
        held.current = { basin: b, error: null };
        setVm(held.current);
      },
      (e: unknown) => {
        reportFailure(e);   // (T368) shown by the screen, reported here
        if (n !== seq.current) return;
        held.current = { basin: undefined, error: e instanceof Error ? e : new Error(String(e)) };
        setVm(held.current);
      });
  }, [source]);
  useEffect(() => {
    if (enabled) ask(at.current);
    else {
      seq.current++;                     // an answer in flight is for a view no longer shown
      if (held.current.basin !== undefined || held.current.error !== null) {
        held.current = { basin: undefined, error: null };
        setVm(held.current);
      }
    }
  }, [enabled, ask]);
  const noteView = useCallback((p: MapView) => {
    at.current = { lon: p.lon, lat: p.lat, ...(p.bounds ? { bounds: p.bounds } : {}) };
    if (on.current) ask(at.current);
  }, [ask]);
  return { viewBasin: vm.basin, viewBasinError: vm.error, noteView };
}
