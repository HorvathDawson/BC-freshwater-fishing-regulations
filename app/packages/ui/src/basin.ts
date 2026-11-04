/**
 * (T376) THE WATERSHED GROUP A CONDITIONS VIEW IS ABOUT — for T377's forecast-day list, which shows one group's standing
 * per day (`basinStandings` in @app/data: the group's id is its key). Two questions, and NEVER one answered for the other
 * (owner: no hidden fallbacks):
 *
 *   browse   the group under the Conditions map's CENTRE, asked when the map comes to rest (`useCentreBasin`, on the
 *            map colouring as `col.centreBasin`); null when the centre is in no group (the sea, outside B.C.)
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

/** The Conditions map's centre group, for the map colouring (`col.centreBasin`). */
export interface CentreBasinVM {
  /** The group under the centre; null: the centre is in none; undefined: not asked (off Conditions) or not answered yet. */
  centreBasin: Basin | null | undefined;
  /** Why the centre's group could not be read (a release without groups, a source without them); null otherwise. */
  centreBasinError: Error | null;
  /** Hand it the camera each time the map comes to rest (`MapProps.onMoved`). Cheap: a ref, plus a lookup while asked. */
  noteCentre(at: LatLon): void;
}

/**
 * THE GROUP UNDER THE MAP'S CENTRE, while `enabled` (the Conditions map): asked on each `noteCentre` — which the map calls
 * once per gesture, when it comes to rest, never per frame — and re-renders only when the group changes. Off Conditions
 * nothing is asked (no release file is opened for it) and the answer is withdrawn (`undefined`); back on, the last centre
 * noted is asked at once. `initial` is where the camera starts, before the map first reports.
 */
export function useCentreBasin(source: WaterSource, enabled: boolean, initial: LatLon): CentreBasinVM {
  const at = useRef<LatLon>(initial);
  const on = useRef(enabled);
  on.current = enabled;
  const seq = useRef(0);
  const [vm, setVm] = useState<{ basin: Basin | null | undefined; error: Error | null }>({ basin: undefined, error: null });
  const held = useRef(vm);
  const ask = useCallback((p: LatLon) => {
    const n = ++seq.current;
    basinAt(source, p).then(
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
  const noteCentre = useCallback((p: LatLon) => {
    at.current = { lon: p.lon, lat: p.lat };
    if (on.current) ask(at.current);
  }, [ask]);
  return { centreBasin: vm.basin, centreBasinError: vm.error, noteCentre };
}
