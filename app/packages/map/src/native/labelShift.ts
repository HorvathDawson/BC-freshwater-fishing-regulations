/**
 * (T734) THE MARKS' LABEL SHIFT ASKS ONLY A MAP THAT IS THERE. `shiftMarks` measures each boundary mark's label with the
 * binding's `project` / `unproject` (T317). Build 32 closed twice with `NullPointerException at MLRNMapView.project`:
 * a water's freshly mounted map was asked before MapLibre had handed it its map (`mapLibreMap!!` on the native main
 * thread, where no JS `catch` reaches). So no call is ISSUED unless the map is mounted, styled and has drawn once
 * (`ready`: a full render means MapLibre's map exists, the same gate feature-state and the queries wait for) — checked
 * before each call, so a map that unmounts between a mark's two calls is not asked again. A skipped call is expected
 * (the shift runs again once the map has drawn and at every settle) and is not reported. A call issued to a ready map
 * that REJECTS is a failure and goes to `report` (the map's `onError`, `reportFailure`); its label is left unshifted.
 */
import { shiftedLabels } from "../point-marks";
import type { MapMark } from "../map-props";

type Pair = [number, number];
/** The two binding calls (`MapRef.project` / `unproject`). */
export interface Projector {
  project(lngLat: Pair): Promise<readonly number[]>;
  unproject(point: Pair): Promise<readonly number[]>;
}

/**
 * Each mark's label centre (null: no label, or not measured), or `undefined` when the map was not ready and nothing was
 * asked of it at all.
 */
export async function nativeLabelShift(marks: readonly MapMark[], width: number, map: Projector | null,
                                       ready: () => boolean, report: (e: unknown) => void) {
  if (!map || !ready()) return undefined;
  const ask = (call: () => Promise<readonly number[]>): Promise<Pair | null> => {
    if (!ready()) return Promise.resolve(null);
    return call().then((p) => [p[0], p[1]] as Pair, (e: unknown) => { report(e); return null; });
  };
  return shiftedLabels(marks, width, (lon, lat) => ask(() => map.project([lon, lat])),
                       (x, y) => ask(() => map.unproject([x, y])));
}
