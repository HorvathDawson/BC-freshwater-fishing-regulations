/**
 * (T734) THE MARKS' LABEL ANCHORS ASK ONLY A MAP THAT IS THERE. `anchorMarks` measures each boundary mark's ring with the
 * binding's `project` (T317; T929: only to choose how its label hangs, the label's point is the mark's own). Build 32
 * closed twice with `NullPointerException at MLRNMapView.project`: a water's freshly mounted map was asked before
 * MapLibre had handed it its map (`mapLibreMap!!` on the native main thread, where no JS `catch` reaches). So no call is
 * ISSUED unless the map is mounted, styled and has drawn once (`ready`: a full render means MapLibre's map exists, the
 * same gate feature-state and the queries wait for) — checked before each call. A skipped call is expected (the anchors
 * are asked again once the map has drawn and at every settle) and is not reported. A call issued to a ready map that
 * REJECTS is a failure and goes to `report` (the map's `onError`, `reportFailure`); its label stays centred.
 */
import { labelAnchors } from "../point-marks";
import type { MapMark } from "../map-props";

type Pair = [number, number];
/** The binding call (`MapRef.project`). */
export interface Projector {
  project(lngLat: Pair): Promise<readonly number[]>;
}

/**
 * Each mark's label anchor (null: no label, or not measured), or `undefined` when the map was not ready and nothing was
 * asked of it at all.
 */
export async function nativeLabelAnchors(marks: readonly MapMark[], width: number, map: Projector | null,
                                         ready: () => boolean, report: (e: unknown) => void) {
  if (!map || !ready()) return undefined;
  return labelAnchors(marks, width, (lon, lat) => {
    if (!ready()) return Promise.resolve(null);
    return map.project([lon, lat]).then((p) => [p[0], p[1]] as Pair, (e: unknown) => { report(e); return null; });
  });
}
