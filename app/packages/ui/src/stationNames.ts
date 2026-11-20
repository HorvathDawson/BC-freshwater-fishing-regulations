/**
 * (T819 item 7) A GAUGE IS NAMED, NOT CODED: a saved spot keeps its stations by code ("08MH001"), and its page names
 * them ("Chilliwack River at Vedder Crossing") from the release's gauges (`WaterSource.gaugePoints`), read once per
 * source. While they load, or where the release has no name for a code, the page says the code, which is still the
 * station's true name; a failed read is reported by `useAsync`, never hidden.
 */
import { useMemo } from "react";
import type { WaterSource } from "@app/data";
import { useAsync } from "./async";

const NONE: ReadonlyMap<string, string> = new Map();

export function useStationNames(source: Pick<WaterSource, "gaugePoints"> | null | undefined): ReadonlyMap<string, string> {
  const points = useAsync(async () => source!.gaugePoints(), "station-names", !!source);
  return useMemo(() => (points.state === "ready" ? new Map(points.value.map((g) => [g.station as string, g.name])) : NONE),
                 [points]);
}
