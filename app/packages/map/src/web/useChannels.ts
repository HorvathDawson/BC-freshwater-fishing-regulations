/**
 * THE CHANNELS (the colouring engine, `colouring.ts`): the day's status and the Conditions standings go to ONE
 * engine, which labels the tiles already loaded and installs the values. New values are one `Colouring.set`: a day is
 * one paint property (the day is in the style); a feed refresh or a horizon relabels the loaded tiles' sections
 * (feature-state only). New labels, or none, are the release-swap sequence in `Colouring.set` — and what it clears that
 * is not ours (the selected highlight, the active mode's pushed data) is put back here.
 */
import { readingDayOf, salmonDayIndex } from "@app/core";
import { useEffect, useRef } from "react";
import type * as maplibregl from "maplibre-gl";
import type { WebRenderer } from "../adapters/web";
import { LABEL_SOURCE_LAYERS, methodInput, runsInput, salmonInput, standingInput, statusInput, stockedInput, surveyInput, withoutEngineKeys,
         type Channel, type ChannelInput, type TileRenderer, type TileSections } from "../colouring";
import type { MapProps } from "../map-props";
import { sourceTiles, tileIds } from "../tile-ids.web";
import { atlasTarget, copyTarget, layerHandle, publicSections } from "./handles";
import { devCost, forcePublic, whenAtlas } from "./ready";
import type { WebMapRefs } from "./refs";
import type { Lit } from "./useHighlight";

type Channels = Pick<MapProps, "at" | "theme" | "data" | "status" | "standing" | "lakes" | "salmon" | "method" | "runs">;

export function useChannels(refs: WebMapRefs, { wasLit, wasWater, wasCue }: Lit,
                            { at, theme, data, status, standing, lakes, salmon, method, runs }: Channels) {
  const { map, renderer, adapter, copy, colouring } = refs;
  const latestData = useRef(data);
  latestData.current = data;
  const latestTheme = useRef(theme);
  latestTheme.current = theme;
  /** The tiles this map holds, and what a clear must put back — the same for every channel. */
  const tileRenderer = (m: maplibregl.Map, r: WebRenderer): TileRenderer => ({
    loaded: () => {
      const held = forcePublic() ? null : sourceTiles(m, "atlas");
      const read = held?.map((t) => tileIds(t, LABEL_SOURCE_LAYERS));
      return read && read.every((x) => x !== null) ? read as TileSections[] : [publicSections(m)];
    },
    restore: () => {
      adapter.current.highlight(r.states(() => []), [...wasLit.current], [...wasWater.current], wasCue.current);
      for (const [layerId, values] of Object.entries(withoutEngineKeys(latestData.current ?? {}, () => {})))
        adapter.current.setData(layerHandle(m, r), layerId, values);
    },
  });
  /** One channel's input, to the engine, once the atlas exists — the one path both channels take. */
  const toChannel = (channel: Channel, input: ChannelInput | null, kind: string) => {
    const m = map.current, c = colouring.current, r = renderer.current;
    if (!m || !c || !r) return;
    return whenAtlas(m, () => {
      const t0 = performance.now(), c0 = c.calls;
      c.set(layerHandle(m, r), atlasTarget(m, r, copy.current, copyTarget(m)), tileRenderer(m, r), channel, input, latestTheme.current);
      devCost({ t: t0, ms: performance.now() - t0, calls: c.calls - c0, kind });
    });
  };
  const statusDay = status ? readingDayOf(status.day) : 0;
  useEffect(() => toChannel("status", status ? statusInput(status) : null, `status:${statusDay}`),
            [status?.index, statusDay, at.atlas]);
  useEffect(() => toChannel("standing", standing ? standingInput(standing) : null, "standing"),
            [standing?.labels, standing?.values, at.atlas]);
  // the lake modes (Depth, Stocked): the release's lake facts — a new release relabels both, a new day the stocked bands
  useEffect(() => toChannel("survey", lakes ? surveyInput(lakes) : null, "survey"), [lakes?.facts, at.atlas]);
  useEffect(() => toChannel("stocked", lakes ? stockedInput(lakes) : null, `stocked:${lakes?.day ?? ""}`),
            [lakes?.facts, lakes?.day, at.atlas]);
  // (T211) the Salmon colouring: chain labels once per release, the day in the style — the toggle never reaches here
  const salmonDay = salmon ? salmonDayIndex(salmon.day) : -1;
  useEffect(() => toChannel("salmon", salmon ? salmonInput(salmon) : null, `salmon:${salmonDay}`),
            [salmon?.facts, salmonDay, at.atlas]);
  // (T213) the gear marks: profile labels once per release, the day in the gear layers' paint
  const methodDay = method ? readingDayOf(method.day) : -1;
  useEffect(() => toChannel("method", method ? methodInput(method) : null, `method:${methodDay}`),
            [method?.facts, methodDay, at.atlas]);
  // (T199) the run timing: the state for the species and month IS the label — a change relabels the loaded tiles
  const runsKey = runs ? `${runs.slot}:${runs.month}` : "";
  useEffect(() => toChannel("runs", runs ? runsInput(runs) : null, `runs:${runsKey}`), [runs?.facts, runsKey, at.atlas]);
}
