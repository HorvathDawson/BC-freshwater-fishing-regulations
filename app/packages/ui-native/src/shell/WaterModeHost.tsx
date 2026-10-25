/**
 * ONE WATER SCREEN (water mode): the view the frame has in front — Regulations, Conditions or the water's Map — for the
 * water (or, T53, the unnamed reach) the route holds (T62 3.4: out of the Shell, unchanged). Every move it makes is a
 * route action or the one opener's.
 */
import type { MutableRefObject } from "react";
import type { ItemId, WaterSource } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { crossAction, exitAction, mapPointTap, partAction, showRegsActions, waterFrame, type MapRail,
         type RegsStart } from "@app/ui";
import type { Camera, MapSpots, TileEndpoints } from "@app/map";
import { WaterScreen } from "../WaterScreen";
import { WaterConditions, WaterMap } from "../WaterViews";
import { WaterMapView } from "../water/WaterMapView";
import type { AdiposeImages, FishImages } from "../regs/types";
import type { Palette } from "../theme";
import type { ShellNav } from "./useShellNav";
import type { MapColouring } from "./useMapColouring";

export function WaterModeHost({ frame, nav, col, source, palette, loader, tiles, theme, feed, rail, spotLayer, camera,
                                waterCamera, fishImages, adiposeImages, mapDay, onDay, start, onStarted }: {
  frame: NonNullable<ReturnType<typeof waterFrame>>; nav: ShellNav; col: MapColouring; source: WaterSource;
  palette: Palette; loader: ReleaseLoader | null; tiles: TileEndpoints; theme: string;
  feed?: import("@app/data").GaugeFeed; rail: MapRail; spotLayer: MapSpots | undefined;
  camera: MutableRefObject<Camera>;
  /**
   * The water's map view (P15, `WaterMapView`) has its own camera, so leaving water mode puts the browse map back where
   * the reader left it (APP-PREP §0 rule 1).
   */
  waterCamera: MutableRefObject<Camera | null>;
  fishImages?: FishImages; adiposeImages?: AdiposeImages;
  mapDay: Date | null; onDay: (d: Date | null) => void;
  start: RegsStart | null; onStarted: () => void;
}) {
  const { go, back, opener } = nav;
  const item: ItemId | null = nav.item;
  const { flowParam, setFlowParam, credits, horizon, pickDay, mm, statusMapData, mapStatus, noteVisible } = col;
  return frame.view === "conditions"
    // THE ONE CONDITIONS PANEL OF A SECTION, from either map (a frame with no item is an unnamed reach).
    ? <WaterConditions source={source} item={item} tapSid={frame.tapSid} palette={palette} loader={loader}
                       tiles={tiles} theme={theme}
                       // Temperature is not a hydrograph, so the chart falls back to whatever the station itself
                       // leads with; Flow and Level pass straight through — the map and the chart ask one question.
                       parameter={flowParam === "temperature" ? undefined : flowParam}
                       onParameter={setFlowParam}
                       from={frame.at}
                       feed={feed}
                       credits={credits} horizon={horizon}
                       onHorizon={pickDay} />
    // the water Map is a named water's (the reducer never puts an unnamed reach on it: `frameViews`)
    : frame.view === "map" && item !== null
      ? <WaterMap source={source} item={item} palette={palette}>
          {/* P15: the parts, lit and framed from the release; ▼ / ▲, marks, the mouth's exit */}
          <WaterMapView loader={loader} item={item} part={frame.part} tapSid={frame.tapSid} whole={frame.whole} palette={palette}
                        map={{ at: tiles, theme, camera: waterCamera.current ?? camera.current,
                               basemap: mm.basemap, data: statusMapData, status: mapStatus, spots: spotLayer,
                               // T145: a tap off every spot clears the selected spot's pin, here as on the browse maps
                               onMapPoint: mapPointTap(rail, null),
                               onVisible: noteVisible, onMoved: (at) => { waterCamera.current = at; },
                               view: "plain", modes: mm.modes, groups: mm.activeGroups,
                               onError: (e) => console.error("map:", e.message) }}
                        // (T58) through the opener: the receiving water's lake areas known, a late answer dropped
                        onOpenWater={(id, p) => void opener.flow(id, (g) => exitAction(id, p, g))}
                        onShowRegs={(id, p) => { for (const a of showRegsActions(item, id, p)) go(a); }}
                        // P15 follow-ups: a step is the frame's part (one water screen); ▲ / ▼ into the water this
                        // one goes on from or as opens it, Map in front (back returns here)
                        onPart={(id, p) => go(partAction(item, id, p))}
                        onCross={(id, p) => go(crossAction(id, p))}
                        // another water opens on top (back returns here)
                        onOtherSection={(sid) => opener.tap(sid, null, "regs", item)} />
        </WaterMap>
      : <WaterScreen source={source} item={item} palette={palette} fishImages={fishImages} adiposeImages={adiposeImages}
                     // T01: what the Regulations page reads — the release, the ONE day (the map's), the part tapped
                     // (T53: an unnamed reach, item null, is answered by the tapped section itself)
                     loader={loader} day={mapDay} onDay={onDay} section={frame.tapSid}
                     part={frame.part} start={start} onStarted={onStarted}
                     // (T58) a Parts sheet row is the frame's part, as a step on the water map is (one water screen)
                     onPart={item === null ? undefined : (p) => go(partAction(item, item, p))}
                     // NO `feed`. The view quotes no number, so it reads no gauge data — the Conditions view, one
                     // swipe away, is where a water's numbers are.
                     onBack={back} />;
}
