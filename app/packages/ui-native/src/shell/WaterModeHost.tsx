/**
 * ONE WATER SCREEN (water mode): the view the frame has in front — Regulations, Conditions or the water's Map — for the
 * water (or, T53, the unnamed reach) the route holds (T62 3.4: out of the Shell, unchanged). Every move it makes is a
 * route action or the one opener's.
 */
import type { MutableRefObject } from "react";
import type { ItemId, WaterSource } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { crossAction, exitAction, mapPointTap, partAction, showRegsActions, waterFrame, type MapRail,
         mapError, type RegsStart } from "@app/ui";
import type { Camera, MapSpots, TileEndpoints } from "@app/map";
import type { WaterScreen as WaterScreenType } from "../WaterScreen";
import { WaterPageLoading } from "../WaterPageLoading";
import type { FishImages } from "../regs/types";
import type { Palette } from "../theme";
import type { ShellNav } from "./useShellNav";
import type { MapColouring } from "./useMapColouring";
import { part } from "../part";

// (T282) loaded later on the web (`part`): not on the first screen's path
const WaterConditions = part(() => import("../later").then((m) => m.WaterConditions));
const WaterMap = part(() => import("../later").then((m) => m.WaterMap));
const WaterMapView = part(() => import("../later").then((m) => m.WaterMapView));
/**
 * (T910, owner 2026-11-19 "build all 3 on webapp") THE REGULATIONS PAGE, loaded later on the web too: its code (the page,
 * the Keep / Gear / Licence / Methods tabs, the Salmon card: ~150 KiB of the first script) is in the later chunk. Until it
 * is in, the page's own loading face (`WaterPageLoading`, the one it draws while its water's sheet comes) — never an empty
 * box. Asked at once by a start on a water's page (apps/mobile startLink.web: `preloadParts`), when first drawn, or once
 * the browser is idle after the first screen; a phone has it at once (one bundle).
 */
const WaterScreen = part<React.ComponentProps<typeof WaterScreenType>>(() => import("../later").then((m) => m.WaterScreen), undefined,
                                                                       (p) => <WaterPageLoading palette={p.palette} />);

/**
 * (T1211 review) A WATER'S OWN MAP VIEW DRAWS EVERY CLASS. A class shown alone on the main map (the key's isolate, T377)
 * is a way of looking at the browse map, with the chip that says so; a water's Map view has no chip, and greying the
 * water you opened (a closed water drawn grey because "Its own rules" was isolated) would misstate its rules. So the
 * day's status goes in whole: `only` is never passed.
 */
export function waterMapStatus<S extends { only?: unknown }>(s: S | null): S | null {
  return s && { ...s, only: null };
}

export function WaterModeHost({ frame, nav, col, source, palette, loader, tiles, theme, feed, rail, spotLayer, camera,
                                waterCamera, fishImages, mapDay, onDay, start, onStarted }: {
  frame: NonNullable<ReturnType<typeof waterFrame>>; nav: ShellNav; col: MapColouring; source: WaterSource;
  palette: Palette; loader: ReleaseLoader | null; tiles: TileEndpoints; theme: string;
  feed?: import("@app/data").GaugeFeed; rail: MapRail; spotLayer: MapSpots | undefined;
  camera: MutableRefObject<Camera>;
  /**
   * The water's map view (P15, `WaterMapView`) has its own camera, so leaving water mode puts the browse map back where
   * the reader left it (APP-PREP §0 rule 1).
   */
  waterCamera: MutableRefObject<Camera | null>;
  fishImages?: FishImages;
  mapDay: Date | null; onDay: (d: Date | null) => void;
  start: RegsStart | null; onStarted: (spent: RegsStart) => void;
}) {
  const { go, back, opener } = nav;
  const item: ItemId | null = nav.item;
  const { credits, horizon, pickDay, mm, mapStatus } = col;
  return frame.view === "conditions"
    // THE ONE CONDITIONS PANEL OF A SECTION, from either map (a frame with no item is an unnamed reach).
    ? <WaterConditions source={source} item={item} tapSid={frame.tapSid} palette={palette} loader={loader}
                       // (T369 D1) the frame's part: the reach shown is the frame's (`frameView.ts`)
                       part={frame.part}
                       tiles={tiles} theme={theme}
                       // (T728, owner 2026-11-14) a Flow / Level pick in the card is the card's: it never sets the map's Show.
                       // (T728, owner 2026-11-16 "fix anything that doesnt seem desirable"; 2026-11-10 "Flow leads, Level
                       // auxiliary") and the map's Show never sets the card's: it OPENS on Flow, whatever the map shows. A
                       // water whose station reads only level: the card says Flow is not measured and offers Level (as before
                       // for a map on Flow)
                       opening="discharge"
                       from={frame.at}
                       feed={feed}
                       credits={credits} horizon={horizon}
                       // (T582) the regulations day: Conditions names its own day and links to this one
                       regsDay={mapDay}
                       onHorizon={pickDay} />
    // the water Map is a named water's (the reducer never puts an unnamed reach on it: `frameViews`)
    : frame.view === "map" && item !== null
      ? <WaterMap source={source} item={item} palette={palette}>
          {/* P15: the parts, lit and framed from the release; ▼ / ▲, marks, the mouth's exit */}
          <WaterMapView loader={loader} day={mapDay} item={item} part={frame.part} tapSid={frame.tapSid} whole={frame.whole} palette={palette}
                        // (T1140) opened from a head's "Other parts of this water have different rules": the list open
                        listOpen={frame.parts === true}
                        map={{ at: tiles, theme, camera: waterCamera.current ?? camera.current,
                               basemap: mm.basemap, status: waterMapStatus(mapStatus), spots: spotLayer,
                               // T145: a tap off every spot clears the selected spot's pin, here as on the browse maps
                               onMapPoint: mapPointTap(rail, null),
                               onMoved: (at) => { waterCamera.current = at; },
                               view: "plain", modes: mm.modes, groups: mm.activeGroups,
                               onError: mapError }}
                        // (T58) through the opener: the receiving water's lake areas known, a late answer dropped
                        onOpenWater={(id, p) => void opener.flow(id, (g) => exitAction(id, p, g))}
                        onShowRegs={(id, p) => { for (const a of showRegsActions(item, id, p)) go(a); }}
                        // P15 follow-ups: a step is the frame's part (one water screen); ▲ / ▼ into the water this
                        // one goes on from or as opens it, Map in front (back returns here)
                        onPart={(id, p) => go(partAction(item, id, p))}
                        onCross={(id, p) => go(crossAction(id, p))}
                        // (T369 D7) ⤢ is the frame's: a route move, kept across views, back and a reload
                        onWhole={() => go({ type: "whole" })}
                        // another water opens on top (back returns here), at the point tapped (its "you are here")
                        onOtherSection={(sid, at) => opener.tap(sid, at, "regs", item)} />
        </WaterMap>
      : <WaterScreen source={source} item={item} palette={palette} fishImages={fishImages}
                     // T01: what the Regulations page reads — the release, the ONE day (the map's), the part tapped
                     // (T53: an unnamed reach, item null, is answered by the tapped section itself)
                     loader={loader} day={mapDay} onDay={onDay} section={frame.tapSid} at={frame.at}
                     part={frame.part} start={start} onStarted={onStarted}
                     // NO `feed`. The view quotes no number, so it reads no gauge data — the Conditions view, one
                     // swipe away, is where a water's numbers are.
                     // (T221, a try) the Keep tab's at-a-glance table, in the app (a screen mounted alone keeps the cards)
                     glance
                     onBack={back} />;
}
