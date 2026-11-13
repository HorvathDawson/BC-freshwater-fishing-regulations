/**
 * THE BROWSE TABS' BODIES: the Map and Conditions tabs' ONE map, the Spots tab and the More tab (T62 3.4: out of the
 * Shell, unchanged). The top bar, the search sheet, the legend and the tab bar are the Shell's, drawn over these.
 */
import { dayLabel, type DayRange, type NotFishingKind, type SectionKey, type SyncVM, now as clockNow } from "@app/core";
import { useCallback, useContext, type MutableRefObject } from "react";
import { Platform, View } from "react-native";
import { openProbe, type SectionId } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { mapPointTap, routeWhere, spotsMenu, tapOpensWater, type Locate, type MyLocation, type MapRail, type MenuControls, type RegsPrefetch,
         mapError, InSeasonContext, type HeldLookStore, type SpotPhotos, type SyncState } from "@app/ui";
import type { Spot } from "@app/data/spots";
import type { Camera, MapSpots, TileEndpoints } from "@app/map";
import { hiddenLayers, shownLayers } from "@app/map/colours";
import { MapDayStrip } from "../DayCalendar";
import { TABS } from "../TabBar";
import type { MenuInfo } from "../menu/MenuHost";
import type { Palette, ThemeName } from "../theme";
import { HANDOVER_Z, HOME, type MapColouring } from "./useMapColouring";
import type { FishImages } from "../regs/types";
import type { ShellNav } from "./useShellNav";
import type { ShellSearch } from "./useShellSearch";
import { part } from "../part";
import { RegsLayerToggle } from "./RegsLayerToggle";

// (T282) loaded later on the web (`part`): not on the first screen's path
const MapScreen = part(() => import("../later").then((m) => m.MapScreen));
const SpotsScreen = part(() => import("../later").then((m) => m.SpotsScreen));
const MoreScreen = part(() => import("../later").then((m) => m.MoreScreen));

/** No highlight — one frozen empty list, so the map's effect is not re-run every render. */
const NONE: readonly SectionKey[] = [];
const NONE_PINS: readonly never[] = [];

export function BrowseHost({ nav, col, search, rail, palette, theme, themeName, onTheme, tiles, camera, insetTop, insetBottom,
                             spots, spotsShown, spotLayer, spotsError, onRefreshSpots, refreshing, locate, me = null,
                             photos, loader, syncState, dfoCheckedAt = null, sync = null, onSync, menu, menuInfo, mapDay, onSaveSpot, onLayers, onRegion,
                             offline = null, onDay, onOpenDay, dayRange, onObstacle, onNotFishing, onRuns, fishImages, regsPrefetch = null,
                             lookKey, look }: {
  nav: ShellNav; col: MapColouring; search: ShellSearch; rail: MapRail; palette: Palette; theme: string;
  themeName: ThemeName; onTheme: (t: ThemeName) => void; tiles: TileEndpoints; camera: MutableRefObject<Camera>;
  insetTop: number; insetBottom: number; spots: readonly Spot[]; spotsShown: boolean; spotLayer: MapSpots | undefined;
  spotsError?: string | null; onRefreshSpots?: () => void | Promise<void>; refreshing?: boolean;
  locate: Locate | null;
  /** (T303) my location on the map (the rail's button and the dot) */
  me?: MyLocation | null;
  photos?: SpotPhotos; loader: ReleaseLoader | null; syncState: SyncState;
  /** (T526) when the feeds worker last checked DFO (the feed's `at`); null: unknown, reported by the hook */
  dfoCheckedAt?: string | null;
  /** (T300) the top bar's sync icon state and tap, for the More tab's button */
  sync?: SyncVM | null; onSync?: () => void;
  menu: MenuControls; menuInfo?: MenuInfo; mapDay: Date | null;
  onSaveSpot?: (spot: Spot) => void | Promise<void>;
  /** Open the Layers sheet (the Shell's). */
  onLayers: () => void;
  /** (T293) where a tour step sends the camera (the Conditions tour: out to a watershed, in to its gauge); first of all */
  /** (T213) The map's day strip: set THE day, open the date sheet, and the days it may offer. */
  onDay: (d: Date | null) => void;
  onOpenDay: () => void;
  dayRange: DayRange;
  /** (T173) A fish obstacle mark tapped on the Map tab: the Shell raises its sheet. */
  onObstacle?: (props: Readonly<Record<string, unknown>>) => void;
  /** (T166) A tap on tidal water (no water under it) on the Map tab: the Shell raises the tidal sheet. */
  /** (T292) A tap on a sewage lagoon / tailings pond / dump (no water under it): its kind, for the not-fishing sheet. */
  onNotFishing?: (kind: NotFishingKind | null) => void;
  /** (T199, T261) A tap on the Runs map (at or above the handover): the Shell raises the runs sheet for that section. */
  /** (T369 D15) and where on it: the water it opens from the sheet is that tap's, "you are here" and all */
  onRuns?: (sid: number, at: { lat: number; lon: number } | null) => void;
  /** (T261) The fish art per species code (apps/mobile `FISH_IMAGES`): the Runs tab's species chips. */
  fishImages?: FishImages;
  /** (T186) The region under the map's centre, for the web prefetch (`useReleasePrefetch`); absent on a phone. */
  onRegion?: (regionId: string | null) => void;
  /** (T286) The regulations of the waters on screen, read while the map is still (`useRegsPrefetch`); null: none. */
  regsPrefetch?: RegsPrefetch | null;
  /** (T253) The regions the web's background prefetch holds for an unnamed-stream tap (More → Data); null: none runs. */
  offline?: { ready: readonly string[]; total: number } | null;
  /** (T566) the colouring asked of the map (`MapProps.lookKey`) and the key held until the map says it drew it */
  lookKey?: string; look?: HeldLookStore<unknown>;
}) {
  const { route, go, browse, tab, back, opener } = nav;
  const { flowParam, mm, layers, imagery, cond, gauges, lakeFeed, salmonFeed, methodFeed, mapStatus, noteVisible, setZoomedOut,
          horizonChipsShown, pickDay, vintage } = col;
  const { searching, session, restore, arrive, wideSearch, closeSearch } = search;
  /**
   * Tapping a map. A tile carries a `section_id`, and a section is not something a person asked about — they tapped a
   * river. `opener.tap` resolves it to the water and opens the water screen (`@app/ui` openWater.ts): with Regulations
   * in front from the Map tab, Conditions in front from the Conditions map. An unnamed reach (T53) opens the same way:
   * its frame offers Regulations (the store answers its section) and Conditions; only the water Map is a named water's.
   *
   * NOTHING IS TAPPABLE ON THE CONDITIONS MAP BELOW THE HANDOVER. Down there it is catchments, and the only thing under
   * a finger is a shape the size of a valley — a reach panel for it would answer a question about one river with a
   * number about a region. The rivers come back at the handover and they are what you tap (`tapOpensWater`).
   */
  const picking = rail.picking;
  const onPressFeature = useCallback(async (_layer: string, featureId: SectionKey, lat?: number, lon?: number) => {
    // PICK ON THE MAP: a tap places the spot's pin (`onMapPoint`) and opens nothing — a water under it included
    if (picking) return;
    // (T199, T261) on Runs a tap answers "which runs, when" for the section, not the regulations — and below the
    // handover it is the watershed groups, where nothing is tapped (as on Conditions)
    if (tab === "runs") {
      if (col.runsFeed && onRuns && (camera.current?.zoom ?? HOME.zoom) >= HANDOVER_Z) onRuns(Number(featureId), lat !== undefined && lon !== undefined ? { lat, lon } : null);
      return;
    }
    const conditions = tab === "conditions";
    if (!tapOpensWater({ tab, flowParam, zoom: camera.current?.zoom ?? HOME.zoom, handover: HANDOVER_Z })) return;
    // The tile's feature id IS the section handle — converted, not cast (see noteVisible).
    openProbe.start(`sid ${featureId}`);
    regsPrefetch?.stop();
    await opener.tap(Number(featureId) as SectionId,
                     lat !== undefined && lon !== undefined ? { lat, lon } : null, conditions ? "conditions" : "regs");
  }, [tab, flowParam, opener, picking, col.runsFeed, onRuns, regsPrefetch]);
  /** What More's Data block reads (T167: the only place a source's date and freshness are drawn): the release's dates, the pair, the feed's age. */
  const inseasonFetchedAt = useContext(InSeasonContext).data?.feed.fetchedAt ?? null;
  const dataFacts = {
    sources: loader?.manifest?.sources ?? null, validUntil: loader?.manifest?.regulations?.valid_until ?? null,
    tiles: vintage.tiles ?? null, tilesPaired: vintage.ok, feedFetchedAt: syncState.feedFetchedAt, dfoCheckedAt, now: clockNow(),
    // (T548) when the feeds worker last wrote the in-season feed this device holds: its Data row's age
    inseasonFetchedAt,
    // the feed's word on HYDAT (its maintenance notice), read with the sync state; never handed to the sync icon
    hydatNotice: syncState.hydatNotice,
    // (T68) the feed index's newest forecast run, for the Forecasts line
    forecastIssuedAt: syncState.forecastIssuedAt,
  };
  const { onConditions, modes, activeGroups } = mm;
  /*
   * (T213, owner 2026-10-28) THE MAP TAB'S OWN SWITCHES, on the map: the day in the corner Conditions keeps its horizons
   * in, and (T425) Trout / Salmon on the rail — while the day's regulations are what the map draws, and not
   * while search's sheet is over it.
   */
  const regsDrawn = tab === "map" && !searching && col.statusOk && layers.water.drawn === "regulations";
  // the day alone: Trout | Salmon is on the rail under locate + layers (T425, `railExtra`), the key at the foot (`MapLegend`)
  const corner = regsDrawn
    ? <View testID="map-corner-row" pointerEvents="box-none" style={{ flexDirection: "row", gap: 8, alignItems: "flex-start" }}>
        <MapDayStrip palette={palette} day={mapDay} range={dayRange} onDay={onDay} onOpen={onOpenDay} />
      </View>
    // (T367) the Runs tab's species moved into its bar at the foot (`RunsLegend`), with the rest of run timing
    : undefined;

  const railExtra = regsDrawn && col.salmonOffered
    ? <RegsLayerToggle col={col} palette={palette} fishImages={fishImages} look={look} /> : undefined;

  return tab === "spots"
    // (T48) THE SPOTS TAB: the list, open one, Show it on the map, add one — the long press on the maps stays
    ? <SpotsScreen palette={palette}
                   // no location module (native today): "unavailable", so the tab says why it is not offered
                   vm={spotsMenu(spots, { error: spotsError, canAdd: !!onSaveSpot, canRefresh: !!onRefreshSpots,
                                          permission: locate ? rail.permission : "unavailable" })}
                   onOpen={(id) => go({ type: "openSpot", id })}
                   onShow={(id) => { const s = spots.find((x) => x.id === id); if (s) rail.show(s); }}
                   // the two ways (owner 2026-10-22), each into the ONE capture flow at a point: where the device
                   // is, or the Map tab in pick mode until a tap places the pin (the drop sheet then asks)
                   onAdd={(k) => (k === "mine" ? void rail.addMine() : rail.pick())}
                   onRefresh={onRefreshSpots} refreshing={refreshing} error={rail.error}
                   photoStore={photos?.store} />
    : tab === "more"
      // (T48) THE MORE TAB: the data's freshness, the display options, the pages (each opens over it: MenuHost, a back
      // layer) and Contact; its ‹ returns to the tab it was opened from (the route's `prev`) — the one back
      ? <MoreScreen palette={palette}
                    data={{ ...dataFacts, newRelease: syncState.newRelease,
                            // (T231) the update service's sources and download, for the Updates row
                            updates: syncState.updates
                              ? { sources: syncState.sources, release: syncState.updates.release } : null,
                            // (T253) "Offline data: Region 2 ready"
                            offline }}
                    // (T231, T298) a phone downloads whole sets: the mobile-data setting (Always / Ask / Wi-Fi only)
                    metered={syncState.updates && syncState.setMobileData && Platform.OS !== "web"
                      ? { value: syncState.updates.mobileData, set: syncState.setMobileData } : null}
                    back={TABS.find((t) => t.k === (browse.prev ?? "map"))!.t} onBack={back}
                    contact={menuInfo?.contact ?? null} sync={sync} onSync={onSync}
                    theme={themeName} onTheme={onTheme}
                    onPick={(k) => (k === "report" ? menu.report(routeWhere(route, mapDay ? dayLabel(mapDay) : null))
                                    : menu.show(k))} />
      : tab === "map" || tab === "conditions" || tab === "runs"
        ? <MapScreen at={tiles} palette={palette} theme={theme} me={me} railExtra={railExtra} lookKey={lookKey} onLookDrawn={look?.drawn}
                     camera={camera.current}
                     // (T48) the top bar over the map: the corner's chips, the rail and the web map's controls go under it
                     insetTop={insetTop} insetBottom={insetBottom}
                     // the provisional pin a long press dropped, else the spot "Show" sent the map to — unless the
                     // spots are drawn, when that spot's own (selected) pin marks it (T131)
                     marker={rail.marker(spotsShown)}
                     // a searched town's pin, in the town's colour (its row's), so pin and row read as one thing
                     pins={searching && session.focus?.marker
                       ? [{ ...session.focus.marker, tone: palette.place }] : NONE_PINS}
                     // (T131) the saved spots, while the Layers sheet's Spots switch is on: each opens its spot
                     spots={spotLayer}
                     basemap={mm.basemap} onImageryError={imagery.tileFailed}
                     // WHERE THE CAMERA IS SENT, one subject at a time: a spot "Show" asked for (the rail forgets it
                     // whenever the route moves), else what search has picked (a water framed, a town flown to), else
                     // — once, as search closes — back to where the reader was before search moved it
                     fit={rail.fly ? { key: rail.fly.key, bbox: rail.fly.bbox }
                       : searching && session.focus
                         ? { key: session.focus.key, bbox: session.focus.bbox, refine: session.focus.refine }
                         : arrive ? arrive
                         : restore ? { key: restore.key, bbox: null, camera: restore.camera } : null}
                     // WHAT SEARCH LIGHTS, whole, while search is open — on either map; closed, nothing (the Conditions
                     // view lights a gauge's route with the same feature-state and must not inherit a search)
                     highlight={searching ? session.focus?.highlight ?? NONE : NONE}
                     standing={cond.standing}
                     lakes={lakeFeed}
                     salmon={salmonFeed}
                     method={methodFeed}
                     runs={col.runsFeed}
                     status={mapStatus}
                     // The dots belong to the Conditions tab — AND to the temperature
                     // choice, which is a question about the stations themselves and so
                     // must be answerable from the map without changing tab.
                     gauges={onConditions ? gauges ?? undefined : undefined}
                     onVisible={mm.asksVisible ? noteVisible
                       // (T286) the Map tab, zoomed in to where taps open waters: the waters on screen read ahead (a
                       // phone: every file is on it; the web fetches region files, and prefetches them its own way, T186)
                       : Platform.OS !== "web" && tab === "map" && !col.zoomedOut && !searching && regsPrefetch
                         ? regsPrefetch.onVisible : undefined}
                     onRegion={onRegion}
                     developer={menuInfo ? { info: menuInfo, release: loader?.release ?? null } : undefined}
                     onMoved={(at) => {
                       camera.current = at;
                       // (T376, T417) the group most on screen, asked at rest by the map's bounds (on Conditions
                       // only; a ref otherwise)
                       col.noteView(at);
                       // The ref is deliberate — see the Shell's camera — but ONE BIT of the camera is
                       // render state: which side of the handover we are on decides whether
                       // the field is drawn and whether a tap does anything. Set only when
                       // it changes, so panning still costs no renders.
                       setZoomedOut((was) => (at.zoom < HANDOVER_Z) === was
                         ? was : at.zoom < HANDOVER_Z);
                     }}
                     view="plain" modes={modes}
                     /*
                      * NOT `groups`. Which layers the Conditions view draws is stated in
                      * the style (`views[].hide`), because that is a property of the view
                      * rather than of a screen. This tried `{...activeGroups, admin: false}`
                      * and `admin` is deliberately not toggleable — the adapter refused the
                      * call, the boundaries kept drawing, and the console filled up.
                      */
                     groups={activeGroups}
                     // The list is the style's; the trigger is this screen's, because the
                     // app drives by mode and never renders the named view.
                     hide={tab === "conditions" ? hiddenLayers("conditions") : []}
                     // And the layers only that view draws (the flow arrows), while their switch is on
                     show={tab === "conditions"
                       ? shownLayers("conditions").filter((l) => layers.effective.layers[l] !== false) : []}
                     // The forecast horizons, on the Conditions tab (owner 2026-10-20, after
                     // P16 removed them). Temperature has no forecast, so it gets none. The
                     // SAME state the panel header sets — `useHorizon` in useMapColouring.
                     horizons={mm.horizonsShown ? { chips: horizonChipsShown, onPick: pickDay } : undefined}
                     corner={corner}
                     // LAYERS ON BOTH MAPS (owner 2026-10-22): on Conditions the sheet is that map's own —
                     // Show (Flow · Level · Temperature), the ground, gauges and arrows — never the Map tab's
                     onLayers={tab === "runs" ? undefined : onLayers}
                     // how many of THIS map's layer choices differ from the defaults: the rail's badge, said aloud
                     layersChanged={layers.changed}
                     // press and hold: a provisional pin, and the sheet that asks before anything is made
                     onLongPress={onSaveSpot ? rail.pressLong : undefined}
                     // either map's tap opens the ONE water screen (see onPressFeature): only the view in front differs
                     // — except in pick mode, where a tap is the spot's place and nothing else
                     // — and on a WIDE screen while search is open, a click on the map beside the panel closes search
                     // and does nothing else (owner 2026-10-22 late): no water opens under it, no pin drops
                     onPressFeature={picking || wideSearch ? undefined : onPressFeature}
                     // (T173) a fish obstacle mark (drawn only while its Layers switch is on, Map tab only): its sheet,
                     // and no water opens under it
                     onPressObstacle={picking || wideSearch || tab !== "map" ? undefined : onObstacle}
                     // (T166) tidal water, asked last (Map tab only): its sheet
                     // (T292) a sewage lagoon / tailings pond / dump (Map tab only): what it is, never regulations
                     onPressNotFishing={picking || wideSearch || tab !== "map" ? undefined : onNotFishing}
                     // and anywhere off a spot, a tap clears the selected spot's pin (T145, `mapPointTap`)
                     onMapPoint={mapPointTap(rail, wideSearch ? closeSearch : null)}
                     onError={mapError} />
        : null;
}
