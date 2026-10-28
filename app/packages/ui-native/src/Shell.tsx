/**
 * The app: the map as home, and ONE PLACE FOR EACH KIND OF THING (T48, owner 2026-10-22): the top bar is the search field
 * alone (with the sync icon at its right end), the side rail is Layers alone, and the bottom bar is where you go — Map ·
 * Conditions · Spots · More. A legend always says what the colours mean.
 *
 * WHAT THE SHELL DOES TODAY (N13 / T62 3.4 — re-specified against this code, not the P14/P17 plans):
 *   - ROUTE. One value (`@app/ui` route.ts) says what is on screen; every move dispatches to it, and ONE back
 *     (`useBack`, the system back and the iOS edge swipe) unwinds it — `shell/useShellNav.ts`.
 *   - ONE WATER SCREEN. Every water opens through `@app/ui` openWater.ts (`useOpenWater`), from either map, a water's
 *     own map or a search result; water mode is a frame (`waterFrame`) whose views — Regulations · Conditions · Map —
 *     sit under one header (`WaterFrame`, fed by `WaterViewsContext`) — `shell/WaterModeHost.tsx`.
 *   - BROWSE. The Map and Conditions tabs share ONE map; Spots and More are lists — `shell/BrowseHost.tsx`.
 *   - COLOURING. The layers choice, the tiles/data pair (nothing is coloured on an unproven pair, a mixed one is said),
 *     the day's status, the lake values and the Conditions colouring at its horizon — `shell/useMapColouring.ts`;
 *     the legend and the pair banner say what it means — `shell/MapLegend.tsx`.
 *   - SEARCH over the same map, giving the camera back as it closes — `shell/useShellSearch.ts`; the field (`TopBar`)
 *     and the results (`SearchSheet`) are mounted here.
 *   - A SPOT open over a map, its edit held here so leaving cannot strand it — `shell/SpotHost.tsx`; the add flow is
 *     the one capture (`SpotCapture`).
 *   - SHEETS. Layers and the map's day (drawn here, beside their open state); the dropped pin and the menu's pages
 *     (`shell/ShellSheets.tsx`) — each a back layer.
 * The Shell holds only the state more than one of these needs (the camera, the day, which sheet is open) and hands it
 * down. Every answer on every screen comes from a hook (rule 25).
 */
import { dayRange, placeholderFor, syncVM } from "@app/core";
import { useMemo, useRef, useState } from "react";
import { View } from "react-native";
import { bottomBarShown, spotPins, useDayFlow, useLikeVisit, useMapRail, useMenu, useDataFacts, useSync,
         BackLayerContext, useReleasePrefetch, type RegsStart } from "@app/ui";
import type { Camera, MapSpots } from "@app/map";
import { connectivity } from "./connectivity";
import { SearchSheet } from "./SearchSheet";
import { TopBar, topBarBottom } from "./TopBar";
import { SpotCapture } from "./SpotCapture";
import { SpotPickBanner } from "./SpotPickBanner";
import { SpotPin } from "./SpotPin";
import { TabBar } from "./TabBar";
import { EdgeBack } from "./EdgeBack";
import { LayersSheet } from "./LayersSheet";
import { DaySheet } from "./regs/DaySheet";
import { WaterViewsContext } from "./ViewSwipe";
import { MenuContext } from "./menu/MenuContext";
import { BrowseHost } from "./shell/BrowseHost";
import { MapLegend, PairBanner } from "./shell/MapLegend";
import { ShellSheets } from "./shell/ShellSheets";
import { SpotHost, useSpotEdit } from "./shell/SpotHost";
import { WaterModeHost } from "./shell/WaterModeHost";
import type { ShellProps } from "./shell/props";
import { HOME, useMapColouring } from "./shell/useMapColouring";
import { useShellNav } from "./shell/useShellNav";
import { useEscapeBack } from "./escapeBack";
import { useShellSearch } from "./shell/useShellSearch";

export type { ShellProps } from "./shell/props";

export function Shell(p: ShellProps) {
  const { source, palette, theme, tiles, spots = [], onSaveSpot, weather, feed, photos, loader = null, locate = null,
          menuInfo, checkRelease = null, releaseNamed = null, hydat } = p;
  const nav = useShellNav({ source, loader, systemBack: p.systemBack ?? null, initialRoute: p.initialRoute,
                           onRoute: p.onRoute });
  /** (T186) the map's region reports drive the web prefetch; undefined (a phone): the map is not asked. (T182) The
   *  Salmon / Bait & fly files wait for its idle start on the web (`settled`), so first paint fetches paint alone. */
  const [settled, setSettled] = useState(!(p.prefetch && loader));
  const onRegion = useReleasePrefetch(p.prefetch ? loader : null, () => setSettled(true));
  const { route, go, tab, frame, adding, openSpot, back, backLayers } = nav;
  /*
   * EVERY SURFACE IS UNDER THE ONE BACK (app/HANDOFF-ui.md "Back, everywhere"): whatever the Shell returns — the spot,
   * the capture, the tabs and the water — is inside `BackLayerContext`, so a sheet, menu, confirm or step drawn
   * anywhere in it registers with the one back (`useBackLayer`) instead of falling back to its own close, which the
   * system back would go under.
   */
  const layered = (n: React.ReactNode) => <BackLayerContext.Provider value={backLayers}>{n}</BackLayerContext.Provider>;
  /** The day the map's status colours answer for; null is today. */
  const [mapDay, setMapDay] = useState<Date | null>(p.initialDay ?? null);
  /** (T41) the start link's page start, until the page it opened has applied it (a later water never sees it) */
  const [start, setStart] = useState<RegsStart | null>(p.regsStart ?? null);
  const [dayOpen, setDayOpen] = useState(false);
  const [layersOpen, setLayersOpen] = useState(false);
  /**
   * The camera survives leaving the map.
   *
   * Opening a water's sheet unmounts the map, so without this it remounted at HOME and a
   * reader who had panned to the Skeena came back to Chilliwack. A ref rather than state:
   * every pan would otherwise re-render the whole shell, and nothing on screen depends on
   * the camera except the map itself.
   */
  const camera = useRef<Camera>(HOME);
  /** The water's map view keeps its own camera (P15), across waters, so the browse map's is left where it was. */
  const waterCamera = useRef<Camera | null>(null);
  const search = useShellSearch({ source, nav, camera });
  const { searching, session, searchMemory, field } = search;
  useEscapeBack(nav.back, nav.escapeBack && !searching);   // T214 item 2: on the desktop web Escape is Back (a no-op on a phone)
  /** ONE placeholder line per opening of the app, never cycled while on screen. */
  const [placeholder] = useState(() => placeholderFor(Math.random() * 1e6));
  /*
   * THE MAP RAIL (owner 2026-10-21): the Spots menu, the provisional pin a long press drops, and where "Show" sent the
   * map — `@app/ui` `useMapRail`. Every way of adding a spot ends in the route's `addSpot` at a point: one capture.
   */
  const rail = useMapRail(go, locate, route);
  /** THE MENU (owner 2026-10-21): its pages, listed by the More tab (T48), mounted below as back layers (menu/MenuHost). */
  const menu = useMenu();
  /*
   * HOW MUCH DATA IS IN HERE — read, never stated (rule 32): the figures are drawn by the search sheet (its total),
   * asked only while it is up (P11: on the release the counts open core, which a cold start does not fetch first).
   */
  const facts = useDataFacts(source, feed, searching);
  const counts = facts.state === "ready" ? facts.value.counts : null;
  const spotEdit = useSpotEdit(photos, openSpot);
  /*
   * THE SYNC STATE (owner 2026-10-22): when the flows were last published, whether a newer data release exists, and the
   * icon's refresh — ONE reading of the feed's age for the top bar's icon and More's Data block alike.
   */
  const syncState = useSync({ feed, release: loader?.release ?? null, checkRelease, named: releaseNamed });
  const col = useMapColouring({ source, feed, status: p.status ?? null, pairing: p.pairing ?? null, tiles,
                                layerStore: p.layerStore, attribution: p.attribution, mapTab: nav.mapTab, mapDay,
                                epoch: syncState.epoch, settled });
  /*
   * YOUR SPOTS ON EVERY MAP (owner 2026-10-22: "toggle our spots on and off in the layer menu"), while the Layers
   * sheet's Spots switch is on (default on); (T131) each a mark that opens its spot, drawn by `SpotPin`, the selected
   * one the rail's; a group no zoom parts opens the Spots list.
   */
  const spotsShown = col.layers.effective.pins.spots;
  const spotPoints = useMemo(() => spotPins(spots, spotsShown), [spots, spotsShown]);
  const spotLayer: MapSpots | undefined = spotPoints.length === 0 ? undefined : {
    points: spotPoints, selected: rail.selected, onOpen: rail.openPin, onList: () => rail.listSpots(),
    face: (m, press) => <SpotPin m={m} palette={palette} press={press} />,
  };
  /** The SYNC ICON's state (`syncVM`): fresh, needs a refresh, checking — none when there is nothing to sync. */
  const sync = feed || checkRelease
    ? syncVM({ feed: !!feed, feedFetchedAt: syncState.feedFetchedAt, checkedAt: syncState.checkedAt,
               checking: syncState.checking, newRelease: syncState.newRelease, now: Date.now() })
    : null;
  const barBottom = topBarBottom();

  const viewing = openSpot === null ? null : spots.find((s) => s.id === openSpot) ?? null;
  // T51: the open spot against the coming days — the feed the rest of the app reads, and the weather port; asked
  // again when the device comes back online
  const like = useLikeVisit(viewing, { weather, feed }, p.likeStore, Date.now, connectivity);
  // T170: the visit day's own flow, and the action that asks for it (waits offline, asks again when back)
  const dayFlow = useDayFlow(viewing, p.onDayFlow, connectivity);
  if (viewing)
    return layered(
      <SpotHost viewing={viewing} spotEdit={spotEdit} nav={nav} palette={palette} camera={camera} like={like}
                dayFlow={dayFlow} photos={photos} onSaveSpot={onSaveSpot} onDeleteSpot={p.onDeleteSpot}
                onRefreshSpots={p.onRefreshSpots} refreshing={p.refreshing} show={rail.show} />);

  if (adding && onSaveSpot)
    return layered(
      <SpotCapture source={source} weather={weather} tiles={tiles} palette={palette} photos={photos}
                   // started at a point (a long press, a tap in pick mode, your location): the pin is already down
                   at={adding.at}
                   // So the spot freezes the ESTIMATE the reader was looking at,
                   // not just the matched station's own number.
                   feed={feed}
                   // Q161: a trip on an earlier day is read from that day's record (HYDAT when online)
                   history={{ ...(hydat ? { hydat } : {}),
                              ...(connectivity.watches ? { online: connectivity.online } : {}) }}
                   theme={theme} camera={camera.current}
                   onCancel={back}
                   // awaited: a rejection reaches SpotCapture, which shows it and stays open (P01)
                   // saved: the spot itself, over the map it was added from (back returns to that map)
                   onSaved={async (s) => { await onSaveSpot(s); go({ type: "openSpot", id: s.id }); }} />
    );

  const body = frame !== null
    ? <WaterModeHost frame={frame} nav={nav} col={col} source={source} palette={palette} loader={loader} tiles={tiles}
                     theme={theme} feed={feed} rail={rail} spotLayer={spotLayer} camera={camera}
                     waterCamera={waterCamera} fishImages={p.fishImages} adiposeImages={p.adiposeImages}
                     mapDay={mapDay} onDay={setMapDay} start={start} onStarted={() => setStart(null)} />
    : <BrowseHost nav={nav} col={col} search={search} rail={rail} palette={palette} theme={theme}
                  themeName={p.themeName} onTheme={p.onTheme} tiles={tiles} camera={camera} insetTop={barBottom}
                  spots={spots} spotsShown={spotsShown} spotLayer={spotLayer} spotsError={p.spotsError}
                  onRefreshSpots={p.onRefreshSpots} refreshing={p.refreshing} locate={locate} photos={photos}
                  loader={loader} syncState={syncState} menu={menu} menuInfo={menuInfo} mapDay={mapDay}
                  onSaveSpot={onSaveSpot} onLayers={() => setLayersOpen(true)} onRegion={onRegion} />;

  /** (T48) The top bar — the search field — is the maps': on the Map and Conditions tabs while browsing. */
  const topBar = route.mode === "browse" && (tab === "map" || tab === "conditions") && !rail.picking;
  // the legend is the map's caption: while search's sheet is over the map it captions nothing in view
  const showLegend = route.mode === "browse" && !searching
    && (tab === "conditions"
        || (tab === "map" && (col.layers.water.drawn !== "regulations" || col.statusOk)));

  return layered(
    // a regulations tab's "Report a problem with this page" reads the menu from here (menu/ReportLink)
    <MenuContext.Provider value={menu}>
    <View style={{ flex: 1, backgroundColor: palette.card }}>
      <PairBanner col={col} palette={palette} />
      <View style={{ flex: 1 }}>
        <EdgeBack drag={nav.edge}>
          <WaterViewsContext.Provider value={nav.waterViews}>
            {/* Regulations and Conditions take the swipe anywhere on them (the Conditions maps and chart shielded),
                Map from its header only (the canvas pans) — see `waterViews` */}
            {/* (T37) the regs sheet is a back layer too: back closes it before it leaves the water */}
            <BackLayerContext.Provider value={backLayers}>{body}</BackLayerContext.Provider>
          </WaterViewsContext.Provider>
        </EdgeBack>
        {/*
          * (T48) search over the SAME map: the sheet rises over it, and the field is drawn over both. The field comes
          * FIRST in the page (T69): Tab reaches the bar, then the results panel under it — the order the eye reads;
          * its zIndex keeps it painted over the sheet and the dim.
          */}
        {topBar && (
          <TopBar ref={field} palette={palette} placeholder={placeholder} sync={sync} onSync={syncState.refresh}
                  open={searching}
                  query={searchMemory.q} onQuery={search.searchType}
                  onOpen={search.onFieldFocus}
                  onClose={search.closeToField} />
        )}
        {topBar && (
          <SearchSheet open={searching} top={barBottom} palette={palette} session={session} pick={searchMemory.pick}
                       onAct={search.searchAct}
                       // the one way out of search into a water: the one opener, with the view this map puts in front;
                       // it consumes the search, so back lands on the map at the water (owner 2026-10-22 late)
                       onOpenWater={search.openSearched}
                       onClose={search.closeToField}
                       statusIndex={col.statusIndex} total={counts?.waters} />
        )}
        {/* PICK ON THE MAP (owner 2026-10-22): the slim banner in the search field's place; a back layer */}
        {rail.picking && route.mode === "browse" && tab === "map" && (
          <SpotPickBanner palette={palette} onCancel={rail.cancelPick} />
        )}
      </View>
      {showLegend && (
        <MapLegend col={col} conditions={tab === "conditions"} palette={palette} theme={theme} mapDay={mapDay}
                   onDay={() => setDayOpen(true)} />
      )}
      {/* A tab change is a change of subject: the remembered reach goes with it. Hidden in water mode (P13/P14). */}
      {bottomBarShown(route) && <TabBar active={tab} palette={palette} onChange={nav.toTab} />}
      {/* G1: an open sheet is a back layer — every platform's back closes it before it pops the route */}
      <LayersSheet open={layersOpen} onClose={() => setLayersOpen(false)} palette={palette}
                   vm={col.layers} onChoose={col.onChooseLayer} />
      {/*
        * (T49) The map's date sheet: its calendar button opens the SAME sheet the Regulations page's date pill does
        * (`regs/DaySheet`), on this one `mapDay` — and the days it offers are the data's calendar year, cut at the
        * release's last day (`dayRange`), the same range the page's sheet is handed by `WaterScreen`.
        */}
      <DaySheet open={dayOpen && route.mode === "browse"} onClose={() => setDayOpen(false)} palette={palette}
                day={mapDay} onDay={setMapDay}
                range={dayRange(new Date(), loader?.manifest?.regulations?.valid_until ?? null)} />
      <ShellSheets nav={nav} col={col} rail={rail} menu={menu} palette={palette} loader={loader} menuInfo={menuInfo}
                   mapDay={mapDay} />
    </View>
    </MenuContext.Provider>
  );
}
