/**
 * The app: the map as home, and ONE PLACE FOR EACH KIND OF THING (T48, owner 2026-10-22): the top bar is the search field
 * alone (with the sync icon at its right end), the side rail is Layers alone, and the bottom bar is where you go — Map ·
 * Conditions · Spots · More. A legend always says what the colours mean.
 *
 * The shell owns the state that more than one screen needs —
 * which tab, which water, which colouring, which layers, which palette — and hands it down.
 * Every answer on every screen comes from a hook (rule 25). The Map tab colours water by the
 * day's status from the status index (closed / own regulations / base); the regulation records
 * themselves are not integrated, so a water's sheet shows a placeholder where they will go.
 */
import { CHANGE_LEGEND, changeMarkWords, chooseLayer, chooseShow, dayLabel, dayRange, layersVM, placeholderFor, syncVM,
         spotBack, TEMP_LEGEND,
         unprovenLine, WATER_STATUS, WATER_STATUSES, waterExit, waterLegend, withImageryCredit,
         type SectionKey } from "@app/core";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Keyboard, Platform, Text, View, useWindowDimensions, type TextInput } from "react-native";
import type { ItemId, RegsSource, SectionId } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { bottomBarShown, frameViews, spotSaveFailed, useMapModes, useOpenWater, crossAction, exitAction, partAction,
         regsGroupOf, showRegsActions, usePhotoEdit, useRoute, waterFrame,
         type BrowseTab, type FlowParam, type RegsStart, type Route, type SpotPhotos } from "@app/ui";
import { BackLayerContext, edgeBackDepth, useBack, useEdgeBack, useMenu, useOnLeave, useViewSwipe, type SystemBack } from "@app/ui";
import { mapPointTap, routeWhere, spotPins, spotsMenu, useLayerChoice, useLikeVisit, useMapRail, type LayerStore,
         type Locate } from "@app/ui";
import { useConditionsColour, useDataFacts, useGaugeGeoJSON, useSync, useLakeColour, lakeModesReady, lakeValuesCarried,
         NO_SEARCH, searchStep, type SearchAction, type SearchMemory, useSearchSession, usePairing, useRegulationKeys,
         useStatusIndex, statusFeed, useHorizon, useHorizonPick, flowHorizon, horizonChips, tapOpensWater, useImageryReach, type Horizon, type PairingInput,
         type StatusSource } from "@app/ui";
import type { LikeStore, Spot, WeatherSource } from "@app/data/spots";
import { FIELD_HANDOVER_Z, hiddenLayers, shownLayers,
         type Camera, type MapSpots, type TileEndpoints, coloursEachTile, imageryTiles } from "@app/map";
import { connectivity } from "./connectivity";
import { imageryProbe } from "./imageryProbe";
import { LegendCount, LegendRamp, LegendStrip } from "./Chrome";
import { MapDayButton } from "./DayCalendar";
import { DaySheet } from "./regs/DaySheet";
import { LayersSheet, legendColour } from "./LayersSheet";
import { MapScreen } from "./MapScreen";
import { SearchSheet } from "./SearchSheet";
import { SpotsScreen } from "./SpotsScreen";
import { TopBar, searchPanelWidth, topBarBottom } from "./TopBar";
import { CLOSE_KEEPS_FOCUS } from "./searchFocus";
import { SpotDropSheet } from "./SpotDropSheet";
import { SpotPickBanner } from "./SpotPickBanner";
import { SpotCapture } from "./SpotCapture";
import { SpotPin } from "./SpotPin";
import { SpotScreen } from "./SpotScreen";
import { TABS, TabBar, type TabKey } from "./TabBar";
import { WaterScreen } from "./WaterScreen";
import { EdgeBack } from "./EdgeBack";
import { WaterViewsContext, type WaterViews } from "./ViewSwipe";
import { WaterConditions, WaterMap } from "./WaterViews";
import { WaterMapView } from "./water/WaterMapView";
import { MenuContext } from "./menu/MenuContext";
import { MenuHost, type MenuInfo } from "./menu/MenuHost";
import { MoreScreen } from "./menu/MoreScreen";
import type { AdiposeImages, FishImages } from "./regs/types";
import { TYPE } from "./type";
import { changeRampStops, flowRampStops, type Palette, type ThemeName } from "./theme";

/** No highlight — one frozen empty list, so the map's effect is not re-run every render. */
const NONE: readonly SectionKey[] = [];
const NONE_PINS: readonly never[] = [];

/** Where the map starts the FIRST time. After that the camera is whatever the user left. */
// z10, above the field's handover: the app opens on rivers you can tap on the Conditions map too
const HOME: Camera = { lon: -121.85, lat: 49.15, zoom: 10 };

/**
 * WHERE THE FIELD HANDS OVER TO THE RIVERS: the style's one number (`FIELD_HANDOVER_Z`, the basin layer's maxzoom),
 * read, never typed here (T130, owner 2026-10-22: "should be able to zoom out quite a bit more first").
 *
 * Below it the Conditions map is catchments and NOTHING IS TAPPABLE: a tap would open a
 * reach sheet for a shape the size of a valley, answering a question nobody asked. Above it
 * the rivers are back and they are what you tap.
 */
const HANDOVER_Z = FIELD_HANDOVER_Z;

export function Shell({ source, palette, theme, themeName, onTheme, tiles,
                       spots = [], onSaveSpot, weather, onRefreshSpots, onDeleteSpot, refreshing, feed,
                       spotsError, attribution, fishImages, adiposeImages, photos, status = null, pairing = null,
                       systemBack = null, loader = null, initialRoute, initialDay = null, regsStart = null, locate = null, menuInfo,
                       checkRelease = null, layerStore, likeStore }: {
  source: RegsSource; palette: Palette;
  /**
   * Where the day's status comes from: the release's paint (P11), or on the dev bundle path status_index.bin. Absent
   * -> the water is drawn plain.
   */
  status?: StatusSource | null;
  /** What says the data and the tiles are one pair (P11 `usePairing`). Absent -> not established: nothing coloured. */
  pairing?: PairingInput | null;
  /** Passed straight to the map. Every theme the style defines is a real map theme,
   *  so nothing has to be laundered into light/dark on the way. */
  theme: string; themeName: ThemeName; onTheme: (t: ThemeName) => void;
  /** Where the two pmtiles archives are served from. */
  tiles: TileEndpoints;
  /** The user's pins. Empty is the normal first-run state, not a failure. */
  spots?: readonly Spot[];
  /** Where a captured spot goes. Absent -> the Add button is inert and says nothing. */
  onSaveSpot?: (spot: Spot) => void | Promise<void>;
  /** Weather at capture time. Absent -> the spot records that it does not know. */
  weather?: WeatherSource;
  /** Fill in what spots could not know when they were made. */
  onRefreshSpots?: () => void | Promise<void>;
  /** Awaited like a save: a rejection is shown and the spot stays open (P01). */
  onDeleteSpot?: (id: string) => void | Promise<void>;
  /** The spots could not be read (a corrupt file, set aside): said in the Spots menu, never an empty list (P01). */
  spotsError?: string | null;
  /**
   * The live gauge feed. Absent means the Conditions view colours nothing — which is the
   * honest rendering of "we could not check", and never a map of droughts.
   */
  feed?: import("@app/data").GaugeFeed;
  refreshing?: boolean;
  attribution?: readonly string[];
  /** The fish pictures by species code, registered by the app (T20). Absent -> silhouettes. */
  fishImages?: FishImages;
  /** (T38) the hatch sheet's two drawings (Wild / Hatchery · clipped), registered by the app. */
  adiposeImages?: AdiposeImages;
  /** S1: a spot's photographs — the store and the platform's picker (the app's). Absent -> no photos offered. */
  photos?: SpotPhotos;
  /** G1: the platform's own back (Android `BackHandler`, the browser's history), from the app. Absent -> ‹ only. */
  systemBack?: SystemBack | null;
  /** T01: the release loader (P11), handed to the water screen for its Regulations page. null: the dev bundle. */
  loader?: ReleaseLoader | null;
  /** Where the app opens (a deep link, a test); the map tab when absent. Read once, on mount. */
  initialRoute?: Route;
  /** (T41) the day it opens on (the web start link); today when absent. Read once, on mount. */
  initialDay?: Date | null;
  /** (T41) where the first water's Regulations page starts (the web start link): handed to that page once. */
  regsStart?: RegsStart | null;
  /** The platform's location, for the Spots menu's "At my location" (the app's). Absent -> not offered. */
  locate?: Locate | null;
  /** The menu's build facts (app build, the one contact value), from the app's config. Absent -> neither stated. */
  menuInfo?: MenuInfo;
  /**
   * (The sync icon) Reads the data release the server names NOW (the manifest's `data_release`), to say when a newer
   * one is available. Absent: nothing to check (the dev bundle). Its identity must be stable (the app's useCallback).
   */
  checkRelease?: (() => Promise<string | null>) | null;
  /**
   * Where the Layers sheet's choice is kept across a reload (`@app/data/spots` `openLayerStore`): the Spots switch and
   * every other (owner 2026-10-22). Absent: the defaults, kept for the session only.
   */
  layerStore?: LayerStore;
  /**
   * T51: where an open spot's last fetched "Like your visit?" forecast is kept (`@app/data/spots` `openLikeStore`), so
   * the strip works offline. Absent: fetched each time a spot opens, nothing kept.
   */
  likeStore?: LikeStore;
}) {
  /*
   * HOW MUCH DATA IS IN HERE — read, never stated.
   *
   * These five figures used to arrive as props from `App.tsx`, hardcoded from the design
   * mock: 255 waters, 6,967 reaches, 35 surveyed lakes, 5 stations and a fixed timestamp.
   * The bundle the app actually opens holds 19,699 waters and 2,324 stations. A screen may
   * not assert a fact about the data it is sitting on; it asks — when a screen that shows them is up (P11: on the
   * release the counts open core, which a cold start does not fetch before the first map). Read below, by `factsWanted`.
   */
  /*
   * THE ROUTE (P13): what is on screen is ONE value, moved by one reducer (`@app/ui` route.ts). The tab, the water
   * open, the spot, the add flow and the Conditions drill are read from it; every way of moving dispatches to it.
   * Water mode keeps the browse state underneath (`from`), so back restores it whole.
   */
  const [route, go] = useRoute(initialRoute);
  const browse = route.mode === "browse" ? route : route.from;
  const tab: TabKey = browse.tab;
  const frame = waterFrame(route);
  const item: ItemId | null = frame?.item ?? null;
  // the water's regulations read (P11): on the release, opening it opens core, water and its region's file — a water in
  // another region fetches that region's. The page that draws them is T01/T03's; until then the placeholder stands.
  useRegulationKeys(source, item);
  const adding = route.mode === "browse" ? route.adding : null;
  // a spot open over the browse map, or (T131) over a water, its pin tapped on the water's map
  const openSpot = route.mode === "browse" ? route.spot : route.spot ?? null;
  const toTab = (k: BrowseTab) => go({ type: "tab", tab: k });
  /*
   * G1: ONE BACK for every input — ‹, the platform's system back, the iOS edge swipe.
   */
  const { back, depth, layers: backLayers } = useBack(route, go, systemBack);
  // iOS: no tab-to-tab swipe (tabs are not history there); Android and the web keep that step
  const edge = useEdgeBack(back, Platform.OS, edgeBackDepth(route, depth));
  /**
   * EVERY SURFACE IS UNDER THE ONE BACK (app/HANDOFF-ui.md "Back, everywhere"): whatever the Shell returns — the spot,
   * the capture, the tabs and the water — is inside `BackLayerContext`, so a sheet, menu, confirm or step drawn
   * anywhere in it registers with the one back (`useBackLayer`) instead of falling back to its own close, which the
   * system back would go under.
   */
  const layered = (n: React.ReactNode) => <BackLayerContext.Provider value={backLayers}>{n}</BackLayerContext.Provider>;
  /*
   * ONE WATER SCREEN, WHICHEVER MAP YOU TAPPED (owner rule 2026-10-21): every water opens through `@app/ui`
   * `useOpenWater` — a tap on the Conditions map, on the Map tab and on a water's own map alike — which resolves the
   * tapped section's water in one place. The two maps' taps differ ONLY in the view put in front.
   */
  /*
   * (T58) The opener reads THE ROUTE NOW, so a slow lookup that finishes after the reader pressed back or × (or moved
   * on) is dropped, and a split lake's areas from the release, so "Flows into" another area of a lake held goes back to it.
   */
  const routeNow = useRef(route);
  routeNow.current = route;
  const now = useCallback(() => routeNow.current, []);
  const groupOf = useMemo(() => (loader ? regsGroupOf(loader) : undefined), [loader]);
  const opener = useOpenWater(source, go, { now, group: groupOf });
  /** G1 (owner 2026-10-21): a sideways swipe moves between a water's views — Regulations · Conditions · Map. */
  const viewSwipe = useViewSwipe(route, go);
  /**
   * THE MAP UNDER A WATER'S MAP VIEW asks the Map tab's questions (the day's status), whatever tab the water was
   * opened from — a water opened from the Conditions map must not paint its map with flow percentiles.
   */
  const mapTab: BrowseTab = route.mode === "water" ? "map" : tab;
  /** The day the map's status colours answer for; null is today. */
  const [mapDay, setMapDay] = useState<Date | null>(initialDay);
  /** (T41) the start link's page start, until the page it opened has applied it (a later water never sees it) */
  const [start, setStart] = useState<RegsStart | null>(regsStart);
  /**
   * (T49) The map's date sheet: its calendar button opens the SAME sheet the Regulations page's date pill does
   * (`regs/DaySheet`), on this one `mapDay` — and the days it offers are the data's calendar year, cut at the release's
   * last day (`dayRange`), the same range the page's sheet is handed by `WaterScreen`.
   */
  const [dayOpen, setDayOpen] = useState(false);
  /*
   * (T48) SEARCH, OVER THE SAME MAP: the top bar's always-open field opens it (route `search`, on either map), and the
   * results sheet rises over the map that is already there — no second map. What the reader typed and picked is kept
   * here while search is open; a water opened from the results consumes it (back lands on the map). ONE session value
   * (`useSearchSession`) feeds both the sheet's rows and what the main map frames, lights and pins.
   */
  const searching = browse.search;
  /** The screen's width: a wide one draws search as a panel beside the map (`searchPanelWidth`). */
  const screenWidth = useWindowDimensions().width;
  const [searchMemory, setSearchMemory] = useState<SearchMemory>(NO_SEARCH);
  const session = useSearchSession(source, searchMemory, searching);
  /** ONE placeholder line per opening of the app, never cycled while on screen. */
  const [placeholder] = useState(() => placeholderFor(Math.random() * 1e6));
  const field = useRef<TextInput>(null);
  const [layersOpen, setLayersOpen] = useState(false);
  /*
   * THE MAP RAIL (owner 2026-10-21): the Spots menu, the provisional pin a long press drops, and where "Show" sent the
   * map — `@app/ui` `useMapRail`. Every way of adding a spot ends in the route's `addSpot` at a point: one capture.
   */
  const rail = useMapRail(go, locate, route);
  /** THE MENU (owner 2026-10-21): its pages, listed by the More tab (T48), mounted below as back layers (menu/MenuHost). */
  const menu = useMenu();
  // the figures are drawn by the search sheet (its total); the feed's age is the sync state's (`useSync`, below)
  const facts = useDataFacts(source, feed, searching);
  const counts = facts.state === "ready" ? facts.value.counts : null;
  // Non-null only while editing. The draft lives here rather than in SpotScreen so that
  // leaving the screen cannot strand half an edit inside an unmounted component.
  const [edit, setEdit] = useState<{ title: string; notes: string } | null>(null);
  // S1: the photos of the spot being edited; files added in an edit that is not saved are deleted with it
  const photoEdit = usePhotoEdit(photos);
  // P01: a failed save or delete, said on the spot's screen; and a tab change asked for while an edit is unsaved
  const [spotError, setSpotError] = useState<string | null>(null);
  const [pendingTab, setPendingTab] = useState<TabKey | null>(null);
  // G1: however the spot is left (‹, Android's back, the browser's back, a tab), its edit ends with it — else the next
  // spot opens in edit mode with this one's text and Save writes it there
  useOnLeave(openSpot, () => { if (edit) void photoEdit.discard(); setEdit(null); setSpotError(null); setPendingTab(null); });
  /**
   * The reaches the map currently has rendered — what the Conditions view asks the panel
   * about. Viewport-scoped because the whole table is far too big to hold for a question
   * about the few hundred reaches actually on screen.
   */
  const [visible, setVisible] = useState<readonly SectionId[]>([]);
  // Percentiles for what is on screen. Empty while the feed is unreachable, which paints
  // nothing rather than painting every reach as a drought.
  // STABLE IDENTITY MATTERS HERE. As an inline arrow this changed every render, so the
  // effect holding the map's `idle` listener tore itself down and re-registered constantly
  // — and `idle` fires once when movement settles, so the listener was frequently absent
  // at the only moment it had anything to hear. The map reported nothing, `visible` stayed
  // empty, and every reach painted as `missing`.
  const noteVisible = useCallback(
    // CONVERTED, NOT CAST. This was `ids as readonly SectionId[]`, which asserted a fact
    // rather than establishing one — and when the map began reporting stringified ids the
    // cast said nothing while every lookup missed. A section handle is an integer; make it
    // one here, at the single point where map ids become bundle keys.
    (ids: readonly SectionKey[]) =>
      setVisible(ids.map((i) => Number(i) as SectionId)), []);
  /*
   * HOW FAR AHEAD the Conditions map is painted and the panel answers — ONE state, two
   * controls (owner 2026-10-20): the map's corner chips and the panel header's both draw
   * `hz.chips` and call `hz.pick`. 0 is now, and is where it opens. NOT reset when the panel
   * closes (P16 did, because the map then had no control saying which horizon it painted;
   * it has one again, showing the same choice).
   */
  const hz = useHorizon();
  // Which side of the handover zoom the camera is on — the one bit of the camera that is render state (see onMoved).
  const [zoomedOut, setZoomedOut] = useState(HOME.zoom < HANDOVER_Z);
  /*
   * THE LAYERS CHOICE (owner 2026-10-21 late, layers menu B): one ground, ONE water colouring for every feature, the
   * Conditions map's ONE quantity (Show: Flow · Level · Temperature — owner 2026-10-22, never two on one ramp) and the
   * switches — the only layer state kept here. The quantity is held here, not in the map, because the panel a tap opens
   * has to be about the same thing the map is showing: its chart's own quantity control sets this same choice. What the map is handed is the registry's (`layersVM(...).effective`,
   * below): an unavailable choice falls back while the choice itself is kept.
   */
  const [layerChoice, setLayerChoice] = useLayerChoice(layerStore);
  /*
   * TILES AND BUNDLE FROM ONE ATLAS, or nothing is coloured.
   *
   * A section is an integer handle into the atlas's table, and both artifacts carry it. A
   * mixed pair does not fail to match — it matches the WRONG SECTION, so every colour on
   * screen would be a real reading about a different river. That is the one outcome this
   * app may never produce, and it has happened once already: the tiles were rebuilt and
   * `bundle.sqlite` was left behind, and the map went quietly grey.
   *
   * COLOUR ONLY ON A PROVEN PAIR (P11, `usePairing`): `ok === true` colours. `false` is a mixed
   * pair, refused and SAID (the banner below); `null` — not established yet, or no tile version
   * known — colours nothing and raises no alarm, which is not evidence of a mismatch.
   */
  const vintage = usePairing(pairing);
  const paired = vintage.ok === true;
  const mixedPair = vintage.ok === false;
  // checked, and no tile version to prove the pair with: nothing is coloured, and one quiet line says why
  const unproven = vintage.ok === null && vintage.unproven;
  /*
   * THE DAY'S STATUS, on the Map tab, from the release's paint (`useStatusIndex`; checked against
   * the release's handles when decoded). Until it has loaded the water stays plain: "not asked" is
   * never drawn as "base".
   */
  const statusIndex = useStatusIndex(status);
  const statusOk = statusIndex !== null && paired;
  /** Whether this platform's map colours each tile itself (`profile`, web) or takes pushed data (`status`, native). */
  const perTile = coloursEachTile(Platform.OS);
  // THE LAKE MODES' VALUES (Stocked, Depth): the release's lake facts, labelled per tile by the same engine
  const lakeFeed = useLakeColour({ source, perTile, paired, today: new Date() });
  // PREPARED, NOT FED (owner 2026-10-21): a lake mode with no values in the release is greyed in Layers and drawn plain
  const lakeReady = useMemo(() => lakeModesReady(lakeFeed), [lakeFeed]);
  /*
   * WHAT EACH LAYER IS COLOURED BY (P12): the quantity, the percentile asked of the panel, the field and every layer's
   * mode, decided in `@app/ui` (`useMapModes`) and only drawn here.
   */
  /*
   * SATELLITE IS ONLINE-ONLY (owner 2026-10-21): can Esri's imagery be reached — the device's word (web), a failed
   * imagery tile (web map), and one probed tile (both; a phone's only signal). `mapModes` turns it and the reader's
   * choice into the ground every map is handed (`mm.basemap`) and the Layers sheet's reason line; the choice is kept.
   */
  const probe = useMemo(() => imageryProbe(tiles.imagery ?? imageryTiles(null)), [tiles.imagery]);
  const imagery = useImageryReach({ wanted: layerChoice.ground === "ground.satellite", connectivity, probe });
  /*
   * THE LAYERS SHEET'S VIEW MODEL, and what every map is handed (`effective`): from the layer registry over what the
   * release carries (Stocked, Depth: prepared, not fed) and whether streamed imagery can be reached. The sheet opens on
   * both browse maps, each with ITS entries (owner 2026-10-22): the Conditions map's is Show, the ground and its gauges
   * and arrows; the Map tab's (and a water's map view's) the ground, the water colouring and its switches.
   */
  const carried = lakeValuesCarried(lakeReady);
  const layersView = mapTab === "conditions" ? "conditions" : "map";
  const layers = useMemo(() => layersVM(layersView, layerChoice, { carries: carried, online: imagery.up }),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- `carried` is rebuilt each render; its two facts are the deps
    [layersView, layerChoice, carried.stocked, carried.surveyed, imagery.up]);
  /** What the Conditions view shows — ONE quantity, the registry's Show (`effective.show`): map, gauges and chart. */
  const flowParam: FlowParam = layers.effective.show;
  // THE HORIZON THIS QUANTITY CAN ANSWER (T52): Change is flow only, so under Level it answers as Now while the choice is
  // kept; its chip stays under Level greyed "Flow only", and a tap on it moves to Flow (T130, `pickDay`).
  const horizon = flowHorizon(hz.horizon, flowParam);
  const horizonChipsShown = useMemo(() => horizonChips(hz.horizon, flowParam), [hz.horizon, flowParam]);
  // the panel's chart quantity control sets the same choice (a Level chart is the Level map)
  const setFlowParam = useCallback((q: FlowParam) => setLayerChoice((c) => chooseShow(c, q)), []);
  // EVERY horizon control picks through this: Change under Level moves the map to Flow first (`pickHorizon`)
  const pickDay = useHorizonPick(hz.pick, flowParam, setFlowParam);
  const onChooseLayer = useCallback((id: string) =>
    setLayerChoice((c) => chooseLayer(c, id, { carries: carried, online: imagery.up })),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [carried.stocked, carried.surveyed, imagery.up]);
  /*
   * YOUR SPOTS ON EVERY MAP (owner 2026-10-22: "toggle our spots on and off in the layer menu"): each saved spot is a
   * mark on the Map tab, the Conditions map and a water's map view — while the Layers sheet's Spots switch
   * is on (`effective.pins.spots`, default on); off, no map is handed one.
   */
  const spotsShown = layers.effective.pins.spots;
  const spotPoints = useMemo(() => spotPins(spots, spotsShown), [spots, spotsShown]);
  /*
   * (T131) EACH A MARK THAT OPENS ITS SPOT: placed and grouped by the map (`@app/core` `spotMarkers`), drawn by `SpotPin`
   * in the spot tokens, the selected one the spot last opened or shown (the rail's). A tap opens the spot over the map it
   * is on — the one back layer the Spots tab's row opens too; a group no zoom parts opens the Spots list.
   */
  const spotLayer: MapSpots | undefined = spotPoints.length === 0 ? undefined : {
    points: spotPoints, selected: rail.selected, onOpen: rail.openPin, onList: () => rail.listSpots(),
    face: (m, press) => <SpotPin m={m} palette={palette} press={press} />,
  };
  const mm = useMapModes({ tab: mapTab, flowParam, horizon, zoomedOut, paired, statusOk, perTile,
                           water: layers.effective.water, groups: layers.effective.groups,
                           basemap: layers.effective.basemap, imageryUp: imagery.up });
  // Esri's credit is listed exactly while its imagery is drawn (the map's own control does the same from the style)
  const credits = useMemo(() => attribution && withImageryCredit(attribution, mm.basemap), [attribution, mm.basemap]);
  const { onConditions, quantity, modes, activeGroups } = mm;
  /*
   * THE CONDITIONS COLOURING, per renderer (`useConditionsColour`): on a map that labels each tile, the release's
   * classes and the feed's values at the horizon (`standing` — one engine call per feed refresh or horizon change);
   * otherwise the visible sections' standings, pushed (`data`). Both FROM THE PANEL, through one arithmetic.
   */
  /*
   * THE SYNC STATE (owner 2026-10-22): when the flows were last published, whether a newer data release exists, and the
   * icon's refresh — ONE reading of the feed's age for the top bar's icon and More's Data block alike.
   */
  const syncState = useSync({ feed, release: loader?.release ?? null, checkRelease });
  const cond = useConditionsColour({ source, feed, perTile, paired, onConditions, quantity: mm.standingParam,
                                     horizon: mm.standingHorizon, fieldOn: mm.fieldOn, visible,
                                     epoch: syncState.epoch });
  // the dots: the Conditions map's, while its Gauges switch is on (off, the roster is not even asked), at the horizon
  // the rivers are coloured for (T126: a dot under +5 says +5, not now)
  const gauges = useGaugeGeoJSON(source, feed, onConditions && layers.effective.layers.gauges === true, quantity,
                                 mm.standingHorizon);
  // The readings on screen, as positions on the legend's own scale (sentinels excluded: a state, not a point).
  const visibleMarks = cond.marks;
  const conditionData = cond.data;
  /**
   * The camera survives leaving the map.
   *
   * Opening a water's sheet unmounts the map, so without this it remounted at HOME and a
   * reader who had panned to the Skeena came back to Chilliwack. A ref rather than state:
   * every pan would otherwise re-render the whole shell, and nothing on screen depends on
   * the camera except the map itself.
   */
  const camera = useRef<Camera>(HOME);
  /*
   * (T48) SEARCH GIVES THE CAMERA BACK. Where it was when search opened is kept; a pick moves the map (a water framed, a
   * town flown to); closing search — ‹, Escape, the system back, a tab — puts it back EXACTLY (`fit.camera`, sent
   * once) and drops the keyboard. A water opened FROM A RESULT consumes search (owner 2026-10-22 late, route
   * `fromSearch`): nothing is restored — the map comes back framed on that water (`arrive`, below).
   */
  const searchCamera = useRef<Camera | null>(null);
  const searchMoved = useRef(false);
  const [restore, setRestore] = useState<{ key: string; camera: Camera } | null>(null);
  const restores = useRef(0);
  const wasSearching = useRef(searching);
  /*
   * (T69) FOCUSED BUT CLOSED. A close the keyboard asked for — Escape, the panel's ✕ — on a wide screen on the web hands
   * the focus back to the field instead of dropping it on the page body (`toField`); the field then holds it with
   * search closed. That focus must not open search again (`quiet`): typing, Enter or ↓ there does, or focusing it
   * afresh. A click on the map, back and a tab change leave the focus where they put it; a phone drops it (its keyboard).
   */
  const toField = useRef(false);
  const quiet = useRef(false);
  useEffect(() => {
    if (searching === wasSearching.current) return;
    wasSearching.current = searching;
    const keep = toField.current && CLOSE_KEEPS_FOCUS && searchPanelWidth(screenWidth) !== null;
    toField.current = false;
    if (searching) { searchCamera.current = { ...camera.current }; searchMoved.current = false; return; }
    if (keep) {
      quiet.current = true;                   // the focus event fires inside focus(): it opens nothing
      try { field.current?.focus(); } finally { quiet.current = false; }
    } else {
      field.current?.blur();
      Keyboard.dismiss();
    }
    const back = searchCamera.current;
    if (searchMoved.current && back) {
      camera.current = back;                       // a map mounted later (a spot was opened) starts there too
      setRestore({ key: `restore:${++restores.current}`, camera: back });
    }
  }, [searching]);
  useEffect(() => { if (searching && session.focus?.bbox) searchMoved.current = true; }, [searching, session.focus?.key]);
  // sent once: the next render hands the map no fit, so a later remount is never dragged back
  useEffect(() => { if (restore) setRestore(null); }, [restore]);
  /*
   * (Owner 2026-10-22 late, "back from a searched water lands on the map, at the water") A water opened from a result
   * consumes search — the route's `from` is the map with search closed — and the map comes back FRAMED ON THAT WATER:
   * the very framing search gave it (its own outline, `session.focus`; the bundle's box when search had not placed it
   * yet). Sent once, as the browse map returns (back or ×, from any water stacked above it), and forgotten at the next
   * move of the route, so a later return (a map tap's water, then back) is never dragged back to it.
   */
  const [arrive, setArrive] = useState<{ key: string; bbox: readonly [number, number, number, number];
                                         refine: boolean } | null>(null);
  const arrivals = useRef(0);
  const arrivedAt = useRef<Route | null>(null);
  useEffect(() => {
    if (!arrive) return;
    // landed (the browse map is back): note where; any later move of the route — another water included — forgets it
    if (arrivedAt.current === null) { if (route.mode === "browse") arrivedAt.current = route; }
    else if (arrivedAt.current !== route) { arrivedAt.current = null; setArrive(null); }
  }, [route, arrive]);
  const openSearched = (id: ItemId) => {
    const n = ++arrivals.current;
    const key = `arrive:${id}:${n}`;
    const f = session.focus?.key === `item:${id}` ? session.focus : null;
    arrivedAt.current = null;
    if (f?.bbox) setArrive({ key, bbox: f.bbox, refine: f.refine });
    else {
      setArrive(null);
      void source.waterBox(id).then((bbox) => { if (bbox && arrivals.current === n) setArrive({ key, bbox, refine: false }); });
    }
    searchMoved.current = false;            // the camera from before search is not put back: the water is the subject
    setSearchMemory(NO_SEARCH);             // the search is consumed, its text with it
    opener.searched(id, tab === "conditions" ? "conditions" : "regs");
  };
  /** One step of search (`searchStep`): a pick lowers the sheet to half, so the keyboard goes with it. */
  const searchAct = (a: SearchAction) => {
    const next = searchStep(searchMemory.pick, a).pick;
    if (next.selected !== null || next.town !== null) { field.current?.blur(); Keyboard.dismiss(); }
    setSearchMemory({ q: searchMemory.q, pick: next });
  };
  const searchType = useCallback((q: string) => setSearchMemory((m) => ({ q, pick: searchStep(m.pick, { t: "typed" }).pick })),
    []);
  /** Close search: the phone's ‹, Escape, and on a wide screen the panel's ✕ and a click on the map beside it. */
  const closeSearch = useCallback(() => go({ type: "search", open: false }), [go]);
  /** (T69) A keyboard's close — Escape, the panel's ✕, the phone's ‹ — that gives the focus back to the field (wide web). */
  const closeToField = useCallback(() => { toField.current = true; closeSearch(); }, [closeSearch]);
  /** (owner 2026-10-22 late) A wide screen's search is a panel beside the map: a click on that map closes it. */
  const wideSearch = searching && searchPanelWidth(screenWidth) !== null;
  /** The SYNC ICON's state (`syncVM`): fresh, needs a refresh, checking — none when there is nothing to sync. */
  const sync = feed || checkRelease
    ? syncVM({ feed: !!feed, feedFetchedAt: syncState.feedFetchedAt, checkedAt: syncState.checkedAt,
               checking: syncState.checking,
               newRelease: syncState.newRelease, now: Date.now() })
    : null;
  /** What More's Data block reads (T167: the only place a source's date and freshness are drawn): the release's dates, the pair, the feed's age. */
  const dataFacts = {
    sources: loader?.manifest?.sources ?? null, validUntil: loader?.manifest?.regulations?.valid_until ?? null,
    tiles: vintage.tiles ?? null, tilesPaired: vintage.ok, feedFetchedAt: syncState.feedFetchedAt, now: Date.now(),
    // the feed's word on HYDAT (its maintenance notice), read with the sync state; never handed to the sync icon
    hydatNotice: syncState.hydatNotice,
    // (T68) the feed index's newest forecast run, for the Forecasts line
    forecastIssuedAt: syncState.forecastIssuedAt,
  };
  const barBottom = topBarBottom();
  /*
   * THE DAY'S STATUS, ONLY on a proven pair with an index that matched (`statusOk`), and the way this platform's map
   * takes it (`statusFeed`): the web map is handed the index and the day and labels each tile as it arrives
   * (colour-latency AP — no `idle`, no round trip through here per pan); the native map is pushed the visible
   * sections' status as `data` (N2 reads only that, in the `status` mode — colour-ap review).
   */
  const { status: mapStatus, data: statusMapData } = useMemo(
    () => statusFeed({ perTile, statusOk, index: statusIndex, tab: mapTab, visible, day: mapDay ?? new Date() }),
    [perTile, statusOk, statusIndex, mapTab, visible, mapDay]);
  /**
   * The water's map view (P15, `WaterMapView`) has its own camera, so leaving water mode puts the browse map back where
   * the reader left it (APP-PREP §0 rule 1).
   */
  const waterCamera = useRef<Camera | null>(null);

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
    const conditions = tab === "conditions";
    if (!tapOpensWater({ tab, flowParam, zoom: camera.current?.zoom ?? HOME.zoom, handover: HANDOVER_Z })) return;
    // The tile's feature id IS the section handle — converted, not cast (see noteVisible above).
    await opener.tap(Number(featureId) as SectionId,
                     lat !== undefined && lon !== undefined ? { lat, lon } : null, conditions ? "conditions" : "regs");
  }, [tab, flowParam, opener, picking]);

  const viewing = openSpot === null ? null : spots.find((s) => s.id === openSpot) ?? null;
  // T51: the open spot against the coming days — the feed the rest of the app reads, and the weather port; asked
  // again when the device comes back online
  const like = useLikeVisit(viewing, { weather, feed }, likeStore, Date.now, connectivity);
  if (viewing) {
    const dirty = edit !== null
      && (edit.title !== viewing.title || edit.notes !== viewing.notes
          || photoEdit.refs.join("\n") !== viewing.photos.join("\n"));
    // an unsaved edit's added photos go with it; after a save there are none left to delete
    const endEdit = () => { if (edit) void photoEdit.discard(); setEdit(null); };
    const close = () => { endEdit(); setSpotError(null); setPendingTab(null); back(); };
    const goTab = (k: TabKey) => { endEdit(); setSpotError(null); setPendingTab(null); toTab(k); };
    return layered(
      <View style={{ flex: 1, backgroundColor: palette.card }}>
        <View style={{ flex: 1 }}>
          <SpotScreen spot={viewing} palette={palette}
                      mode={edit ? "edit" : "view"} likeVisit={like}
                      photoStore={photos?.store} photoEdit={edit ? photoEdit : undefined}
                      title={edit?.title} notes={edit?.notes}
                      onTitle={edit ? (t) => setEdit({ ...edit, title: t }) : undefined}
                      onNotes={edit ? (n) => setEdit({ ...edit, notes: n }) : undefined}
                      dirty={dirty}
                      onBack={close}
                      // (T131) the ‹ names where back goes: the tab the spot is over, or Back over a water
                      back={spotBack(route.mode === "water" ? null : TABS.find((t) => t.k === tab)!.t)}
                      onEdit={onSaveSpot && (() => {
                        setEdit({ title: viewing.title, notes: viewing.notes });
                        photoEdit.begin(viewing.id, viewing.photos);
                      })}
                      onDiscard={() => { endEdit(); if (pendingTab) goTab(pendingTab); }}
                      error={spotError}
                      askToDiscard={pendingTab !== null}
                      onKeepEditing={() => setPendingTab(null)}
                      onSave={edit && onSaveSpot ? async () => {
                        try {
                          await onSaveSpot({ ...viewing, title: edit.title || viewing.title,
                                             notes: edit.notes, photos: [...photoEdit.refs],
                                             updatedAt: Date.now() });
                          photoEdit.committed();
                          setEdit(null);
                          setSpotError(null);
                          if (pendingTab) goTab(pendingTab);
                        } catch (e) {
                          setPendingTab(null);
                          setSpotError(spotSaveFailed(e));    // the edit stays open, nothing dropped
                        }
                      } : undefined}
                      onDelete={onDeleteSpot && (async () => {
                        try { await onDeleteSpot(viewing.id); close(); }
                        catch (e) { setSpotError(spotSaveFailed(e)); }
                      })}
                      onShowOnMap={() => {
                        // THE ONE SHOW (the Spots tab's row does the same): the Map tab, framed on the spot with its
                        // pin. The camera is a ref, so setting it first means the map mounts there already.
                        camera.current = { lon: viewing.lon, lat: viewing.lat, zoom: 14 };
                        endEdit(); setSpotError(null); setPendingTab(null);
                        rail.show(viewing);
                      }}
                      onRefresh={onRefreshSpots && (() => onRefreshSpots())}
                      refreshing={refreshing} />
        </View>
        <TabBar active={tab} palette={palette}
                // an unsaved edit is never dropped by a tab change without asking (P01)
                onChange={(k) => (dirty ? setPendingTab(k) : goTab(k))} />
      </View>
    );
  }

  if (adding && onSaveSpot)
    return layered(
      <SpotCapture source={source} weather={weather} tiles={tiles} palette={palette} photos={photos}
                   // started at a point (a long press, a tap in pick mode, your location): the pin is already down
                   at={adding.at}
                   // So the spot freezes the ESTIMATE the reader was looking at,
                   // not just the matched station's own number.
                   feed={feed}
                   theme={theme} camera={camera.current}
                   onCancel={back}
                   // awaited: a rejection reaches SpotCapture, which shows it and stays open (P01)
                   // saved: the spot itself, over the map it was added from (back returns to that map)
                   onSaved={async (s) => { await onSaveSpot(s); go({ type: "openSpot", id: s.id }); }} />
    );

  const body = frame !== null
    ? frame.view === "conditions"
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
            <WaterMapView loader={loader} item={item} part={frame.part} tapSid={frame.tapSid} palette={palette}
                          map={{ at: tiles, theme, camera: waterCamera.current ?? camera.current,
                                 basemap: mm.basemap, data: statusMapData, status: mapStatus, spots: spotLayer,
                                 // T145: a tap off every spot clears the selected spot's pin, here as on the browse maps
                                 onMapPoint: mapPointTap(rail, null),
                                 onVisible: noteVisible, onMoved: (at) => { waterCamera.current = at; },
                                 view: "plain", modes, groups: activeGroups,
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
                       loader={loader} day={mapDay} onDay={setMapDay} section={frame.tapSid}
                       part={frame.part} start={start} onStarted={() => setStart(null)}
                       // (T58) a Parts sheet row is the frame's part, as a step on the water map is (one water screen)
                       onPart={item === null ? undefined : (p) => go(partAction(item, item, p))}
                       // NO `feed`. The view quotes no number, so it reads no gauge data — the Conditions view, one
                       // swipe away, is where a water's numbers are.
                       onBack={back} />
    : tab === "spots"
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
                    data={{ ...dataFacts, newRelease: syncState.newRelease }}
                    back={TABS.find((t) => t.k === (browse.prev ?? "map"))!.t} onBack={back}
                    contact={menuInfo?.contact ?? null}
                    theme={themeName} onTheme={onTheme}
                    onPick={(k) => (k === "report" ? menu.report(routeWhere(route, mapDay ? dayLabel(mapDay) : null))
                                    : menu.show(k))} />
      : tab === "map" || tab === "conditions"
        ? <MapScreen at={tiles} palette={palette} theme={theme}
                     camera={camera.current}
                     // (T48) the top bar over the map: the corner's chips, the rail and the web map's controls go under it
                     insetTop={barBottom}
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
                     data={tab === "conditions" ? conditionData : statusMapData}
                     standing={cond.standing}
                     lakes={lakeFeed}
                     status={mapStatus}
                     // The dots belong to the Conditions tab — AND to the temperature
                     // choice, which is a question about the stations themselves and so
                     // must be answerable from the map without changing tab.
                     gauges={onConditions ? gauges ?? undefined : undefined}
                     onVisible={mm.asksVisible ? noteVisible : undefined}
                     onMoved={(at) => {
                       camera.current = at;
                       // The ref is deliberate — see below — but ONE BIT of the camera is
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
                     // SAME state the panel header sets — see `hz` above.
                     horizons={mm.horizonsShown ? { chips: horizonChipsShown, onPick: pickDay } : undefined}
                     // LAYERS ON BOTH MAPS (owner 2026-10-22): on Conditions the sheet is that map's own —
                     // Show (Flow · Level · Temperature), the ground, gauges and arrows — never the Map tab's
                     onLayers={() => setLayersOpen(true)}
                     // how many of THIS map's layer choices differ from the defaults: the rail's badge, said aloud
                     layersChanged={layers.changed}
                     // press and hold: a provisional pin, and the sheet that asks before anything is made
                     onLongPress={onSaveSpot ? rail.pressLong : undefined}
                     // either map's tap opens the ONE water screen (see onPressFeature): only the view in front differs
                     // — except in pick mode, where a tap is the spot's place and nothing else
                     // — and on a WIDE screen while search is open, a click on the map beside the panel closes search
                     // and does nothing else (owner 2026-10-22 late): no water opens under it, no pin drops
                     onPressFeature={picking || wideSearch ? undefined : onPressFeature}
                     // and anywhere off a spot, a tap clears the selected spot's pin (T145, `mapPointTap`)
                     onMapPoint={mapPointTap(rail, wideSearch ? closeSearch : null)}
                     onError={(e) => console.error("map:", e.message)} />
        : null;

  /*
   * THE LEGEND BELONGS TO THE MAP, so it goes when the map does: in water mode (any view, from either map) the browse
   * map is not on screen, and a Showing control or ramp there would control nothing a reader can see.
   */
  /*
   * THE MAP TAB HAS A LEGEND ONLY WHEN SOMETHING IS COLOURED BY A VALUE: the day's status
   * (once the index is in), or the lakes' Stocked colouring. Plain water needs no key.
   */
  /*
   * THE WATER'S HEADER (FaceBar): the views its frame offers (all three; an unnamed reach, Regulations and Conditions — T53), the one on
   * screen, and where back goes, in the reader's words — the views are lateral, so back always leaves the water: to
   * the tab it was opened from (the Conditions map or the Map tab), or "Back" to the water flowed in from. The swipe
   * listens on the header and on the body: the header is the water frame's (T44 `WaterFrame`, above every view's body,
   * a sibling of it) and always listens; Regulations and Conditions listen on their bodies too (the panel's maps and
   * chart shielded); on Map the canvas is the body, header only.
   */
  // ‹ and × name where they go: the tab, or search when it is still open under the water (a map tap beside it)
  const exit = route.mode === "water"
    ? waterExit(route.stack.length, TABS.find((t) => t.k === route.from.tab)!.t, route.from.search) : null;
  const waterViews: WaterViews | null = route.mode === "water" && frame !== null && exit !== null
    ? { views: frameViews(frame), on: frame.view, onView: (v) => go({ type: "view", view: v }), onBack: back,
        backLabel: exit.back,
        // (owner 2026-10-22 late) the one-tap way out of every water, back to the tab the first was opened from
        close: () => go({ type: "close" }), closeLabel: exit.close,
        swipe: viewSwipe, bodySwipe: frame.view === "map" ? null : viewSwipe }
    : null;

  /** (T48) The top bar — the search field — is the maps': on the Map and Conditions tabs while browsing. */
  const topBar = route.mode === "browse" && (tab === "map" || tab === "conditions") && !picking;
  // the legend is the map's caption: while search's sheet is over the map it captions nothing in view
  const showLegend = route.mode === "browse" && !searching
    && (tab === "conditions"
        || (tab === "map" && (layers.water.drawn !== "regulations" || statusOk)));

  return layered(
    // a regulations tab's "Report a problem with this page" reads the menu from here (menu/ReportLink)
    <MenuContext.Provider value={menu}>
    <View style={{ flex: 1, backgroundColor: palette.card }}>
      {/*
        * SAY WHY THE MAP IS GREY.
        *
        * Refusing to colour a mixed pair is only half the fix — an uncoloured map and a map
        * whose data is unusable look identical, and that ambiguity IS the bug this guards:
        * when it happened, the Conditions map went quietly grey and read as a slow feed.
        * So the refusal is stated, in the one place a reader is already looking for what
        * the colours mean.
        */}
      {mixedPair && (
        <View style={{ paddingVertical: 8, paddingHorizontal: 12,
                       backgroundColor: palette.closed }}>
          {/* The error red is the closed red, and its words the on-accent knock-out — the
              same pair a danger button wears. It was its own #7A1F2B behind a white typed
              in twice, a third red with no theme. */}
          <Text style={{ color: palette.onAccent, fontSize: 12, fontWeight: "600" }}>
            Map data is out of step — the tiles and the data bundle came from different
            builds, so nothing is coloured. Rebuild or re-download both.
          </Text>
          <Text style={{ color: palette.onAccent, fontSize: 11, opacity: 0.85, marginTop: 2 }}>
            tiles {vintage.tiles ?? "unknown"} · bundle {vintage.data ?? "unknown"}
          </Text>
        </View>
      )}
      {unproven && (
        <Text style={{ paddingVertical: 6, paddingHorizontal: 12, color: palette.sub, fontSize: 12 }}>
          {unprovenLine(vintage.data)}
        </Text>
      )}
      <View style={{ flex: 1 }}>
        <EdgeBack drag={edge}>
          <WaterViewsContext.Provider value={waterViews}>
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
                  query={searchMemory.q} onQuery={searchType}
                  onOpen={() => { if (!quiet.current) go({ type: "search", open: true }); }}
                  onClose={closeToField} />
        )}
        {topBar && (
          <SearchSheet open={searching} top={barBottom} palette={palette} session={session} pick={searchMemory.pick}
                       onAct={searchAct}
                       // the one way out of search into a water: the one opener, with the view this map puts in front;
                       // it consumes the search, so back lands on the map at the water (owner 2026-10-22 late)
                       onOpenWater={openSearched}
                       onClose={closeToField}
                       statusIndex={statusIndex} total={counts?.waters} />
        )}
        {/* PICK ON THE MAP (owner 2026-10-22): the slim banner in the search field's place; a back layer */}
        {picking && route.mode === "browse" && tab === "map" && (
          <SpotPickBanner palette={palette} onCancel={rail.cancelPick} />
        )}
      </View>
      {showLegend && (
        <LegendStrip palette={palette} full={onConditions} wrap={!onConditions}>
          {/* NO QUANTITY SWITCH HERE (owner 2026-10-22): Flow · Level · Temperature is the Layers sheet's Show, one
              choice — a second switch in the strip would be two answers to "where do I change this". The ramp below
              names the quantity shown; the horizon control stays on the map (`horizons`). */}
          {tab === "conditions" ? (
            // READ FROM THE STYLE, never restated. These seven hex values used to be
            // written out here while the map's own ramp resolved to three shades of one
            // blue — so the legend promised red-through-cyan and the map drew a wash of
            // blue. A legend that disagrees with its map is worse than no legend.
            flowParam === "temperature" ? (
              // A DIFFERENT SCALE ENTIRELY, so a different legend. The flow ramp under
              // temperature dots was the map promising a ranking it was not drawing —
              // and these thresholds are absolute degrees, not positions in a record.
              // ONE ROW. The Conditions strip renders its children in a plain column so
              // the flow ramp can span the screen — which stacked these three bands
              // vertically and took three lines of map to say what fits on one. A legend
              // is a caption; the moment it needs three rows it has become a panel.
              // The bands and their words are the publisher's (`TEMP_LEGEND`, from `_TEMP_BANDS`); the colours are
              // the map's own for them (`palette.temp`, from @app/core `TEMP_BAND_TOKEN`, as the dots are painted).
              <View style={{ flexDirection: "row", alignItems: "center", gap: 18,
                             flexWrap: "wrap" }}>
                {TEMP_LEGEND.map((b) => (
                  <LegendCount key={b.band} palette={palette} colour={palette.temp[b.band]}
                               label={b.label} />
                ))}
              </View>
            ) : horizon === "change" ? (
              // THE EXPECTED CHANGE (T52): the same map mode's change stops, its words from core — falling · steady ·
              // rising, the bracket over "little change" — and the gauges on screen as marks, like the flow legend
              <>
                <LegendRamp palette={palette} title={CHANGE_LEGEND.title}
                            low={CHANGE_LEGEND.low} mid={CHANGE_LEGEND.mid} high={CHANGE_LEGEND.high}
                            band={CHANGE_LEGEND.steady} markWords={changeMarkWords}
                            describe={CHANGE_LEGEND.describe}
                            stops={changeRampStops(theme)} marks={visibleMarks} />
                {/* (T157) the low-water caveat on the map: what a dot's "low-water model" mark means */}
                <Text testID="change-legend-low-water" style={{ ...TYPE.small, fontSize: 11.5, lineHeight: 15, color: palette.sub }}>
                  {CHANGE_LEGEND.lowWater}
                </Text>
              </>
            ) : (
              <LegendRamp palette={palette}
                          title={flowParam === "level" ? "Water level vs. past years, this date"
                                 : "Flow vs. past years, this date"}
                          low="Low" mid="Normal" high="High"
                          stops={flowRampStops(theme)} marks={visibleMarks} />
            )
          ) : layers.water.drawn !== "regulations" ? (
            // ONE COLOURING FOR ALL WATER: the measure's key, ending with what draws neutral and why
            waterLegend(layers.water.drawn).map((r) => (
              <LegendCount key={r.label} palette={palette} colour={legendColour(palette, r.swatch)} label={r.label} />
            ))
          ) : (
            // The three statuses, in the words and colours every surface uses (@app/core
            // WATER_STATUS through the map's theme) — for today.
            <>
              <MapDayButton palette={palette} day={mapDay} onPress={() => setDayOpen(true)} />
              {WATER_STATUSES.map((s) => (
                <LegendCount key={s} palette={palette} colour={palette.waterStatus[s]}
                             line
                             label={s === "closed" ? (mapDay ? `Closed on ${dayLabel(mapDay)}` : "Closed today")
                                    : WATER_STATUS[s].label} />
              ))}
            </>
          )}
        </LegendStrip>
      )}
      {/* A tab change is a change of subject: the remembered reach goes with it. Hidden in water mode (P13/P14). */}
      {bottomBarShown(route) && <TabBar active={tab} palette={palette} onChange={toTab} />}

      {/* G1: an open sheet is a back layer — every platform's back closes it before it pops the route */}
      <LayersSheet open={layersOpen} onClose={() => setLayersOpen(false)} palette={palette}
                   vm={layers} onChoose={onChooseLayer} />
      <DaySheet open={dayOpen && route.mode === "browse"} onClose={() => setDayOpen(false)} palette={palette}
                day={mapDay} onDay={setMapDay}
                range={dayRange(new Date(), loader?.manifest?.regulations?.valid_until ?? null)} />
      <SpotDropSheet at={route.mode === "browse" ? rail.drop : null} palette={palette}
                     onCancel={rail.cancelDrop} onContinue={rail.continueDrop} />
      {/* the menu's pages: each open level a back layer (back closes a term, then the page) */}
      <MenuHost menu={menu} palette={palette} loader={loader} info={menuInfo} tiles={vintage.tiles ?? null}
                route={route} day={mapDay} />
    </View>
    </MenuContext.Provider>
  );
}

