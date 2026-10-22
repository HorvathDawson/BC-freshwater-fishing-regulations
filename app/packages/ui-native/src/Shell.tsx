/**
 * The app: three tabs, the map as home (its rail: Layers, Spots), and a legend that always says what the colours mean.
 *
 * Drawn to `design/riffle.html`. The shell owns the state that more than one screen needs —
 * which tab, which water, which colouring, which layers, which palette — and hands it down.
 * Every answer on every screen comes from a hook (rule 25). The Map tab colours water by the
 * day's status from the status index (closed / own regulations / base); the regulation records
 * themselves are not integrated, so a water's sheet shows a placeholder where they will go.
 */
import { chooseLayer, chooseShow, dayLabel, dayRange, LAYER_DEFAULTS, layersVM, TEMP_LEGEND, unprovenLine, WATER_STATUS, WATER_STATUSES,
         waterLegend, withImageryCredit, type LayerChoice, type SectionKey, type TempBand } from "@app/core";
import { useCallback, useMemo, useRef, useState } from "react";
import { Platform, Text, View } from "react-native";
import type { ItemId, RegsSource, SectionId } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { bottomBarShown, frameViews, spotSaveFailed, useMapModes, useOpenWater, crossAction, exitAction, partAction,
         showRegsActions, usePhotoEdit, useRoute, waterFrame,
         type BrowseTab, type FlowParam, type RegsStart, type Route, type SpotPhotos } from "@app/ui";
import { BackLayerContext, edgeBackDepth, useBack, useEdgeBack, useMenu, useOnLeave, useViewSwipe, type SystemBack } from "@app/ui";
import { spotsMenu, useMapRail, type Locate } from "@app/ui";
import { useConditionsColour, useDataFacts, useGaugeGeoJSON, useLakeColour, lakeModesReady, lakeValuesCarried,
         NO_SEARCH, type SearchMemory, usePickFocus, usePairing, useRegulationKeys, useStatusIndex, statusFeed,
         useHorizon, useImageryReach, type Horizon, type PairingInput, type StatusSource } from "@app/ui";
import type { Spot, WeatherSource } from "@app/data/spots";
import { hiddenLayers, shownLayers,
         type Camera, type TileEndpoints, coloursEachTile, imageryTiles } from "@app/map";
import { connectivity } from "./connectivity";
import { imageryProbe } from "./imageryProbe";
import { LegendCount, LegendRamp, LegendStrip } from "./Chrome";
import { MapDayButton } from "./DayCalendar";
import { DaySheet } from "./regs/DaySheet";
import { LayersSheet, legendColour } from "./LayersSheet";
import { MapScreen } from "./MapScreen";
import { SearchScreen } from "./SearchScreen";
import { SpotsSheet } from "./SpotsSheet";
import { SpotDropSheet } from "./SpotDropSheet";
import { SpotCapture } from "./SpotCapture";
import { SpotScreen } from "./SpotScreen";
import { TABS, TabBar, type TabKey } from "./TabBar";
import { WaterScreen } from "./WaterScreen";
import { EdgeBack } from "./EdgeBack";
import { WaterViewsContext, type WaterViews } from "./ViewSwipe";
import { WaterConditions, WaterMap } from "./WaterViews";
import { WaterMapView } from "./water/WaterMapView";
import { MenuContext } from "./menu/MenuButton";
import { MenuHost, type MenuInfo } from "./menu/MenuHost";
import type { AdiposeImages, FishImages } from "./regs/types";
import { TYPE } from "./type";
import { flowRamp, type Palette, type ThemeName } from "./theme";

/** No highlight — one frozen empty list, so the map's effect is not re-run every render. */
const NONE: readonly SectionKey[] = [];

/** Where the map starts the FIRST time. After that the camera is whatever the user left. */
const HOME: Camera = { lon: -121.85, lat: 49.15, zoom: 9.4 };

/**
 * WHERE THE FIELD HANDS OVER TO THE RIVERS. Mirrors `BASIN_HANDOVER_Z` in the tile builder
 * and `minzoomByView` in the style — three places, one number, and a test holds them equal.
 *
 * Below it the Conditions map is catchments and NOTHING IS TAPPABLE: a tap would open a
 * reach sheet for a shape the size of a valley, answering a question nobody asked. Above it
 * the rivers are back and they are what you tap.
 */
const HANDOVER_Z = 9;

/** A temperature band's colour: the "may I fish here" colours, as the map paints the dots (runtime-style). */
const tempColour = (p: Palette): Record<TempBand, string> =>
  ({ cool: p.open, warm: p.restricted, critical: p.closed });

export function Shell({ source, palette, theme, themeName, onTheme, tiles,
                       spots = [], onSaveSpot, weather, onRefreshSpots, onDeleteSpot, refreshing, feed,
                       spotsError, attribution, fishImages, adiposeImages, photos, status = null, pairing = null,
                       systemBack = null, loader = null, initialRoute, initialDay = null, regsStart = null, locate = null, menuInfo }: {
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
  const openSpot = route.mode === "browse" ? route.spot : null;
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
  const opener = useOpenWater(source, go);
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
  /**
   * The water search's eye SHOWED on the main map (route `show`): framed by its outline and lit, whole. It is the
   * route's, so back returns to search, and a tab change forgets it — a later remount of the map never drags the
   * camera back to an old search. Where search was (query, selection) is kept here, for that back.
   */
  const shown: ItemId | null = browse.shown;
  const pickFocus = usePickFocus(source, shown);
  const [searchMemory, setSearchMemory] = useState<SearchMemory>(NO_SEARCH);
  const [layersOpen, setLayersOpen] = useState(false);
  /*
   * THE MAP RAIL (owner 2026-10-21): the Spots menu, the provisional pin a long press drops, and where "Show" sent the
   * map — `@app/ui` `useMapRail`. Every way of adding a spot ends in the route's `addSpot` at a point: one capture.
   */
  const rail = useMapRail(go, locate, route);
  /** THE MENU (owner 2026-10-21): the hamburger's drawer and its pages, mounted below as back layers (menu/MenuHost). */
  const menu = useMenu();
  // the figures are drawn by the search screen (its total) and the menu's
  // Data sources page (the feed's age)
  const factsWanted = tab === "search" || menu.state.page === "sources";
  const facts = useDataFacts(source, feed, factsWanted);
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
  // A pin dropped on the map when you ask to see a spot there. Cleared when you pan away
  // to something else, so it never lingers as a mystery dot.
  const [focus, setFocus] = useState<{ lat: number; lon: number } | null>(null);
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
  const horizon = hz.horizon;
  // Which side of the handover zoom the camera is on — the one bit of the camera that is render state (see onMoved).
  const [zoomedOut, setZoomedOut] = useState(HOME.zoom < HANDOVER_Z);
  /*
   * THE LAYERS CHOICE (owner 2026-10-21 late, layers menu B): one ground, ONE water colouring for every feature, the
   * Conditions map's ONE quantity (Show: Flow · Level · Temperature — owner 2026-10-22, never two on one ramp) and the
   * switches — the only layer state kept here. The quantity is held here, not in the map, because the panel a tap opens
   * has to be about the same thing the map is showing: its chart's own quantity control sets this same choice. What the map is handed is the registry's (`layersVM(...).effective`,
   * below): an unavailable choice falls back while the choice itself is kept.
   */
  const [layerChoice, setLayerChoice] = useState<LayerChoice>(LAYER_DEFAULTS);
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
  // the panel's chart quantity control sets the same choice (a Level chart is the Level map)
  const setFlowParam = useCallback((q: FlowParam) => setLayerChoice((c) => chooseShow(c, q)), []);
  const onChooseLayer = useCallback((id: string) =>
    setLayerChoice((c) => chooseLayer(c, id, { carries: carried, online: imagery.up })),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [carried.stocked, carried.surveyed, imagery.up]);
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
  const cond = useConditionsColour({ source, feed, perTile, paired, onConditions, quantity: mm.standingParam,
                                     horizon: mm.standingHorizon, fieldOn: mm.fieldOn, visible });
  // the dots: the Conditions map's, while its Gauges switch is on (off, the roster is not even asked)
  const gauges = useGaugeGeoJSON(source, feed, onConditions && layers.effective.layers.gauges === true, quantity);
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
   * in front from the Map tab, Conditions in front from the Conditions map. An unnamed reach has no page of rules, so
   * from either map it opens the same Conditions-only frame.
   *
   * NOTHING IS TAPPABLE ON THE CONDITIONS MAP BELOW THE HANDOVER. Down there it is catchments, and the only thing under
   * a finger is a shape the size of a valley — a reach panel for it would answer a question about one river with a
   * number about a region. The rivers come back at z9 and they are what you tap.
   */
  const onPressFeature = useCallback(async (_layer: string, featureId: SectionKey, lat?: number, lon?: number) => {
    const conditions = tab === "conditions";
    if (conditions && (camera.current?.zoom ?? HOME.zoom) < HANDOVER_Z) return;
    // The tile's feature id IS the section handle — converted, not cast (see noteVisible above).
    await opener.tap(Number(featureId) as SectionId,
                     lat !== undefined && lon !== undefined ? { lat, lon } : null, conditions ? "conditions" : "regs");
  }, [tab, opener]);

  const viewing = openSpot === null ? null : spots.find((s) => s.id === openSpot) ?? null;
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
                      mode={edit ? "edit" : "view"}
                      photoStore={photos?.store} photoEdit={edit ? photoEdit : undefined}
                      title={edit?.title} notes={edit?.notes}
                      onTitle={edit ? (t) => setEdit({ ...edit, title: t }) : undefined}
                      onNotes={edit ? (n) => setEdit({ ...edit, notes: n }) : undefined}
                      dirty={dirty}
                      onBack={close}
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
                        // The camera is a ref, so setting it and then mounting the map is
                        // what moves it — MapScreen reads the ref when it mounts.
                        camera.current = { lon: viewing.lon, lat: viewing.lat, zoom: 14 };
                        setFocus({ lat: viewing.lat, lon: viewing.lon });
                        goTab("map");
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
                   // started at a point (a long press, the map's middle, your location): the pin is already down
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
    ? frame.view === "conditions" || item === null
      // THE ONE CONDITIONS PANEL OF A SECTION, from either map (a frame with no item is an unnamed reach).
      ? <WaterConditions source={source} item={item} tapSid={frame.tapSid} palette={palette}
                         tiles={tiles} theme={theme}
                         // Temperature is not a hydrograph, so the chart falls back to whatever the station itself
                         // leads with; Flow and Level pass straight through — the map and the chart ask one question.
                         parameter={flowParam === "temperature" ? undefined : flowParam}
                         onParameter={setFlowParam}
                         from={frame.at}
                         feed={feed}
                         credits={credits} horizon={horizon}
                         onHorizon={hz.pick} />
      : frame.view === "map"
        ? <WaterMap source={source} item={item} palette={palette}>
            {/* P15: the parts, lit and framed from the release; ▼ / ▲, marks, the mouth's exit */}
            <WaterMapView loader={loader} item={item} part={frame.part} tapSid={frame.tapSid} palette={palette}
                          map={{ at: tiles, theme, camera: waterCamera.current ?? camera.current,
                                 basemap: mm.basemap, data: statusMapData, status: mapStatus,
                                 onVisible: noteVisible, onMoved: (at) => { waterCamera.current = at; },
                                 view: "plain", modes, groups: activeGroups,
                                 onError: (e) => console.error("map:", e.message) }}
                          onOpenWater={(id, p) => go(exitAction(id, p))}
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
                       loader={loader} day={mapDay} onDay={setMapDay} section={frame.tapSid}
                       part={frame.part} start={start} onStarted={() => setStart(null)}
                       // NO `feed`. The view quotes no number, so it reads no gauge data — the Conditions view, one
                       // swipe away, is where a water's numbers are.
                       onBack={back} />
    : tab === "search"
      ? <SearchScreen source={source} palette={palette} status={status}
                      // the eye: SHOW it on the main map, framed and lit; back returns here as it was
                      onPick={(id) => go({ type: "show", item: id })}
                      memory={searchMemory} onMemory={setSearchMemory}
                      total={counts?.waters} tiles={tiles} theme={theme}
                      basemap={mm.basemap} />
      : tab === "map" || tab === "conditions"
        ? <MapScreen at={tiles} palette={palette} theme={theme}
                     // the way back to search, while the map shows what search's eye sent it to
                     onBack={tab === "map" && shown !== null ? back : undefined}
                     camera={camera.current}
                     // the provisional pin a long press dropped, else the spot "Show" sent the map to
                     marker={rail.drop ?? rail.fly?.at ?? focus}
                     basemap={mm.basemap} onImageryError={imagery.tileFailed}
                     // THE CHOSEN WATER, whole, on the Map tab only — the Conditions view
                     // lights a gauge's route with the same feature-state and must not
                     // inherit a search.
                     // The Spots menu's "Show" outranks it: the rail forgets `fly` whenever the route moves (a
                     // new search pick included), so a `fly` here was asked for AFTER the water was shown.
                     fit={rail.fly ? { key: rail.fly.key, bbox: rail.fly.bbox }
                       : tab === "map" && pickFocus
                         ? { key: pickFocus.key, bbox: pickFocus.bbox, refine: pickFocus.refine } : null}
                     highlight={tab === "map" ? pickFocus?.highlight ?? NONE : NONE}
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
                     horizons={mm.horizonsShown ? { chips: hz.chips, onPick: hz.pick } : undefined}
                     // LAYERS ON BOTH MAPS (owner 2026-10-22): on Conditions the sheet is that map's own —
                     // Show (Flow · Level · Temperature), the ground, gauges and arrows — never the Map tab's
                     onLayers={() => setLayersOpen(true)}
                     // how many of THIS map's layer choices differ from the defaults: the rail's badge, said aloud
                     layersChanged={layers.changed}
                     // THE RAIL'S SPOTS (owner 2026-10-21): on both browse maps, where the Spots tab was
                     onSpots={rail.openSpots}
                     // press and hold: a provisional pin, and the sheet that asks before anything is made
                     onLongPress={onSaveSpot ? rail.pressLong : undefined}
                     // either map's tap opens the ONE water screen (see onPressFeature): only the view in front differs
                     onPressFeature={onPressFeature}
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
   * THE WATER'S HEADER (FaceBar): the views its frame offers (all three; an unnamed reach, Conditions alone), the one on
   * screen, and where back goes, in the reader's words — the views are lateral, so back always leaves the water: to
   * the tab it was opened from (the Conditions map or the Map tab), or "Back" to the water flowed in from. The swipe
   * listens on the header and on the body: the header is the water frame's (T44 `WaterFrame`, above every view's body,
   * a sibling of it) and always listens; Regulations and Conditions listen on their bodies too (the panel's maps and
   * chart shielded); on Map the canvas is the body, header only.
   */
  const waterViews: WaterViews | null = route.mode === "water" && frame !== null
    ? { views: frameViews(frame), on: frame.view, onView: (v) => go({ type: "view", view: v }), onBack: back,
        backLabel: route.stack.length > 1 ? "Back" : TABS.find((t) => t.k === route.from.tab)!.t,
        swipe: viewSwipe, bodySwipe: frame.view === "map" ? null : viewSwipe }
    : null;

  const showLegend = route.mode === "browse"
    && (tab === "conditions"
        || (tab === "map" && (layers.water.drawn !== "regulations" || statusOk)));

  return layered(
    // the hamburger on each browse screen reads the menu from here (menu/MenuButton)
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
              // the map's own for them (runtime-style `tempExpression`).
              <View style={{ flexDirection: "row", alignItems: "center", gap: 18,
                             flexWrap: "wrap" }}>
                {TEMP_LEGEND.map((b) => (
                  <LegendCount key={b.band} palette={palette} colour={tempColour(palette)[b.band]}
                               label={b.label} />
                ))}
              </View>
            ) : (
              <LegendRamp palette={palette}
                          low={flowParam === "level" ? "low stage for the date"
                               : flowParam === "discharge" ? "low flow for the date"
                               : "low for the date"}
                          mid="normal" high="high"
                          stops={flowRamp(theme)} marks={visibleMarks} />
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
      <SpotsSheet open={rail.spotsOpen && route.mode === "browse"} onClose={rail.closeSpots} palette={palette}
                  vm={spotsMenu(spots, { error: spotsError, canAdd: !!onSaveSpot, canRefresh: !!onRefreshSpots,
                                         permission: rail.permission })}
                  onOpen={(id) => { rail.closeSpots(); go({ type: "openSpot", id }); }}
                  onShow={(id) => { const s = spots.find((x) => x.id === id); if (s) rail.show(s); }}
                  onAdd={(k) => (k === "mine" ? void rail.addMine()
                                 : rail.addAt({ lat: camera.current.lat, lon: camera.current.lon }))}
                  onRefresh={onRefreshSpots} refreshing={refreshing} error={rail.error}
                  photoStore={photos?.store} />
      <DaySheet open={dayOpen && route.mode === "browse"} onClose={() => setDayOpen(false)} palette={palette}
                day={mapDay} onDay={setMapDay}
                range={dayRange(new Date(), loader?.manifest?.regulations?.valid_until ?? null)} />
      <SpotDropSheet at={route.mode === "browse" ? rail.drop : null} palette={palette}
                     onCancel={rail.cancelDrop} onContinue={rail.continueDrop} />
      {/* the menu's drawer and pages: each open level a back layer (back closes a term, a page, the drawer) */}
      <MenuHost menu={menu} palette={palette} loader={loader} info={menuInfo} tiles={vintage.tiles ?? null}
                feedFetchedAt={facts.state === "ready" ? facts.value.fetchedAt ?? null : null}
                route={route} day={mapDay} theme={themeName} onTheme={onTheme} />
    </View>
    </MenuContext.Provider>
  );
}

