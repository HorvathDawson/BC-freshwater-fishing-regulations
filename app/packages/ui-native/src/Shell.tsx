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
 *     sit under one header (`WaterFrame`, fed by `WaterViewsContext`) — `shell/WaterModeHost.tsx`. (T498) A water
 *     opened over the browse screen is built hidden and shown whole, the browse screen held as it was meanwhile —
 *     `shell/paneReveal.ts`.
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
import { GUIDE_WATER, WATER_SCREEN_WORDS, bellLabel, releaseNoticeVM, dayRange, placeholderFor, runSheet, stepDay, syncVM, type NotFishingKind, bcNow, now as clockNow, reportFailure } from "@app/core";
import type { ItemId } from "@app/data";
import { useEffect, useMemo, useRef, useState } from "react";
import { Platform, View } from "react-native";
import { bottomBarShown, spotPins, useDayFlow, useLikeVisit, useMapRail, useMyLocation, useOpenAtMe, opensAtMe, useShare, useGaugePageCheck, useMenu, useDataFacts, useSync, useAppUpdate,
         BackLayerContext, InSeasonContext, useIdleOnce, useInSeason, useInSeasonNotice, useDfoNotice, useDfoCheckedAt, useReleasePrefetch, useBackgroundPrefetch, useNet, useNetQuiet, useRegsPrefetch, holdsMapStatus, openWater, useHeldLookStore, type HeldLookStore, type RegsStart } from "@app/ui";
import type { Camera, MapSpots } from "@app/map";
import { visitSpot } from "@app/data/spots";
import { connectivity } from "./connectivity";
import { SearchSheet } from "./SearchSheet";
import { TopBar, topBarBottom } from "./TopBar";
import { SpotPickBanner } from "./SpotPickBanner";
import { SynopsisLoading } from "./regs/SynopsisLoading";
import { PdfViewer } from "./regs/PdfViewer";
import { TabBar } from "./TabBar";
import { EdgeBack } from "./EdgeBack";
import { LayersSheet } from "./LayersSheet";
import { InSeasonNotice } from "./InSeasonNotice";
import { DaySheet } from "./regs/DaySheet";
import { WaterViewsContext } from "./ViewSwipe";
import { SharePortContext } from "./SharePortContext";
import { MenuContext } from "./menu/MenuContext";
import { BrowseHost } from "./shell/BrowseHost";
import { MapLegend, PairBanner, RunsKey, RunsLegend } from "./shell/MapLegend";
import { HeldKey } from "./shell/HeldKey";
import { NetBanner } from "./NetBanner";
import { ShellSheets } from "./shell/ShellSheets";
import { useSpotEdit } from "./shell/useSpotEdit";
import { WaterModeHost } from "./shell/WaterModeHost";
import { PaneReadyContext, usePaneHold } from "./shell/paneReveal";
import { holdsView } from "./shell/holdViews";
import type { ShellProps } from "./shell/props";
import { HOME, useMapColouring } from "./shell/useMapColouring";
import { useShellNav } from "./shell/useShellNav";
import { useResumed } from "./shell/useResumed";
import { useEscapeBack } from "./escapeBack";
import { useShellSearch } from "./shell/useShellSearch";
import { FirstRunNotice, useFirstRun } from "./FirstRunNotice";
import { GUIDE_STEPS, GuideCard, useGuide, type GuideActs, type GuideFacts } from "./Walkthrough";
import { CoachContext, CoachItemContext, useCoachRegistry } from "./CoachMarks";
import { GuideControlContext, useGuideControlHost } from "./guideControls";
import { useGuideClosed } from "./guideClosed";
import { part, preloadParts } from "./part";
import { LoadingState } from "./LoadingState";

// (T282) loaded later on the web (`part`): not on the first screen's path
const SpotCapture = part(() => import("./later").then((m) => m.SpotCapture));
const ObstacleSheet = part(() => import("./later").then((m) => m.ObstacleSheet), (p) => p.props !== null);
const TidalSheet = part(() => import("./later").then((m) => m.TidalSheet), (p) => p.open);
const NotFishingSheet = part(() => import("./later").then((m) => m.NotFishingSheet), (p) => p.kind !== undefined);
const RunsSheet = part(() => import("./later").then((m) => m.RunsSheet), (p) => p.vm !== null);
const SyncErrorSheet = part(() => import("./later").then((m) => m.SyncErrorSheet), (p) => p.open);
const SpotHost = part(() => import("./later").then((m) => m.SpotHost));

export type { ShellProps } from "./shell/props";

/** (owner 2026-10-29) the map readied behind a landing's water: the whole box, unseen (painted, not shown) */
const BEHIND = { position: "absolute", top: 0, left: 0, right: 0, bottom: 0, opacity: 0 } as const;
/**
 * (T498) A HELD PANE: laid out at its final size, beside the screen rather than over the browse map's surface. (T508)
 * Its FINAL size: while held, the frozen tab bar still takes its row under the body, and a pane shown grows into it —
 * the water's map then framed its part again, visibly, after it was shown (emulator: map 334 → 389 dp, a second fit).
 * So the held pane (`heldStyle`) hangs `tabsH` below the body, the height it will have once shown.
 */
const heldStyle = (tabsH: number) => ({ ...BEHIND, bottom: -tabsH, transform: [{ translateX: 100_000 }] }) as const;

/** (T293) The fish obstacles' switch in the Layers sheet (`@app/core` layers). */
const OBSTACLES = "overlay.obstacles";

/** (T293) The room the bottom tab bar takes, kept clear by the walkthrough's card docked at the foot. */
const TAB_BAR_ROOM = 64;

/**
 * (T293) THE WALKTHROUGH'S WATER (`GUIDE_WATER`): found by its name in the release once, a stream of exactly that name.
 * Not found (a release without it): null, and the walkthrough takes any water the reader picks. Asked only once the
 * walkthrough is due or replayed (`wanted`): the search reads `core` and the whole `water` file, which a first paint
 * must never fetch (the request walk, T129).
 */
function useGuideWater(source: ShellProps["source"], wanted: boolean): { item: string; name: string } | null {
  const [w, setW] = useState<{ item: string; name: string } | null>(null);
  useEffect(() => {
    if (!wanted) return;
    let live = true;
    void source.searchNames(GUIDE_WATER.name, 10).then((hits) => {
      const h = hits.find((x) => x.name === GUIDE_WATER.name && x.kind === GUIDE_WATER.kind) ?? hits.find((x) => x.name === GUIDE_WATER.name);
      if (live && h) setW({ item: h.item, name: h.name });
    }).catch(reportFailure);   // (T368) the walk goes on with any water the reader picks
    return () => { live = false; };
  }, [source, wanted]);
  return w;
}

/** (T234) The last notification tap opened, outside React: it outlives a remount of the Shell. */
const openedTap = { n: 0, update: 0 };

/** (T287) How long the map must stand still before a downloaded release is swapped in. */
const APPLY_STILL_MS = 3000;

/** (T254) How long low-bandwidth mode waits with nothing downloading before the map's bait & fly marks are fetched. */
const GEAR_QUIET_MS = 15_000;

export function Shell(p: ShellProps) {
  const { source, palette, theme, tiles, spots = [], onSaveSpot, weather, feed, photos, loader = null, locate = null,
          menuInfo, checkRelease = null, releaseNamed = null, hydat, updates = null } = p;
  const nav = useShellNav({ source, loader, systemBack: p.systemBack ?? null, initialRoute: p.initialRoute,
                           onRoute: p.onRoute });
  /** (T186) the map's region reports drive the web prefetch; undefined (a phone): the map is not asked. (T182) The
   *  Salmon / Bait & fly files wait for its idle start on the web (`settled`), so first paint fetches paint alone. */
  const [settled, setSettled] = useState(!(p.prefetch && loader));
  // (T253) and the background tier after it: every file an unnamed-stream tap reads, the region in view first
  const background = useBackgroundPrefetch(p.prefetch ? loader : null, { onEvent: p.prefetchLog ?? undefined });
  // (T254) LOW-BANDWIDTH MODE: the background tier holds, the idle tier fetches only the region's file (no core), and the
  // map's bait & fly marks wait for a quiet line (15 s with nothing downloading)
  const netState = useNet();
  const lowNow = useRef(netState.mode.low);
  lowNow.current = netState.mode.low;
  useEffect(() => { background.scheduler?.signals({ lowData: netState.mode.low }); }, [background.scheduler, netState.mode.low]);
  const onRegion = useReleasePrefetch(p.prefetch ? loader : null, () => setSettled(true), background.scheduler,
                                      () => lowNow.current);
  const quietLine = useNetQuiet(GEAR_QUIET_MS);
  // (T282) the parts loaded later (`part`): fetched once the line is quiet and the browser idle — so a later tap on
  // More, Conditions or a spot finds its code here. Not sooner: on a water's own page the chunk would compete with the
  // release files the hand-over waits for (any other start asked for it at once: startLink.web)
  const partsIdle = useIdleOnce(Platform.OS === "web" && quietLine);
  useEffect(() => { if (partsIdle) void preloadParts(); }, [partsIdle]);
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
  // (T286) the waters on screen, their regulations read while the map is still (the Map tab, zoomed in)
  const regsPrefetch = useRegsPrefetch(loader, source, mapDay);
  /** (T41) the start link's page start, until the page it opened has applied it (a later water never sees it) */
  const [start, setStart] = useState<RegsStart | null>(p.regsStart ?? null);
  const [dayOpen, setDayOpen] = useState(false);
  const [layersOpen, setLayersOpen] = useState(false);
  /** (T173) The tapped fish obstacle mark's tile properties, while its sheet is up. */
  const [obstacle, setObstacle] = useState<Readonly<Record<string, unknown>> | null>(null);
  /** (T166) Whether the tidal water sheet is up (a tap on tidal water, no water under it). */
  const [tidalOpen, setTidalOpen] = useState(false);
  /** (T292) The tapped not-fishing water's kind (`undefined`: no sheet; null: a tile with no kind), while its sheet is up. */
  const [notFishing, setNotFishing] = useState<NotFishingKind | null | undefined>(undefined);
  /** (T199) The section a tap asked about under Run timing (its runs sheet), else null. */
  // (T275) the foot block's height, for the credits and scale above it
  const [footH, setFootH] = useState(0);
  /** (T508) the tab bar's height, last measured: the row a held pane will grow into once shown (`heldStyle`) */
  const [tabsH, setTabsH] = useState(0);
  const [runsAt, setRunsAt] = useState<number | null>(null);
  /** (T369 D15) where on that section the Runs tap landed: the water it opens is that tap's */
  const [runsPoint, setRunsPoint] = useState<{ lat: number; lon: number } | null>(null);
  /**
   * The camera survives leaving the map.
   *
   * Opening a water's sheet unmounts the map, so without this it remounted at HOME and a
   * reader who had panned to the Skeena came back to Chilliwack. A ref rather than state:
   * every pan would otherwise re-render the whole shell, and nothing on screen depends on
   * the camera except the map itself.
   */
  const ownCamera = useRef<Camera>(HOME);
  /** The water's map view keeps its own camera (P15), across waters, so the browse map's is left where it was. */
  const ownWaterCamera = useRef<Camera | null>(null);
  // (T369 D12) held by the app when it asks (`cameraHold`): the refs outlive a recreated activity's remount
  const camera = p.cameraHold ? (p.cameraHold.browse ??= ownCamera) : ownCamera;
  const waterCamera = p.cameraHold ? (p.cameraHold.water ??= ownWaterCamera) : ownWaterCamera;
  const search = useShellSearch({ source, nav, camera, recentStore: p.recentSearches ?? null });
  const { searching, session, searchMemory, field } = search;
  /** (owner 2026-10-29) the page started ON a water (a landing): the map is readied behind it once idle (below) */
  const [landed] = useState(() => !!p.arriveAt && p.initialRoute?.mode === "water");
  // ...and only once the water's own page is up (its start applied, `start` null): the map must not race the page's files
  const warm = useIdleOnce(landed && frame !== null && start === null);
  // (T269) a page started on (or at) a water: the map comes back framed on it
  useEffect(() => { if (p.arriveAt) search.landOn(p.arriveAt); }, []);   // eslint-disable-line react-hooks/exhaustive-deps -- once, on mount
  useEscapeBack(nav.back, nav.escapeBack && !searching);   // T214 item 2: on the desktop web Escape is Back (a no-op on a phone)
  /** ONE placeholder line per opening of the app, never cycled while on screen. */
  const [placeholder] = useState(() => placeholderFor(Math.random() * 1e6));
  /*
   * THE MAP RAIL (owner 2026-10-21): the Spots menu, the provisional pin a long press drops, and where "Show" sent the
   * map — `@app/ui` `useMapRail`. Every way of adding a spot ends in the route's `addSpot` at a point: one capture.
   */
  const rail = useMapRail(go, locate, route);
  /** (T303) my location on the browse maps: asked on the button's tap, never before */
  const me = useMyLocation(locate);
  /** (T384) a plain open (no start link took it anywhere, no camera held from before) goes to the reader: read once */
  const [opensHere] = useState(() => opensAtMe(p.initialRoute, p.arriveAt ?? p.regsStart ?? null, !!p.cameraHold?.browse));
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
  const syncState = useSync({ feed, release: loader?.release ?? null, checkRelease, named: releaseNamed, updates });
  /*
   * (T256) A START ON A REGULATIONS PAGE DRAWS THE PAGE FIRST, THE MAP AFTER: the map's status (paint, 0.7 MB) is not
   * asked until the page it opened is drawn (`start` applied) or the reader leaves the water — on one bar the page's own
   * files are not sharing the line with the map's (the boot did not wait for paint either: `setRegsFirstStart`).
   */
  const [statusHeld, setStatusHeld] = useState(() => holdsMapStatus(route, p.regsStart ?? null));
  // released once the page has applied its start, or the page is not in front (the water's Map or Conditions, another
  // screen): released for good (T345: a start on the water's Map held it for ever)
  useEffect(() => { if (statusHeld && !holdsMapStatus(route, start)) setStatusHeld(false); }, [statusHeld, start, route]);
  const col = useMapColouring({ source, feed, status: statusHeld ? null : p.status ?? null, pairing: p.pairing ?? null, tiles,
                                layerStore: p.layerStore, attribution: p.attribution, mapTab: nav.mapTab, mapDay,
                                epoch: syncState.epoch, settled, gearSettled: settled && quietLine,
                                salmonOnDemand: !!(p.prefetch && loader),
                                runTiming: p.runTiming ?? null, runSpeciesStore: p.runSpeciesStore ?? null,
                                initialHorizon: p.initialHorizon ?? 0 });
  /*
   * (T566) THE KEY FOLLOWS THE MAP (`useHeldUntilDrawn`): a Trout / Salmon / Runs press re-lays out every tile on screen,
   * and the map draws the previous colouring until it has (`MapProps.onLookDrawn`). So the key under the map — and which
   * tab's key it is — stays on the colouring the map last said it drew, and moves with the map; meanwhile the pressed
   * button says it is busy (`painting`).
   */
  const lookStore = useHeldLookStore<{ col: typeof col; tab: typeof tab }>();
  const lookKey = JSON.stringify(col.mm.modes);
  lookStore.ask({ col, tab }, lookKey);
  // (T369 D10) the day and the Conditions horizon are the address's (and a recreated activity's): told on each change
  const tellAddress = useRef(p.onAddress);
  tellAddress.current = p.onAddress;
  useEffect(() => { tellAddress.current?.({ day: mapDay, horizon: col.pickedHorizon }); }, [mapDay, col.pickedHorizon]);
  /** (T385) the water header's Share: the same address (route + day + horizon) on the production site */
  const share = useShare(p.share ?? null, loader, route, { day: mapDay, horizon: col.pickedHorizon });
  const hasGaugePage = useGaugePageCheck(loader);
  const sharePorts = useMemo(() => ({ port: p.share ?? null, hasGaugePage }), [p.share, hasGaugePage]);
  // (T199, T261) the runs sheet's words, for the tapped section, the species chosen and the slider's pentad
  const runsVM = useMemo(() => {
    const f = col.runs.facts, sp = col.runs.chosen;
    return runsAt !== null && f && sp.length ? runSheet(f, runsAt, sp, col.runs.pentad) : null;
  }, [runsAt, col.runs.facts, col.runs.chosen, col.runs.pentad]);
  /*
   * YOUR SPOTS ON EVERY MAP (owner 2026-10-22: "toggle our spots on and off in the layer menu"), while the Layers
   * sheet's Spots switch is on (default on); (T131) each a mark that opens its spot, drawn by the map (T316 `spot-marks`), the selected
   * one the rail's; a group no zoom parts opens the Spots list.
   */
  const spotsShown = col.layers.effective.pins.spots;
  const spotPoints = useMemo(() => spotPins(spots, spotsShown), [spots, spotsShown]);
  const spotLayer: MapSpots | undefined = spotPoints.length === 0 ? undefined : {
    points: spotPoints, selected: rail.selected, onOpen: rail.openPin, onList: () => rail.listSpots(),
    // (T316) the marks are map layers, drawn in the spot tokens
    colours: palette.spot,
  };
  /** The SYNC ICON's state (`syncVM`): fresh, needs a refresh, checking — none when there is nothing to sync. */
  const sync = feed || checkRelease || updates
    ? syncVM({ feed: !!feed, feedFetchedAt: syncState.feedFetchedAt, checkedAt: syncState.checkedAt,
               checking: syncState.checking, newRelease: syncState.newRelease, now: clockNow(),
               ...(syncState.update ? { update: syncState.update } : {}) })
    : null;
  /** (T231) the red badge's tap: what could not be updated, and Retry; any other state's tap checks now */
  const [syncErrorOpen, setSyncErrorOpen] = useState(false);
  /** (T231) the bell opened by hand: its sync section and the in-season rows together */
  const [bellOpen, setBellOpen] = useState(false);
  const onSync = sync?.kind === "error" ? () => setSyncErrorOpen(true) : syncState.refresh;
  /*
   * (T231) A DOWNLOADED RELEASE IS APPLIED AT A SAFE MOMENT: nothing the reader is reading changes under them — a
   * browse tab with no water, spot, capture or search open. The app boots onto it and says so ("Updated to …").
   */
  const releaseReady = syncState.updates?.release?.phase === "ready" ? syncState.updates.release.target : null;
  const safe = route.mode === "browse" && frame === null && openSpot === null && !adding && !searching;
  const askedApply = useRef<string | null>(null);
  /*
   * (T287, owner 2026-10-29: "after a download finishes the app freezes for a tiny bit … it should update in background
   * cleanly") AND THE READER STILL: opening the new set (its paint decoded, every screen re-keyed onto it) holds the JS
   * thread for a few hundred ms — felt as a freeze when it lands mid-pan, the moment the download finishes. So the
   * swap also waits for the map to have stood still `APPLY_STILL_MS` (the camera unchanged); a pan restarts the wait.
   * Back from the background stays the other safe moment (App).
   */
  useEffect(() => {
    if (!releaseReady || !safe || askedApply.current === releaseReady) return;
    // a 250 ms poll, re-armed by setTimeout (no interval timer on the JS thread: tools/no-per-frame-animation.test.ts)
    let seen = camera.current, still = clockNow();
    let t: ReturnType<typeof setTimeout>;
    const poll = () => {
      if (camera.current !== seen) { seen = camera.current; still = clockNow(); }
      else if (clockNow() - still >= APPLY_STILL_MS) { askedApply.current = releaseReady; p.onSafeToApply?.(); return; }
      t = setTimeout(poll, 250);
    };
    t = setTimeout(poll, 250);
    return () => clearTimeout(t);
  }, [releaseReady, safe, p.onSafeToApply]);
  /*
   * (T263) THE BELL'S "DATA UPDATE" ROW: the release's state (waiting for Wi-Fi, downloading, ready).
   * "Download now" turns on large updates over mobile data (the sheet asked first); "Apply now" is the safe-moment
   * apply, asked for.
   */
  const releaseVM = releaseNoticeVM(syncState.updates?.release ?? null);
  /*
   * (T390) THE BELL'S "APP UPDATE" SECTION: a newer app (gentle), or a release refused because it needs one (strong:
   * "New data needs the latest app"), with one Update button — Play's in-app update, the App Store page, or the staging
   * APK page (`p.appUpdater`). Checked on every opening and every return to the app, never offline.
   */
  // (T605 item 3) the same return also tells the shared clock: every "today" is re-read at once (`useResumed`)
  const resumed = useResumed();
  const appUpdate = useAppUpdate(p.appUpdater ?? null, p.appUpdateStore ?? null,
                                 { offline: netState.mode.offline, tooOld: syncState.updates?.release?.phase === "app-too-old",
                                   ask: p.openUpdate ?? 0, resume: resumed });
  const appVM = appUpdate?.vm ?? null;
  const releaseCount = (releaseVM?.counts ? 1 : 0) + (appVM?.counts ? 1 : 0);
  const onReleaseAction = (kind: "download" | "apply") => {
    if (kind === "download") syncState.allowMetered?.(true);
    else p.onSafeToApply?.();
  };
  const barBottom = topBarBottom();
  /*
   * (T230) THE PROVINCIAL IN-SEASON CHANGES: read once here and handed to every Regulations page (`InSeasonContext`:
   * the banners); the ones not on the map yet are the notice, which opens by itself once per row per device and from
   * the top bar's bell. Informational: nothing here changes an answer.
   */
  const inSeason = useInSeason(p.inSeason ?? null, syncState.inSeasonEpoch, loader?.manifest?.data_release ?? null);
  const notice = useInSeasonNotice(inSeason.unmatched, p.inSeasonSeen ?? null);
  // (T521) the DFO feed's rules on a water no curated binding knows: the bell's DFO section, counted with the rest
  const dfoNotice = useDfoNotice(p.salmonFeed);
  const dfoCheckedAt = useDfoCheckedAt(p.salmonFeed, loader);   // (T526) when DFO was last CHECKED: the Data line's age
  // (T236) the first-run notice (not legal advice) comes first: nothing else opens by itself while it is up
  const firstRun = useFirstRun(p.firstRun ?? null);
  // (T384) ...once the first-run notice is answered: the platform's prompt never comes up over it
  useOpenAtMe(me, locate, p.locationAsk ?? null, opensHere, !firstRun.open);
  /*
   * (T293) THE GUIDED WALKTHROUGH comes SECOND: held while the notice is up, and the in-season notice waits for it. It
   * starts on the Map tab (where its first step, search, is) and, once started, follows the reader wherever its steps
   * take them — a water, its Conditions, the spot capture, a spot. It reads what is on screen (`guideFacts`) and does a
   * step on request ("Show me", `guideActs`) through the route, search and the opener: nothing of its own. More → Help
   * replays it from the Map tab.
   */
  const coach = useCoachRegistry();
  const onMapTab = route.mode === "browse" && tab === "map" && !rail.picking;
  const [guideWanted, setGuideWanted] = useState(false);
  const guideWater = useGuideWater(source, guideWanted);
  // (T466) the part closed all year the walk walks up to (none in this release: the walk says so), and the walk's pin
  const guideClosed = useGuideClosed(source, loader, guideWanted);
  const [guidePin, setGuidePin] = useState<{ lat: number; lon: number; sid?: number } | null>(null);
  useEffect(() => {
    if (!guideWater || !source.waterPoint) return;
    let live = true;
    void source.waterPoint(guideWater.item as ItemId).then((at) => { if (live) setGuidePin(at); }).catch(reportFailure);
    return () => { live = false; };
  }, [source, guideWater]);
  // (T466) the example spot the walk made (its id), and what became of it at the end: removed, or kept (it could not be)
  const [example, setExample] = useState<{ id: string; state: "removed" | "kept" | null } | null>(null);
  const picked = searchMemory.pick.selected;
  // the layers as the reader had them when the walk reached its Layers steps: put back at the last of them (or a skip)
  const groundNow = col.layers.ground.tiles.find((t) => t.chosen)?.id ?? "";
  const obstaclesNow = !!col.layers.overlays.rows.find((r) => r.id === OBSTACLES)?.chosen;
  const [layersWas, setLayersWas] = useState<{ ground: string; obstacles: boolean } | null>(null);
  // (T456) the controls a screen keeps to itself (a fold's Details, the chart's toggles, the panel's forecast day)
  const guideCtl = useGuideControlHost();
  const guideFacts: GuideFacts = {
    water: guideWater, searching, query: searchMemory.q,
    selected: picked?.kind === "water" ? picked.item : null,
    open: frame ? { item: frame.item, view: frame.view, part: frame.part } : null,
    adding: adding !== null, viewing: openSpot, spots: spots.length,
    closed: guideClosed, example: example?.state ?? null,
    spotsTab: route.mode === "browse" && tab === "spots" && adding === null && openSpot === null && !searching,
    picking: rail.picking, dropped: route.mode === "browse" && rail.drop !== null,
    layersOpen, ground: groundNow, obstacles: obstaclesNow, layersWas,
    onMap: tab === "map" && frame === null && !searching && adding === null && openSpot === null && !rail.picking,
    dayPicked: mapDay !== null, controls: guideCtl.values,
  };
  const restoreLayers = () => {
    if (layersWas) {
      if (groundNow !== layersWas.ground) col.onChooseLayer(layersWas.ground);
      if (obstaclesNow !== layersWas.obstacles) col.onChooseLayer(OBSTACLES);
    }
    setLayersWas(null); setLayersOpen(false);
  };
  // (T234) A NOTIFICATION'S TAP: a gauge alert opens its water's Conditions (an unnamed reach's: the Conditions map)
  const openReq = p.openRequest ?? null;
  useEffect(() => {
    // a remount (an applied update, a recreated activity) does not open the same tap again
    if (!openReq || openReq.n <= openedTap.n) return;
    openedTap.n = openReq.n;
    if (searching) go({ type: "search", open: false });
    if (openReq.item) nav.opener.item(openReq.item as ItemId, openReq.view ?? "conditions");
    else { go({ type: "close" }); nav.toTab("conditions"); }
  }, [openReq?.n]);   // eslint-disable-line react-hooks/exhaustive-deps -- once per tap
  // (T234 v1.1) A NEW-APP-VERSION TAP: the bell opens on its "App update" (with what's new), the version asked now
  useEffect(() => {
    const n = p.openUpdate ?? 0;
    if (n <= openedTap.update) return;
    openedTap.update = n;
    setBellOpen(true);
  }, [p.openUpdate]);
  const guideActs: GuideActs = {
    search: () => { go({ type: "close" }); if (tab !== "map") nav.toTab("map"); go({ type: "search", open: true });
                    search.searchType(guideWater?.name ?? GUIDE_WATER.name); },
    select: () => {
      if (guideWater) search.searchAct({ t: "select", target: { kind: "water", item: guideWater.item as ItemId, name: guideWater.name } });
    },
    openWater: (v) => {
      if (!guideWater) return;
      if (searching) { go({ type: "search", open: false }); search.searchType(""); }
      nav.opener.item(guideWater.item as ItemId, v);
    },
    view: (v) => go({ type: "view", view: v }),
    addSpot: () => {
      go({ type: "close" });
      // the pin ON the water: its own line's point nearest the middle of it (`waterPoint`); none known, no pin
      go({ type: "addSpot", at: guidePin ? { lat: guidePin.lat, lon: guidePin.lon } : null });
    },
    cancelAdd: () => { if (adding) go({ type: "back" }); },
    toSpots: () => go({ type: "tab", tab: "spots" }),
    openSpot: () => {
      const newest = [...spots].sort((a, b) => b.createdAt - a.createdAt)[0];
      if (newest) go({ type: "openSpot", id: newest.id as never });
    },
    openClosed: (v) => {
      if (!guideClosed) return;
      if (searching) { go({ type: "search", open: false }); search.searchType(""); }
      go(openWater({ item: guideClosed.item as ItemId, part: guideClosed.part, view: v }));   // the one builder (one water screen)
    },
    // (T466) adding a spot, the reader's way: the Spots tab's Pick on the map, the pin on the walk's water, Continue
    pickOnMap: () => rail.pick(),
    dropPin: () => { if (guidePin) rail.pressLong(guidePin.lat, guidePin.lon); },
    continueDrop: () => rail.continueDrop(),
    capturePick: () => { if (guidePin?.sid !== undefined) guideCtl.set("capture", { pick: guidePin.sid }); },
    showSpot: () => {
      const s = spots.find((x) => x.id === openSpot);
      if (!s) return;
      camera.current = { lon: s.lon, lat: s.lat, zoom: 14 };
      rail.show(s);
    },
    removeExample: () => {
      const ex = example;
      if (!ex || ex.state !== null || !p.onDeleteSpot) return;
      if (openSpot === ex.id) go({ type: "back" });
      void Promise.resolve(p.onDeleteSpot(ex.id)).then(() => setExample({ ...ex, state: "removed" }),
        (e: unknown) => { reportFailure(e); setExample({ ...ex, state: "kept" }); });
    },
    openLayers: () => {
      go({ type: "close" }); if (tab !== "map") nav.toTab("map");
      if (!layersWas) setLayersWas({ ground: groundNow, obstacles: obstaclesNow });
      setLayersOpen(true);
    },
    otherGround: () => {
      const other = col.layers.ground.tiles.find((t) => !t.chosen && t.enabled);
      if (other) col.onChooseLayer(other.id);
    },
    obstaclesOn: () => { if (!obstaclesNow) col.onChooseLayer(OBSTACLES); },
    restoreLayers,
    toMap: () => { setLayersOpen(false); go({ type: "close" }); if (searching) { go({ type: "search", open: false }); search.searchType(""); }
                   if (tab !== "map") nav.toTab("map"); },
    leaveWater: () => { if (frame) back(); },
    pickDay: () => {
      const d = stepDay(mapDay, 1, bcNow(), dayRange(bcNow(), loader?.manifest?.regulations?.valid_until ?? null));
      if (d !== undefined) setMapDay(d);
    },
    control: guideCtl.set,
  };
  const guide = useGuide(p.walkthrough ?? null, firstRun.open || !onMapTab, guideFacts, guideActs);
  useEffect(() => { if (guide.open || guide.pending) setGuideWanted(true); }, [guide.open, guide.pending]);
  // the Layers sheet opened by the reader at the walk's Layers step: what they had is kept from that moment
  const atKey = guide.open ? GUIDE_STEPS[guide.at]?.key : undefined;
  useEffect(() => {
    if (atKey === "layers" && layersOpen && !layersWas) setLayersWas({ ground: groundNow, obstacles: obstaclesNow });
  }, [atKey, layersOpen]);   // eslint-disable-line react-hooks/exhaustive-deps -- at the moment it opens
  // the walk left (skipped, or Back out of it) mid-Layers: the layers go back as they were
  useEffect(() => { if (!guide.open && layersWas) restoreLayers(); }, [guide.open]);   // eslint-disable-line react-hooks/exhaustive-deps
  // from the start, on the Map tab, with no water open and nothing typed in search
  const replayTour = () => { go({ type: "close" }); nav.toTab("map"); search.searchType(""); setExample(null); guide.reopen(); };
  /** (T466) the drop sheet (a pin down, Continue to come) is up: the card goes in its window */
  const dropUp = route.mode === "browse" && rail.drop !== null;
  /** the walkthrough's card, over whatever is on screen (a spot and the capture included) */
  const guideCard = (
    <GuideCard guide={guide} facts={guideFacts} acts={guideActs} palette={palette} registry={coach}
               top={(route.mode === "browse" && !adding && !openSpot ? topBarBottom() : 0) + 8}
               bottom={bottomBarShown(route) ? TAB_BAR_ROOM : 0} />
  );
  /** (T293, T456) what the walk reads from the screens: the controls' places, their own state, and which water is its */
  const coached = (n: React.ReactNode) => (
    <CoachContext.Provider value={coach}>
      <GuideControlContext.Provider value={guideCtl.sink}>
        <CoachItemContext.Provider value={guideWater?.item ?? null}>{n}</CoachItemContext.Provider>
      </GuideControlContext.Provider>
    </CoachContext.Provider>
  );
  const withGuide = (n: React.ReactNode) => coached(<View style={{ flex: 1 }}>{n}{guideCard}</View>);

  const viewing = openSpot === null ? null : spots.find((s) => s.id === openSpot) ?? null;
  // T51: the open spot against the coming days — the feed the rest of the app reads, and the weather port; asked
  // again when the device comes back online
  // (T294) the visit the page shows (null: the latest): the strip compares with it while no visit is rated, and the
  // day's flow is that visit's
  const shownVisit = viewing ? visitSpot(viewing, spotEdit.shownVisit) : null;
  const like = useLikeVisit(viewing, { weather, feed }, p.likeStore, Date.now, connectivity, shownVisit?.visitId ?? null);
  // T170: the visit day's own flow, and the action that asks for it (waits offline, asks again when back)
  const dayFlow = useDayFlow(shownVisit, p.onDayFlow, connectivity);
  const withInSeason = (n: React.ReactNode) => <InSeasonContext.Provider value={inSeason}>{n}</InSeasonContext.Provider>;
  // (T498) A PANE OPENED OVER THE MAP IS BUILT HIDDEN and shown whole (`shell/paneReveal.ts`): held only when the last
  // screen committed was the browse body (a map or a list tab), never over a spot or the add flow, never at a landing.
  // (T508) Every view on a phone: the Conditions and Map views wait for their own live maps to draw too (`holdViews`)
  const browseShown = useRef(false);
  const pane = usePaneHold(frame !== null && !nav.carrying, browseShown.current && !!frame && holdsView(frame.view));
  // (T498) while a pane is held, the screen is the one the reader left, AS IT WAS: the elements of the last browse
  // render (the map, and the search bar, the map's foot and the tab bar over it), so the route's change (a search
  // consumed, the water's fit, the bars a water hides) reaches none of them under the hold — the screen changes once
  const lastBrowse = useRef<{ map: React.ReactNode; top: React.ReactNode; foot: React.ReactNode; tabs: React.ReactNode } | null>(null);
  const browseNow = frame === null && !viewing && !(adding && onSaveSpot);
  useEffect(() => { browseShown.current = browseNow; });
  if (viewing)
    return layered(withGuide(
      <SpotHost viewing={viewing} spotEdit={spotEdit} nav={nav} palette={palette} camera={camera} like={like}
                dayFlow={dayFlow} photos={photos} onSaveSpot={onSaveSpot} onDeleteSpot={p.onDeleteSpot}
                onRefreshSpots={p.onRefreshSpots} refreshing={p.refreshing} show={rail.show}
                tiles={tiles} theme={theme} locate={locate}
                // (T294) "Add a visit" reads the water the way adding a spot does
                visitSources={{ source, weather, feed,
                                history: { ...(hydat ? { hydat } : {}),
                                           ...(connectivity.watches ? { online: connectivity.online } : {}) } }} />));

  if (adding && onSaveSpot)
    return layered(withGuide(
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
                   onSaved={async (s) => {
                     await onSaveSpot(s);
                     // (T466) made on the walk's spot steps: the example spot, removed as the walk ends (and said)
                     if (guide.open && GUIDE_STEPS[guide.at]?.phase === "spot") setExample({ id: s.id, state: null });
                     go({ type: "openSpot", id: s.id });
                   }} />
    ));

  // (T369 D8) a new release applied with a water open: the water says it is updating while it is carried to the same
  // place in the new data (`useShellNav`'s `carrying`), never drawn from the old release's part indexes
  const water = frame !== null && nav.carrying
    ? <LoadingState palette={palette} size={110} label={WATER_SCREEN_WORDS.updating} shown
                    style={{ flex: 1, alignItems: "center", justifyContent: "center" }} />
    : frame !== null
    ? <WaterModeHost frame={frame} nav={nav} col={col} source={source} palette={palette} loader={loader} tiles={tiles}
                     theme={theme} feed={feed} rail={rail} spotLayer={spotLayer} camera={camera}
                     waterCamera={waterCamera} fishImages={p.fishImages} adiposeImages={p.adiposeImages}
                     mapDay={mapDay} onDay={setMapDay} start={start} onStarted={() => setStart(null)} />
    : null;
  /*
   * (owner 2026-10-29) THE MAP MADE READY BEHIND A LANDING: a page that started on a water (a `/water/<slug>` page) has
   * no map under it, so back used to wait for the map to mount, fetch and colour. Once the page has gone idle the map
   * is mounted behind the water, unseen and untouchable, at the water's own framing (`landOn`'s arrival): its tiles and
   * colours load then, and back shows it painted — the same map, not a new one. Only for a landing: a water tapped from
   * the map, and an ordinary start, are as before.
   */
  const browseLive = frame === null || (landed && warm)
    ? <BrowseHost nav={nav} col={col} search={search} rail={rail} palette={palette} theme={theme}
                  themeName={p.themeName} onTheme={p.onTheme} tiles={tiles} camera={camera} insetTop={barBottom} insetBottom={footH}
                  spots={spots} spotsShown={spotsShown} spotLayer={spotLayer} spotsError={p.spotsError}
                  onRefreshSpots={p.onRefreshSpots} refreshing={p.refreshing} locate={locate} me={me} photos={photos}
                  loader={loader} syncState={syncState} dfoCheckedAt={dfoCheckedAt} sync={sync} onSync={onSync} menu={menu} menuInfo={menuInfo} mapDay={mapDay} regsPrefetch={regsPrefetch}
                  onDay={setMapDay} onOpenDay={() => setDayOpen(true)}
                  dayRange={dayRange(bcNow(), loader?.manifest?.regulations?.valid_until ?? null)}
                  onSaveSpot={onSaveSpot} onLayers={() => setLayersOpen(true)} onRegion={onRegion}
                  offline={background.scheduler ? background.status : null}
                  onObstacle={setObstacle} onTidal={() => setTidalOpen(true)} onNotFishing={setNotFishing} onRuns={(sid, at) => { setRunsAt(sid); setRunsPoint(at); }}
                  fishImages={p.fishImages}
                  lookKey={lookKey} look={lookStore as HeldLookStore<unknown>} />
    : null;

  /** (T48) The top bar — the search field, the sync icon and the bell — is the maps': on Map, Conditions and (T261)
   *  Runs while browsing, so the bell is where it always is on every map. */
  const topBar = route.mode === "browse" && (tab === "map" || tab === "conditions" || tab === "runs") && !rail.picking;
  // the legend is the map's caption: while search's sheet is over the map it captions nothing in view
  // (T566) for the tab and colouring the key is held at — and only while a map tab is in front (no map: no key)
  const mapTabShown = tab === "map" || tab === "conditions" || tab === "runs";
  const showLegendFor = (t: typeof tab, c: typeof col) => route.mode === "browse" && !searching && mapTabShown
    && (t === "conditions"
        || (t === "map" && (c.layers.water.drawn !== "regulations" || c.statusOk)));

  const chromeLive = {
    top: (
      <>
        {topBar && (
          <TopBar ref={field} palette={palette} placeholder={placeholder} sync={sync} onSync={onSync}
                  // (T231, owner 2026-10-28) one bell: the in-season changes not on the map, and the updates that failed
                  notices={{ count: notice.count + dfoNotice.count + syncState.failed.length + releaseCount,
                             label: bellLabel([notice.count ? notice.label : null, dfoNotice.label].filter(Boolean).join("; ") || null, syncState.failed.length, releaseVM?.counts ? releaseVM : null,
                                              appVM?.counts ? appVM : null),
                             onPress: () => setBellOpen(true) }}
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
                       statusIndex={col.statusIndex} day={mapDay} total={counts?.waters}
                       recents={search.recents.list} onRecent={search.pickRecent} onClearRecents={search.recents.clear} />
        )}
      </>
    ),
    foot: (
      <>
          {/* (T566) the key the map is drawn in (`look`): a tab or colouring the map has not drawn yet keeps the last one */}
          <HeldKey store={lookStore}>{({ col: keyCol, tab: keyTab }) => <>
          {showLegendFor(keyTab, keyCol) && (
            <MapLegend col={keyCol} conditions={keyTab === "conditions"} palette={palette} theme={theme} mapDay={mapDay} />
          )}
          {/* (T261) the Runs tab's foot: the date slider, the key, the run-size scale and the credit */}
          {/* (T566) held like the key above: leaving Runs, the run key stays until the map has drawn the Map tab's colours */}
          {route.mode === "browse" && mapTabShown && keyTab === "runs" && !searching && <RunsKey theme={theme} palette={palette} bottom={footH} />}
          {route.mode === "browse" && mapTabShown && keyTab === "runs" && !searching && <RunsLegend col={keyCol} palette={palette} theme={theme} fishImages={p.fishImages} />}
          </>}</HeldKey>
      </>
    ),
    tabs: bottomBarShown(route) ? <TabBar active={tab} palette={palette} onChange={nav.toTab} /> : null,
  };
  if (frame === null) lastBrowse.current = { map: browseLive, ...chromeLive };
  const held = pane.holding ? lastBrowse.current : null;
  const browse = held && !browseLive ? held.map : browseLive;
  const chrome = held ?? chromeLive;
  const browseView = browse && (
    // (T498) while a pane is held the map stays on screen, untouchable, until the pane is shown whole. NEVER COLLAPSED
    // (Fabric): a layout-only wrapper is flattened away, and the hold's pointerEvents made it a real view — the map was
    // re-parented into it, its surface torn down: black, then grey, for every hold (emulator)
    <View key="browse" collapsable={false} style={frame === null || pane.holding ? { flex: 1 } : BEHIND}
          pointerEvents={frame === null ? "auto" : "none"} aria-hidden={frame !== null && !pane.holding}>{browse}</View>
  );
  const waterView = water && (
    <View key="water" collapsable={false} style={pane.holding ? heldStyle(held?.tabs ? tabsH : 0) : { flex: 1 }} pointerEvents={pane.holding ? "none" : "auto"}
          aria-hidden={pane.holding}>
      <PaneReadyContext.Provider value={pane.hold}>{water}</PaneReadyContext.Provider>
    </View>
  );
  const body = <View style={{ flex: 1 }}>{browseView}{waterView}</View>;
  return layered(withInSeason(coached(<>
    {/* a regulations tab's "Report a problem with this page" reads the menu from here (menu/ReportLink) */}
    <MenuContext.Provider value={menu}>
    <SharePortContext.Provider value={sharePorts}>
    <View style={{ flex: 1, backgroundColor: palette.card }}>
      <PairBanner col={col} palette={palette} />
      {/* (T254) a slow or missing line, said once over everything: "Slow connection — showing what we have" */}
      <NetBanner palette={palette} onPress={() => nav.toTab("more")} />
      <View style={{ flex: 1 }}>
        <EdgeBack drag={nav.edge}>
          <WaterViewsContext.Provider value={nav.waterViews && share ? { ...nav.waterViews, share } : nav.waterViews}>
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
        {/* (T498) held, the frozen bars take no press (their handlers are the last browse render's); one wrapper always,
            so the hold neither mounts nor remounts them */}
        <View collapsable={false} style={{ position: "absolute", left: 0, right: 0, top: 0, bottom: 0, zIndex: 2 }}
              pointerEvents={held ? "none" : "box-none"}>{chrome.top}</View>
        {/* PICK ON THE MAP (owner 2026-10-22): the slim banner in the search field's place; a back layer */}
        {rail.picking && route.mode === "browse" && tab === "map" && (
          <SpotPickBanner palette={palette} onCancel={rail.cancelPick} />
        )}
        {/* (T275) THE FOOT OVERLAYS THE MAP: a block at the bottom of the map area that hangs up over the map. Its height
            changes with the mode (Standard / Salmon / Runs / Conditions) and as a flex sibling it resized the map each
            time; now the map keeps its size and camera, and only the block changes. (T397) It is INSIDE the map area,
            not hung from a zero-height row: Android delivers a native drag only within the parent's bounds, so the Runs
            species row (a native horizontal ScrollView) never scrolled, while taps (hit-tested in JS) still worked. */}
        <View testID="map-foot" style={{ position: "absolute", left: 0, right: 0, bottom: 0, zIndex: 1 }} pointerEvents="box-none"
              onLayout={(e) => setFootH(Math.round(e.nativeEvent.layout.height))}>
          {chrome.foot}
        </View>
      </View>
      {/* A tab change is a change of subject: the remembered reach goes with it. Hidden in water mode (P13/P14). */}
      {chrome.tabs && <View collapsable={false} pointerEvents={held ? "none" : "auto"}
                            onLayout={(e) => setTabsH(Math.round(e.nativeEvent.layout.height))}>{chrome.tabs}</View>}
      {/* (T215) the synopsis PDF is loading / failed: progress, Cancel, Retry */}
      <SynopsisLoading palette={palette} floating />
      {/* (T354) the rule book itself, in the app (native; the web opens the browser's tab) */}
      <PdfViewer palette={palette} />
      {/* G1: an open sheet is a back layer — every platform's back closes it before it pops the route */}
      <LayersSheet open={layersOpen} onClose={() => setLayersOpen(false)} palette={palette}
                   vm={col.layers} onChoose={col.onChooseLayer}
                   // (T293) while the sheet is up the walkthrough's card is drawn in its window (a Modal is on top)
                   overlay={layersOpen && guide.open ? guideCard : undefined} />
      {/*
        * (T49) The map's date sheet: its calendar button opens the SAME sheet the Regulations page's date pill does
        * (`regs/DaySheet`), on this one `mapDay` — and the days it offers are the data's calendar year, cut at the
        * release's last day (`dayRange`), the same range the page's sheet is handed by `WaterScreen`.
        */}
      <DaySheet open={dayOpen && route.mode === "browse"} onClose={() => setDayOpen(false)} palette={palette}
                day={mapDay} onDay={setMapDay}
                range={dayRange(bcNow(), loader?.manifest?.regulations?.valid_until ?? null)} />
      {/* (T173) a tapped fish obstacle mark: what it is (Map tab, while browsing) */}
      <ObstacleSheet props={route.mode === "browse" ? obstacle : null} palette={palette} onClose={() => setObstacle(null)} />
      {/* (T166) a tap on tidal water: why the freshwater regulations stop here (Map tab, while browsing) */}
      <TidalSheet open={route.mode === "browse" && tidalOpen} palette={palette} onClose={() => setTidalOpen(false)} />
      {/* (T292) a tap on a sewage lagoon / tailings pond / dump: what it is, and that it is not a fishing water */}
      <NotFishingSheet kind={route.mode === "browse" ? notFishing : undefined} palette={palette}
                       onClose={() => setNotFishing(undefined)} />
      {/* (T199, T261) a tap on the Runs map: the runs on that section, and on to its regulations */}
      <RunsSheet vm={route.mode === "browse" && tab === "runs" ? runsVM : null} palette={palette} theme={theme}
                 fishImages={p.fishImages} onClose={() => setRunsAt(null)}
                 onOpenWater={runsAt === null ? undefined : () => {
                   const sid = runsAt;
                   setRunsAt(null);
                   void nav.opener.tap(sid as never, runsPoint, "regs");
                 }} />
      <ShellSheets nav={nav} col={col} rail={rail} menu={menu} palette={palette} loader={loader} menuInfo={menuInfo} source={source}
                   mapDay={mapDay} theme={theme} onWalkthrough={replayTour} sync={syncState}
                   // (T466) the drop sheet is its own window: the walk's card is drawn in it while it is up
                   dropOverlay={dropUp && guide.open ? guideCard : undefined}
                   // (T352) where the map was looking, read at this render (a report freezes it when it opens): the water's
                   // own map while it is in front, else the browse map's
                   camera={frame?.view === "map" ? waterCamera.current ?? camera.current : camera.current} />
      {/* (T230) the in-season changes not on the map yet: once per row per device, and from the bell */}
      <InSeasonNotice open={(notice.open && !firstRun.open && !guide.open) || bellOpen} rows={notice.rows} dfo={dfoNotice}
                      palette={palette} onClose={() => { setBellOpen(false); notice.close(); }}
                      sync={{ failed: syncState.failed, checking: syncState.checking, onRetry: syncState.refresh }}
                      release={releaseVM ? { vm: releaseVM, onAction: onReleaseAction } : null}
                      app={appVM && appUpdate ? { vm: appVM, onUpdate: appUpdate.update } : null} />
      {/* (T236) not legal advice: once per device, before anything else */}
      {/* (T257) its "Sources and licences" opens that page: the notice steps aside while a page is up and is back after */}
      <FirstRunNotice state={{ ...firstRun, open: firstRun.open && menu.state.page === null }} palette={palette}
                      onSources={() => menu.show("sources")} />
      {!layersOpen && !(dropUp && guide.open) && guideCard}
      {/* (T231) the sync icon's red badge: what could not be updated, when each last succeeded, Retry */}
      <SyncErrorSheet open={syncErrorOpen && syncState.failed.length > 0} failed={syncState.failed} palette={palette}
                      checking={syncState.checking} onRetry={syncState.refresh} onClose={() => setSyncErrorOpen(false)} />
    </View>
    </SharePortContext.Provider>
    </MenuContext.Provider>
  </>)));
}
