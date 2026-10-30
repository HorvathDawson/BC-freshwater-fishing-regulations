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
import { GUIDE_WATER, TAB_TOUR_WORDS, bellLabel, releaseNoticeVM, dayRange, placeholderFor, runSheet, syncVM, type NotFishingKind } from "@app/core";
import type { ItemId } from "@app/data";
import { useEffect, useMemo, useRef, useState } from "react";
import { Linking, Platform, View } from "react-native";
import { bottomBarShown, spotPins, useDayFlow, useLikeVisit, useMapRail, useMyLocation, useMenu, useDataFacts, useSync,
         BackLayerContext, InSeasonContext, useIdleOnce, useInSeason, useInSeasonNotice, useReleasePrefetch, useBackgroundPrefetch, useNet, useNetQuiet, useRegsPrefetch, type RegsStart } from "@app/ui";
import type { Camera, MapSpots } from "@app/map";
import { visitSpot } from "@app/data/spots";
import { connectivity } from "./connectivity";
import { SearchSheet } from "./SearchSheet";
import { TopBar, topBarBottom } from "./TopBar";
import { SpotCapture } from "./SpotCapture";
import { SpotPickBanner } from "./SpotPickBanner";
import { SynopsisLoading } from "./regs/SynopsisLoading";
import { SpotPin } from "./SpotPin";
import { TabBar } from "./TabBar";
import { EdgeBack } from "./EdgeBack";
import { LayersSheet } from "./LayersSheet";
import { ObstacleSheet } from "./ObstacleSheet";
import { TidalSheet } from "./TidalSheet";
import { NotFishingSheet } from "./NotFishingSheet";
import { RunsSheet } from "./RunsSheet";
import { InSeasonNotice } from "./InSeasonNotice";
import { SyncErrorSheet } from "./SyncErrorSheet";
import { DaySheet } from "./regs/DaySheet";
import { WaterViewsContext } from "./ViewSwipe";
import { MenuContext } from "./menu/MenuContext";
import { BrowseHost } from "./shell/BrowseHost";
import { MapLegend, PairBanner, RunsKey, RunsLegend } from "./shell/MapLegend";
import { NetBanner } from "./NetBanner";
import { ShellSheets } from "./shell/ShellSheets";
import { SpotHost, useSpotEdit } from "./shell/SpotHost";
import { WaterModeHost } from "./shell/WaterModeHost";
import type { ShellProps } from "./shell/props";
import { HOME, useMapColouring } from "./shell/useMapColouring";
import { useShellNav } from "./shell/useShellNav";
import { useEscapeBack } from "./escapeBack";
import { useShellSearch } from "./shell/useShellSearch";
import { FirstRunNotice, useFirstRun } from "./FirstRunNotice";
import { GUIDE_STEPS, GuideCard, TabTourSheet, useGuide, useTabTour, type GuideActs, type GuideFacts } from "./Walkthrough";
import { CoachContext, useCoachRegistry } from "./CoachMarks";

export type { ShellProps } from "./shell/props";

/** (owner 2026-10-29) the map readied behind a landing's water: the whole box, unseen (painted, not shown) */
const BEHIND = { position: "absolute", top: 0, left: 0, right: 0, bottom: 0, opacity: 0 } as const;

/** (T293) The Conditions tour's gauge: of the bigger-river gauges, the nearest to the camera, with its catchment's area. */
async function nearGauge(source: ShellProps["source"], at: Camera): Promise<{ lon: number; lat: number; km2: number } | null> {
  try {
    const pts = await source.gaugePoints();
    if (!pts.length) return null;
    const mags = pts.map((p) => p.mag ?? 0).sort((a, b) => a - b);
    const floor = mags[Math.floor(mags.length * 0.75)] ?? 0;
    const k = Math.cos((at.lat * Math.PI) / 180);
    const d = (p: { lon: number; lat: number }) => ((p.lon - at.lon) * k) ** 2 + (p.lat - at.lat) ** 2;
    const big = pts.filter((p) => (p.mag ?? 0) >= floor);
    const g = (big.length ? big : pts).reduce((a, b) => (d(b) < d(a) ? b : a));
    let km2 = 0;
    for (const ms of (await source.basinMembers().catch(() => new Map())).values())
      for (const m of ms) if (m.station === g.station && (m.areaKm2 ?? 0) > km2) km2 = m.areaKm2 ?? 0;
    return { lon: g.lon, lat: g.lat, km2: km2 || 1000 };
  } catch { return null; }
}

/** (T293) The fish obstacles' switch in the Layers sheet (`@app/core` layers). */
const OBSTACLES = "overlay.obstacles";

/** (T293) The room the bottom tab bar takes, kept clear by the walkthrough's card docked at the foot. */
const TAB_BAR_ROOM = 64;

/**
 * (T293) THE WALKTHROUGH'S WATER (`GUIDE_WATER`): found by its name in the release once, a stream of exactly that name.
 * Not found (a release without it): null, and the walkthrough takes any water the reader picks.
 */
function useGuideWater(source: ShellProps["source"]): { item: string; name: string } | null {
  const [w, setW] = useState<{ item: string; name: string } | null>(null);
  useEffect(() => {
    let live = true;
    void source.searchNames(GUIDE_WATER.name, 10).then((hits) => {
      const h = hits.find((x) => x.name === GUIDE_WATER.name && x.kind === GUIDE_WATER.kind) ?? hits.find((x) => x.name === GUIDE_WATER.name);
      if (live && h) setW({ item: h.item, name: h.name });
    }).catch(() => {});
    return () => { live = false; };
  }, [source]);
  return w;
}

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
  const [runsAt, setRunsAt] = useState<number | null>(null);
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
  const [statusHeld, setStatusHeld] = useState(() => !!p.regsStart && route.mode === "water");
  useEffect(() => { if (statusHeld && (start === null || route.mode !== "water")) setStatusHeld(false); },
            [statusHeld, start, route.mode]);
  const col = useMapColouring({ source, feed, status: statusHeld ? null : p.status ?? null, pairing: p.pairing ?? null, tiles,
                                layerStore: p.layerStore, attribution: p.attribution, mapTab: nav.mapTab, mapDay,
                                epoch: syncState.epoch, settled, gearSettled: settled && quietLine,
                                salmonOnDemand: !!(p.prefetch && loader),
                                runTiming: p.runTiming ?? null, runSpeciesStore: p.runSpeciesStore ?? null });
  // (T199, T261) the runs sheet's words, for the tapped section, the species chosen and the slider's pentad
  const runsVM = useMemo(() => {
    const f = col.runs.facts, sp = col.runs.chosen;
    return runsAt !== null && f && sp.length ? runSheet(f, runsAt, sp, col.runs.pentad) : null;
  }, [runsAt, col.runs.facts, col.runs.chosen, col.runs.pentad]);
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
  const sync = feed || checkRelease || updates
    ? syncVM({ feed: !!feed, feedFetchedAt: syncState.feedFetchedAt, checkedAt: syncState.checkedAt,
               checking: syncState.checking, newRelease: syncState.newRelease, now: Date.now(),
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
    let seen = camera.current, still = Date.now();
    let t: ReturnType<typeof setTimeout>;
    const poll = () => {
      if (camera.current !== seen) { seen = camera.current; still = Date.now(); }
      else if (Date.now() - still >= APPLY_STILL_MS) { askedApply.current = releaseReady; p.onSafeToApply?.(); return; }
      t = setTimeout(poll, 250);
    };
    t = setTimeout(poll, 250);
    return () => clearTimeout(t);
  }, [releaseReady, safe, p.onSafeToApply]);
  /*
   * (T263) THE BELL'S "DATA UPDATE" ROW: the release's state (waiting for Wi-Fi, downloading, ready, needs a newer app).
   * "Download now" turns on large updates over mobile data (the sheet asked first); "Apply now" is the safe-moment
   * apply, asked for; "Update the app" opens the store listing, or in a dev/staging build the APK page (EXPO_PUBLIC_DEV_APK_URL).
   */
  // staging / dev builds (any environment but production): the APK page; production: the store listing by platform
  // (placeholder listings until the real ones exist; the build's EXPO_PUBLIC_* values win)
  const devBuild = (process.env.EXPO_PUBLIC_APP_ENV || "") !== "production";
  const storeUrl = Platform.OS === "ios"
    ? process.env.EXPO_PUBLIC_APP_STORE_URL || "https://apps.apple.com/ca/app/canifishthis/id0000000000"
    : process.env.EXPO_PUBLIC_PLAY_STORE_URL || "https://play.google.com/store/apps/details?id=ca.canifishthis.app";
  const appUrl = devBuild ? process.env.EXPO_PUBLIC_DEV_APK_URL || "" : storeUrl;
  const releaseVM = releaseNoticeVM(syncState.updates?.release ?? null, { devBuild });
  const releaseCount = releaseVM?.counts ? 1 : 0;
  const onReleaseAction = (kind: "download" | "apply" | "update-app") => {
    if (kind === "download") syncState.allowMetered?.(true);
    else if (kind === "apply") p.onSafeToApply?.();
    else if (appUrl) void Linking.openURL(appUrl);
  };
  const barBottom = topBarBottom();
  /*
   * (T230) THE PROVINCIAL IN-SEASON CHANGES: read once here and handed to every Regulations page (`InSeasonContext`:
   * the banners); the ones not on the map yet are the notice, which opens by itself once per row per device and from
   * the top bar's bell. Informational: nothing here changes an answer.
   */
  const inSeason = useInSeason(p.inSeason ?? null, syncState.inSeasonEpoch, loader?.manifest?.data_release ?? null);
  const notice = useInSeasonNotice(inSeason.unmatched, p.inSeasonSeen ?? null);
  // (T236) the first-run notice (not legal advice) comes first: nothing else opens by itself while it is up
  const firstRun = useFirstRun(p.firstRun ?? null);
  /*
   * (T293) THE GUIDED WALKTHROUGH comes SECOND: held while the notice is up, and the in-season notice waits for it. It
   * starts on the Map tab (where its first step, search, is) and, once started, follows the reader wherever its steps
   * take them — a water, its Conditions, the spot capture, a spot. It reads what is on screen (`guideFacts`) and does a
   * step on request ("Show me", `guideActs`) through the route, search and the opener: nothing of its own. More → Help
   * replays it from the Map tab.
   */
  const coach = useCoachRegistry();
  const onMapTab = route.mode === "browse" && tab === "map" && !rail.picking;
  const guideWater = useGuideWater(source);
  const picked = searchMemory.pick.selected;
  // the layers as the reader had them when the walk reached its Layers steps: put back at the last of them (or a skip)
  const groundNow = col.layers.ground.tiles.find((t) => t.chosen)?.id ?? "";
  const obstaclesNow = !!col.layers.overlays.rows.find((r) => r.id === OBSTACLES)?.chosen;
  const [layersWas, setLayersWas] = useState<{ ground: string; obstacles: boolean } | null>(null);
  const guideFacts: GuideFacts = {
    water: guideWater, searching, query: searchMemory.q,
    selected: picked?.kind === "water" ? picked.item : null,
    open: frame ? { item: frame.item, view: frame.view } : null,
    adding: adding !== null, viewing: openSpot, spots: spots.length,
    layersOpen, ground: groundNow, obstacles: obstaclesNow, layersWas,
  };
  const restoreLayers = () => {
    if (layersWas) {
      if (groundNow !== layersWas.ground) col.onChooseLayer(layersWas.ground);
      if (obstaclesNow !== layersWas.obstacles) col.onChooseLayer(OBSTACLES);
    }
    setLayersWas(null); setLayersOpen(false);
  };
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
      if (!guideWater) { go({ type: "addSpot" }); return; }
      // the pin ON the water: its own line's point nearest the middle of it (`waterPoint`); none known, no pin
      void (source.waterPoint?.(guideWater.item as ItemId) ?? Promise.resolve(null)).catch(() => null)
        .then((at) => go({ type: "addSpot", at }));
    },
    cancelAdd: () => { if (adding) go({ type: "back" }); },
    toSpots: () => go({ type: "tab", tab: "spots" }),
    openSpot: () => {
      const newest = [...spots].sort((a, b) => b.createdAt - a.createdAt)[0];
      if (newest) go({ type: "openSpot", id: newest.id as never });
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
  };
  const guide = useGuide(p.walkthrough ?? null, firstRun.open || !onMapTab, guideFacts, guideActs);
  // the Layers sheet opened by the reader at the walk's Layers step: what they had is kept from that moment
  const atKey = guide.open ? GUIDE_STEPS[guide.at]?.key : undefined;
  useEffect(() => {
    if (atKey === "layers" && layersOpen && !layersWas) setLayersWas({ ground: groundNow, obstacles: obstaclesNow });
  }, [atKey, layersOpen]);   // eslint-disable-line react-hooks/exhaustive-deps -- at the moment it opens
  // the walk left (skipped, or Back out of it) mid-Layers: the layers go back as they were
  useEffect(() => { if (!guide.open && layersWas) restoreLayers(); }, [guide.open]);   // eslint-disable-line react-hooks/exhaustive-deps
  // from the start, on the Map tab, with no water open and nothing typed in search
  const replayTour = () => { go({ type: "close" }); nav.toTab("map"); search.searchType(""); guide.reopen(); };
  // (T276, T293) the mode hints — Runs, the Conditions map, Salmon: on a mode's first use, after the notice and the
  // walkthrough; More → Help replays each
  const tabBusy = firstRun.open || guide.open || guide.pending || searching;
  const runsTour = useTabTour(p.walkthrough ?? null, "runs", route.mode === "browse" && tab === "runs", tabBusy);
  const condTour = useTabTour(p.walkthrough ?? null, "conditions", route.mode === "browse" && tab === "conditions", tabBusy);
  const salmonTour = useTabTour(p.walkthrough ?? null, "salmon",
                                onMapTab && col.regsLayer === "salmon" && !col.salmonLoading, tabBusy);
  // (T303) one hint at the my-location button, on the Map tab
  const locateTour = useTabTour(me ? p.walkthrough ?? null : null, "locate", onMapTab, tabBusy);
  /*
   * (T293, owner 2026-10-29) THE CONDITIONS TOUR MOVES THE REAL CAMERA: out to the watershed of a gauge near where the
   * map is (its catchment's size sets how far: about twice the width of a square of that area), then in to the gauge.
   * The gauge: of the bigger-river gauges (stream magnitude in the top quarter), the nearest to the map's centre.
   */
  const [tourFit, setTourFit] = useState<{ key: string; camera: Camera } | null>(null);
  const tourGauge = useRef<Promise<{ lon: number; lat: number; km2: number } | null> | null>(null);
  const onCondStep = (key: string) => {
    const kind = TAB_TOUR_WORDS.conditions.find((c) => c.key === key)?.camera;
    if (!kind) return;
    tourGauge.current ??= nearGauge(source, camera.current);
    void tourGauge.current.then((g) => {
      if (!g) return;
      const width = Math.max(320, Platform.OS === "web" && typeof window !== "undefined" ? window.innerWidth : 390);
      const spanM = 2.2 * Math.sqrt(g.km2) * 1000;
      const fitZ = Math.log2((156543 * Math.cos((g.lat * Math.PI) / 180) * width) / spanM);
      const zoom = kind === "watershed" ? Math.max(6, Math.min(8.5, fitZ)) : 10.5;
      setTourFit({ key: `tour:${key}:${Date.now()}`, camera: { lon: g.lon, lat: g.lat, zoom } });
    });
  };
  useEffect(() => { if (!condTour.open) { setTourFit(null); tourGauge.current = null; } }, [condTour.open]);
  const replayTab = (k: "runs" | "conditions" | "salmon") => {
    go({ type: "close" });
    if (k === "salmon") { nav.toTab("map"); col.setRegsLayer("salmon"); salmonTour.reopen(); return; }
    nav.toTab(k); (k === "runs" ? runsTour : condTour).reopen();
  };
  /** the walkthrough's card, over whatever is on screen (a spot and the capture included) */
  const guideCard = (
    <GuideCard guide={guide} facts={guideFacts} acts={guideActs} palette={palette} registry={coach}
               top={(route.mode === "browse" && !adding && !openSpot ? topBarBottom() : 0) + 8}
               bottom={bottomBarShown(route) ? TAB_BAR_ROOM : 0} />
  );
  const withGuide = (n: React.ReactNode) => (
    <CoachContext.Provider value={coach}>
      <View style={{ flex: 1 }}>{n}{guideCard}</View>
    </CoachContext.Provider>
  );

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
  if (viewing)
    return layered(withGuide(
      <SpotHost viewing={viewing} spotEdit={spotEdit} nav={nav} palette={palette} camera={camera} like={like}
                dayFlow={dayFlow} photos={photos} onSaveSpot={onSaveSpot} onDeleteSpot={p.onDeleteSpot}
                onRefreshSpots={p.onRefreshSpots} refreshing={p.refreshing} show={rail.show}
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
                   onSaved={async (s) => { await onSaveSpot(s); go({ type: "openSpot", id: s.id }); }} />
    ));

  const water = frame !== null
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
  const browse = frame === null || (landed && warm)
    ? <BrowseHost nav={nav} col={col} search={search} rail={rail} palette={palette} theme={theme}
                  themeName={p.themeName} onTheme={p.onTheme} tiles={tiles} camera={camera} insetTop={barBottom} insetBottom={footH}
                  spots={spots} spotsShown={spotsShown} spotLayer={spotLayer} spotsError={p.spotsError}
                  onRefreshSpots={p.onRefreshSpots} refreshing={p.refreshing} locate={locate} me={me} photos={photos}
                  loader={loader} syncState={syncState} sync={sync} onSync={onSync} menu={menu} menuInfo={menuInfo} mapDay={mapDay} regsPrefetch={regsPrefetch}
                  onDay={setMapDay} onOpenDay={() => setDayOpen(true)}
                  dayRange={dayRange(new Date(), loader?.manifest?.regulations?.valid_until ?? null)}
                  onSaveSpot={onSaveSpot} onLayers={() => setLayersOpen(true)} onRegion={onRegion}
                  offline={background.scheduler ? background.status : null}
                  onObstacle={setObstacle} onTidal={() => setTidalOpen(true)} onNotFishing={setNotFishing} onRuns={setRunsAt}
                  fishImages={p.fishImages} tourFit={tourFit} />
    : null;
  const body = (
    <View style={{ flex: 1 }}>
      {browse && (
        <View key="browse" style={frame === null ? { flex: 1 } : BEHIND} pointerEvents={frame === null ? "auto" : "none"}
              aria-hidden={frame !== null}>{browse}</View>
      )}
      {water && <View key="water" style={{ flex: 1 }}>{water}</View>}
    </View>
  );

  /** (T48) The top bar — the search field, the sync icon and the bell — is the maps': on Map, Conditions and (T261)
   *  Runs while browsing, so the bell is where it always is on every map. */
  const topBar = route.mode === "browse" && (tab === "map" || tab === "conditions" || tab === "runs") && !rail.picking;
  // the legend is the map's caption: while search's sheet is over the map it captions nothing in view
  const showLegend = route.mode === "browse" && !searching
    && (tab === "conditions"
        || (tab === "map" && (col.layers.water.drawn !== "regulations" || col.statusOk)));

  return layered(withInSeason(
    <CoachContext.Provider value={coach}>
    {/* a regulations tab's "Report a problem with this page" reads the menu from here (menu/ReportLink) */}
    <MenuContext.Provider value={menu}>
    <View style={{ flex: 1, backgroundColor: palette.card }}>
      <PairBanner col={col} palette={palette} />
      {/* (T254) a slow or missing line, said once over everything: "Slow connection — showing what we have" */}
      <NetBanner palette={palette} onPress={() => nav.toTab("more")} />
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
          <TopBar ref={field} palette={palette} placeholder={placeholder} sync={sync} onSync={onSync}
                  // (T231, owner 2026-10-28) one bell: the in-season changes not on the map, and the updates that failed
                  notices={{ count: notice.count + syncState.failed.length + releaseCount,
                             label: bellLabel(notice.count ? notice.label : null, syncState.failed.length, releaseVM?.counts ? releaseVM : null),
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
                       statusIndex={col.statusIndex} total={counts?.waters}
                       recents={search.recents.list} onRecent={search.pickRecent} onClearRecents={search.recents.clear} />
        )}
        {/* PICK ON THE MAP (owner 2026-10-22): the slim banner in the search field's place; a back layer */}
        {rail.picking && route.mode === "browse" && tab === "map" && (
          <SpotPickBanner palette={palette} onCancel={rail.cancelPick} />
        )}
      </View>
      {/* (T275) THE FOOT OVERLAYS THE MAP: a zero-height row at the bottom of the page whose block hangs up over the map. The
          block's height changes with the mode (Standard / Salmon / Runs / Conditions) and as a flex sibling it resized the
          map each time; now the map keeps its size and camera, and only the block changes. */}
      <View testID="map-foot" style={{ height: 0, zIndex: 1 }} pointerEvents="box-none">
        <View style={{ position: "absolute", left: 0, right: 0, bottom: 0 }} pointerEvents="box-none"
              onLayout={(e) => setFootH(Math.round(e.nativeEvent.layout.height))}>
          {showLegend && (
            <MapLegend col={col} conditions={tab === "conditions"} palette={palette} theme={theme} mapDay={mapDay} />
          )}
          {/* (T261) the Runs tab's foot: the date slider, the key, the run-size scale and the credit */}
          {route.mode === "browse" && tab === "runs" && !searching && <RunsKey theme={theme} palette={palette} bottom={footH} />}
          {route.mode === "browse" && tab === "runs" && !searching && <RunsLegend col={col} palette={palette} theme={theme} onHelp={runsTour.reopen} />}
        </View>
      </View>
      {/* A tab change is a change of subject: the remembered reach goes with it. Hidden in water mode (P13/P14). */}
      {bottomBarShown(route) && <TabBar active={tab} palette={palette} onChange={nav.toTab} />}
      {/* (T215) the synopsis PDF is loading / failed: progress, Cancel, Retry */}
      <SynopsisLoading palette={palette} floating />
      {/* G1: an open sheet is a back layer — every platform's back closes it before it pops the route */}
      <LayersSheet open={layersOpen} onClose={() => setLayersOpen(false)} palette={palette}
                   vm={col.layers} onChoose={col.onChooseLayer}
                   // (T293) while the sheet is up the walkthrough's card is drawn in its window (a Modal is on top)
                   overlay={layersOpen ? guideCard : undefined} />
      {/*
        * (T49) The map's date sheet: its calendar button opens the SAME sheet the Regulations page's date pill does
        * (`regs/DaySheet`), on this one `mapDay` — and the days it offers are the data's calendar year, cut at the
        * release's last day (`dayRange`), the same range the page's sheet is handed by `WaterScreen`.
        */}
      <DaySheet open={dayOpen && route.mode === "browse"} onClose={() => setDayOpen(false)} palette={palette}
                day={mapDay} onDay={setMapDay}
                range={dayRange(new Date(), loader?.manifest?.regulations?.valid_until ?? null)} />
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
                   void nav.opener.tap(sid as never, null, "regs");
                 }} />
      <ShellSheets nav={nav} col={col} rail={rail} menu={menu} palette={palette} loader={loader} menuInfo={menuInfo}
                   mapDay={mapDay} theme={theme} onWalkthrough={replayTour} onTabTour={replayTab} sync={syncState} />
      {/* (T230) the in-season changes not on the map yet: once per row per device, and from the bell */}
      <InSeasonNotice open={(notice.open && !firstRun.open && !guide.open) || bellOpen} rows={notice.rows}
                      palette={palette} onClose={() => { setBellOpen(false); notice.close(); }}
                      sync={{ failed: syncState.failed, checking: syncState.checking, onRetry: syncState.refresh }}
                      release={releaseVM ? { vm: releaseVM.phase === "app-too-old" && !appUrl ? { ...releaseVM, action: null } : releaseVM,
                                             onAction: onReleaseAction } : null} />
      {/* (T236) not legal advice: once per device, before anything else */}
      {/* (T257) its "Sources and licences" opens that page: the notice steps aside while a page is up and is back after */}
      <FirstRunNotice state={{ ...firstRun, open: firstRun.open && menu.state.page === null }} palette={palette}
                      onSources={() => menu.show("sources")} />
      <TabTourSheet tour={runsTour} which="runs" palette={palette} registry={coach} />
      <TabTourSheet tour={condTour} which="conditions" palette={palette} registry={coach} onStep={onCondStep} />
      <TabTourSheet tour={salmonTour} which="salmon" palette={palette} registry={coach} />
      <TabTourSheet tour={locateTour} which="locate" palette={palette} registry={coach} />
      {!layersOpen && guideCard}
      {/* (T231) the sync icon's red badge: what could not be updated, when each last succeeded, Retry */}
      <SyncErrorSheet open={syncErrorOpen && syncState.failed.length > 0} failed={syncState.failed} palette={palette}
                      checking={syncState.checking} onRetry={syncState.refresh} onClose={() => setSyncErrorOpen(false)} />
    </View>
    </MenuContext.Provider>
    </CoachContext.Provider>
  ));
}
