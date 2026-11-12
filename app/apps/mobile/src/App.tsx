/**
 * The app shell. Views only — no domain logic ever lands here (layers.json).
 *
 * ONE tree for all three targets: expo start --web renders this through react-native-web,
 * expo run:android / run:ios render the same file natively. Every screen lives in
 * @app/ui-native, so mobile web and the native app cannot show different things.
 *
 * THE DATA IS THE RELEASE (P11): the manifest, then paint, then the Shell (`useReleaseBoot` → `bootRelease`). On the
 * phone the first run downloads the whole release with progress and every later launch opens it from disk, checking for
 * a newer one in the background; on the web each file is fetched on first need into Cache Storage. It is the only data
 * path (the development bundle path was retired, T61). `atlas.meta.json` is read only while the manifest names no tile
 * set: the map then draws the development tiles, and their stamped `tile_version` is what proves the pair (`pairingOf`).
 */
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { AppState, Platform, Share, StatusBar, Text, View, useColorScheme, useWindowDimensions } from "react-native";
import { SafeAreaProvider, SafeAreaView, useSafeAreaInsets } from "react-native-safe-area-context";
import { useFonts } from "expo-font";
import {
  BricolageGrotesque_700Bold, BricolageGrotesque_800ExtraBold,
} from "@expo-google-fonts/bricolage-grotesque";
import {
  Archivo_400Regular, Archivo_500Medium, Archivo_600SemiBold, Archivo_700Bold, Archivo_800ExtraBold,
} from "@expo-google-fonts/archivo";
import { JetBrainsMono_500Medium } from "@expo-google-fonts/jetbrains-mono";
import { DOWNLOAD_NOTE_WORDS, downloadFraction, INSTALL_WORDS, MAP_CREDITS, MOBILE_DATA_WORDS, mobileDataDecision, installLine, SETUP_WORDS, setupFraction, setupLine, netBanner, progressWords, rolledBackLine, splashLineFor, startStage, updatedToast, UPDATE_LOG_WORDS, UPDATE_WORDS, versionHeaders,
         versionLine, dataProvenanceLine, now as clockNow } from "@app/core";
import { createNet, shareFetch, createReportOutbox, createUpdateLog, createUpdateService, phoneNetwork, versionedFetch, httpFeed, gaugeHistory, httpInSeason, httpRunTiming, httpSalmonFeed, manifestDate, noteHeld, followSavedItems, openRunTimingStore,
         type Held, type UpdateService, type WaterSource } from "@app/data";
import { bootSaid, type MobileDataGate, type ReleaseLoader } from "@app/data/release";
import * as platform from "@app/data/release/platform";
import { fillDayFlow, onVisit, visitIdOf, storedHydat, openLayerStore, openLikeStore, openSalmonFeedStore, openGaugeHistoryStore, openInSeasonStore, openInSeasonSeenStore, openFirstRunStore, openDataModeStore, openWalkthroughStore, openLocationAskStore, openAppUpdateStore, openRecentSearchStore, openRunSpeciesStore, openUpdateStore, openUpdateLogStore, openReportOutboxStore, openCrashSettingStore, openCrashPendingStore, openCrashExitStore, openInstallIdStore, openMeteo, openPhotos, openSpots, needsRefresh, refreshSpot,
         type DayFlowAnswer, type Spot } from "@app/data/spots";
import { Button, connectivity, ErrorState, LoadingState, SayingLine, setSynopsisNet, setSynopsisSource, Shell, StageBar, THEMES, UpdateToast, type ThemeName,
         platformLine, type ShellProps, type UpdatesLogControls } from "@app/ui-native";
import { LayoutWidthContext, liveFetch, NetRoot, useNet, useNetActivity, releaseMapInput, releaseStatus, setAnglerProfile, teeConsoleErrors, createCrashReporter, createCrashSetting, CrashSettingRoot, NotifyRoot, AlertLinesRefresh, installCrashHandlers, installId, reportPastExits, crashWhere, useRefreshOnReconnect, useReleaseBoot, useSpots, type MapTiles,
         DataProvenanceContext, loadRegsStore, reportFailure, startNeedsRelease, startNoticeLine,
         type AddressState, type PairingInput, type Route, type SpotPhotos, type StartLink, type StatusSource,
         useAppUpdate } from "@app/ui";
import { onAppLink } from "./appLink";
import { PHOTO_SOURCES, pickPhoto } from "./pickPhoto";
import { routeAddress, routeLinks, routeUrl, systemBack } from "./systemBack";
import { locate } from "./locate";
import { share } from "./share";
// (T390) the platform's way to a newer app: Play's in-app update, the App Store page, the staging APK page; none on the web
import { makeAppUpdater } from "./appUpdater";
import { resolveStart, startLink } from "./startLink";
import { exitSource } from "./exitInfo";
import { trimOnPressure } from "./memoryTrim";
import { backgroundDownloads } from "./bgDownload";
import { releaseSplash } from "./splash";
// (T234) the phone's local notifications (gauge alerts, new data ready) and their background check; none on the web
import { notifications, onTap } from "./notify";

import { APP_ENV, APP_VERSION, BUILT_CHANNEL, ENDPOINTS, IMAGERY_TILES, MENU_INFO, CRASH_URL, TEST_REPORTS, pickChannel, storedChannel, REPORT_URL, UPDATE_CHANNEL, UPDATES_LOG } from "./config";

/*
 * THE RECENT ERROR LOG (menu: Report a problem): every console.error the app already logs is also kept in a small
 * in-memory ring, which a reader sees — and can switch off — in a report before sending it. The ring itself is never
 * stored or sent; uncaught errors go separately, scrubbed, under the Data switch (T242, below).
 */
teeConsoleErrors(console);
import { ADIPOSE_IMAGES, FISH_IMAGES } from "./fishImages.generated";

/**
 * (T41) THE WEB START LINK, read once (a test affordance; null on native and on a plain visit): the water, part, day,
 * tab, sheet, angler and palette the browser tools put on screen. The angler is the session's profile (D8), set here.
 */
const START = startLink();
/**
 * (T50) WHERE THE READER WAS, outside React. Android recreates the activity on a configuration change the manifest does
 * not take in place (the system font size, the display size): the JS runtime lives on but the React tree is mounted
 * again, and it opened on the Map, the water the reader had open gone. The Shell tells every move here; a remount opens
 * where it was. (Declaring `fontScale` handled instead left text laid out at the old size: Fabric's text measurements
 * do not hear the change.) A cold start begins empty, from the start link.
 */
const place: { route: Route | null; address: AddressState | null; cameras: NonNullable<ShellProps["cameraHold"]> } =
  { route: null, address: null, cameras: {} };
// (T221) and on the web the address bar follows it, so a refresh opens where the reader was
const holdPlace = (r: Route) => { place.route = r; routeUrl(r); };
// (T369 D10/D12) the day and the Conditions horizon: the address keeps them, and so does a recreated activity
const holdAddress = (a: AddressState) => { place.address = a; routeAddress(a); };
if (START?.who) setAnglerProfile(START.who);

/**
 * Where everything is served from: ONE base, in `config.ts` (P10). In production these are R2 URLs and nothing else
 * changes.
 */
const FEED = ENDPOINTS.feed;

/**
 * (T251) EVERY DATA REQUEST SAYS WHICH APP ASKED, so the data worker's logs can tell versions apart: on a phone the
 * `X-App-Version` header and a User-Agent of our own on every request (the boot's and the update service's); on the web
 * the header on the update service's checks only (a custom header is a CORS preflight; the staging and production data
 * workers allow it, the local dev server's `serve-tiles` too once restarted), and the browser's own User-Agent.
 */
const WEB = Platform.OS === "web";
const VERSION_HEADERS = WEB
  ? (APP_ENV.env === "local" ? {} : versionHeaders(APP_VERSION, { platform: "web", ua: false }))
  : versionHeaders(APP_VERSION, { platform: platformLine(), ua: true });
// (T265) one request per URL in the air: the update service's check and the feed loader's read of the same index share it
const SHARED_FETCH = shareFetch((u, i) => fetch(u, i));
const DATA_FETCH: typeof fetch = Object.keys(VERSION_HEADERS).length
  ? versionedFetch(SHARED_FETCH, VERSION_HEADERS) : SHARED_FETCH;
/**
 * (T242) APP ERRORS to our own server (the feeds worker's `POST /crash`; no third-party service): the Data switch "Send
 * crash reports" (default on), the uncaught-error catchers, and a fatal error kept from the last run, sent now.
 * The screen is reported as a KIND, with (T352) the open water's id and its pane, all read from the route the Shell
 * last told (`crashWhere(place.route)`), and the install id; nothing is sent offline or when the switch is off.
 */
const crashSetting = createCrashSetting(openCrashSettingStore());
const crashContext = { release: "none" };
/** (T352) the random install id, made once on this device: crash reports and problem reports carry the same one */
const INSTALL = installId(openInstallIdStore());
if (!INSTALL.kept) console.warn("[crash-reports] the install id could not be stored; this run has its own");
if (CRASH_URL) {
  const reporter = createCrashReporter({
    url: CRASH_URL, test: TEST_REPORTS, setting: crashSetting, online: () => connectivity.online(), store: openCrashPendingStore(),
    context: () => ({ app: MENU_INFO.appBuild, env: APP_ENV.env, platform: platformLine(),
                     release: crashContext.release, install: INSTALL.id, ...crashWhere(place.route) }),
  });
  installCrashHandlers(reporter);
  // a kept JS fatal error first, then why earlier runs ended (Android: native crashes, ANRs, low-memory kills)
  void reporter.flush().then((jsFatalSent) => reportPastExits({
    reporter, source: exitSource, seen: openCrashExitStore(), setting: crashSetting, online: () => connectivity.online(), jsFatalSent }));
}
/** (T251) the Updates log's "simulate offline": the update service is told the network is down while on */
const devOffline = { on: false };
/**
 * (T254) THE APP'S LINE: one `Net` — the low-bandwidth mode (set by `useNetState` below from the Data setting, the
 * platform's word and the app's own meter), the order a poor line takes the release files in, the resilient download
 * and what is downloading now. The boot's web source, the rule book and every screen share it.
 */
const NET = createNet({ online: { online: () => !devOffline.on && connectivity.online(),
                                  subscribe: (on) => connectivity.subscribe(on) } });
/** (T255) the spots' reconnect refresh: the device's word, the Updates log's simulated offline included */
const SPOT_NET = { online: () => !devOffline.on && connectivity.online(), subscribe: (on: (o: boolean) => void) => connectivity.subscribe(on) };
setSynopsisNet({ low: () => NET.mode().low, bps: () => NET.meter.bps(), meter: NET.meter, online: NET.online,
                 waitOnline: NET.waitOnline });

/** (T254) How long the app, its data open, waits for its type faces before it draws in the system face. */
const FONT_WAIT_MS = 4000;

/** (T254) Under the start spinner on a slow or missing line: said, and how far the map's first file has come. */
function BootNetLine({ colour }: { colour: string }) {
  const { mode } = useNet();
  const now = useNetActivity(NET);
  const said = netBanner(mode);
  if (!said && !now.downloads.length) return null;
  const loaded = now.downloads.reduce((a, d) => a + d.loaded, 0);
  const total = now.downloads.every((d) => d.total !== null) ? now.downloads.reduce((a, d) => a + (d.total ?? 0), 0) : null;
  return (
    <Text testID="boot-slow" style={{ color: colour, fontSize: 12, textAlign: "center", paddingHorizontal: 24 }}>
      {[said, progressWords(loaded, total)].filter(Boolean).join(" · ")}
    </Text>
  );
}

/**
 * (T244) THE LOADING SCREEN: the loading fish at the centre of the WINDOW, its pun beneath, then the step and the
 * progress. The fish is drawn at once (`immediate`: no 300 ms hold) at the size and centre of the native splash picture
 * (`pnpm splash`, 110 dp), so the native splash can hand off to it with nothing to jump: the centre is the window's, not
 * the safe area's (the native splash is centred in the window), so the insets' difference is taken back out. Everything
 * else hangs below the fish, so what arrives late (the step, a download bar) never moves it. The native splash is let go
 * once the fish is laid out.
 */
const BOOT_FISH = 110;
function BootScreen({ palette, line, words, saying = false, children }: {
  palette: (typeof THEMES)[ThemeName]; line: string; words: string; saying?: boolean; children?: React.ReactNode;
}) {
  const insets = useSafeAreaInsets();
  const onLayout = useCallback(() => {
    // two frames: the fish's strip is drawn on the first one after layout
    requestAnimationFrame(() => requestAnimationFrame(releaseSplash));
  }, []);
  return (
    <View testID="boot-screen" style={{ flex: 1, transform: [{ translateY: (insets.bottom - insets.top) / 2 }] }}>
      <View style={{ position: "absolute", top: "50%", marginTop: -BOOT_FISH / 2, left: 0, right: 0, alignItems: "center" }}
            onLayout={onLayout}>
        <LoadingState palette={palette} size={BOOT_FISH} label={words} immediate />
      </View>
      <View style={{ position: "absolute", top: "50%", marginTop: BOOT_FISH / 2 + 8, left: 0, right: 0, alignItems: "center", gap: 10 }}>
        {saying
          ? <SayingLine style={{ color: palette.ink, fontSize: 15, textAlign: "center", paddingHorizontal: 24 }} />
          : <Text testID="boot-line" style={{ color: palette.ink, fontSize: 15, textAlign: "center", paddingHorizontal: 24 }}>{line}</Text>}
        <Text style={{ color: palette.sub, fontSize: 13 }}>{words}</Text>
        {children}
      </View>
    </View>
  );
}

export default function App() {
  const scheme = useColorScheme();
  const [theme, setTheme] = useState<ThemeName | null>(START?.theme ?? null);

  // Spots live in their own store — a `.spots` file on a phone, browser storage on the
  // web. Never in the release: that is replaced wholesale on every update.
  const spotStore = useMemo(() => openSpots(), []);
  // The Layers sheet's choice (the Spots switch and the rest), kept beside the spots so a reload opens the maps as left
  const layerStore = useMemo(() => openLayerStore(), []);
  // T51: the last fetched "Like your visit?" forecast per spot, so a spot opened offline still compares (a cache)
  const likeStore = useMemo(() => openLikeStore(), []);
  // Open-Meteo: no key, and it has an archive endpoint — which is what lets a spot pinned
  // with no signal be filled in later from the day it was actually made.
  // (T298) live data counts as loading (the phone's slow-line banner is said only while it does)
  const weather = useMemo(() => openMeteo(liveFetch(fetch)), []);
  // T170/T274: HYDAT's daily means from our data host, for a spot's visit day older than the feed's record (one small file per station)
  const hydat = useMemo(() => storedHydat(ENDPOINTS.hydat), []);
  // S1: a spot's photographs — files beside the spots file on a phone, IndexedDB in a browser — and the platform's
  // picker (`./pickPhoto.native.ts` / `.web.ts`). The user's own: never in a release, never uploaded.
  const photos = useMemo<SpotPhotos>(
    () => ({ store: openPhotos(), pick: pickPhoto, sources: PHOTO_SOURCES }), []);
  const [refreshing, setRefreshing] = useState(false);
  // P01: every write awaited and serialised; a failure reaches the screen that asked
  const spotsHook = useSpots(spotStore);
  const spots: Spot[] = spotsHook.spots.state === "ready" ? spotsHook.spots.value : [];
  // (T231) the gauge index is the update service's to renew: once read it is held until the service offers a newer one
  // (T315) and every station's last week kept on the phone (each station file read, and the publisher's feed-wide
  // newest rows on each feed update): offline, a Conditions chart still draws its measured line, "as of" its age
  const feed = useMemo(() => httpFeed(FEED, liveFetch(SHARED_FETCH), {
    indexHeld: true, history: gaugeHistory(openGaugeHistoryStore()),
    online: () => !devOffline.on && connectivity.online() }), []);
  useEffect(() => { void feed.syncObservations?.(); }, [feed]);
  // T211: the live DFO salmon rules, the last capture kept on the device for offline
  const salmonFeed = useMemo(() => httpSalmonFeed(ENDPOINTS.dfoFeed, openSalmonFeedStore()), []);
  // T230: the provincial in-season changes and where each applies, the last capture kept for offline; the ids shown
  const inSeason = useMemo(() => httpInSeason(ENDPOINTS.inSeason, openInSeasonStore()), []);
  const inSeasonSeen = useMemo(() => openInSeasonSeenStore(), []);
  // T236: the first-run notice (not legal advice), once per device
  const firstRun = useMemo(() => openFirstRunStore(), []);
  const walkthrough = useMemo(() => openWalkthroughStore(), []);
  // (T384) the first open asks for the reader's location once; this keeps that it has
  const locationAsk = useMemo(() => openLocationAskStore(), []);
  // (T390) a newer app, asked at most once a device day (the bell's "App update" section)
  const appUpdater = useMemo(() => makeAppUpdater(APP_VERSION), []);
  const appUpdateStore = useMemo(() => (appUpdater ? openAppUpdateStore() : null), [appUpdater]);
  const recentSearches = useMemo(() => openRecentSearchStore(), []);
  // (T261) the species the Runs tab paints, kept between visits
  const runSpeciesStore = useMemo(() => openRunSpeciesStore(), []);
  // (T254) low-bandwidth mode: the More → Data setting (kept on the device), the platform's word, the meter → one mode
  const dataModeStore = useMemo(() => openDataModeStore(), []);
  // T199: the run timing (its own feed, `runtiming/` on the data origin) — read the first time Run timing is chosen
  // (T552 item 6) the section handles of the tiles this device draws: a capture bound to them is never overwritten by
  // a run-timing set published for another atlas (set from the booted release below)
  const drawnHandles = useRef<string | null>(null);
  const runTiming = useMemo(() => httpRunTiming(`${ENDPOINTS.base}/runtiming`, openRunTimingStore(), undefined,
                                                { handles: () => drawnHandles.current }), []);

  // THE RELEASE: manifest → (native: install / update) → paint, then the Shell. (T231) The phone's background install
  // is the update service's (`deferUpdate`: it holds a large set for Wi-Fi); `generation` re-boots onto a staged update.
  const [generation, setGeneration] = useState(0);
  // (T262) a failed first start is tried again: twice by itself (a DNS miss, a dropped line), then on the reader's Retry
  const [attempt, setAttempt] = useState(0);
  const [autoTries, setAutoTries] = useState(0);
  /*
   * (T298) THE FIRST INSTALL ON MOBILE DATA: the reader's setting (the update service keeps it: Always / Ask / Wi-Fi
   * only) decides; on Ask the boot screen asks once — "Download now" / "Wait for Wi-Fi" — and the answer is SAVED as
   * the setting (owner 2026-10-29), said in one line with where to change it. Held for Wi-Fi, the screen offers
   * "Download now" (this time only); the system's downloader starts by itself on Wi-Fi, the app open or not.
   */
  const updatesRef = useRef<UpdateService | null>(null);
  const [mobileAsk, setMobileAsk] = useState<{ bytes: number; answer: (a: "now" | "wifi") => void } | null>(null);
  const [mobileSaved, setMobileSaved] = useState(false);
  const [wifiWait, setWifiWait] = useState<(() => void) | null>(null);
  // (T298) Android 13+: the system's download notification needs the app's leave to notify — asked once, with why
  const [askingNotify, setAskingNotify] = useState(false);
  const beforeBackground = useCallback(async () => {
    const bg = backgroundDownloads;
    if (!bg?.notificationsAllowed || !bg.askNotifications || await bg.notificationsAllowed()) return;
    setAskingNotify(true);
    try { await bg.askNotifications(); } finally { setAskingNotify(false); }
  }, []);
  const mobileGate = useMemo((): MobileDataGate | null => WEB ? null : {
    decide: async (bytes) => {
      const u = updatesRef.current;
      const metered = (await connectivity.meteredNow?.()) ?? false;
      const d = mobileDataDecision(u?.snapshot().mobileData ?? "ask", metered, bytes);
      if (d !== "ask") return d === "go" ? "go" : "wifi";
      const a = await new Promise<"now" | "wifi">((answer) => setMobileAsk({ bytes, answer }));
      setMobileAsk(null);
      u?.setMobileData(a === "now" ? "always" : "wifi");
      setMobileSaved(true);
      return a === "now" ? "go" : "wifi";
    },
    untilAllowed: async () => {
      if (!((await connectivity.meteredNow?.()) ?? false)) return "wifi";
      return new Promise<"wifi" | "now">((resolve) => {
        let off: (() => void) | undefined;
        const end = (r: "wifi" | "now") => { off?.(); setWifiWait(null); resolve(r); };
        off = connectivity.watchMetered?.((m) => { if (!m) end("wifi"); });
        setWifiWait(() => () => end("now"));
      });
    },
  }, []);
  const boot = useReleaseBoot(useMemo(() => ({
    manifestUrl: ENDPOINTS.manifest, releaseBase: ENDPOINTS.release, wasmBase: ENDPOINTS.sqlWasm, platform, feed,
    salmonFeed, deferUpdate: true, net: NET, background: backgroundDownloads, mobileData: mobileGate, beforeBackground,
    // (T251) a phone's requests name the app; its first install is gated by the release's minAppVersion (the web's app
    // ships with its data: never gated)
    ...(WEB ? {} : { fetch: DATA_FETCH, appVersion: APP_VERSION }),
  }), [feed, salmonFeed, mobileGate, beforeBackground]), generation, attempt);
  const booted = boot.state === "ready" ? boot.booted : null;
  const bootedRef = useRef(booted);
  bootedRef.current = booted;
  if (booted) crashContext.release = booted.manifest.data_release;
  // (T234) the background check's "new data ready" compares against the release in use: noted on every open
  const heldRelease = booted?.manifest.data_release ?? null;
  useEffect(() => {
    if (!heldRelease || !notifications) return;
    try { notifications.settings.update(noteHeld(heldRelease)); } catch (e) { reportFailure(e); }
  }, [heldRelease]);
  // (T234) a notification's tap: a gauge alert opens its water's Conditions (the one that started the app too)
  const [openRequest, setOpenRequest] = useState<{ item: string | null; n: number; view?: "regs" | "conditions" | "map" } | null>(null);
  // (T234 v1.1) a new-app-version tap: the bell opens on its App update, the version asked now
  const [openUpdate, setOpenUpdate] = useState(0);
  useEffect(() => onTap((d) => {
    if (d.kind === "gauge") setOpenRequest((r) => ({ item: d.item, n: (r?.n ?? 0) + 1 }));
    if (d.kind === "app") setOpenUpdate((n) => n + 1);
  }), []);
  // (T625) a tapped share link (Android App Links) opens its water through the same request, on the view the link names
  useEffect(() => onAppLink(({ item, view }) => setOpenRequest((r) => ({ item, view, n: (r?.n ?? 0) + 1 }))), []);

  /*
   * (T231) THE UPDATE SERVICE (owner 2026-10-28: "check the data origin for new updates and download them"): the three
   * feeds and the release, checked on start, on the way back to the foreground, on an interval and on the sync icon;
   * an unchanged object costs a 304. A changed feed is handed to its loader whole; a newer release is staged beside the
   * one in use (verified) and applied at a safe moment (the Shell says when), with "Updated to …".
   */
  // (T251) the Updates log: development and staging builds keep the last 200 events on the device
  const updateLog = useMemo(() => (UPDATES_LOG ? createUpdateLog({ store: openUpdateLogStore() }) : null), []);
  // (T237) problem reports: POSTed to the report endpoint, or kept on the device and sent with the service's retry
  const reportOutbox = useMemo(() => (REPORT_URL ? createReportOutbox({
    url: REPORT_URL, test: TEST_REPORTS, store: openReportOutboxStore(), online: () => !devOffline.on && connectivity.online() }) : null), []);
  const updates = useMemo(() => createUpdateService({
    fetch: DATA_FETCH,
    outbox: reportOutbox,
    onEvent: updateLog ? updateLog.append : undefined,
    // (T251) a store build can lag behind the data: a release asking for a newer app is not applied. The web's code ships
    // with its data, so it is never gated — save `EXPO_PUBLIC_RELEASE_GATE=1`, the browser test's way to see the gate
    appVersion: WEB && process.env.EXPO_PUBLIC_RELEASE_GATE !== "1" ? null : APP_VERSION,
    feeds: [
      { id: "gauges", url: ENDPOINTS.checks.gauges,
        apply: (text) => { feed.offer!(JSON.parse(text)); void feed.syncObservations?.(); } },
      { id: "dfo", url: ENDPOINTS.checks.dfo, apply: (text) => salmonFeed.offer!(text) },
      // (T548) a feed this app cannot read throws: shown and reported, never recorded as updated
      { id: "inseason", url: ENDPOINTS.checks.inseason, apply: (text) => inSeason.offer!(text) },
    ],
    release: {
      manifestUrl: ENDPOINTS.manifest,
      held: () => bootedRef.current?.held ?? null,
      stage: async (m, on) => {
        const b = bootedRef.current;
        if (!b) throw new Error(UPDATE_WORDS.sheet.notOpen);
        return b.stage(m, (p) => on(p.bytes, p.of));
      },
    },
    store: openUpdateStore(),
    // (T254) a slow line holds a LARGE update the way mobile data does (the setting for mobile data still lets it go):
    // a release set on one bar would starve every answer the reader asks for; (T265) the slow mode clearing looks again
    network: phoneNetwork(connectivity, NET, { offline: () => devOffline.on }),
    lifecycle: {
      subscribe: (on) => {
        let was = AppState.currentState;
        const sub = AppState.addEventListener("change", (st) => { if (st === "active" && was !== "active") on(); was = st; });
        return () => sub.remove();
      },
    },
  }), [feed, salmonFeed, inSeason, updateLog, reportOutbox]);
  updatesRef.current = updates;
  // started once the release is open (the service compares against the set in use); the boot's manifest read seeds it
  const started = useRef(false);
  useEffect(() => {
    if (!booted || started.current) return;
    started.current = true;
    if (booted.latest) updates.seedRelease(booted.latest);
    updates.start();
  }, [booted, updates]);
  useEffect(() => () => updates.stop(), [updates]);
  /** Apply a staged update: boot onto it, the set in use on screen until the new one is ready. */
  const applying = useRef<{ before: Held | null; at: number } | null>(null);
  const apply = useCallback(() => {
    if (updates.snapshot().release?.phase !== "ready") return;
    applying.current = { before: bootedRef.current?.held ?? null, at: clockNow() };
    if ((globalThis as { __openProbe?: boolean }).__openProbe) console.log("[swap] apply");
    setGeneration((g) => g + 1);
  }, [updates]);
  // back from the background with an update ready: the safest moment there is
  useEffect(() => {
    let was = AppState.currentState;
    const sub = AppState.addEventListener("change", (st) => { if (st === "active" && was !== "active") apply(); was = st; });
    return () => sub.remove();
  }, [apply]);
  // THE BOOT THAT APPLIED A SET: told to the service, and said ("Updated to …") — one rule (`bootSaid`) for every boot.
  // (T585 F12) A COLD START that opens a set staged earlier applies it too (a phone killed before the safe moment came
  // moved to the newer release without a word, and the Updates row kept the old "updated" time).
  const [toast, setToast] = useState<string | null>(null);
  const saidGen = useRef(-1);
  useEffect(() => {
    if (boot.state !== "ready" || saidGen.current === boot.generation) return;
    saidGen.current = boot.generation;
    const a = applying.current;
    applying.current = null;
    const said = bootSaid({ generation: boot.generation, applyError: boot.applyError, booted: boot.booted, before: a?.before ?? null });
    // (T555) the boot went back to the set it had (the release was refused or is being retried): nothing was applied —
    // the banner (`rolledBackLine`) says so; the service names the refused release and never toasts "Updated to …"
    if (said.kind === "not-applied") { updates.notApplied(said.refused, said.reason); return; }
    if (said.kind !== "applied") return;
    // (T287) the test bed's stall log reads the swap off this line
    if (a && (globalThis as { __openProbe?: boolean }).__openProbe)
      console.log(`[swap] the new set committed, ${clockNow() - a.at} ms after apply`);
    updates.applied(said.release, { from: said.from, ...(a ? { ms: clockNow() - a.at } : {}) });
    setToast(updatedToast(manifestDate(boot.booted.manifest)));
  }, [boot, updates]);
  const clearToast = useCallback(() => setToast(null), []);
  // (T585 F12, F13) a development or staging build says which data release it answers from on the Conditions card (and
  // the data source / channel when they are not this build's own); null in a production build
  const provenance = useMemo(() => dataProvenanceLine({
    release: heldRelease, env: APP_ENV.env, built: APP_ENV.built, switchable: APP_ENV.switchable,
    channel: UPDATE_CHANNEL, builtChannel: BUILT_CHANNEL }), [heldRelease]);

  // (T251, T476) the Updates log's levers (development and staging builds): the Developer card (map credits long press) draws them
  const releaseNow = boot.state === "ready" ? boot.booted.manifest.data_release : null;
  const releaseRef = useRef(releaseNow);
  releaseRef.current = releaseNow;
  const updatesLog = useMemo((): UpdatesLogControls | null => updateLog && ({
    log: updateLog, updates,
    header: () => ({ version: versionLine(APP_VERSION), platform: platformLine(),
                     env: APP_ENV.env, origin: APP_ENV.origin, channel: UPDATE_CHANNEL, release: releaseRef.current }),
    channel: APP_ENV.switchable ? { current: UPDATE_CHANNEL, stored: storedChannel(), pick: pickChannel } : null,
    offline: { get: () => devOffline.on,
               set: (on) => { devOffline.on = on; updateLog.append({ at: clockNow(), source: "app", kind: "action",
                                                                       note: UPDATE_LOG_WORDS.notes.offline(on) }); } },
    forceRedownload: async () => { updates.forget(); await updates.check({ manual: true }); },
    clearCache: async () => {
      updates.forget();
      // the web's release files (Cache Storage); a phone's installed set is the data in use and is never dropped here
      const caches = (globalThis as { caches?: CacheStorage }).caches;
      const dropped = caches ? await caches.delete("release-v1").catch(() => false) : false;
      updateLog.append({ at: clockNow(), source: "app", kind: "action",
                         note: UPDATE_LOG_WORDS.notes.cache(WEB, dropped) });
      await updates.check({ manual: true });
    },
    apply,
    copy: async (text) => {
      try {
        const clip = (globalThis as { navigator?: { clipboard?: { writeText(t: string): Promise<void> } } }).navigator?.clipboard;
        if (WEB && clip) { await clip.writeText(text); return true; }
        await Share.share({ message: text });
        return true;
      } catch { return false; }
    },
    // (T242) the crash test bed's levers, in a tool's build only (never a store build): each goes the way a real one would,
    // a throw outside React (the global handler: fatal, kept, the app ends) and a rejection nobody handles (Hermes' tracker)
    crashTest: TEST_REPORTS ? {
      js: () => { setTimeout(() => { throw new Error("T242 test bed: JS error"); }, 0); },
      promise: () => { void Promise.reject(new Error("T242 test bed: unhandled rejection")); },
    } : null,
  }), [updateLog, updates, apply]);
  const menuInfo = useMemo(() => ({ ...MENU_INFO, updatesLog, report: reportOutbox, install: INSTALL.id }), [updatesLog, reportOutbox]);

  const data = useMemo((): { source: WaterSource; status: StatusSource; pairing: PairingInput;
                             tiles: MapTiles & { imagery: string }; loader: ReleaseLoader } | null => {
    if (boot.state !== "ready") return null;
    const b = boot.booted;
    // the tiles, mask, basemap and map assets from the release when it names them, else the dev server's; the pair
    // is proven against the tiles DRAWN (`releaseMapInput`, tested in ui/release.test.ts)
    // T01: the loader goes to the water screen too — the Regulations page reads water / regs-<region> / regs-sid
    // through it (no regs store file of its own)
    // the satellite imagery is Esri's, online (never in a release): `IMAGERY_TILES`
    const m = releaseMapInput(b, ENDPOINTS.tiles);
    return { source: b.source, status: releaseStatus(b.loader), ...m, tiles: { ...m.tiles, imagery: IMAGERY_TILES },
             loader: b.loader };
  }, [boot]);
  useEffect(() => { drawnHandles.current = data?.pairing.manifest.tiles.current?.id?.split(".")[0] ?? null; }, [data]);

  // (T369) THE START LINK, resolved in the release now open (web-only: `./startLink` `resolveStart`: a place → the section of the
  // stream that spans it → its water and part HERE; a water of one part named alone → that part) before the Shell
  // opens on it, so the first frame drawn is the resolved one; and the address bar makes its places from the same
  // release (`routeLinks`)
  const [start, setStart] = useState<StartLink | null | undefined>(() => (startNeedsRelease(START) ? undefined : START));
  useEffect(() => {
    if (!data) return;
    let live = true;
    // the regs store opens when the address first needs a place (a water on screen), never on a first paint of the map
    routeLinks(() => loadRegsStore(data.loader), data.loader);
    if (START && startNeedsRelease(START)) {
      const link = START;
      loadRegsStore(data.loader).then((st) => resolveStart(link, st, data.loader, data.source))
        .then((l) => { if (live) setStart(l); },
              // (T369 D9) a place that could not be READ (offline, a failed fetch) is said as that, never "not on this map"
              (e: unknown) => { reportFailure(e); if (live) setStart({ ...link, place: null, tap: null, notice: link.place ? "place-unread" : link.notice }); });
    }
    return () => { live = false; };
  }, [data]);
  // (T360) Android asking for memory back: the maps drop what they do not draw (native), the databases their page caches
  useEffect(() => (data ? trimOnPressure(data.loader, reportFailure) : undefined), [data]);
  const [noticeShut, setNoticeShut] = useState(false);
  const startNotice = noticeShut ? null : startNoticeLine(start ?? null);

  const dataSource = data?.source ?? null;
  const refreshSpots = useCallback(async () => {
    if (!dataSource) return;
    setRefreshing(true);
    // through `patch`: a spot deleted or edited meanwhile is never overwritten (P01)
    // the source reads the station's daily means for a spot's week before (S1); the feed's index tells a
    // station it does not carry (settled) from a feed not reached (asked again)
    // (T170) and HYDAT for the visit day's own flow where the reading is not the day's
    try { await spotsHook.refresh((s) => refreshSpot(s, weather, dataSource, feed, { hydat })); }
    catch { /* said by spotsHook.error; the list keeps what was saved */ }
    finally { setRefreshing(false); }
  }, [dataSource, spotsHook, weather, feed, hydat]);
  // (T255) back online (or opened online) with a spot made offline: its weather and gauge context fill in by themselves
  useRefreshOnReconnect(dataSource ? SPOT_NET : null, spots.some(needsRefresh), refreshSpots);
  // (T555 item 5) ONCE PER RELEASE OPENED (a cold start and an applied update, the one path): a spot's or an alert's
  // water id that this release absorbed into another water is rewritten to the id in use (`core.item_alias`)
  const spotsRef = useRef(spotsHook);
  spotsRef.current = spotsHook;
  useEffect(() => {
    if (!dataSource) return;
    followSavedItems(dataSource, {
      spots: { list: () => spotStore.list(), rewrite: (fill) => spotsRef.current.refresh(fill) },
      alerts: notifications ? notifications.settings : null,
    }).catch(reportFailure);
  }, [dataSource, spotStore]);

  // (T262) what a failed start says: the reader's words (an app too old to read this release says so); the exception
  // and the dev server hint only in a local build
  const tooOld = boot.state === "failed" && boot.error.message === UPDATE_WORDS.row.appTooOld;
  useEffect(() => {
    if (boot.state !== "failed" || tooOld || autoTries >= 2) return;
    const t = setTimeout(() => { setAutoTries((n) => n + 1); setAttempt((a) => a + 1); }, 2000 * (autoTries + 1));
    return () => clearTimeout(t);
  }, [boot, tooOld, autoTries]);
  const failed = boot.state === "failed" && (tooOld || autoTries >= 2) ? boot.error.message : null;
  // (T390) a first start refused for an old app: the strong prompt and the same Update button as the bell's
  const bootUpdate = useAppUpdate(tooOld ? appUpdater : null, appUpdateStore, { offline: false, tooOld });
  const bootUpdateVM = tooOld ? bootUpdate?.vm ?? null : null;
  const firstRunOffline = boot.state === "failed" && boot.firstRunOffline;
  const retryBoot = useCallback(() => setAttempt((a) => a + 1), []);
  const stage = boot.state === "loading" ? boot.stage : boot.state === "failed" ? "manifest" : "paint";
  const progress = boot.state === "loading" ? boot.progress : null;
  // (T272) a first install's second stage: everything is down, and it is being set up on the device
  const setup = progress?.setup ?? null;
  // T215: our copy of the synopsis PDF (the release names it; fetched on the first highlighted open, from the release
  // base on web and phone alike — it is not part of the installed set). None: "open the PDF" stays the plain link.
  const synopsis = boot.state === "ready" ? boot.booted.manifest.synopsis : null;
  useEffect(() => {
    setSynopsisSource(synopsis?.sha256 && synopsis.bytes
      ? { url: `${ENDPOINTS.release}${synopsis.url}`, sha256: synopsis.sha256, bytes: synopsis.bytes, append: synopsis.append ?? null,
          // (T285) this release's files may be gone (404): the live manifest names the same synopsis under the live release
          alternate: async () => {
            const live = (await (await fetch(ENDPOINTS.manifest)).json() as { synopsis?: { sha256?: string; url?: string } }).synopsis;
            return live?.sha256 === synopsis.sha256 && live?.url ? `${ENDPOINTS.release}${live.url}` : null;
          } } : null);
  }, [synopsis]);

  // a phone whose newest set would not open went back to the one it replaced: said, above the map
  const rolledBack = boot.state === "ready" ? boot.booted.rolledBack : null;

  // (T244) one pun per opening, under the loading fish
  const splashLine = useMemo(() => splashLineFor(Math.random() * 1e6), []);
  const name: ThemeName = theme ?? (scheme === "dark" ? "dark" : "light");
  const palette = THEMES[name];

  // The faces are named in @app/ui-native/type.ts and loaded here, so that package stays
  // free of platform imports. Until they land, hold the splash rather than paint the whole
  // app in the system face and reflow it a beat later.
  const [fontsIn, fontError] = useFonts({
    BricolageGrotesque_700Bold, BricolageGrotesque_800ExtraBold,
    Archivo_400Regular, Archivo_500Medium, Archivo_600SemiBold, Archivo_700Bold, Archivo_800ExtraBold,
    JetBrainsMono_500Medium,
  });
  // (T254) BUT NEVER FOREVER: on a slow line the web's font check gives up after 12 s (expo-font's FontObserver) and
  // `useFonts` reports an error, never ready — the app sat on "Loading type…" for good (measured: Slow 3G, a local
  // build). A failed check, or FONT_WAIT_MS with the data already open, draws the app in the system face; the faces swap
  // in when their files land (the @font-face rules are injected either way).
  const [fontWaitOver, setFontWaitOver] = useState(false);
  useEffect(() => {
    if (fontsIn || !data) return;
    const t = setTimeout(() => setFontWaitOver(true), FONT_WAIT_MS);
    return () => clearTimeout(t);
  }, [fontsIn, data]);
  const ready = fontsIn || !!fontError || fontWaitOver;
  // (T244) the native splash is let go when the boot screen's fish is up (BootScreen); a start that skips that screen lets it go here
  useEffect(() => { if (failed || (ready && data)) releaseSplash(); }, [failed, ready, data]);
  const bootWords = setup ? SETUP_WORDS.title : startStage(!ready && data ? "fonts" : stage);

  // (T45) THE WINDOW'S WIDTH, read here and nowhere else in the app: every screen takes its layout class from it
  // (@app/ui `useLayoutClass`), so a component arranges itself for a phone, a tablet or a desktop without reading the window
  const { width } = useWindowDimensions();

  return (
    <LayoutWidthContext.Provider value={width}>
    {/* THE SYSTEM BARS' INSETS, read here and nowhere else (T50): Android draws edge-to-edge (API 35), so without them the
        status bar sat over the top bar, the back links and Close (and swallowed their taps), and the gesture handle over
        the tab bar. RN's own SafeAreaView padded iOS only; react-native-safe-area-context pads every edge on both. */}
    <SafeAreaProvider>
    <NetRoot net={NET} connectivity={connectivity} store={dataModeStore}>
    <CrashSettingRoot setting={CRASH_URL ? crashSetting : null}>
    <NotifyRoot controls={notifications}>
    <DataProvenanceContext.Provider value={provenance}>
    <SafeAreaView style={{ flex: 1, backgroundColor: palette.card }}>
      <StatusBar barStyle={name === "dark" ? "light-content" : "dark-content"} />
      {failed ? (
        <View testID="boot-failed" style={{ flex: 1, alignItems: "center", justifyContent: "center", padding: 32,
                       gap: 8 }}>
          <Text style={{ color: palette.ink, fontSize: 16, textAlign: "center" }}>
            {INSTALL_WORDS.failed}
          </Text>
          {bootUpdateVM ? (
            <ErrorState palette={palette} size="body" align="center" testID="boot-app-update"
                        message={[bootUpdateVM.text, bootUpdateVM.said].filter(Boolean).join(" ")}
                        onRetry={bootUpdateVM.button ? bootUpdate!.update : undefined}
                        retryLabel={bootUpdateVM.button?.label} retryTestID="boot-app-update-action" />
          ) : (
            <ErrorState palette={palette} size="body" align="center"
                        message={tooOld ? failed : firstRunOffline ? INSTALL_WORDS.firstRunOffline : INSTALL_WORDS.again}
                        onRetry={tooOld ? undefined : retryBoot} retryLabel={INSTALL_WORDS.retry} retryTestID="boot-retry" />
          )}
          {APP_ENV.env === "local" && (
            <Text style={{ color: palette.faint, fontSize: 12, textAlign: "center" }}>
              {failed} {INSTALL_WORDS.serve}
            </Text>
          )}
        </View>
      ) : ready && data && start !== undefined ? (
        <View style={{ flex: 1 }}>
        {/* (T369) a start that opened less than its link named says so (a place this release does not hold) */}
        {startNotice && (
          <Text testID="start-notice" accessibilityRole="button" onPress={() => setNoticeShut(true)}
                style={{ color: palette.sub, fontSize: 12, paddingVertical: 6, paddingHorizontal: 12 }}>
            {startNotice}
          </Text>
        )}
        {rolledBack && (
          <Text style={{ color: palette.sub, fontSize: 12, paddingVertical: 6, paddingHorizontal: 12 }}>
            {rolledBackLine(rolledBack)}
          </Text>
        )}
        <AlertLinesRefresh source={data.source} controls={notifications} />
        <Shell source={data.source} palette={palette} loader={data.loader} menuInfo={menuInfo}
               // (T231) the sync icon reads the update service; a ready update is applied at the Shell's safe moment
               updates={updates} onSafeToApply={apply}
               // T129: the boot's manifest read seeds the service's first release check (`seedRelease`): no second request
               // (T186, Q164) the web fetches ahead after first paint; a phone holds the release installed
               prefetch={Platform.OS === "web"}
               // (T253) its background tier's steps go to the Updates log (development and staging builds)
               prefetchLog={updateLog ? updateLog.append : null}
               layerStore={layerStore} likeStore={likeStore}
               theme={name} themeName={name}
               onTheme={setTheme} tiles={data.tiles}
               status={data.status} pairing={data.pairing}
               spots={spots}
               fishImages={FISH_IMAGES}
               adiposeImages={ADIPOSE_IMAGES}
               systemBack={systemBack}
               initialRoute={place.route ?? start?.route} onRoute={holdPlace} openRequest={openRequest} openUpdate={openUpdate}
               initialDay={place.address ? place.address.day : start?.day ?? null}
               initialHorizon={place.address ? place.address.horizon : start?.horizon ?? null} onAddress={holdAddress}
               cameraHold={place.cameras} regsStart={start?.regs ?? null} arriveAt={start?.arrive ?? null}
               locate={locate} locationAsk={locationAsk} share={share}
               appUpdater={appUpdater} appUpdateStore={appUpdateStore}
               weather={weather}
               photos={photos}
               onSaveSpot={spotsHook.save}
               spotsError={spotsHook.spots.state === "failed" ? spotsHook.error : null}
               feed={feed}
               inSeason={inSeason} inSeasonSeen={inSeasonSeen} salmonFeed={salmonFeed} firstRun={firstRun} walkthrough={walkthrough} recentSearches={recentSearches}
               runTiming={runTiming} runSpeciesStore={runSpeciesStore}
               refreshing={refreshing}
               onDeleteSpot={spotsHook.remove}
               onRefreshSpots={refreshSpots}
               hydat={hydat}
               onDayFlow={async (spot) => {
                 // T170 "Get the flow for that day": that one spot, through the same fill a refresh uses; the others
                 // come back as they are (no write), and an edit made meanwhile wins (`refresh`'s patch)
                 let answer: DayFlowAnswer | null = null;
                 await spotsHook.refresh(async (s) => {
                   if (s.id !== spot.id) return s;
                   // (T294) the visit the page shows (`spot` is the spot read as it), written back to that visit alone
                   return onVisit(s, visitIdOf(spot), async (v) => {
                     const got = await fillDayFlow(v, { source: data.source, hydat });
                     answer = got.answer;
                     return got.spot;
                   });
                 });
                 return answer;
               }}
               // the map's credits name the publishers the Sources page names (`MAP_CREDITS`, @app/core menu/sources.ts)
               attribution={MAP_CREDITS} />
        {/* (T231) a downloaded update was put in use at a safe moment: said */}
        <UpdateToast text={toast} palette={palette} onDone={clearToast} />
        </View>
      ) : (
        // THE LOADING SCREEN, and on a phone's first run the download screen: which step, and how much is down
        <BootScreen palette={palette} line={splashLine} words={bootWords}
                    // (T298) the fish sayings take turns while the first install downloads or waits
                    saying={(stage === "install" || stage === "wifi") && !setup}>
          {/* (T298) on mobile data, asked once before the first download; the answer is kept as the setting */}
          {mobileAsk && (
            <View testID="boot-mobile-ask" style={{ alignItems: "center", gap: 10, paddingHorizontal: 24 }}>
              <Text style={{ color: palette.ink, fontSize: 15, textAlign: "center" }}>{MOBILE_DATA_WORDS.prompt(mobileAsk.bytes)}</Text>
              <View style={{ flexDirection: "row", gap: 10 }}>
                <Button palette={palette} label={MOBILE_DATA_WORDS.now} testID="boot-mobile-now" grow={false}
                        onPress={() => mobileAsk.answer("now")} />
                <Button palette={palette} label={MOBILE_DATA_WORDS.wait} kind="ghost" testID="boot-mobile-wait" grow={false}
                        onPress={() => mobileAsk.answer("wifi")} />
              </View>
            </View>
          )}
          {askingNotify && (
            <Text testID="boot-notify-why" style={{ color: palette.sub, fontSize: 13, textAlign: "center", paddingHorizontal: 24 }}>
              {DOWNLOAD_NOTE_WORDS.why}</Text>
          )}
          {stage === "wifi" && (
            <View testID="boot-wifi-wait" style={{ alignItems: "center", gap: 10, paddingHorizontal: 24 }}>
              <Text style={{ color: palette.sub, fontSize: 13, textAlign: "center" }}>{MOBILE_DATA_WORDS.waiting}</Text>
              {wifiWait && <Button palette={palette} label={MOBILE_DATA_WORDS.now} kind="ghost" grow={false}
                                   testID="boot-wifi-now" onPress={wifiWait} />}
            </View>
          )}
          {mobileSaved && !setup && (
            <Text testID="boot-mobile-saved" style={{ color: palette.faint, fontSize: 12, textAlign: "center" }}>{MOBILE_DATA_WORDS.saved}</Text>
          )}
          {/* (T254) on a slow or missing line: said, and how far the map's first file has come */}
          {stage !== "install" && !setup && <BootNetLine colour={palette.faint} />}
          {/* (T272) the first bar: the download; it stays (full) under the second stage's bar */}
          {(stage === "install" || setup) && progress && progress.of > 0 && (
            <>
              <StageBar palette={palette} value={downloadFraction(progress)} testID="boot-download-bar" />
              <Text style={{ color: palette.sub, fontSize: 13 }}>{installLine(progress)}</Text>
              {/* (T298) with the system's downloader the download carries on with the app closed: said */}
              {!setup && <Text style={{ color: palette.faint, fontSize: 12 }}>
                {backgroundDownloads ? MOBILE_DATA_WORDS.closeOk : INSTALL_WORDS.wifi}</Text>}
            </>
          )}
          {setup && (
            <>
              <StageBar palette={palette} value={setupFraction(setup)} testID="boot-setup-bar" />
              <Text style={{ color: palette.sub, fontSize: 13 }}>{setupLine(setup)}</Text>
            </>
          )}
        </BootScreen>
      )}
    </SafeAreaView>
    </DataProvenanceContext.Provider>
    </NotifyRoot>
    </CrashSettingRoot>
    </NetRoot>
    </SafeAreaProvider>
    </LayoutWidthContext.Provider>
  );
}
