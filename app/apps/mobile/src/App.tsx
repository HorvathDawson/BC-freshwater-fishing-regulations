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
import { AppState, Platform, StatusBar, Text, View, useColorScheme, useWindowDimensions } from "react-native";
import { SafeAreaProvider, SafeAreaView } from "react-native-safe-area-context";
import { useFonts } from "expo-font";
import {
  BricolageGrotesque_700Bold, BricolageGrotesque_800ExtraBold,
} from "@expo-google-fonts/bricolage-grotesque";
import {
  Archivo_400Regular, Archivo_500Medium, Archivo_600SemiBold, Archivo_700Bold, Archivo_800ExtraBold,
} from "@expo-google-fonts/archivo";
import { JetBrainsMono_500Medium } from "@expo-google-fonts/jetbrains-mono";
import { INSTALL_WORDS, MAP_CREDITS, installLine, rolledBackLine, startStage, updatedToast, UPDATE_WORDS } from "@app/core";
import { createUpdateService, httpFeed, httpInSeason, httpSalmonFeed, manifestDate, type WaterSource } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import * as platform from "@app/data/release/platform";
import { fillDayFlow, geometHydat, openLayerStore, openLikeStore, openSalmonFeedStore, openInSeasonStore, openInSeasonSeenStore, openUpdateStore, openMeteo, openPhotos, openSpots, refreshSpot,
         type DayFlowAnswer, type Spot } from "@app/data/spots";
import { connectivity, FishSpinner, setSynopsisSource, Shell, THEMES, UpdateToast, type ThemeName } from "@app/ui-native";
import { LayoutWidthContext, releaseMapInput, releaseStatus, setAnglerProfile, teeConsoleErrors, useReleaseBoot, useSpots, type MapTiles,
         type PairingInput, type Route, type SpotPhotos, type StatusSource } from "@app/ui";
import { PHOTO_SOURCES, pickPhoto } from "./pickPhoto";
import { routeUrl, systemBack } from "./systemBack";
import { locate } from "./locate";
import { startLink } from "./startLink";

import { ENDPOINTS, IMAGERY_TILES, MENU_INFO } from "./config";

/*
 * THE RECENT ERROR LOG (menu: Report a problem): every console.error the app already logs is also kept in a small
 * in-memory ring, which a reader sees — and can switch off — in a report before sending it. Nothing is stored on disk
 * and nothing is sent in the background (owner 2026-10-21).
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
const place: { route: Route | null } = { route: null };
// (T221) and on the web the address bar follows it, so a refresh opens where the reader was
const holdPlace = (r: Route) => { place.route = r; routeUrl(r); };
if (START?.who) setAnglerProfile(START.who);

/**
 * Where everything is served from: ONE base, in `config.ts` (P10). In production these are R2 URLs and nothing else
 * changes.
 */
const FEED = ENDPOINTS.feed;

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
  const weather = useMemo(() => openMeteo(), []);
  // T170: HYDAT's daily means through ECCC GeoMet, for a spot's visit day older than the feed's record (one GET each)
  const hydat = useMemo(() => geometHydat(), []);
  // S1: a spot's photographs — files beside the spots file on a phone, IndexedDB in a browser — and the platform's
  // picker (`./pickPhoto.native.ts` / `.web.ts`). The user's own: never in a release, never uploaded.
  const photos = useMemo<SpotPhotos>(
    () => ({ store: openPhotos(), pick: pickPhoto, sources: PHOTO_SOURCES }), []);
  const [refreshing, setRefreshing] = useState(false);
  // P01: every write awaited and serialised; a failure reaches the screen that asked
  const spotsHook = useSpots(spotStore);
  const spots: Spot[] = spotsHook.spots.state === "ready" ? spotsHook.spots.value : [];
  // (T231) the gauge index is the update service's to renew: once read it is held until the service offers a newer one
  const feed = useMemo(() => httpFeed(FEED, undefined, { indexHeld: true }), []);
  // T211: the live DFO salmon rules, the last capture kept on the device for offline
  const salmonFeed = useMemo(() => httpSalmonFeed(ENDPOINTS.dfoFeed, openSalmonFeedStore()), []);
  // T230: the provincial in-season changes and where each applies, the last capture kept for offline; the ids shown
  const inSeason = useMemo(() => httpInSeason(ENDPOINTS.inSeason, openInSeasonStore()), []);
  const inSeasonSeen = useMemo(() => openInSeasonSeenStore(), []);

  // THE RELEASE: manifest → (native: install / update) → paint, then the Shell. (T231) The phone's background install
  // is the update service's (`deferUpdate`: it holds a large set for Wi-Fi); `generation` re-boots onto a staged update.
  const [generation, setGeneration] = useState(0);
  const boot = useReleaseBoot(useMemo(() => ({
    manifestUrl: ENDPOINTS.manifest, releaseBase: ENDPOINTS.release, wasmBase: ENDPOINTS.sqlWasm, platform, feed,
    salmonFeed, deferUpdate: true,
  }), [feed, salmonFeed]), generation);
  const booted = boot.state === "ready" ? boot.booted : null;
  const bootedRef = useRef(booted);
  bootedRef.current = booted;

  /*
   * (T231) THE UPDATE SERVICE (owner 2026-10-28: "check the data origin for new updates and download them"): the three
   * feeds and the release, checked on start, on the way back to the foreground, on an interval and on the sync icon;
   * an unchanged object costs a 304. A changed feed is handed to its loader whole; a newer release is staged beside the
   * one in use (verified) and applied at a safe moment (the Shell says when), with "Updated to …".
   */
  const updates = useMemo(() => createUpdateService({
    feeds: [
      { id: "gauges", url: `${ENDPOINTS.feed}/index.json`, apply: (text) => feed.offer!(JSON.parse(text)) },
      { id: "dfo", url: `${ENDPOINTS.dfoFeed}/index.json`, apply: (text) => salmonFeed.offer!(text) },
      { id: "inseason", url: `${ENDPOINTS.inSeason}/index.json`, apply: () => inSeason.refresh?.() },
    ],
    release: {
      manifestUrl: ENDPOINTS.manifest,
      held: () => bootedRef.current?.held ?? null,
      stage: async (m, on) => {
        const b = bootedRef.current;
        if (!b) throw new Error(UPDATE_WORDS.sheet.notOpen);
        await b.stage(m, (p) => on(p.bytes, p.of));
      },
    },
    store: openUpdateStore(),
    network: {
      online: () => connectivity.online(),
      metered: () => connectivity.metered?.() ?? false,
      subscribe: (on) => {
        const a = connectivity.subscribe(on);
        // off a metered connection (onto Wi-Fi): a download held for Wi-Fi looks again
        const b = connectivity.watchMetered?.((m) => { if (!m) on(true); });
        return () => { a(); b?.(); };
      },
    },
    lifecycle: {
      subscribe: (on) => {
        let was = AppState.currentState;
        const sub = AppState.addEventListener("change", (st) => { if (st === "active" && was !== "active") on(); was = st; });
        return () => sub.remove();
      },
    },
  }), [feed, salmonFeed, inSeason]);
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
  const apply = useCallback(() => {
    if (updates.snapshot().release?.phase === "ready") setGeneration((g) => g + 1);
  }, [updates]);
  // back from the background with an update ready: the safest moment there is
  useEffect(() => {
    let was = AppState.currentState;
    const sub = AppState.addEventListener("change", (st) => { if (st === "active" && was !== "active") apply(); was = st; });
    return () => sub.remove();
  }, [apply]);
  // the boot that applied it: told to the service, and said ("Updated to …")
  const [toast, setToast] = useState<string | null>(null);
  const appliedGen = useRef(0);
  useEffect(() => {
    if (boot.state !== "ready" || boot.generation === 0 || appliedGen.current === boot.generation) return;
    appliedGen.current = boot.generation;
    if (boot.applyError) return;
    updates.applied(boot.booted.manifest.data_release);
    setToast(updatedToast(manifestDate(boot.booted.manifest)));
  }, [boot, updates]);
  const clearToast = useCallback(() => setToast(null), []);

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

  const failed = boot.state === "failed" ? boot.error.message : null;
  const firstRunOffline = boot.state === "failed" && boot.firstRunOffline;
  const stage = boot.state === "loading" ? boot.stage : "paint";
  const progress = boot.state === "loading" ? boot.progress : null;
  // T215: our copy of the synopsis PDF (the release names it; fetched on the first highlighted open, from the release
  // base on web and phone alike — it is not part of the installed set). None: "open the PDF" stays the plain link.
  const synopsis = boot.state === "ready" ? boot.booted.manifest.synopsis : null;
  useEffect(() => {
    setSynopsisSource(synopsis?.sha256 && synopsis.bytes
      ? { url: `${ENDPOINTS.release}${synopsis.url}`, sha256: synopsis.sha256, bytes: synopsis.bytes } : null);
  }, [synopsis]);

  // a phone whose newest set would not open went back to the one it replaced: said, above the map
  const rolledBack = boot.state === "ready" ? boot.booted.rolledBack : null;

  const name: ThemeName = theme ?? (scheme === "dark" ? "dark" : "light");
  const palette = THEMES[name];

  // The faces are named in @app/ui-native/type.ts and loaded here, so that package stays
  // free of platform imports. Until they land, hold the splash rather than paint the whole
  // app in the system face and reflow it a beat later.
  const [ready] = useFonts({
    BricolageGrotesque_700Bold, BricolageGrotesque_800ExtraBold,
    Archivo_400Regular, Archivo_500Medium, Archivo_600SemiBold, Archivo_700Bold, Archivo_800ExtraBold,
    JetBrainsMono_500Medium,
  });

  // (T45) THE WINDOW'S WIDTH, read here and nowhere else in the app: every screen takes its layout class from it
  // (@app/ui `useLayoutClass`), so a component arranges itself for a phone, a tablet or a desktop without reading the window
  const { width } = useWindowDimensions();

  return (
    <LayoutWidthContext.Provider value={width}>
    {/* THE SYSTEM BARS' INSETS, read here and nowhere else (T50): Android draws edge-to-edge (API 35), so without them the
        status bar sat over the top bar, the back links and Close (and swallowed their taps), and the gesture handle over
        the tab bar. RN's own SafeAreaView padded iOS only; react-native-safe-area-context pads every edge on both. */}
    <SafeAreaProvider>
    <SafeAreaView style={{ flex: 1, backgroundColor: palette.card }}>
      <StatusBar barStyle={name === "dark" ? "light-content" : "dark-content"} />
      {failed ? (
        <View style={{ flex: 1, alignItems: "center", justifyContent: "center", padding: 32,
                       gap: 8 }}>
          <Text style={{ color: palette.closed, fontSize: 16, textAlign: "center" }}>
            {INSTALL_WORDS.failed}
          </Text>
          {firstRunOffline && (
            <Text style={{ color: palette.sub, fontSize: 13, textAlign: "center" }}>
              {INSTALL_WORDS.firstRunOffline}
            </Text>
          )}
          <Text style={{ color: palette.sub, fontSize: 13, textAlign: "center" }}>
            {failed}
          </Text>
          <Text style={{ color: palette.faint, fontSize: 12, textAlign: "center" }}>
            {INSTALL_WORDS.serve}
          </Text>
        </View>
      ) : ready && data ? (
        <View style={{ flex: 1 }}>
        {rolledBack && (
          <Text style={{ color: palette.sub, fontSize: 12, paddingVertical: 6, paddingHorizontal: 12 }}>
            {rolledBackLine(rolledBack)}
          </Text>
        )}
        <Shell source={data.source} palette={palette} loader={data.loader} menuInfo={MENU_INFO}
               // (T231) the sync icon reads the update service; a ready update is applied at the Shell's safe moment
               updates={updates} onSafeToApply={apply}
               // T129: the boot's manifest read seeds the service's first release check (`seedRelease`): no second request
               // (T186, Q164) the web fetches ahead after first paint; a phone holds the release installed
               prefetch={Platform.OS === "web"}
               layerStore={layerStore} likeStore={likeStore}
               theme={name} themeName={name}
               onTheme={setTheme} tiles={data.tiles}
               status={data.status} pairing={data.pairing}
               spots={spots}
               fishImages={FISH_IMAGES}
               adiposeImages={ADIPOSE_IMAGES}
               systemBack={systemBack}
               initialRoute={place.route ?? START?.route} onRoute={holdPlace} initialDay={START?.day ?? null} regsStart={START?.regs ?? null}
               locate={locate}
               weather={weather}
               photos={photos}
               onSaveSpot={spotsHook.save}
               spotsError={spotsHook.spots.state === "failed" ? spotsHook.error : null}
               feed={feed}
               inSeason={inSeason} inSeasonSeen={inSeasonSeen}
               refreshing={refreshing}
               onDeleteSpot={spotsHook.remove}
               onRefreshSpots={async () => {
                 setRefreshing(true);
                 // through `patch`: a spot deleted or edited meanwhile is never overwritten (P01)
                 // the source reads the station's daily means for a spot's week before (S1); the feed's index tells a
                 // station it does not carry (settled) from a feed not reached (asked again)
                 // (T170) and HYDAT for the visit day's own flow where the reading is not the day's
                 try { await spotsHook.refresh((s) => refreshSpot(s, weather, data.source, feed, { hydat })); }
                 catch { /* said by spotsHook.error; the list keeps what was saved */ }
                 finally { setRefreshing(false); }
               }}
               hydat={hydat}
               onDayFlow={async (spot) => {
                 // T170 "Get the flow for that day": that one spot, through the same fill a refresh uses; the others
                 // come back as they are (no write), and an edit made meanwhile wins (`refresh`'s patch)
                 let answer: DayFlowAnswer | null = null;
                 await spotsHook.refresh(async (s) => {
                   if (s.id !== spot.id) return s;
                   const got = await fillDayFlow(s, { source: data.source, hydat });
                   answer = got.answer;
                   return got.spot;
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
        <View style={{ flex: 1, alignItems: "center", justifyContent: "center", gap: 14 }}>
          <FishSpinner palette={palette} size={110} label={startStage(!ready && data ? "fonts" : stage)} />
          <Text style={{ color: palette.sub, fontSize: 13 }}>
            {startStage(!ready && data ? "fonts" : stage)}
          </Text>
          {stage === "install" && progress && (
            <>
              <Text style={{ color: palette.sub, fontSize: 13 }}>{installLine(progress)}</Text>
              <Text style={{ color: palette.faint, fontSize: 12 }}>{INSTALL_WORDS.wifi}</Text>
            </>
          )}
        </View>
      )}
    </SafeAreaView>
    </SafeAreaProvider>
    </LayoutWidthContext.Provider>
  );
}
