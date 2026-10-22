/**
 * The app shell. Views only — no domain logic ever lands here (layers.json).
 *
 * ONE tree for all three targets: expo start --web renders this through react-native-web,
 * expo run:android / run:ios render the same file natively. Every screen lives in
 * @app/ui-native, so mobile web and the native app cannot show different things.
 *
 * THE DATA IS THE RELEASE (P11): the manifest, then paint, then the Shell (`useReleaseBoot` → `bootRelease`). On the
 * phone the first run downloads the whole release with progress and every later launch opens it from disk, checking for
 * a newer one in the background; on the web each file is fetched on first need into Cache Storage. Nothing on this path
 * reads `/province.sqlite` or `/status_index.bin`: those belong to the development bundle path,
 * `EXPO_PUBLIC_DATA_SOURCE=bundle`, until P20. `atlas.meta.json` is read only while the manifest names no tile set: the
 * map then draws the development tiles, and their stamped `tile_version` is what proves the pair (`pairingOf`).
 */
import { useEffect, useMemo, useState } from "react";
import { SafeAreaView, StatusBar, Text, View, useColorScheme, useWindowDimensions } from "react-native";
import { useFonts } from "expo-font";
import {
  BricolageGrotesque_700Bold, BricolageGrotesque_800ExtraBold,
} from "@expo-google-fonts/bricolage-grotesque";
import {
  Archivo_400Regular, Archivo_500Medium, Archivo_600SemiBold, Archivo_700Bold, Archivo_800ExtraBold,
} from "@expo-google-fonts/archivo";
import { JetBrainsMono_500Medium } from "@expo-google-fonts/jetbrains-mono";
import { INSTALL_WORDS, installLine, rolledBackLine, startStage } from "@app/core";
import { makeBundleSource } from "@app/data/bundle";
import { httpFeed, type RegsSource } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import * as platform from "@app/data/release/platform";
import { openMeteo, openPhotos, openSpots, refreshSpot, type Spot } from "@app/data/spots";
import { loadBundle, setSqlWasmUrl, type LoadedBundle } from "@app/data/bundle/open";
import { FishSpinner, Shell, THEMES, type ThemeName } from "@app/ui-native";
import { devStatus, LayoutWidthContext, releaseMapInput, releaseStatus, teeConsoleErrors, useReleaseBoot, useSpots, type MapTiles,
         type PairingInput, type SpotPhotos, type StatusSource } from "@app/ui";
import { PHOTO_SOURCES, pickPhoto } from "./pickPhoto";
import { systemBack } from "./systemBack";
import { locate } from "./locate";

import { DATA_SOURCE, ENDPOINTS, IMAGERY_TILES, MENU_INFO } from "./config";

/*
 * THE RECENT ERROR LOG (menu: Report a problem): every console.error the app already logs is also kept in a small
 * in-memory ring, which a reader sees — and can switch off — in a report before sending it. Nothing is stored on disk
 * and nothing is sent in the background (owner 2026-10-21).
 */
teeConsoleErrors(console);
import { ADIPOSE_IMAGES, FISH_IMAGES } from "./fishImages.generated";

/**
 * Where everything is served from: ONE base, in `config.ts` (P10). In production these are R2 URLs and nothing else
 * changes.
 */
const FEED = ENDPOINTS.feed;
const RELEASE = DATA_SOURCE === "release";

/** THE DEVELOPMENT BUNDLE PATH (`EXPO_PUBLIC_DATA_SOURCE=bundle`, until P20): the whole province.sqlite, as before. */
function useDevBundle(on: boolean): { bundle: LoadedBundle | null; failed: string | null; stage: string } {
  const [bundle, setBundle] = useState<LoadedBundle | null>(null);
  const [failed, setFailed] = useState<string | null>(null);
  const [stage, setStage] = useState("bundle");
  useEffect(() => {
    if (!on) return;
    let live = true;
    setSqlWasmUrl(ENDPOINTS.sqlWasm);
    loadBundle(ENDPOINTS.bundle, undefined, () => { if (live) setStage("bundle"); })
      .then((b) => { if (live) setBundle(b); else b.close?.(); })
      .catch((e: unknown) => { if (live) setFailed(e instanceof Error ? e.message : String(e)); });
    return () => { live = false; };
  }, [on]);
  return { bundle, failed, stage };
}

export default function App() {
  const scheme = useColorScheme();
  const [theme, setTheme] = useState<ThemeName | null>(null);

  // Spots live in their own store — a `.spots` file on a phone, browser storage on the
  // web. Never in the release: that is replaced wholesale on every update.
  const spotStore = useMemo(() => openSpots(), []);
  // Open-Meteo: no key, and it has an archive endpoint — which is what lets a spot pinned
  // with no signal be filled in later from the day it was actually made.
  const weather = useMemo(() => openMeteo(), []);
  // S1: a spot's photographs — files beside the spots file on a phone, IndexedDB in a browser — and the platform's
  // picker (`./pickPhoto.native.ts` / `.web.ts`). The user's own: never in a release, never uploaded.
  const photos = useMemo<SpotPhotos>(
    () => ({ store: openPhotos(), pick: pickPhoto, sources: PHOTO_SOURCES }), []);
  const [refreshing, setRefreshing] = useState(false);
  // P01: every write awaited and serialised; a failure reaches the screen that asked
  const spotsHook = useSpots(spotStore);
  const spots: Spot[] = spotsHook.spots.state === "ready" ? spotsHook.spots.value : [];
  const feed = useMemo(() => httpFeed(FEED), []);

  // THE RELEASE: manifest → (native: install / update) → paint, then the Shell
  const boot = useReleaseBoot(useMemo(() => (RELEASE ? {
    manifestUrl: ENDPOINTS.manifest, releaseBase: ENDPOINTS.release, wasmBase: ENDPOINTS.sqlWasm, platform, feed,
  } : null), [feed]));
  const dev = useDevBundle(!RELEASE);

  const data = useMemo((): { source: RegsSource; status: StatusSource; pairing: PairingInput;
                             tiles: MapTiles & { imagery: string }; loader: ReleaseLoader | null } | null => {
    if (RELEASE) {
      if (boot.state !== "ready") return null;
      const b = boot.booted;
      // the tiles, mask, basemap and map assets from the release when it names them, else the dev server's; the pair
      // is proven against the tiles DRAWN (`releaseMapInput`, tested in ui/release.test.ts)
      // T01: the loader goes to the water screen too — the Regulations page reads water / regs-<region> / regs-sid
      // through it (no regs store file of its own)
      // the satellite imagery is Esri's, online, on both paths (never in a release): `IMAGERY_TILES`
      const m = releaseMapInput(b, ENDPOINTS.tiles);
      return { source: b.source, status: releaseStatus(b.loader), ...m, tiles: { ...m.tiles, imagery: IMAGERY_TILES },
               loader: b.loader };
    }
    if (!dev.bundle) return null;
    const source = makeBundleSource(dev.bundle, { feed });
    return { source, status: devStatus(source, ENDPOINTS.tiles.atlas),
             pairing: { kind: "dev", source, atlasUrl: ENDPOINTS.tiles.atlas },
             tiles: { ...ENDPOINTS.tiles, imagery: IMAGERY_TILES },
             loader: null };      // the dev bundle holds no regulations
  }, [boot, dev.bundle, feed]);

  const failed = RELEASE ? (boot.state === "failed" ? boot.error.message : null) : dev.failed;
  const firstRunOffline = boot.state === "failed" && boot.firstRunOffline;
  const stage = RELEASE ? (boot.state === "loading" ? boot.stage : "paint") : dev.stage;
  const progress = boot.state === "loading" ? boot.progress : null;
  // a phone whose newest set would not open went back to the one it replaced: said, above the map
  const rolledBack = RELEASE && boot.state === "ready" ? boot.booted.rolledBack : null;

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
            Run `pnpm tiles` to serve the release.
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
               theme={name} themeName={name}
               onTheme={setTheme} tiles={data.tiles}
               status={data.status} pairing={data.pairing}
               spots={spots}
               fishImages={FISH_IMAGES}
               adiposeImages={ADIPOSE_IMAGES}
               systemBack={systemBack}
               locate={locate}
               weather={weather}
               photos={photos}
               onSaveSpot={spotsHook.save}
               spotsError={spotsHook.spots.state === "failed" ? spotsHook.error : null}
               feed={feed}
               refreshing={refreshing}
               onDeleteSpot={spotsHook.remove}
               onRefreshSpots={async () => {
                 setRefreshing(true);
                 // through `patch`: a spot deleted or edited meanwhile is never overwritten (P01)
                 // the source reads the station's daily means for a spot's week before (S1)
                 try { await spotsHook.refresh((s) => refreshSpot(s, weather, data.source)); }
                 catch { /* said by spotsHook.error; the list keeps what was saved */ }
                 finally { setRefreshing(false); }
               }}
               attribution={[
                 "Basemap © OpenStreetMap contributors · © Protomaps",
                 "Hydrometric data: Environment and Climate Change Canada",
                 // REQUIRED VERBATIM by the Province wherever a forecast appears —
                 // confirmed with the River Forecast Centre. See
                 // `pipeline/hydro/forecast.py`, which holds the same string.
                 "Forecast data provided by the BC River Forecast Centre, Province of " +
                 "British Columbia, and used under the Province's copyright terms " +
                 "(https://www2.gov.bc.ca/gov/content/home/copyright). Users should use " +
                 "the information on this website with caution and at their own risk.",
                 "Stocking: Province of British Columbia, Fisheries Inventory Data Queries",
                 "Weather from Open-Meteo.com (CC BY 4.0)",
                 // NO satellite line here: Esri's credit is added by the Shell exactly while its
                 // imagery is drawn (`withImageryCredit`, @app/core IMAGERY_CREDIT), as the map's
                 // own attribution control does from the style.
               ]} />
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
    </LayoutWidthContext.Provider>
  );
}
