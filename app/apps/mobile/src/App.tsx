/**
 * The app shell. Views only — no domain logic ever lands here (layers.json).
 *
 * ONE tree for all three targets: expo start --web renders this through react-native-web,
 * expo run:android / run:ios render the same file natively. Every screen lives in
 * @app/ui-native, so mobile web and the native app cannot show different things.
 *
 * The source is the FIXTURE for now — a real slice of the Chilliwack valley out of build
 * 54ea0bb4, not invented data. Swapping it for the bundle-backed source is a one-line
 * change here precisely because nothing above `RegsSource` knows which one it has.
 */
import { useEffect, useMemo, useState } from "react";
import { SafeAreaView, StatusBar, Text, View, useColorScheme } from "react-native";
import { useFonts } from "expo-font";
import {
  BricolageGrotesque_700Bold, BricolageGrotesque_800ExtraBold,
} from "@expo-google-fonts/bricolage-grotesque";
import {
  Archivo_400Regular, Archivo_500Medium, Archivo_600SemiBold, Archivo_700Bold, Archivo_800ExtraBold,
} from "@expo-google-fonts/archivo";
import { JetBrainsMono_500Medium } from "@expo-google-fonts/jetbrains-mono";
import { makeBundleSource } from "@app/data/bundle";
import { httpFeed } from "@app/data";
import { openMeteo, openSpots, refreshSpot, type Spot } from "@app/data/spots";
import { loadBundle, setSqlWasmUrl, type LoadedBundle } from "@app/data/bundle/open";
import { FishSpinner, Shell, THEMES, type ThemeName } from "@app/ui-native";
import { useSpots } from "@app/ui";


import { ENDPOINTS } from "./config";
import { FISH_IMAGES } from "./fishImages.generated";

/**
 * Where everything is served from: ONE base, in `config.ts` (P10). The tiles, bundle and feed must be one set — they
 * join on section and station ids, and mixing vintages paints an uncoloured map that looks exactly like a broken feed.
 * In production these are R2 URLs and nothing else changes.
 */
const TILES = ENDPOINTS.tiles;
const BUNDLE = ENDPOINTS.bundle;
const FEED = ENDPOINTS.feed;
const SQL_WASM = ENDPOINTS.sqlWasm;

export default function App() {
  const scheme = useColorScheme();
  const [theme, setTheme] = useState<ThemeName | null>(null);
  // The bundle has to arrive before anything can be asked of it. Held as state rather than
  // faked with an empty source: a source that answers "nothing here" while it loads is
  // indistinguishable from one that answers "nothing here" because there is nothing.
  const [bundle, setBundle] = useState<LoadedBundle | null>(null);
  const [failed, setFailed] = useState<string | null>(null);
  // Which step we are on. A spinner that says nothing is the same picture whether the app
  // is working or wedged.
  const [stage, setStage] = useState("starting");

  // Spots live in their own store — a `.spots` file on a phone, browser storage on the
  // web. Never in the bundle: that is replaced wholesale on every update.
  const spotStore = useMemo(() => openSpots(), []);
  // Open-Meteo: no key, and it has an archive endpoint — which is what lets a spot pinned
  // with no signal be filled in later from the day it was actually made.
  const weather = useMemo(() => openMeteo(), []);
  const [refreshing, setRefreshing] = useState(false);
  // P01: every write awaited and serialised; a failure reaches the screen that asked
  const spotsHook = useSpots(spotStore);
  const spots: Spot[] = spotsHook.spots.state === "ready" ? spotsHook.spots.value : [];
  useEffect(() => {
    let live = true;
    setSqlWasmUrl(SQL_WASM);
    loadBundle(BUNDLE, undefined, (s) => { if (live) setStage(s); })
      .then((b) => { if (live) setBundle(b); else b.close?.(); })
      .catch((e: unknown) => { if (live) setFailed(e instanceof Error ? e.message : String(e)); });
    return () => { live = false; };
  }, []);
  const feed = useMemo(() => httpFeed(FEED), []);
  const source = useMemo(
    () => (bundle ? makeBundleSource(bundle, { feed }) : null), [bundle, feed]);
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

  return (
    <SafeAreaView style={{ flex: 1, backgroundColor: palette.card }}>
      <StatusBar barStyle={name === "dark" ? "light-content" : "dark-content"} />
      {failed ? (
        <View style={{ flex: 1, alignItems: "center", justifyContent: "center", padding: 32,
                       gap: 8 }}>
          <Text style={{ color: palette.closed, fontSize: 16, textAlign: "center" }}>
            The map data could not be loaded.
          </Text>
          <Text style={{ color: palette.sub, fontSize: 13, textAlign: "center" }}>
            {failed}
          </Text>
          <Text style={{ color: palette.faint, fontSize: 12, textAlign: "center" }}>
            Run `pnpm tiles` to serve the bundle.
          </Text>
        </View>
      ) : ready && source ? (
        <Shell source={source} palette={palette}
               theme={name} themeName={name}
               onTheme={setTheme} tiles={TILES}
               spots={spots}
               fishImages={FISH_IMAGES}
               weather={weather}
               onSaveSpot={spotsHook.save}
               spotsError={spotsHook.spots.state === "failed" ? spotsHook.error : null}
               feed={feed}
               refreshing={refreshing}
               onDeleteSpot={spotsHook.remove}
               onRefreshSpots={async () => {
                 setRefreshing(true);
                 // through `patch`: a spot deleted or edited meanwhile is never overwritten (P01)
                 try { await spotsHook.refresh((s) => refreshSpot(s, weather)); }
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
                 // The Satellite basemap. EOX's required form; the map's own attribution
                 // control carries it too while the imagery is on screen.
                 "Satellite imagery: EOxCloudless 2016 by EOX IT Services GmbH (Contains " +
                 "modified Copernicus Sentinel data 2016), CC BY 4.0",
               ]} />
      ) : (
        <View style={{ flex: 1, alignItems: "center", justifyContent: "center", gap: 14 }}>
          <FishSpinner palette={palette} size={110} label={`Loading — ${stage}`} />
          <Text style={{ color: palette.sub, fontSize: 13 }}>
            {stage === "fetching" ? "Downloading the map data…"
             : stage === "opening" ? "Opening the map data…"
             : !ready ? "Loading type…" : "Starting…"}
          </Text>
        </View>
      )}
    </SafeAreaView>
  );
}
