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
  Archivo_400Regular, Archivo_500Medium, Archivo_600SemiBold, Archivo_700Bold,
} from "@expo-google-fonts/archivo";
import { JetBrainsMono_500Medium } from "@expo-google-fonts/jetbrains-mono";
import { makeBundleSource } from "@app/data/bundle";
import { httpFeed } from "@app/data";
import { openMeteo, openSpots, refreshSpot, type Spot } from "@app/data/spots";
import { loadBundle, setSqlWasmUrl, type LoadedBundle } from "@app/data/bundle/open";
import { FishSpinner, Shell, THEMES, type ThemeName } from "@app/ui-native";


/**
 * Where the tiles are served from. `pnpm tiles` runs a range-capable server over the two
 * archives in development; in production these are R2 URLs and nothing else changes,
 * which is the whole point of addressing them as endpoints rather than as files.
 */
const TILES = {
  atlas: "http://localhost:39217/atlas.pmtiles",
  basemap: "http://localhost:39217/basemap.pmtiles",
  outside: "http://localhost:39217/bc_outside.geojson",
};
/**
 * The data bundle. `province.sqlite` is the real build — 19,862 waters — and
 * `bundle.sqlite` is the small Chilliwack slice that has gauges in it. Both are
 * the same format, which is the point: swapping this line is the whole difference between
 * a development bundle and a shipped one.
 */
/**
 * THE THREE MUST BE ONE SET. Tiles, bundle and feed are joined on section ids and station
 * ids, and mixing vintages joins nothing: pairing the PROVINCE tiles with the FIXTURE
 * bundle asks a Chilliwack-sized database about section ids from the whole province, gets
 * no rows, and paints an uncoloured map that looks exactly like a broken feed.
 *
 *   province set   /atlas.pmtiles  +  /province.sqlite  +  /feeds/live
 *   fixture set    /atlas.pmtiles  +  /bundle.sqlite    +  /feeds/gauge
 *
 * The province set is the real one. The fixture set exists so tests and the design
 * comparison have a small, fixed world.
 */
const BUNDLE = "http://localhost:39217/province.sqlite";

/**
 * The live gauge feed.
 *
 * Only this string changes between dev and production — the artifacts are byte-identical,
 * so the feed path the app exercises here is the one it runs in production. Nothing in the
 * app ever calls ECCC directly: a browser full of users hitting the source we scrape is how
 * an open dataset gets closed.
 *
 * A FEED IS ONLY MEANINGFUL PAIRED WITH ITS BUNDLE, so this tracks BUNDLE above:
 *
 *   /feeds/gauge  the fixture feed — five stations, matching the fixture bundle
 *   /feeds/live   real ECCC data from `python -m pipeline.hydro.publish`, matching
 *                 data/generated/bundle/bundle.sqlite (served here as /province.sqlite)
 *
 * Point both at the province pair to see live numbers over the whole map.
 */
const FEED = "http://localhost:39217/feeds/live";
/** Where sql.js finds its engine on the web. Same origin as the bundle it opens. */
const SQL_WASM = "http://localhost:39217/";

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
  const [spots, setSpots] = useState<Spot[]>([]);
  useEffect(() => { void spotStore.list().then(setSpots); }, [spotStore]);
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
    Archivo_400Regular, Archivo_500Medium, Archivo_600SemiBold, Archivo_700Bold,
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
               weather={weather}
               onSaveSpot={async (s) => {
                 await spotStore.put(s);
                 setSpots(await spotStore.list());
               }}
               feed={feed}
               refreshing={refreshing}
               onDeleteSpot={async (id) => {
                 await spotStore.remove(id);
                 setSpots(await spotStore.list());
               }}
               onRefreshSpots={async () => {
                 setRefreshing(true);
                 try {
                   for (const s of await spotStore.list()) {
                     const filled = await refreshSpot(s, weather);
                     if (filled !== s) await spotStore.put(filled);
                   }
                   setSpots(await spotStore.list());
                 } finally { setRefreshing(false); }
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
