/**
 * THE SHELL'S INPUTS: everything the app hands the Shell (T62 3.4: the props type, out of Shell.tsx unchanged).
 */
import type { Horizon, UpdateEvent } from "@app/core";
import type { ItemId, WaterSource } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import type { LayerStore, Locate, PairingInput, RegsStart, Route, SpotPhotos, StatusSource, SystemBack } from "@app/ui";
import type { DayFlowAnswer, HydatDaily, LikeStore, Spot, WeatherSource } from "@app/data/spots";
import type { Camera, TileEndpoints } from "@app/map";
import type { MutableRefObject } from "react";
import type { MenuInfo } from "../menu/MenuHost";
import type { AdiposeImages, FishImages } from "../regs/types";
import type { Palette, ThemeName } from "../theme";

export interface ShellProps {
  source: WaterSource; palette: Palette;
  /**
   * Where the day's status comes from: the release's paint (P11). Absent -> the water is drawn plain.
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
  /**
   * T170: ask for one spot's visit-day flow from the day's record and save it (`fillDayFlow`); resolves to what was
   * learned (null: nothing to ask). Absent -> the spot sheet shows the record and offers no action.
   */
  onDayFlow?: (spot: Spot) => Promise<DayFlowAnswer | null>;
  /** Q161: HYDAT's daily means, so a spot added for an earlier day reads that day's gauges. Absent: feed days only. */
  hydat?: HydatDaily;
  /** Awaited like a save: a rejection is shown and the spot stays open (P01). */
  onDeleteSpot?: (id: string) => void | Promise<void>;
  /** The spots could not be read (a corrupt file, set aside): said in the Spots menu, never an empty list (P01). */
  spotsError?: string | null;
  /**
   * The live gauge feed. Absent means the Conditions view colours nothing — which is the
   * honest rendering of "we could not check", and never a map of droughts.
   */
  feed?: import("@app/data").GaugeFeed;
  /** (T230) the provincial in-season changes + their curated links (`httpInSeason`). Absent: no banner, no notice. */
  inSeason?: import("@app/data").InSeasonSource | null;
  /** (T521) the live DFO feed (`httpSalmonFeed`, the one the release loader lays over its salmon): its rules on a water
   *  no curated binding knows are the bell's DFO section. Absent: no section. */
  salmonFeed?: import("@app/data").SalmonFeedSource | null;
  /** (T230) the in-season row ids this device has been shown in the notice (once per row per device). */
  inSeasonSeen?: import("@app/ui").InSeasonSeenStore | null;
  /**
   * (T390) the platform's way to a newer app (`apps/mobile` `appUpdater`: Play's in-app update, the App Store page, the
   * staging APK page) and where its once-a-day check is kept. Absent: no gentle prompt; a release refused for an old
   * app still says so (the strong prompt), with no button.
   */
  appUpdater?: import("@app/ui").AppUpdatePort | null;
  appUpdateStore?: import("@app/ui").AppUpdateStore | null;
  /** (T236) where the first-run notice's acceptance is kept (`openFirstRunStore`). Absent: the notice never opens. */
  firstRun?: import("../FirstRunNotice").FirstRunStore | null;
  /** (T247) where recent searches are kept (`openRecentSearchStore`, device-only). Absent: kept for this run. */
  recentSearches?: import("@app/ui").RecentStore | null;
  /** (T248) where the first-run walkthrough's "seen" is kept (`openWalkthroughStore`). Absent: it never opens by itself. */
  walkthrough?: import("../Walkthrough").WalkthroughStore | null;
  /** (T199) the run-timing feed (`httpRunTiming`; its own stream, not the release). Absent: no Run timing mode. */
  runTiming?: import("@app/data").RunTimingSource | null;
  /** (T261) where the Runs tab's species are kept between visits (`openRunSpeciesStore`). Absent: kept for this run. */
  runSpeciesStore?: import("@app/ui").RunSpeciesStore | null;
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
  /** T01: the release loader (P11), handed to the water screen for its Regulations page. null: none (a test). */
  loader?: ReleaseLoader | null;
  /** Where the app opens (a deep link, a test); the map tab when absent. Read once, on mount. */
  initialRoute?: Route;
  /**
   * (T234) Open a water's Conditions now — a notification's tap (a gauge alert): `item` its durable id, null an unnamed
   * reach's alert (the Conditions map); `n` counts taps, so the same water asked again opens again.
   */
  openRequest?: { item: string | null; n: number; view?: "regs" | "conditions" | "map" } | null;
  /** (T234 v1.1) a new-app-version notification was tapped (a count): the bell opens on its "App update", checked now */
  openUpdate?: number;
  /** (T50) every route the Shell moves to, for an app that must put the reader back after Android recreates the
   *  activity (a font-size or display change) — the JS outlives the React tree, so the app holds the last one. */
  onRoute?: (r: Route) => void;
  /** (T41) the day it opens on (the web start link); today when absent. Read once, on mount. */
  initialDay?: Date | null;
  /** (T369 D10) the Conditions forecast horizon it opens on (the address's; a recreated activity's). Read once. */
  initialHorizon?: Horizon | null;
  /** (T369 D10/D11) the day and horizon on screen, told on every change: the address keeps them, and an app whose
   *  activity Android recreates reopens on them. */
  onAddress?: (a: { day: Date | null; horizon: Horizon }) => void;
  /**
   * (T369 D12) THE CAMERAS, held by the app across a recreated activity: the Shell keeps the browse map's and the
   * water map's camera in these refs (made on first mount, then reused), so a reader put back on a water by Android
   * goes back to where the map was, not HOME. Absent: the Shell's own.
   */
  cameraHold?: { browse?: MutableRefObject<Camera>; water?: MutableRefObject<Camera | null> };
  /** (T41) where the first water's Regulations page starts (the web start link): handed to that page once. */
  regsStart?: RegsStart | null;
  /** (T269) the water the page started on or at (the web start link's `arrive`): the map comes back framed on it (nothing lit). Read once. */
  arriveAt?: ItemId | null;
  /** The platform's location, for the Spots menu's "At my location" (the app's). Absent -> not offered. */
  locate?: Locate | null;
  /**
   * (T384) Where the first open's location ask is kept (`openLocationAskStore`): the app opens on the reader, asking once.
   * Absent: an open never asks by itself (a permission already given still centres it).
   */
  locationAsk?: import("@app/ui").LocationAskStore | null;
  /** (T385) The platform's share (the phone's sheet; the web's, else the clipboard) for the water header's Share. Absent: none. */
  share?: import("@app/ui").SharePort | null;
  /** The menu's build facts (app build, the one contact value), from the app's config. Absent -> neither stated. */
  menuInfo?: MenuInfo;
  /**
   * (The sync icon) Reads the data release the server names NOW (the manifest's `data_release`), to say when a newer
   * one is available. Absent: nothing to check. Its identity must be stable (the app's useCallback).
   */
  checkRelease?: (() => Promise<string | null>) | null;
  /**
   * (T129) The release the server named when this launch read its manifest, standing in for `checkRelease`'s first ask
   * (`useSync` `named`): the web's boot read it an instant before. Absent or null (the phone): the first check asks.
   */
  releaseNamed?: string | null;
  /**
   * (T231) THE UPDATE SERVICE (`@app/data` `createUpdateService`, the app's): the sync icon reads it — checking, a
   * download's progress, an update ready, a failure (red badge; the tap opens what failed and Retry) — and its tap is a
   * manual check. Given, `checkRelease`/`releaseNamed` are not used.
   */
  updates?: import("@app/data").UpdateService | null;
  /**
   * (T231) A downloaded release is ready and the reader is at a SAFE MOMENT (a browse map or the Spots / More tab, with
   * no water, spot, capture or search open): the app applies it (boots onto it and says "Updated to …"). Called once
   * per ready release.
   */
  onSafeToApply?: () => void;
  /**
   * (T186, Q164) Fetch ahead after first paint: the regs file of the region in view, and core + sql-wasm when the
   * browser is idle and not on Save-Data / a slow connection (`@app/ui` `useReleasePrefetch`). The WEB only (App passes
   * it there): a phone holds the release installed.
   */
  prefetch?: boolean;
  /**
   * (T253) Where the background prefetch (every file an unnamed-stream tap reads, the region in view first) tells each
   * step: the Updates log's `append` (T251, development and staging builds). Absent: not logged.
   */
  prefetchLog?: ((e: UpdateEvent) => void) | null;
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
}
