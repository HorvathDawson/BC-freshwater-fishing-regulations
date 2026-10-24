/**
 * THE SHELL'S INPUTS: everything the app hands the Shell (T62 3.4: the props type, out of Shell.tsx unchanged).
 */
import type { RegsSource } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import type { LayerStore, Locate, PairingInput, RegsStart, Route, SpotPhotos, StatusSource, SystemBack } from "@app/ui";
import type { DayFlowAnswer, HydatDaily, LikeStore, Spot, WeatherSource } from "@app/data/spots";
import type { TileEndpoints } from "@app/map";
import type { MenuInfo } from "../menu/MenuHost";
import type { AdiposeImages, FishImages } from "../regs/types";
import type { Palette, ThemeName } from "../theme";

export interface ShellProps {
  source: RegsSource; palette: Palette;
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
   * one is available. Absent: nothing to check. Its identity must be stable (the app's useCallback).
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
}
