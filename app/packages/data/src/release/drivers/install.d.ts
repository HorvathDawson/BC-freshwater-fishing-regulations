/**
 * The shared declaration for `./install` (P11): the device a release is INSTALLED on, or null.
 *
 *   install.native.ts   expo-file-system under `documentDirectory` (not the cache directory, which the OS may purge):
 *                       the `ReleaseFs` `install`/`update` swap a pointer in, `fetchVerified` for the data files, and
 *                       `rangedDownload` into `.part` files for the tiles (resumable across a crash).
 *   install.web.ts      null: the web installs nothing. It reads lazily over the network into Cache Storage, and a
 *                       browser cannot reliably hold 1.45 GB of map (MINIMAL-PACKAGING §5).
 */
import type { Download, DownloadTo, ReleaseFs } from "../install";
import type { BackgroundNative, BgItem } from "../bgDownload";

export interface Device {
  fs: ReleaseFs;
  /** A data file's raw bytes, verified (they are small: ≤ 7 MB). */
  download: Download;
  /** A file too big to hold, streamed to disk by ranges and verified off it. */
  toDisk: DownloadTo;
  /** The file:// uri of a folder under the install root (the opener opens files in place there). */
  dirUri(path: string): string;
  /** The file:// uri of a file under the install root (the map reads the installed tiles and mask from it). */
  fileUri(path: string): string;
  /**
   * (T298) The system's downloader, when the app has one (Android): `queue` hands it every big file of a set at once,
   * so they download with the app closed. `metered` false: they wait for an unmetered network.
   */
  background?: { queue(items: BgItem[], metered: boolean): Promise<void> };
}

export declare function device(fetchImpl?: typeof fetch, bg?: BackgroundNative | null): Device | null;
