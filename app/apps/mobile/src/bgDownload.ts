/**
 * (T298) THE SYSTEM'S DOWNLOADER (modules/bg-download): the first install's big files (and an update's) download out of
 * the app's process — on with the app in the background or the screen off — and are hashed natively.
 *   Android  the DownloadManager (Kotlin): also on after the app is swiped away, with the system's progress notification.
 *   iOS      (T722, owner 2026-11-14) a background URLSession (Swift), re-attached at each launch; iOS cancels it when
 *            the app is swiped away in the app switcher, and the next open resumes it. No notification to allow.
 * `null` on the web and in a development client built before the module (the install then downloads in the app).
 * The JS state machine over it is one for both (`@app/data` `release/bgDownload.ts`).
 */
import { requireOptionalNativeModule } from "expo";
import type { BackgroundNative, BgJob } from "@app/data/release";

const native = requireOptionalNativeModule<{
  jobs(): Promise<BgJob[]>;
  enqueue(url: string, name: string, title: string, text: string, metered: boolean): Promise<number>;
  remove(id: number): Promise<void>;
  claim(id: number, toUri: string): Promise<void>;
  sha256(uri: string): Promise<string>;
  /** Android only (13+: its download notification needs the leave to notify); iOS shows none, so has neither */
  notificationsAllowed?(): Promise<boolean>;
  askNotifications?(): Promise<boolean>;
  /** iOS only: `isExcludedFromBackup` on a file or folder (Android's module has none) */
  excludeFromBackup?(uri: string): Promise<void>;
}>("BgDownload");

/** the module has the notification pair (Android); each call goes through the module object, as before */
const ask = !!(native?.notificationsAllowed && native.askNotifications);

export const backgroundDownloads: BackgroundNative | null = native ? {
  jobs: () => native.jobs(),
  enqueue: (url, name, title, text, metered) => native.enqueue(url, name, title, text, metered),
  remove: (id) => native.remove(id),
  claim: (id, to) => native.claim(id, to),
  sha256: (uri) => native.sha256(uri),
  ...(ask ? { notificationsAllowed: () => native.notificationsAllowed!(), askNotifications: () => native.askNotifications!() } : {}),
} : null;

/** (T722 F6) The iOS module's backup exclusion (`BootOptions.noBackup`); null on Android and the web. */
export const excludeFromBackup: ((uri: string) => Promise<void>) | null = native?.excludeFromBackup
  ? (uri) => native.excludeFromBackup!(uri) : null;
