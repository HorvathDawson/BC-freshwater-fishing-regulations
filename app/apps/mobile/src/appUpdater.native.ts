/**
 * (T390) THE PHONE'S WAY TO A NEWER APP, by channel (`@app/core` `appUpdateChannel`, from the build's own env):
 *
 *   play       the check is Google Play's own (`expo-in-app-updates`, app/deps.md): a version is said only once Play
 *              offers it to this phone (owner 2026-11-06: our `play.json` announced 0.6.4 at upload, before Play had
 *              it). (T894) The version and its "What's new" come from `app/android/play/<versionCode>.json`, the file
 *              of the very build Play offers (its `availableVersionCode`), never play.json (the last upload, any
 *              track). The button is Play's in-app update: the flexible download, or the immediate, full-screen flow
 *              when new data needs the app.
 *   app-store  iOS, once the listing exists (T88: `EXPO_PUBLIC_APP_STORE_URL`, which build-ios-testflight.sh sets from
 *              ~/.canifishthis/app-store-url): the App Store lookup names the version; (T894) our
 *              `app/ios/<that version>.json` (build-ios-testflight.sh, one per upload) gives its bullets. The button
 *              opens the listing. Without the URL there is no channel.
 *   apk-page   a sideloaded staging APK: `latest.json` beside the APK page (`deploy/staging/build-apk.sh`) for the
 *              version, the button opens the page.
 * (T234 v1.1) The incoming version's "What's new" bullets come from the version file on our server (`latest.json`, core
 * `appVersionFileUrl`; the stores' per-build files, core `storeVersionFileUrl`), read by the check itself on every
 * channel: a newer version is never in the bundled changelog, and each file holds its own platform's notes (the build
 * scripts write them).
 *
 * The store package is loaded only on the first Play / App Store call, never for the APK page.
 */
import { Platform } from "react-native";
import { apkIsNewer, appUpdateChannel, reportFailure, storeUpdateCheck, type AppVersion, type AppVersionFile } from "@app/core";
import type { AppUpdatePort } from "@app/ui";
import { openLinkShown } from "@app/ui-native";
import { ENDPOINTS } from "./config";

const store = () => import("expo-in-app-updates");
/** app.json `android.package` */
const PLAY_PACKAGE = "ca.canifishthis.app";
/** (T894) the store build's own version file, read on our data origin; a failed read is reported (core storeUpdateCheck) */
const STORE_FILES = {
  dataBase: ENDPOINTS.base, reportFailure,
  readJson: async (url: string): Promise<unknown> => {
    const r = await fetch(url, { cache: "no-store" });
    if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
    return r.json();
  },
};

export function makeAppUpdater(own: AppVersion): AppUpdatePort | null {
  const apkPage = process.env.EXPO_PUBLIC_DEV_APK_URL || null;
  const appStoreUrl = process.env.EXPO_PUBLIC_APP_STORE_URL || null;
  const channel = appUpdateChannel({ os: Platform.OS, env: process.env.EXPO_PUBLIC_APP_ENV || "",
                                     distribution: process.env.EXPO_PUBLIC_APP_DISTRIBUTION || null, apkPage, appStoreUrl });
  const open = async (url: string) => { await openLinkShown(url); return "opened" as const; };
  switch (channel) {
    case "none":
      return null;
    case "apk-page": {
      const page = apkPage!;
      const latest = new URL("latest.json", page).toString();
      return {
        channel, installed: own.version,
        check: async () => {
          const r = await fetch(latest, { cache: "no-store" });
          if (!r.ok) throw new Error(`HTTP ${r.status} for ${latest}`);
          const l = await r.json() as Partial<AppVersionFile>;
          return { available: apkIsNewer(l, own), version: l.version ?? null, notes: l.notes ?? null };
        },
        start: () => open(page),
      };
    }
    case "app-store": {
      return {
        channel, installed: own.version,
        // (T894) the App Store names the version; app/ios/<that version>.json gives its bullets (core storeUpdateCheck)
        check: async () => storeUpdateCheck("app-store", await (await store()).checkForUpdate(), STORE_FILES),
        start: () => open(appStoreUrl!),
      };
    }
    case "play": {
      const listing = `https://play.google.com/store/apps/details?id=${PLAY_PACKAGE}`;
      return {
        channel, installed: own.version,
        // (T894) Play decides; app/android/play/<the versionCode Play offers>.json names that build's version and bullets
        // (core storeUpdateCheck), never play.json's last upload on another track
        check: async () => storeUpdateCheck("play", await (await store()).checkForUpdate(), STORE_FILES),
        start: async ({ immediate }) => {
          const m = await store();
          const u = await m.checkForUpdate();
          // the check said available a moment ago; Play no longer offering it opens the listing
          if (!u.updateAvailable) { await openLinkShown(listing); return "listing"; }
          // immediate only when Play allows it for this update; else the flexible download
          const started = await m.startUpdate(immediate && u.immediateAllowed === true);
          return started ? "downloading" : "none";
        },
      };
    }
  }
}
