/**
 * (T390) THE PHONE'S WAY TO A NEWER APP, by channel (`@app/core` `appUpdateChannel`, from the build's own env):
 *
 *   play       the check is Google Play's own (`expo-in-app-updates`, app/deps.md): a version is said only once Play
 *              offers it to this phone (owner 2026-11-06: our `play.json` announced 0.6.4 at upload, before Play had
 *              it); `play.json` gives only its "What's new" bullets. The button is Play's in-app update: the flexible
 *              download, or the immediate, full-screen flow when new data needs the app.
 *   app-store  iOS, once the listing exists (T88: `EXPO_PUBLIC_APP_STORE_URL`, which build-ios-testflight.sh sets from
 *              ~/.canifishthis/app-store-url): the App Store lookup names the version; (T888) our `appstore.json`
 *              (build-ios-testflight.sh, after the upload) gives its bullets when it names that same version. The button
 *              opens the listing. Without the URL there is no channel.
 *   apk-page   a sideloaded staging APK: `latest.json` beside the APK page (`deploy/staging/build-apk.sh`) for the
 *              version, the button opens the page.
 * (T234 v1.1) The incoming version's "What's new" bullets come from the version file on our server (`latest.json` /
 * `play.json` / `appstore.json`, core `appVersionFileUrl`), read by the check itself on every channel: a newer version is
 * never in the bundled changelog, and each file holds its own platform's notes (the build scripts write them).
 *
 * The store package is loaded only on the first Play / App Store call, never for the APK page.
 */
import { Platform } from "react-native";
import { apkIsNewer, appUpdateChannel, appVersionFileUrl, reportFailure, type AppVersion, type AppVersionFile } from "@app/core";
import type { AppUpdatePort } from "@app/ui";
import { openLinkShown } from "@app/ui-native";
import { ENDPOINTS } from "./config";

const store = () => import("expo-in-app-updates");
/** app.json `android.package` */
const PLAY_PACKAGE = "ca.canifishthis.app";

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
      const file = appVersionFileUrl("app-store", { apkPage: null, dataBase: ENDPOINTS.base })!;
      return {
        channel, installed: own.version,
        check: async () => {
          const u = await (await store()).checkForUpdate();
          const version = u.storeVersion ?? null;
          if (!u.updateAvailable) return { available: false, version: null };
          // (T888) the App Store names the version; our appstore.json gives its bullets when it names the same one (a
          // TestFlight upload ahead of the store's does not); a failed read is reported and the notice still stands
          // (it says no notes were published)
          try {
            const r = await fetch(file, { cache: "no-store" });
            if (!r.ok) throw new Error(`HTTP ${r.status} for ${file}`);
            const l = await r.json() as Partial<AppVersionFile>;
            if (version && l.version === version) return { available: true, version, notes: l.notes ?? null };
          } catch (e) { reportFailure(e); }
          return { available: true, version };
        },
        start: () => open(appStoreUrl!),
      };
    }
    case "play": {
      const file = appVersionFileUrl("play", { apkPage: null, dataBase: ENDPOINTS.base })!;
      const listing = `https://play.google.com/store/apps/details?id=${PLAY_PACKAGE}`;
      return {
        channel, installed: own.version,
        check: async () => {
          const u = await (await store()).checkForUpdate();
          if (!u.updateAvailable) return { available: false, version: null };
          // Play offers a newer build: our play.json names it and its bullets when it is that build; a failed read is
          // reported and the notice still stands (it says no notes were published)
          try {
            const r = await fetch(file, { cache: "no-store" });
            if (!r.ok) throw new Error(`HTTP ${r.status} for ${file}`);
            const l = await r.json() as Partial<AppVersionFile>;
            if (apkIsNewer(l, own)) return { available: true, version: l.version ?? null, notes: l.notes ?? null };
          } catch (e) { reportFailure(e); }
          return { available: true, version: null };
        },
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
