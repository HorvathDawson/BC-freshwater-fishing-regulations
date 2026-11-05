/**
 * (T390) THE PHONE'S WAY TO A NEWER APP, by channel (`@app/core` `appUpdateChannel`, from the build's own env):
 *
 *   play       the check is our `play.json` (every opening and return, T390 11-05); the button is Google Play's in-app
 *              update (`expo-in-app-updates`, app/deps.md): the flexible download (the immediate, full-screen flow when
 *              new data needs the app), or the Play listing while Play does not offer this phone the version yet.
 *   app-store  iOS, once the listing exists (T88: `EXPO_PUBLIC_APP_STORE_URL`): the store lookup for the version, the
 *              button opens the listing. Until then there is no channel (STUB until T88).
 *   apk-page   a sideloaded staging APK: `latest.json` beside the APK page (`deploy/staging/build-apk.sh`) for the
 *              version, the button opens the page.
 * (T234 v1.1) The incoming version's "What's new" bullets come from the version file on our server (`latest.json` /
 * `play.json`, core `appVersionFileUrl`), read by the check itself on both channels.
 *
 * The store package is loaded only on the first Play / App Store call, never for the APK page.
 */
import { Platform } from "react-native";
import { apkIsNewer, appUpdateChannel, appVersionFileUrl, type AppVersion, type AppVersionFile } from "@app/core";
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
        channel,
        check: async () => {
          const r = await fetch(latest, { cache: "no-store" });
          if (!r.ok) throw new Error(`HTTP ${r.status} for ${latest}`);
          const l = await r.json() as Partial<AppVersionFile>;
          return { available: apkIsNewer(l, own), version: l.version ?? null, notes: l.notes ?? null };
        },
        start: () => open(page),
      };
    }
    case "app-store":
      return {
        channel,
        check: async () => {
          const u = await (await store()).checkForUpdate();
          return { available: u.updateAvailable, version: u.storeVersion ?? null };
        },
        start: () => open(appStoreUrl!),
      };
    case "play": {
      // (T390 2026-11-05) the check is OUR play.json (build-apk.sh --play writes it after the upload): Play's own check
      // says "available" only after the phone's Play Store refreshes, minutes to hours later, so 0.6.0 missed 0.6.1
      const file = appVersionFileUrl("play", { apkPage: null, dataBase: ENDPOINTS.base })!;
      const listing = `https://play.google.com/store/apps/details?id=${PLAY_PACKAGE}`;
      return {
        channel,
        check: async () => {
          const r = await fetch(file, { cache: "no-store" });
          if (!r.ok) throw new Error(`HTTP ${r.status} for ${file}`);
          const l = await r.json() as Partial<AppVersionFile>;
          return { available: apkIsNewer(l, own), version: l.version ?? null, notes: l.notes ?? null };
        },
        start: async ({ immediate }) => {
          const m = await store();
          const u = await m.checkForUpdate();
          // our file is ahead of what Play offers this phone yet: the listing (Play may show it there first)
          if (!u.updateAvailable) { await openLinkShown(listing); return "listing"; }
          // immediate only when Play allows it for this update; else the flexible download
          const started = await m.startUpdate(immediate && u.immediateAllowed === true);
          return started ? "downloading" : "none";
        },
      };
    }
  }
}
