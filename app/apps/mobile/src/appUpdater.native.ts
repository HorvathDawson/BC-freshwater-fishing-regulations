/**
 * (T390) THE PHONE'S WAY TO A NEWER APP, by channel (`@app/core` `appUpdateChannel`, from the build's own env):
 *
 *   play       Google Play's in-app update (`expo-in-app-updates`, app/deps.md): the check is Play's, the button starts
 *              the flexible download (the immediate, full-screen flow when new data needs the app). Play installs only
 *              (internal testing, closed, production): a sideloaded copy of a Play build fails the check (reported).
 *   app-store  iOS, once the listing exists (T88: `EXPO_PUBLIC_APP_STORE_URL`): the store lookup for the version, the
 *              button opens the listing. Until then there is no channel (STUB until T88).
 *   apk-page   a sideloaded staging APK: `latest.json` beside the APK page (`deploy/staging/build-apk.sh`) for the
 *              version, the button opens the page.
 *
 * The store package is loaded only on the first Play / App Store call, never for the APK page.
 */
import { Platform } from "react-native";
import { apkIsNewer, appUpdateChannel, type AppVersion } from "@app/core";
import type { AppUpdatePort } from "@app/ui";
import { openLinkShown } from "@app/ui-native";

const store = () => import("expo-in-app-updates");

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
          const l = await r.json() as { version?: string };
          return { available: apkIsNewer(l, own), version: l.version ?? null };
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
    case "play":
      return {
        channel,
        check: async () => {
          const u = await (await store()).checkForUpdate();
          return { available: u.updateAvailable, version: u.storeVersion ?? null };
        },
        start: async ({ immediate }) => {
          const m = await store();
          const u = await m.checkForUpdate();
          if (!u.updateAvailable) return "none";
          // immediate only when Play allows it for this update; else the flexible download
          const started = await m.startUpdate(immediate && u.immediateAllowed === true);
          return started ? "downloading" : "none";
        },
      };
  }
}
