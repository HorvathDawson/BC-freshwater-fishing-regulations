/**
 * (T234) THE PHONE'S NOTIFICATIONS — local only (owner 2026-11-05: no server, no push token, no account).
 *
 *   the check   `expo-background-task` (Android WorkManager, iOS BGTaskScheduler) runs `runBackgroundCheck` (@app/data)
 *               every ~15 min at best — the system decides; Doze stretches it while the phone is idle. Defined HERE, at
 *               module scope, because a headless start (the app closed) evaluates the bundle and runs the task without
 *               mounting anything: App.tsx imports this file. It reads the same feed file (`fetchObs`, the feed base the
 *               app reads) and the same release manifest the update service checks; it never downloads a release (the
 *               app takes the update when it is opened, under the mobile-data setting, T231/T298).
 *   raising     `expo-notifications`, local only (`scheduleNotificationAsync`, trigger now, on its channel). The two Android
 *               channels are made before the first one is raised.
 *   permission  Android 13+ POST_NOTIFICATIONS: read without a dialog, asked only through `ask` (the reader turned
 *               something on, `@app/ui` `useNotifications`).
 *   taps        a tap hands its data to the app (`onTap`): a gauge alert opens that water's Conditions; "new data ready"
 *               just opens the app, which applies the update as it always does; (v1.1) "a new version of the app" opens
 *               the bell on its App update (what's new + Update, T390).
 *   versions    (v1.1) the new-version notice reads the version file OUR server publishes for this build's channel
 *               (core `appVersionFileUrl`: `latest.json` beside the staging APK page, `play.json` for a Play build).
 *               Never Play's own check: Play's in-app update API needs the app in the foreground.
 */
import { Linking, Platform } from "react-native";
import * as BackgroundTask from "expo-background-task";
import * as Notifications from "expo-notifications";
import * as TaskManager from "expo-task-manager";
import { appUpdateChannel, appVersionFileUrl, NOTIFY_WORDS, now, reportFailure, versionHeaders } from "@app/core";
import { createNotifySettings, fetchObs, fetchStationRows, parseManifest, runBackgroundCheck, versionedFetch,
         type Notice, type NotifyData } from "@app/data";
import { openNotifyStore } from "@app/data/spots";
import type { NotifyControls, NotifyPermission } from "@app/ui";
import { APP_VERSION, ENDPOINTS } from "./config";

const TASK = "canifishthis-background-check";
/** minutes; Android's WorkManager floor */
const EVERY_MIN = 15;

const settings = createNotifySettings(openNotifyStore());
/** (v1.1) this build's version file on our server, or null (no channel: a local build, iOS before T88) */
const VERSION_FILE = appVersionFileUrl(
  appUpdateChannel({ os: Platform.OS, env: process.env.EXPO_PUBLIC_APP_ENV || "",
                     distribution: process.env.EXPO_PUBLIC_APP_DISTRIBUTION || null,
                     apkPage: process.env.EXPO_PUBLIC_DEV_APK_URL || null,
                     appStoreUrl: process.env.EXPO_PUBLIC_APP_STORE_URL || null }),
  { apkPage: process.env.EXPO_PUBLIC_DEV_APK_URL || null, dataBase: ENDPOINTS.base });
const FETCH = versionedFetch((u, i) => fetch(u, i),
                             versionHeaders(APP_VERSION, { platform: `${Platform.OS} ${Platform.Version}`, ua: true }));

let channels: Promise<void> | null = null;
function ensureChannels(): Promise<void> {
  if (Platform.OS !== "android") return Promise.resolve();
  channels ??= Promise.all([
    Notifications.setNotificationChannelAsync("gauges", { name: NOTIFY_WORDS.channels.gauges.name,
      description: NOTIFY_WORDS.channels.gauges.description, importance: Notifications.AndroidImportance.HIGH }),
    Notifications.setNotificationChannelAsync("updates", { name: NOTIFY_WORDS.channels.updates.name,
      description: NOTIFY_WORDS.channels.updates.description, importance: Notifications.AndroidImportance.DEFAULT }),
    Notifications.setNotificationChannelAsync("app", { name: NOTIFY_WORDS.channels.app.name,
      description: NOTIFY_WORDS.channels.app.description, importance: Notifications.AndroidImportance.DEFAULT }),
  ]).then(() => undefined, (e: unknown) => { channels = null; throw e; });
  return channels;
}

async function raise(n: Notice): Promise<void> {
  await ensureChannels();
  await Notifications.scheduleNotificationAsync({
    content: { title: n.title, body: n.body, data: n.data as unknown as Record<string, unknown> },
    trigger: Platform.OS === "android" ? { channelId: n.channel } : null,
  });
}

TaskManager.defineTask(TASK, async () => {
  try {
    await runBackgroundCheck({
      settings, now, appVersion: APP_VERSION, notify: raise,
      obs: () => fetchObs(ENDPOINTS.alertFeed, FETCH),
      stationRows: (station) => fetchStationRows(ENDPOINTS.alertFeed, station, FETCH),
      latestApp: VERSION_FILE ? async () => {
        const res = await FETCH(VERSION_FILE, { cache: "no-store" });
        if (!res.ok) throw new Error(`${VERSION_FILE}: HTTP ${res.status}`);
        return res.json();
      } : null,
      manifest: async () => {
        const res = await FETCH(ENDPOINTS.manifest, { cache: "no-store" });
        if (!res.ok) throw new Error(`${ENDPOINTS.manifest}: HTTP ${res.status}`);
        return parseManifest(await res.json());
      },
    });
    return BackgroundTask.BackgroundTaskResult.Success;
  } catch (e) {
    // runBackgroundCheck never throws; this is a defect in the glue, said
    reportFailure(e);
    return BackgroundTask.BackgroundTaskResult.Failed;
  }
});

// a notification raised while the app is open is shown too (the check also runs in the same runtime)
Notifications.setNotificationHandler({
  handleNotification: async () => ({ shouldShowBanner: true, shouldShowList: true, shouldPlaySound: false, shouldSetBadge: false }),
});

const permissionOf = (p: { status: string }): NotifyPermission =>
  (p.status === "granted" ? "granted" : p.status === "denied" ? "denied" : "undetermined");

export const notifications: NotifyControls | null = {
  settings,
  appVersion: VERSION_FILE !== null,
  platform: {
    permission: async () => permissionOf(await Notifications.getPermissionsAsync()),
    ask: async () => {
      await ensureChannels();   // Android 13+: the dialog shows only once a channel exists
      return permissionOf(await Notifications.requestPermissionsAsync());
    },
    openSettings: () => { Linking.openSettings().catch(reportFailure); },
    schedule: async (active) => {
      const has = await TaskManager.isTaskRegisteredAsync(TASK);
      if (active && !has) await BackgroundTask.registerTaskAsync(TASK, { minimumInterval: EVERY_MIN });
      if (!active && has) await BackgroundTask.unregisterTaskAsync(TASK);
    },
  },
};

const isData = (d: unknown): d is NotifyData =>
  !!d && typeof d === "object" && ["gauge", "update", "app"].includes((d as NotifyData).kind);

/** Tell `on` of every tap on one of our notifications: the one that started the app first, then each later one. */
export function onTap(on: (d: NotifyData) => void): () => void {
  const seen = (r: Notifications.NotificationResponse | null) => {
    const d = r?.notification.request.content.data;
    if (isData(d)) on(d);
  };
  seen(Notifications.getLastNotificationResponse());
  Notifications.clearLastNotificationResponse();
  const sub = Notifications.addNotificationResponseReceivedListener((r) => {
    seen(r);
    Notifications.clearLastNotificationResponse();
  });
  return () => sub.remove();
}

