/**
 * (T739, owner 2026-11-14 "for android too.") Android: the FCM device token through expo-notifications — the same seam
 * as the iPhone's. FCM needs the owner's Firebase project file (`google-services.json`); until a build carries it and
 * says `EXPO_PUBLIC_FCM=1` (app.config.js wires the file and refuses the flag without it), this is null: no token is
 * asked, no push task is registered, and the phone checks on its WorkManager schedule exactly as before.
 * Firebase is in the app for push delivery only: no Analytics (app.config.js turns its collection off in the manifest).
 */
import * as Notifications from "expo-notifications";
import type { PushSeam } from "@app/data";

/** the build carries google-services.json (app.config.js) */
const FCM_ON = process.env.EXPO_PUBLIC_FCM === "1";

export const pushSeam: PushSeam | null = !FCM_ON ? null : {
  platform: "android",
  env: "production",
  token: async () => String((await Notifications.getDevicePushTokenAsync()).data),
  onToken: (on) => {
    const sub = Notifications.addPushTokenListener((t) => on(String(t.data)));
    return () => sub.remove();
  },
};
