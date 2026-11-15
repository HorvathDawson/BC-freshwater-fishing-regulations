/**
 * (T739) The shared declaration for `./pushToken`: the platform's push token, or null where this build has no push.
 *   iOS      `pushToken.ios.ts`: APNs through expo-notifications' device token (`aps-environment` from its config
 *            plugin; "sandbox" unless the build says `EXPO_PUBLIC_APNS_ENV=production`, as the TestFlight build does).
 *   Android  `pushToken.android.ts`: FCM through the same call — null (no push, no token asked, no task registered)
 *            until the build carries the owner's `google-services.json` and says `EXPO_PUBLIC_FCM=1` (app.config.js).
 * Bundlers pick by platform extension, TypeScript does not.
 */
import type { PushSeam } from "@app/data";

export declare const pushSeam: PushSeam | null;
