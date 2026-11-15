/** (T739) iPhone: the APNs device token (expo-notifications), and its environment. See `pushToken.d.ts`. */
import * as Notifications from "expo-notifications";
import type { PushSeam } from "@app/data";

export const pushSeam: PushSeam | null = {
  platform: "ios",
  // a simulator / development / local build is signed with the development entitlement; the TestFlight / App Store
  // export is production (deploy/staging/build-ios-testflight.sh sets it)
  env: process.env.EXPO_PUBLIC_APNS_ENV === "production" ? "production" : "sandbox",
  token: async () => String((await Notifications.getDevicePushTokenAsync()).data),
  onToken: (on) => {
    const sub = Notifications.addPushTokenListener((t) => on(String(t.data)));
    return () => sub.remove();
  },
};
