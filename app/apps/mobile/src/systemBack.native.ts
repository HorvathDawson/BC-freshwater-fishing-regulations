/**
 * Android's system back — the button and the edge gesture both arrive as `hardwareBackPress` — calls the one back
 * (G1). Returning false at the browse root hands the press back to Android, which leaves the app as usual. iOS has no
 * system back; its left-edge swipe is `@app/ui-native` `<EdgeBack>`.
 */
import { BackHandler } from "react-native";
import type { Route, SystemBack } from "@app/ui";

export const systemBack: SystemBack = (back) => {
  const sub = BackHandler.addEventListener("hardwareBackPress", () => back());
  return { sync: () => {}, dispose: () => sub.remove() };
};

/** (T221) No address bar on a phone: the route has nothing to follow. */
export function routeUrl(_r: Route): void {}
