/**
 * THE NATIVE DRIVER, WHERE THERE IS ONE. iOS and Android run an `Animated` loop on the UI thread when asked
 * (`useNativeDriver: true`); react-native-web has no native animated module, and asking for it there logs
 * "useNativeDriver is not supported … Falling back to JS-based animation" on every spring. So the platform decides:
 * the driver on a phone, plain JS on the web (where the browser composites transforms anyway).
 */
import { useEffect, useState } from "react";
import { AccessibilityInfo, Platform } from "react-native";

export const NATIVE_DRIVER: boolean = Platform.OS !== "web";

/**
 * (T48) THE OS "REDUCE MOTION" SETTING, live: what slides or scales cross-fades instead while it is on. False until the
 * platform answers, and false where it cannot (a test, an old browser) — motion is the default, never assumed off.
 */
export function useReducedMotion(): boolean {
  const [on, setOn] = useState(false);
  useEffect(() => {
    let live = true;
    AccessibilityInfo.isReduceMotionEnabled?.().then((v) => { if (live) setOn(v); }, () => {});
    const sub = AccessibilityInfo.addEventListener?.("reduceMotionChanged", (v: boolean) => { if (live) setOn(v); });
    return () => { live = false; sub?.remove?.(); };
  }, []);
  return on;
}
