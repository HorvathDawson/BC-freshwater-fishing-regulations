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
 * (refinement pass, owner 2026-10-22) HOW A BOTTOM SHEET COMES AND GOES — the sheet and its scrim are two things that move
 * differently. The owner: "when menus close the screen opacity slides down with the menu instead of staying in the back
 * then disappearing after menu slides down". The Modal's own `slide` moved its whole content, scrim included, so the dim
 * travelled down the screen as a band. Now the SHEET slides and the SCRIM stays where it is and only fades: in, alongside
 * the slide up; out, holding until the sheet is nearly gone and ending after it has left (the dim is the last to go).
 * Reduced motion: nothing slides — the sheet and the scrim fade together, quickly.
 */
export const SHEET_MOTION = {
  /** the sheet rising into place (ease-out: it arrives gently) */
  inMs: 260,
  /** the scrim fading in, from the press, alongside the rise */
  fadeInMs: 200,
  /** the sheet leaving (ease-in: it accelerates away) */
  outMs: 220,
  /** the scrim holds until the sheet is nearly gone… */
  fadeOutDelayMs: 160,
  /** …then fades: gone 340 ms after the press, 120 ms after the sheet */
  fadeOutMs: 180,
  /** reduced motion: the whole thing fades, in or out */
  reducedMs: 120,
} as const;

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
