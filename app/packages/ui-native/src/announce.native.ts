import { AccessibilityInfo } from "react-native";

/** The phone's screen reader says `text` now (the web twin is a no-op: a live region says it there). */
export function announce(text: string): void { AccessibilityInfo.announceForAccessibility?.(text); }
