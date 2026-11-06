import type { PanGestureConfig } from "react-native-gesture-handler";

/** The browser has no cancel to beat: the pan activates on its own offsets (the phone twin activates by hand, T300). */
export function activateByHand(_held: () => boolean): Partial<PanGestureConfig> { return {}; }
