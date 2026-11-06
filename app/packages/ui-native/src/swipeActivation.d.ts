import type { PanGestureConfig } from "react-native-gesture-handler";

/**
 * (T300) How the view swipe's pan goes active (T250: the part of its config that says so). A phone (`swipeActivation.native.ts`): by hand, so a press that began on a
 * shielded thing (`held`), a second finger or a vertical move FAILS it before it takes the touch. The web
 * (`swipeActivation.web.ts`): the recogniser's own offsets, unchanged — the browser has no such cancel.
 */
export declare function activateByHand(held: () => boolean): Partial<PanGestureConfig>;
