import type { PanGesture } from "react-native-gesture-handler";

/**
 * (T300) How the view swipe's pan goes active. A phone (`swipeActivation.native.ts`): by hand, so a press that began on a
 * shielded thing (`held`), a second finger or a vertical move FAILS it before it takes the touch. The web
 * (`swipeActivation.web.ts`): the recogniser's own offsets, unchanged — the browser has no such cancel.
 */
export declare function activateByHand(pan: PanGesture, held: () => boolean): void;
