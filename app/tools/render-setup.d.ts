/**
 * jsdom has no `matchMedia`, and react-native-web reads it AT MODULE LOAD to answer
 * `AccessibilityInfo.isReduceMotionEnabled()`. With it missing that call resolves to
 * `true` — so without this file every component under test would render in its
 * reduced-motion state and the animated path would never be exercised.
 *
 * The stub reads a mutable flag through a getter rather than capturing a boolean, because
 * react-native-web holds one MediaQueryList for the life of the module. That lets a test
 * flip `reduceMotion` and have the next query see it.
 */
declare global {
    var reduceMotion: boolean;
}
/** Flip the preference and tell everyone listening, the way a real browser would. */
export declare function setReduceMotion(on: boolean): void;
