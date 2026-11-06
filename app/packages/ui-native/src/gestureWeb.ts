import type { ComponentProps } from "react";
import type { GestureDetector } from "react-native-gesture-handler";

/** What a detector writes onto its view in a browser: `touch-action`, `user-select`, the context menu. Ignored on native. */
export type DetectorWebProps = Pick<ComponentProps<typeof GestureDetector>, "touchAction" | "userSelect" | "enableContextMenu">;

/**
 * (T250) RNGH 3.3 ON THE WEB: a hook gesture's own config is applied AFTER its detector's props (`useGesture`'s effect
 * runs after the detector's, and the web handler's `setGestureConfig` resets every field first), so a detector's
 * `touchAction` / `userSelect` / `enableContextMenu` were wiped to the defaults ("none": no page scroll, no pinch-zoom, no
 * text selection — gestures.test.tsx caught it). They ride in the gesture's config as well, which the web handler reads
 * on every reconfigure; RNGH passes these keys through (`allowedNativeProps`) though its config type leaves them out.
 */
export function withWebProps<T extends object>(config: T, web: DetectorWebProps): T {
  return { ...config, ...web };
}
