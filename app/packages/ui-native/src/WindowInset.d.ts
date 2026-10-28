/**
 * (T50) THE SYSTEM BARS' INSETS FOR A SURFACE IN ITS OWN WINDOW. The app's frame (`apps/mobile` App.tsx) owns the insets
 * of the app's window; a `Modal` (a sheet, a menu page) is a window of its own drawn edge-to-edge on Android, so its
 * content went under the 3-button bar. These are the ONLY other readers of insets. `.native.tsx` is the phone's
 * (react-native-safe-area-context, measured natively, no provider needed); `.web.tsx` the browser's (no bars: the minimum).
 */
import type { ReactNode } from "react";
import type { StyleProp, ViewStyle } from "react-native";

/** A spacer at the bottom of a surface: `min` px, or the window's bottom inset where that is taller. */
export declare function BottomInset(props: { min: number }): ReactNode;
/** A full-window surface padded clear of the bars on every edge (a menu page). */
export declare function WindowInsets(props: { style?: StyleProp<ViewStyle>; children?: ReactNode }): ReactNode;
