/**
 * (T1164) THE LAST RESORT: the whole app under one `ScreenBoundary` ("app"). Each screen has its own inside the shell
 * (`Shell.tsx` `screen(…)`), so a screen's fault leaves the tab bar working; this one catches what those cannot: a
 * fault in the shell's own chrome (the bars, the rail, the sheets), the boot screen, or `App`'s own render. It shows
 * "The app hit a problem and could not be drawn." with Try again, and the fault is reported (`reportFailure`).
 * Drawn in the light palette: the app's own theme is read inside the tree that may be the one that failed.
 */
import type { ComponentType } from "react";
import { ScreenBoundary, THEMES } from "@app/ui-native";

export function withRootBoundary<P extends object>(Root: ComponentType<P>): ComponentType<P> {
  return function RootBoundary(props: P) {
    return <ScreenBoundary screen="app" resetKey="app" palette={THEMES.light}><Root {...props} /></ScreenBoundary>;
  };
}
