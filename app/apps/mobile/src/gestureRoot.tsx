/**
 * THE GESTURE ROOT (G1): react-native-gesture-handler's recognisers only work under a `GestureHandlerRootView`, and it
 * has to sit at the very top. Wrapped around the app here (`index.js`) rather than inside `App.tsx`, so the app's own
 * tree does not change shape for it. (A `Modal` is a root of its own and carries one too — `@app/ui-native` `Sheet`.)
 */
import type { ComponentType } from "react";
import { GestureHandlerRootView } from "react-native-gesture-handler";

export function withGestureRoot<P extends object>(App: ComponentType<P>): ComponentType<P> {
  return function GestureRoot(props: P) {
    return (
      <GestureHandlerRootView style={{ flex: 1 }}>
        <App {...props} />
      </GestureHandlerRootView>
    );
  };
}
