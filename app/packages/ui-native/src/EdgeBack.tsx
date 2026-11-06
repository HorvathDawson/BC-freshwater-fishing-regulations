/**
 * THE LEFT-EDGE BACK SWIPE (G1) — the prototype's, and only where the platform has no system back of its own: iOS.
 * On Android the system's edge gesture owns that strip (and reaches the same back through `BackHandler`); on the web
 * the browser owns it. `drag.available` says which (`@app/ui` `useEdgeBack`, from `@app/core` `edgeBackEnabled`).
 *
 * Measures and draws only: the view follows the finger, and on release `drag.release` decides (past 80 px or a
 * flick) and calls the one back. Everything else springs home.
 */
import { useMemo, useRef } from "react";
import { Animated } from "react-native";
import { GestureDetector, usePanGesture, type PanGestureConfig } from "react-native-gesture-handler";
import type { EdgeBackDrag } from "@app/ui";
import { NATIVE_DRIVER } from "./motion";

export function EdgeBack({ drag, children }: { drag: EdgeBackDrag; children: React.ReactNode }) {
  // `available` is fixed per platform, so this never remounts the screen underneath. (T250) The recogniser lives in its
  // own component: RNGH 3's hook gestures exist from the hook call, so where there is no edge swipe there is no handler.
  if (!drag.available) return <>{children}</>;
  return <EdgePan drag={drag}>{children}</EdgePan>;
}

function EdgePan({ drag, children }: { drag: EdgeBackDrag; children: React.ReactNode }) {
  const x = useRef(new Animated.Value(0)).current;
  // (T250) RNGH 3's hook API: one config object (memoised: a new object reconfigures the native handler), callbacks on JS
  const config = useMemo((): PanGestureConfig => {
    let live = false;
    const home = () => Animated.spring(x, { toValue: 0, useNativeDriver: NATIVE_DRIVER, bounciness: 0 }).start();
    return {
      runOnJS: true, disableReanimated: true, enabled: drag.enabled, testID: "edge-back",
      // only a press that starts in the edge zone is ours: everything else (the map's pan) never sees this handler
      hitSlop: { left: 0, width: drag.zone },
      activeOffsetX: 10, failOffsetY: [-20, 20],
      onBegin: (e) => { live = drag.starts(e.absoluteX); },
      onUpdate: (e) => { if (live) x.setValue(drag.offset(e.translationX)); },
      // `canceled` when the system cancelled the swipe after it went active: that is not a release
      onDeactivate: (e) => {
        if (live && !e.canceled && drag.release(e.translationX, e.velocityX)) x.setValue(0);
        else home();
        live = false;
      },
      onFinalize: () => { if (live) home(); live = false; },
    };
  }, [drag, x]);
  const gesture = usePanGesture(config);
  return (
    <GestureDetector gesture={gesture}>
      <Animated.View style={{ flex: 1, transform: [{ translateX: x }] }}>{children}</Animated.View>
    </GestureDetector>
  );
}
