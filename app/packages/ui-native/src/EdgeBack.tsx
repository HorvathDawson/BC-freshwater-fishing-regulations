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
import { Gesture, GestureDetector } from "react-native-gesture-handler";
import type { EdgeBackDrag } from "@app/ui";
import { NATIVE_DRIVER } from "./motion";

export function EdgeBack({ drag, children }: { drag: EdgeBackDrag; children: React.ReactNode }) {
  const x = useRef(new Animated.Value(0)).current;
  const gesture = useMemo(() => {
    let live = false;
    const home = () => Animated.spring(x, { toValue: 0, useNativeDriver: NATIVE_DRIVER, bounciness: 0 }).start();
    return Gesture.Pan().runOnJS(true).enabled(drag.enabled)
      // only a press that starts in the edge zone is ours: everything else (the map's pan) never sees this handler
      .hitSlop({ left: 0, width: drag.zone })
      .activeOffsetX(10).failOffsetY([-20, 20])
      .onBegin((e) => { live = drag.starts(e.absoluteX); })
      .onUpdate((e) => { if (live) x.setValue(drag.offset(e.translationX)); })
      // `success` is false when the system cancelled the swipe after it went active: that is not a release
      .onEnd((e, success) => {
        if (live && success && drag.release(e.translationX, e.velocityX)) x.setValue(0);
        else home();
        live = false;
      })
      .onFinalize(() => { if (live) home(); live = false; })
      .withTestId("edge-back");
  }, [drag, x]);
  // `available` is fixed per platform, so this never remounts the screen underneath
  if (!drag.available) return <>{children}</>;
  return (
    <GestureDetector gesture={gesture}>
      <Animated.View style={{ flex: 1, transform: [{ translateX: x }] }}>{children}</Animated.View>
    </GestureDetector>
  );
}
