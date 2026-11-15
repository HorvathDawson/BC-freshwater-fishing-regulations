/**
 * (T723 F7) The modal edge-back's drag, as React Native responder callbacks (`ModalBack.native.tsx` uses it on iOS; a
 * plain module so the web and Android halves of `ModalBack` keep one export surface). The decisions are `@app/ui`
 * `useEdgeBack`'s: where a drag starts, how far, a flick.
 */
import { Animated, type PanResponderCallbacks } from "react-native";
import type { EdgeBackDrag } from "@app/ui";
import { NATIVE_DRIVER } from "./motion";

/** The responder's callbacks over `drag` (`x`: the content's sideways offset while dragged). */
export function modalEdgeResponder(drag: EdgeBackDrag, x: Animated.Value): PanResponderCallbacks {
  const home = () => Animated.spring(x, { toValue: 0, useNativeDriver: NATIVE_DRIVER, bounciness: 0 }).start();
  return {
    // sideways from the edge zone, past the slop: the same thresholds as `EdgeBack` (activeOffsetX 10, failOffsetY 20)
    onMoveShouldSetPanResponderCapture: (_e, g) =>
      drag.enabled && drag.starts(g.x0) && g.dx > 10 && Math.abs(g.dy) < 20,
    onPanResponderMove: (_e, g) => x.setValue(drag.offset(g.dx)),
    // the responder's velocity is px per ms; the decision takes px per s
    onPanResponderRelease: (_e, g) => { if (drag.release(g.dx, g.vx * 1000)) x.setValue(0); else home(); },
    onPanResponderTerminate: home,
    onPanResponderTerminationRequest: () => false,
  };
}
