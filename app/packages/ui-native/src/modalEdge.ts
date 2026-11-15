/**
 * (T723 F7) The modal edge-back's drag, as React Native responder callbacks for the edge strip `ModalBack.native.tsx`
 * lays over a modal on iOS (a plain module, so ModalBack's web and native halves keep one export surface). The decisions
 * are `@app/ui` `useEdgeBack`'s: where a drag starts, how far, a flick.
 */
import { Animated, type PanResponderCallbacks } from "react-native";
import type { EdgeBackDrag } from "@app/ui";
import { GUTTER } from "./theme";
import { NATIVE_DRIVER } from "./motion";

/**
 * How wide the strip is: the page gutter, so no control under it loses a tap (content starts at the gutter), and inside
 * `EDGE_BACK.zone`. A thumb's back swipe starts at the bezel.
 */
export const MODAL_EDGE = GUTTER;

/** The strip's callbacks over `drag` (`x`: the content's sideways offset while dragged). */
export function modalEdgeResponder(drag: EdgeBackDrag, x: Animated.Value): PanResponderCallbacks {
  const home = () => Animated.spring(x, { toValue: 0, useNativeDriver: NATIVE_DRIVER, bounciness: 0 }).start();
  return {
    // every press on the strip is the edge's (the strip is only where the zone is)
    onStartShouldSetPanResponder: (e) => drag.enabled && drag.starts(e.nativeEvent.pageX),
    onPanResponderMove: (_e, g) => x.setValue(drag.offset(g.dx)),
    // the responder's velocity is px per ms; the decision takes px per s
    onPanResponderRelease: (_e, g) => { if (drag.release(g.dx, g.vx * 1000)) x.setValue(0); else home(); },
    onPanResponderTerminate: home,
    onPanResponderTerminationRequest: () => false,
  };
}
