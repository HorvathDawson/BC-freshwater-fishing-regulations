/**
 * (T723 F7) The modal edge-back's drag, as React Native responder callbacks for the edge strip `ModalBack.native.tsx`
 * lays over a modal on iOS (a plain module, so ModalBack's web and native halves keep one export surface). The decisions
 * are `@app/ui` `useEdgeBack`'s: where a drag starts, how far, a flick.
 */
import { Animated, Dimensions, Easing, type PanResponderCallbacks } from "react-native";
import type { EdgeBackDrag } from "@app/ui";
import { GUTTER } from "./theme";
import { NATIVE_DRIVER } from "./motion";

/**
 * How wide the strip is: the page gutter, so no control under it loses a tap (content starts at the gutter), and inside
 * `EDGE_BACK.zone`. A thumb's back swipe starts at the bezel.
 */
export const MODAL_EDGE = GUTTER;

/**
 * The strip's callbacks over `drag` (`x`: the content's sideways offset while dragged).
 *
 * (T821) A swipe that goes back carries the content on off the right edge while the modal closes. It used to be put back
 * at 0 (`x.setValue(0)`) the moment the back was decided: the page the reader had just swiped away stood whole again for
 * as long as the close took to start and to run (the Modal's own slide, a sheet's leave), the owner's "it flashes the menu
 * back for a second before it goes back" (iPhone build 36). Nothing puts it back: a modal that closes unmounts what is in
 * it (React Native's Modal draws nothing when not visible), so the next open starts from a new offset of 0.
 */
export function modalEdgeResponder(drag: EdgeBackDrag, x: Animated.Value): PanResponderCallbacks {
  const home = () => Animated.spring(x, { toValue: 0, useNativeDriver: NATIVE_DRIVER, bounciness: 0 }).start();
  const away = () => Animated.timing(x, { toValue: Dimensions.get("window").width, duration: 160,
                                         easing: Easing.out(Easing.quad), useNativeDriver: NATIVE_DRIVER }).start();
  return {
    // every press on the strip is the edge's (the strip is only where the zone is)
    onStartShouldSetPanResponder: (e) => drag.enabled && drag.starts(e.nativeEvent.pageX),
    onPanResponderMove: (_e, g) => x.setValue(drag.offset(g.dx)),
    // the responder's velocity is px per ms; the decision takes px per s
    onPanResponderRelease: (_e, g) => { if (drag.release(g.dx, g.vx * 1000)) away(); else home(); },
    onPanResponderTerminate: home,
    onPanResponderTerminationRequest: () => false,
  };
}
