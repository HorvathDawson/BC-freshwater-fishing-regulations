/**
 * (T723 F7) The phone's: see `ModalBack.d.ts`. `useEdgeBack(back, os, depth)` says whether the platform has an edge swipe
 * (`@app/core` `edgeBackEnabled`: iOS only) and decides it (where it starts, how far, a flick); where there is none
 * (Android, whose system back reaches the modal through `onRequestClose`) the children are drawn untouched.
 *
 * React Native's own responder (`PanResponder`), not a gesture-handler recogniser as the Shell's `EdgeBack`: inside a
 * Modal (a window of its own) RNGH attached its root recogniser late, once to a 0 × 0 view, and the simulator's edge
 * drags never reached it (T723 run a1); the responder system is the Modal's own touch handling. The drag is taken only
 * when it starts in the edge zone and moves sideways (capture on move), so taps and vertical scrolls are left alone.
 */
import { useCallback, useMemo, useRef, type ReactNode } from "react";
import { Animated, PanResponder, Platform, type PanResponderCallbacks } from "react-native";
import { useEdgeBack, type EdgeBackDrag } from "@app/ui";
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

export function ModalBack({ onBack, children }: { onBack: () => void; children: ReactNode }): ReactNode {
  // depth 1: the modal itself is the one step back; `onBack` closes it and nothing under it
  const back = useCallback(() => { onBack(); return true; }, [onBack]);
  const drag = useEdgeBack(back, Platform.OS, 1);
  const x = useRef(new Animated.Value(0)).current;
  const responder = useMemo(() => PanResponder.create(modalEdgeResponder(drag, x)), [drag, x]);
  if (!drag.available) return <>{children}</>;
  return (
    <Animated.View testID="modal-edge-back" style={{ flex: 1, transform: [{ translateX: x }] }} {...responder.panHandlers}>
      {children}
    </Animated.View>
  );
}
