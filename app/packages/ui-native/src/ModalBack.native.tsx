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
import { Animated, PanResponder, Platform } from "react-native";
import { useEdgeBack } from "@app/ui";
import { modalEdgeResponder } from "./modalEdge";

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
