/**
 * (T723 F7) The phone's: see `ModalBack.d.ts`. `useEdgeBack(back, os, depth)` says whether the platform has an edge swipe
 * (`@app/core` `edgeBackEnabled`: iOS only) and decides it (where it starts, how far, a flick); where there is none
 * (Android, whose system back reaches the modal through `onRequestClose`) the children are drawn untouched.
 *
 * A strip down the left edge, over the content, owns the drag with React Native's own responder. Two ways that did not
 * work inside a Modal on the simulator (T723 runs a1, a3): a gesture-handler recogniser (RNGH attached its root once to
 * a 0 × 0 view) and a responder on the whole content (a menu page is a native scroll view, whose own pan took the drag
 * and cancelled ours). A press on the strip never reaches the scroll view under it.
 */
import { useCallback, useMemo, useRef, type ReactNode } from "react";
import { Animated, PanResponder, Platform, View } from "react-native";
import { useEdgeBack } from "@app/ui";
import { MODAL_EDGE, modalEdgeResponder } from "./modalEdge";

export function ModalBack({ onBack, children }: { onBack: () => void; children: ReactNode }): ReactNode {
  // depth 1: the modal itself is the one step back; `onBack` closes it and nothing under it
  const back = useCallback(() => { onBack(); return true; }, [onBack]);
  const drag = useEdgeBack(back, Platform.OS, 1);
  const x = useRef(new Animated.Value(0)).current;
  const responder = useMemo(() => PanResponder.create(modalEdgeResponder(drag, x)), [drag, x]);
  if (!drag.available) return <>{children}</>;
  return (
    <View style={{ flex: 1 }}>
      <Animated.View style={{ flex: 1, transform: [{ translateX: x }] }}>{children}</Animated.View>
      <View testID="modal-edge-back" {...responder.panHandlers}
            style={{ position: "absolute", left: 0, top: 0, bottom: 0, width: MODAL_EDGE }} />
    </View>
  );
}
