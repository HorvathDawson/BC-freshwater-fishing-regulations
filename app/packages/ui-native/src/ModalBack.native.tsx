/**
 * (T723 F7) The phone's: see `ModalBack.d.ts`. `useEdgeBack(back, os, depth)` says whether the platform has an edge swipe
 * (`@app/core` `edgeBackEnabled`: iOS only); where it has none (Android, whose system back reaches the modal through
 * `onRequestClose`) the children are drawn untouched, no gesture root and no recogniser.
 */
import { useCallback, type ReactNode } from "react";
import { Platform } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { useEdgeBack } from "@app/ui";
import { EdgeBack } from "./EdgeBack";

export function ModalBack({ onBack, children }: { onBack: () => void; children: ReactNode }): ReactNode {
  // depth 1: the modal itself is the one step back; `onBack` closes it and nothing under it
  const back = useCallback(() => { onBack(); return true; }, [onBack]);
  const drag = useEdgeBack(back, Platform.OS, 1);
  if (!drag.available) return <>{children}</>;
  // a Modal is its own native root: its gestures need a root view of their own (as Sheet.tsx)
  return <GestureHandlerRootView style={{ flex: 1 }}><EdgeBack drag={drag}>{children}</EdgeBack></GestureHandlerRootView>;
}
