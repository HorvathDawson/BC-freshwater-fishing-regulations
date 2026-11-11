/**
 * (T566) THE KEY UNDER THE MAP, as the map last drew it (`@app/ui` `HeldLookStore`): only this subtree re-renders when the
 * map says a new colouring is drawn, so the key changes with the map instead of after a whole-screen render.
 */
import type { ReactNode } from "react";
import { useHeldLook, type HeldLookStore } from "@app/ui";

export function HeldKey<T>({ store, children }: { store: HeldLookStore<T>; children: (shown: T) => ReactNode }) {
  const { shown } = useHeldLook(store);
  return <>{children(shown)}</>;
}
