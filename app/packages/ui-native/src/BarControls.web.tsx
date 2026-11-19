/** (T909) The browser's bar: no bell, no refresh button. See `BarControls.d.ts`. */
import type { ReactNode } from "react";
import type { Palette } from "./theme";
import type { SyncVM } from "@app/core";

export const BAR_HAS_CONTROLS = false;
export function BarControls(_props: {
  palette: Palette;
  notices: { count: number; label: string; onPress: () => void } | null;
  sync: SyncVM | null;
  onSync?: () => void;
}): ReactNode {
  return null;
}
