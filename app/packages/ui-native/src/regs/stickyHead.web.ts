import { View, type ViewStyle } from "react-native";
import type { Palette } from "../theme";
import type { StickyHead, StickyBox } from "./stickyHead";
import type { StickyPin } from "./useAnchors";

// react-native's ViewStyle has no "sticky"; react-native-web passes it through to CSS
const STICKY = { position: "sticky" } as const;

/** (owner 2026-10-27) The web pins the size header with CSS `position: sticky` inside its box (the phone twin translates it). */
export function useStickyHead({ pinned, stackTop, palette }: { stick: StickyPin | null; pinned: boolean; boxId: string;
                                stackTop: number; palette: Palette; box: StickyBox }): StickyHead {
  return {
    style: !pinned ? undefined
      : { ...(STICKY as unknown as ViewStyle), top: stackTop, zIndex: 1, backgroundColor: palette.card, borderBottomWidth: 1, borderBottomColor: palette.line2 },
    View,
    onLayout: undefined,
  };
}
