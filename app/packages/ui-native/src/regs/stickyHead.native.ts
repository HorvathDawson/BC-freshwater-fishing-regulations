import { useState, useSyncExternalStore } from "react";
import { Animated, View } from "react-native";
import type { Palette } from "../theme";
import type { StickyHead, StickyBox } from "./stickyHead";
import type { StickyPin } from "./useAnchors";

const NO_SUB = () => () => {};

/**
 * (owner 2026-10-27) A phone has no container-bounded sticky, so the size header is translated by the page's scroll (an
 * Animated value set by the scroll handler: no React render per frame), from where it would stick to where the box's
 * content ends. The web twin (stickyHead.web.ts) uses CSS `position: sticky`.
 */
export function useStickyHead({ stick, pinned, boxId, stackTop, palette, box: geom }: { stick: StickyPin | null; pinned: boolean;
                                boxId: string; stackTop: number; palette: Palette; box: StickyBox }): StickyHead {
  const box = useSyncExternalStore(stick?.store.subscribe ?? NO_SUB, () => (pinned ? stick!.store.getBoxes().get(boxId) : undefined), () => undefined);
  const [headH, setHeadH] = useState(0);
  let slide: Animated.AnimatedInterpolation<number> | undefined;
  if (pinned && stick && box && headH > 0) {
    const from = box.top + geom.border - stackTop, travel = box.height - 2 * geom.border - geom.padBottom - headH;
    if (travel > 0) slide = stick.scrollY.interpolate({ inputRange: [from, from + travel], outputRange: [0, travel], extrapolate: "clamp" });
  }
  return {
    style: !pinned ? undefined
      : { zIndex: 1, backgroundColor: palette.card, borderBottomWidth: 1, borderBottomColor: palette.line2, ...(slide ? { transform: [{ translateY: slide }] } : null) },
    View: pinned ? Animated.View : View,
    onLayout: pinned ? (e) => setHeadH(e.nativeEvent.layout.height) : undefined,
  };
}
