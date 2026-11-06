import type { Animated, View } from "react-native";
import type { Palette } from "../theme";
import type { StickyPin } from "./useAnchors";

/** What the size section's header is drawn with: its style, its view and (native) the layout it measures. */
export interface StickyHead {
  readonly style: object | undefined;
  readonly View: typeof View | typeof Animated.View;
  readonly onLayout: ((e: { nativeEvent: { layout: { height: number } } }) => void) | undefined;
}

/** The box the header pins inside: its border and bottom padding (GroupCard draws it). */
export interface StickyBox { readonly border: number; readonly padBottom: number }

/**
 * (owner 2026-10-27) THE SIZE SECTION'S HEADER PINS while its kinds scroll under it, and goes with the box. The web
 * (`stickyHead.web.ts`): CSS `position: sticky` inside the box. A phone (`stickyHead.native.ts`) has no container-bounded
 * sticky, so the header is translated by the page's scroll (an Animated value, no React render per frame) from where it
 * would stick to where the box's content ends. `pinned` false: a plain header.
 */
export declare function useStickyHead(a: { stick: StickyPin | null; pinned: boolean; boxId: string; stackTop: number;
                                           palette: Palette; box: StickyBox }): StickyHead;
