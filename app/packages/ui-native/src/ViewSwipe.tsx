/**
 * THE VIEW SWIPE (G1, owner 2026-10-21: "a sideways swipe moves between a water's views"). On a water, finger left
 * brings in the view written to the right in its strip (Regulations · Conditions · Map), finger right the one to the
 * left. At either end the view gives a little and springs back; no wrap.
 *
 * Measures and draws only — `swipe` (`@app/ui` `useViewSwipe`) decides where a swipe may start (never in the 24 px
 * edge zones), how far the view follows and whether a release moves. Vertical scrolling is never taken: the recogniser
 * fails on a vertical move (direction lock) and, on the web, leaves `pan-y` and pinch-zoom to the browser (and the
 * reader's text selection and context menu: see WEB_PAGE_PROPS).
 *
 * WHERE IT LISTENS is the caller's: a map canvas pans, so on the Map view only the header (the back row, the view
 * strip and the name row) takes the swipe; `WaterViews` carries the swipe to the water frame's header (`WaterFrame`,
 * T44) on whatever view is drawn.
 *
 * WHAT FOLLOWS THE FINGER is the view's own content, never what the views share (T44): a `still` detector (the frame's
 * header, the regs header) listens without moving, and every detector inside one `WaterFrame` drives the frame's one
 * offset (`SwipeOffsetContext`), which only the body follows. So a drag on the header slides the body, as a drag on
 * the body does, and the back, the strip and the name row hold still wherever the drag began.
 *
 * SHIELDS (`useSwipeShield`): on a body that also holds things a finger works on (the Conditions view's route map, gauge
 * minimap and chart), those things wear a shield. A press that lands on one claims the drag for it: the swipe does not
 * follow and does not move, however sideways the drag. The text, the estimate and the reading around them still swipe.
 */
import { createContext, useContext, useMemo, useRef, type ComponentProps } from "react";
import { Animated, View, useWindowDimensions } from "react-native";
import { Gesture, GestureDetector } from "react-native-gesture-handler";
import type { WaterView } from "@app/core";
import type { ViewSwipe as ViewSwipeState } from "@app/ui";
import { NATIVE_DRIVER } from "./motion";

export function ViewSwipe({ swipe, children, style, testId = "view-swipe", fill = true, still = false }: {
  swipe: ViewSwipeState; children: React.ReactNode;
  /** Takes the room it is given (a view's body); false: as tall as its content (a header). */
  fill?: boolean;
  /** Listens but does not follow the finger (a header both views draw): it moves the frame's body instead. */
  still?: boolean;
  style?: import("react-native").StyleProp<import("react-native").ViewStyle>;
  /** Two detectors on one screen (the header and a body) are told apart by this in tests. */
  testId?: string;
}) {
  // the frame's one offset when there is a frame (a drag anywhere moves its body); else this detector's own
  const own = useRef(new Animated.Value(0)).current;
  const x = useContext(SwipeOffsetContext) ?? own;
  // when a shielded thing was last pressed (see useSwipeShield) — a drag that began on one is that thing's
  const heldAt = useRef(-Infinity);
  const shield = useRef<Shield>(() => { heldAt.current = Date.now(); }).current;
  // the edge zones are the SCREEN's (the system's edge gestures), so a start is measured against the window
  const width = useRef(0);
  width.current = useWindowDimensions().width;
  const gesture = useMemo(() => {
    let live = false;
    let begun = 0;
    // the shield's press and the recogniser's begin arrive in either order (on the web the detector's own listener runs
    // first, React's after; on a device it is not promised), so a press within a moment of the begin counts
    const held = () => heldAt.current >= begun - SHIELD_MS;
    const home = () => Animated.spring(x, { toValue: 0, useNativeDriver: NATIVE_DRIVER, bounciness: 4 }).start();
    return Gesture.Pan().runOnJS(true).enabled(swipe.enabled)
      .activeOffsetX([-15, 15]).failOffsetY([-12, 12])
      .onBegin((e) => { begun = Date.now(); live = swipe.starts(e.absoluteX, width.current); })
      .onUpdate((e) => { if (live && held()) live = false; if (live) x.setValue(swipe.offset(e.translationX)); })
      // `success` is false when the system cancelled the swipe after it went active: that is not a release
      .onEnd((e, success) => { if (live && success && !held()) swipe.release(e.translationX, e.translationY, e.velocityX); })
      .onFinalize(() => { live = false; home(); })
      .withTestId(testId);
  }, [swipe, x, testId]);
  return (
    <GestureDetector gesture={gesture} {...WEB_PAGE_PROPS}>
      <Animated.View style={[fill ? { flex: 1 } : null, still ? null : { transform: [{ translateX: x }] }, style]}>
        <ShieldContext.Provider value={shield}>{children}</ShieldContext.Provider>
      </Animated.View>
    </GestureDetector>
  );
}

/**
 * WEB: RNGH writes these onto the wrapped view (a whole Regulations or Conditions body) while the swipe is enabled, and
 * its defaults (touch-action "none", user-select "none", the context menu suppressed) would take the reader's
 * pinch-zoom (WCAG 1.4.4) and the selecting and copying of regulation text. The browser keeps vertical scroll and
 * pinch-zoom; the swipe takes only the sideways pan. Ignored on native.
 *
 * "pan-y pinch-zoom" is a valid CSS list, which RNGH's `TouchAction` type (single keywords only) does not spell, so it
 * is widened through string.
 */
export const WEB_PAGE_PROPS = {
  touchAction: "pan-y pinch-zoom" as string as ComponentProps<typeof GestureDetector>["touchAction"],
  userSelect: "auto",
  enableContextMenu: true,
} as const satisfies Partial<ComponentProps<typeof GestureDetector>>;

/** The water frame's one swipe offset: every detector in the frame writes it, only the view's body follows it. */
export const SwipeOffsetContext = createContext<Animated.Value | null>(null);

/** Content that follows the frame's swipe without a detector of its own (the Map view's canvas: the header moves it). */
export function SwipeFollow({ children }: { children: React.ReactNode }) {
  const x = useContext(SwipeOffsetContext);
  return <Animated.View style={[{ flex: 1 }, x ? { transform: [{ translateX: x }] } : null]}>{children}</Animated.View>;
}

type Shield = () => void;
const SHIELD_MS = 400;
const ShieldContext = createContext<Shield | null>(null);

/**
 * Props for the root View of something a finger works on (a map canvas, a chart): a press on it keeps the view swipe
 * off for that drag. Outside a `ViewSwipe` it is nothing. Touch for fingers; pointer for a mouse on the web.
 */
export function useSwipeShield(): { onTouchStart?: () => void; onPointerDown?: () => void } {
  const hold = useContext(ShieldContext);
  return hold ? { onTouchStart: hold, onPointerDown: hold } : {};
}

/**
 * The water's views as the Shell has them, for the header of whichever screen is drawn (`FaceBar` reads it): the
 * views on offer in strip order, the one on screen, how to move, where back goes and what it is called, and the swipe
 * for the header (`swipe`: the water frame's header, T44, on every view) and for the view's body (`bodySwipe`:
 * Regulations, and Conditions with its maps and chart shielded — see useSwipeShield; Map has none, its canvas is the
 * body). The frame's header is a SIBLING of every body, never inside it: nested detectors would race.
 *
 * WHY THE BODY'S DETECTOR IS THE SCREEN'S, inside its ScrollView: a browser decides who owns a touch from `touch-action`
 * on the elements between the finger and the NEAREST SCROLL CONTAINER only. A detector wrapped around a ScrollView is
 * beyond it, so its `pan-y` is ignored and the browser cancels a sideways drag; inside the ScrollView it counts.
 */
export interface WaterViews {
  views: readonly WaterView[];
  on: WaterView;
  onView: (v: WaterView) => void;
  onBack: () => void;
  backLabel: string;
  /**
   * (Owner 2026-10-22 late) The header's ×: leave every water at once for the tab the first was opened from (route
   * `close`), however deep the chain; `closeLabel` says where, for a screen reader (`@app/core` `waterExit`).
   */
  close: () => void;
  closeLabel: string;
  swipe: ViewSwipeState | null;
  bodySwipe: ViewSwipeState | null;
}

export const WaterViewsContext = createContext<WaterViews | null>(null);

/** A plain wrapper wearing the shield, for a thing that cannot take the props itself (the chart). */
export function SwipeShield({ children }: { children: React.ReactNode }) {
  return <View {...useSwipeShield()}>{children}</View>;
}
