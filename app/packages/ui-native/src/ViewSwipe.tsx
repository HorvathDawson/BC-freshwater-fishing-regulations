/**
 * THE VIEW SWIPE (G1, owner 2026-10-21: "a sideways swipe moves between a water's views"). On a water, finger left
 * brings in the view written to the right in its strip (Regulations · Conditions · Map), finger right the one to the
 * left. At either end the view gives a little and springs back; no wrap.
 *
 * Measures and draws only — `swipe` (`@app/ui` `useViewSwipe`) decides where a swipe may start (never in the 24 px
 * edge zones), how far the view follows and whether a release moves. Vertical scrolling is never taken: the recogniser
 * fails on a vertical move (direction lock) and, on the web, leaves `pan-y` to the browser.
 *
 * WHERE IT LISTENS is the caller's: a map canvas pans, so on the Map view only the header (the back row and the view
 * strip) takes the swipe; `WaterViews` carries the swipe to the header (`FaceBar`) of whatever screen is drawn.
 *
 * SHIELDS (`useSwipeShield`): on a body that also holds things a finger works on (the Conditions view's route map, gauge
 * minimap and chart), those things wear a shield. A press that lands on one claims the drag for it: the swipe does not
 * follow and does not move, however sideways the drag. The text, the estimate and the reading around them still swipe.
 */
import { createContext, useContext, useMemo, useRef } from "react";
import { Animated, View, useWindowDimensions } from "react-native";
import { Gesture, GestureDetector } from "react-native-gesture-handler";
import type { WaterView } from "@app/core";
import type { ViewSwipe as ViewSwipeState } from "@app/ui";

export function ViewSwipe({ swipe, children, style, testId = "view-swipe", fill = true }: {
  swipe: ViewSwipeState; children: React.ReactNode;
  /** Takes the room it is given (a view's body); false: as tall as its content (a header). */
  fill?: boolean;
  style?: import("react-native").StyleProp<import("react-native").ViewStyle>;
  /** Two detectors on one screen (the header and a body) are told apart by this in tests. */
  testId?: string;
}) {
  const x = useRef(new Animated.Value(0)).current;
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
    const home = () => Animated.spring(x, { toValue: 0, useNativeDriver: true, bounciness: 4 }).start();
    return Gesture.Pan().runOnJS(true).enabled(swipe.enabled)
      .activeOffsetX([-15, 15]).failOffsetY([-12, 12])
      .onBegin((e) => { begun = Date.now(); live = swipe.starts(e.absoluteX, width.current); })
      .onUpdate((e) => { if (live && held()) live = false; if (live) x.setValue(swipe.offset(e.translationX)); })
      .onEnd((e) => { if (live && !held()) swipe.release(e.translationX, e.translationY, e.velocityX); })
      .onFinalize(() => { live = false; home(); })
      .withTestId(testId);
  }, [swipe, x, testId]);
  return (
    <GestureDetector gesture={gesture} touchAction="pan-y">
      <Animated.View style={[fill ? { flex: 1 } : null, { transform: [{ translateX: x }] }, style]}>
        <ShieldContext.Provider value={shield}>{children}</ShieldContext.Provider>
      </Animated.View>
    </GestureDetector>
  );
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
 * for the header (`swipe`) and for the view's body (`bodySwipe`: Regulations, and Conditions with its maps and chart
 * shielded — see useSwipeShield; Map has none, its canvas is the body). Where the header sits INSIDE the body
 * (Regulations) only `bodySwipe` is set: nested detectors would race. On Conditions the header is above the panel's
 * ScrollView, a sibling, so both are set.
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
  swipe: ViewSwipeState | null;
  bodySwipe: ViewSwipeState | null;
}

export const WaterViewsContext = createContext<WaterViews | null>(null);

/** A plain wrapper wearing the shield, for a thing that cannot take the props itself (the chart). */
export function SwipeShield({ children }: { children: React.ReactNode }) {
  return <View {...useSwipeShield()}>{children}</View>;
}
