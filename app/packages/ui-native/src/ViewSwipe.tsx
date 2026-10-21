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
 */
import { createContext, useMemo, useRef } from "react";
import { Animated, useWindowDimensions } from "react-native";
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
  // the edge zones are the SCREEN's (the system's edge gestures), so a start is measured against the window
  const width = useRef(0);
  width.current = useWindowDimensions().width;
  const gesture = useMemo(() => {
    let live = false;
    const home = () => Animated.spring(x, { toValue: 0, useNativeDriver: true, bounciness: 4 }).start();
    return Gesture.Pan().runOnJS(true).enabled(swipe.enabled)
      .activeOffsetX([-15, 15]).failOffsetY([-12, 12])
      .onBegin((e) => { live = swipe.starts(e.absoluteX, width.current); })
      .onUpdate((e) => { if (live) x.setValue(swipe.offset(e.translationX)); })
      .onEnd((e) => { if (live) swipe.release(e.translationX, e.translationY, e.velocityX); })
      .onFinalize(() => { live = false; home(); })
      .withTestId(testId);
  }, [swipe, x, testId]);
  return (
    <GestureDetector gesture={gesture} touchAction="pan-y">
      <Animated.View style={[fill ? { flex: 1 } : null, { transform: [{ translateX: x }] }, style]}>
        {children}
      </Animated.View>
    </GestureDetector>
  );
}

/**
 * The water's views as the Shell has them, for the header of whichever screen is drawn (`FaceBar` reads it): the
 * views on offer in strip order, the one on screen, how to move, where back goes and what it is called, and the swipe
 * for the header (`swipe`) and for the view's body (`bodySwipe`: the Regulations view only — Conditions and Map hold
 * maps that pan and charts read with a finger). One of the two is set, never both: nested detectors would race.
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
