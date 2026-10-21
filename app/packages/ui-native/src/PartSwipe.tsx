/**
 * THE PART SWIPE (G1, owner "maybe" — the feel is reviewed before it is final). A sideways swipe on a water's view
 * steps to the next or previous part: finger left is the next part upstream, finger right the next part downstream,
 * the water map's ▲ Upstream / ▼ Downstream order. At either end the view gives a little and springs back; no wrap.
 *
 * Measures and draws only — `swipe` (`@app/ui` `usePartSwipe`) decides where a swipe may start, how far the view
 * follows and whether a release steps. Vertical scrolling is never taken: the recogniser fails on a vertical move
 * (direction lock) and, on the web, leaves `pan-y` to the browser. On the Map view wrap the part header or strip, not
 * the canvas: the canvas pans.
 */
import { useMemo, useRef } from "react";
import { Animated, useWindowDimensions } from "react-native";
import { Gesture, GestureDetector } from "react-native-gesture-handler";
import type { PartSwipe as PartSwipeState } from "@app/ui";

export function PartSwipe({ swipe, children, style }: {
  swipe: PartSwipeState; children: React.ReactNode;
  style?: import("react-native").StyleProp<import("react-native").ViewStyle>;
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
      .withTestId("part-swipe");
  }, [swipe, x]);
  return (
    <GestureDetector gesture={gesture} touchAction="pan-y">
      <Animated.View style={[{ flex: 1, transform: [{ translateX: x }] }, style]}>
        {children}
      </Animated.View>
    </GestureDetector>
  );
}
