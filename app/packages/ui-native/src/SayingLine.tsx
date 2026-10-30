/**
 * (T298, owner 2026-10-29) THE FISH SAYINGS under the first download's bars: a short line that takes turns every
 * `SAYING_MS` (`@app/core` `DOWNLOAD_SAYINGS`). With motion, each new line fades in (opacity only, native driver); with
 * the OS's reduce-motion on, it just swaps — no animation at all. Starts at a random saying so two first runs differ.
 */
import { useEffect, useRef, useState } from "react";
import { Animated, type TextStyle } from "react-native";
import { SAYING_MS, sayingAt } from "@app/core";
import { NATIVE_DRIVER, useReducedMotion } from "./motion";

export function SayingLine({ style, testID = "boot-saying", start }: { style?: TextStyle; testID?: string; start?: number }) {
  const still = useReducedMotion();
  const [n, setN] = useState(() => start ?? Math.floor(Math.random() * 1e6));
  const fade = useRef(new Animated.Value(1)).current;
  // one turn every SAYING_MS: a timer per line, never a frame loop
  useEffect(() => {
    const t = setTimeout(() => setN((x) => x + 1), SAYING_MS);
    return () => clearTimeout(t);
  }, [n]);
  useEffect(() => {
    if (still) { fade.setValue(1); return; }
    fade.setValue(0);
    Animated.timing(fade, { toValue: 1, duration: 400, useNativeDriver: NATIVE_DRIVER }).start();
  }, [n, still, fade]);
  return (
    <Animated.Text testID={testID} accessibilityLiveRegion="none" style={[style, { opacity: fade }]}>
      {sayingAt(n)}
    </Animated.Text>
  );
}
