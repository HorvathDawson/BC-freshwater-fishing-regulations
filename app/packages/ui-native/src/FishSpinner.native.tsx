/**
 * The loading indicator: a fish swimming a slow orbit, trailing bubbles.
 *
 * NO PER-FRAME JS. That is the whole design.
 *
 * `archive/webapp/src/components/FishLoader.tsx` drove a canvas from
 * `requestAnimationFrame`, so every frame was JS work on the same thread that is busy doing
 * the thing you are waiting for. A loader that freezes exactly when there is something to
 * wait for is worse than no loader: it reads as a hung app.
 *
 * A platform animation driver can run `transform` and `opacity` without JS, and nothing
 * else. So the animation is split along that line:
 *
 *   the orbit        — a rotation, so it stays live and stays resolution-independent
 *   the body wave,   — shape changes, which `transform` cannot express, so they are
 *   fins, tail,        rendered ahead of time by `pnpm spinner` into a sprite strip and
 *   bubbles            paged through by translating that strip behind a clip
 *
 * Both are one `Animated.Value` on `useNativeDriver`. Parsing a 10 MB bundle on the JS
 * thread cannot touch either.
 *
 * ON WEB THAT IS ONLY HALF TRUE, and the browser says so out loud: react-native-web has no
 * native animated module, so it warns and falls back to a JS-driven loop. The baked frames
 * still buy the larger half — a spine, two fins, a curved tail and five bubbles are no
 * longer recomputed per frame, only a translate and a rotate are — but the transform
 * interpolation itself is JS there. Closing that gap means a `FishSpinner.web.tsx` running
 * a CSS `steps()` keyframe over the same strip, which the sprite is already shaped for.
 * Not done; see RENDER-TESTS.md.
 *
 * The strip is grayscale+alpha and tinted at runtime, so one baked file serves the light,
 * dark and colour-blind palettes and cannot drift from them.
 *
 * The fish is `palette.live` — the water colour — not `accent`, which in this app means
 * "you chose this" and belongs on controls. The archive's loader was steel blue for the
 * same reason: a loading fish is about water, not about selection.
 *
 * TWO colours, though, and only one of them is in the strip. The orbit ring is drawn here
 * as a bordered view in `palette.line`. Baking it made everything the fish's colour, and a
 * monochrome fish-and-ring is most of why the first version read worse than the canvas
 * loader it replaced — that one was a steel-blue fish on a pale grey orbit.
 */
import { useEffect, useRef, useState } from "react";
import { AccessibilityInfo, Animated, Easing, View } from "react-native";
import { spriteStaircase } from "@app/ui";
import { FISH_SPRITE } from "./fish-sprite.generated";
import type { Palette } from "./theme";

export function FishSpinner({ palette, size = 96, label = "Loading", colour }:
  { palette: Palette; size?: number; label?: string; colour?: string }) {
  const phase = useRef(new Animated.Value(0)).current;
  const orbit = useRef(new Animated.Value(0)).current;
  const [still, setStill] = useState(false);

  useEffect(() => {
    // A spinner is the one thing on screen that always moves, so it is the one thing
    // "reduce motion" is most likely to have been turned on for.
    let live = true;
    AccessibilityInfo.isReduceMotionEnabled().then((on) => { if (live) setStill(on); });
    // react-native-web returns UNDEFINED here when the browser has no matchMedia, so the
    // optional call is load-bearing, not defensive noise.
    const sub = AccessibilityInfo.addEventListener("reduceMotionChanged", setStill);
    return () => { live = false; sub?.remove(); };
  }, []);

  useEffect(() => {
    if (still) return;
    const loop = (value: Animated.Value, duration: number) =>
      Animated.loop(Animated.timing(value, {
        toValue: 1, duration, easing: Easing.linear, useNativeDriver: true,
      }));
    const a = loop(phase, FISH_SPRITE.cycleMs);
    const b = loop(orbit, FISH_SPRITE.orbitMs);
    a.start();
    b.start();
    return () => { a.stop(); b.stop(); };
  }, [phase, orbit, still]);

  const rotate = orbit.interpolate({ inputRange: [0, 1], outputRange: ["0deg", "360deg"] });
  const translateX = phase.interpolate(spriteStaircase(FISH_SPRITE.frames, size));

  const ring = size * FISH_SPRITE.ring.radius * 2;

  return (
    <View accessibilityRole="progressbar" accessibilityLabel={label}
          style={{ width: size, height: size, alignItems: "center", justifyContent: "center" }}>
      {/* Behind, and not rotated — a circle looks identical at every angle, so spinning it
          would be pure cost. */}
      <View style={{ position: "absolute", width: ring, height: ring, borderRadius: ring / 2,
                     borderWidth: Math.max(size * FISH_SPRITE.ring.width, 1),
                     borderColor: palette.line }} />
      <Animated.View style={{ position: "absolute", width: size, height: size,
                              overflow: "hidden", transform: still ? [] : [{ rotate }] }}>
        <Animated.Image
          testID="fish-sprite"
          source={{ uri: FISH_SPRITE.uri }}
          tintColor={colour ?? palette.live}
          resizeMode="stretch"
          style={{ width: size * FISH_SPRITE.frames, height: size,
                   transform: still ? [] : [{ translateX }] }}
        />
      </Animated.View>
    </View>
  );
}
