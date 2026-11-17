/**
 * (T814, owner 2026-11-17) THE COHO LOADER, on a phone: the owner's two-block linocut spawning coho, swimming two tail
 * beats and gliding, on the start / download / setup screen and the applying curtain. The circling `FishSpinner` stays
 * for every other wait.
 *
 * NO PER-FRAME JS, as the circling fish: the 80 frames are baked into one sprite sheet (`pnpm coho:build`, the owner's
 * `tools/fish-loader/fish_loader.py`), and the sheet is paged behind a clip by two transforms off ONE `Animated.Value` on
 * the platform driver — `translateX` steps every frame and wraps each row, `translateY` steps a row (`spriteGridStaircase`).
 * Kept running by `usePlatformLoops` (app-active, first layout, a dropped loop). It always moves: owner 2026-11-17, "we
 * will ignore reduced motion for this".
 *
 * Two colours, so the sheet is PRINTED in them (`palette.coho.ink` / `.red`, one sheet per pair), not tinted; the ground
 * behind it (`palette.coho.ground`) is a view.
 */
import { useRef } from "react";
import { Animated, View } from "react-native";
import { spriteGridStaircase } from "@app/ui";
import { SPINNER_WORDS } from "@app/core";
import { COHO } from "./coho.generated";
import { COHO_GROUND_PAD, COHO_WIDTH, cohoHeight, cohoSheet } from "./coho";
import { usePlatformLoops } from "./platformLoops";
import type { Palette } from "./theme";

export function CohoLoader({ palette, width = COHO_WIDTH, label = SPINNER_WORDS.loading }:
  { palette: Palette; width?: number; label?: string }) {
  const t = useRef(new Animated.Value(0)).current;
  const onLayout = usePlatformLoops([[t, COHO.loopMs]]);
  const height = cohoHeight(width);
  const step = spriteGridStaircase(COHO.frames, COHO.columns, width, height);
  const translateX = t.interpolate(step.x);
  const translateY = t.interpolate(step.y);

  return (
    <View accessibilityRole="progressbar" accessibilityLabel={label} onLayout={onLayout} style={{ width, height }}>
      <View testID="coho-ground" style={{ position: "absolute", top: -COHO_GROUND_PAD, bottom: -COHO_GROUND_PAD,
                                          left: -COHO_GROUND_PAD, right: -COHO_GROUND_PAD,
                                          backgroundColor: palette.coho.ground }} />
      <View style={{ width, height, overflow: "hidden" }}>
        <Animated.Image
          testID="coho-sheet"
          source={cohoSheet(palette)}
          resizeMode="stretch"
          fadeDuration={0}
          style={{ width: width * COHO.columns, height: height * COHO.rows, transform: [{ translateX }, { translateY }] }}
        />
      </View>
    </View>
  );
}
