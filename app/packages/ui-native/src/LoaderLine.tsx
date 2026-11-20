/**
 * (T852, owner 2026-11-18: "the quote text should be formated in a nicer way too on loader"; he picked setting B from
 * the pictures) THE LINE UNDER THE COHO, set one way wherever the coho waits: a short rule in the coho's own red
 * (`palette.coho.red`), then the line small and muted (`TYPE.small` in Archivo Medium, tracked a hair, `palette.sub`),
 * centred. The applying curtain's line (`ApplyCurtain`), the boot screen's pun and the first install's sayings
 * (`SayingLine`, apps/mobile `BootScreen`) all hang under it. One file for both targets: tokens only, no platform code.
 *
 * The rule is drawing, not words: hidden from the screen reader, which hears the line (and the loader's label).
 */
import type { ReactNode } from "react";
import { View, type TextStyle } from "react-native";
import { FACE, TYPE } from "./type";
import type { Palette } from "./theme";

/** The rule above the line, in dp: short and thin, square-cornered. */
const LOADER_RULE = { width: 24, height: 2, above: 6, gap: 10 } as const;

/** The line's type and colour: `TYPE.small`, a step heavier (Archivo 500), tracked 0.4, in the muted ink. */
export const loaderLineText = (palette: Palette): TextStyle => ({
  ...TYPE.small, fontFamily: FACE.textMedium, letterSpacing: 0.4, color: palette.sub, textAlign: "center",
});

/** The rule, then `children` (the line, styled with `loaderLineText`) beneath it, centred. */
export function LoaderLine({ palette, children }: { palette: Palette; children: ReactNode }) {
  return (
    <View style={{ alignItems: "center", gap: LOADER_RULE.gap, alignSelf: "stretch" }}>
      <View testID="loader-rule" aria-hidden
            style={{ width: LOADER_RULE.width, height: LOADER_RULE.height, marginTop: LOADER_RULE.above,
                     backgroundColor: palette.coho.red }} />
      {children}
    </View>
  );
}
