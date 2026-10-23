/**
 * ONE SAVED SPOT ON THE MAP (T131; T145 reworked small — owner 2026-10-23: "look really bad and take up a ton of space …
 * they get way too big too"), drawn as the map asks (`MapSpots.face`) inside its invisible 44 px target:
 *
 *   the face     a small SQUARE of the spot mark, framed in the spot ground so it reads on water, land and imagery —
 *                square, so it is never a gauge's round dot or a boundary mark's ring
 *   selected     the spot last opened or shown: the same face ringed in the mark (a gap, then the ring) — 8 px wider
 *   a group      a larger square with its count in the ground colour
 *   the name     above the face from the name zoom in (`spotMarkers` decides, and drops a name that would cross
 *                another); one line, small, on a ground-coloured strip. Never pressable: the face is the target
 *
 * Every size is `@app/core` `spotFace` / `SPOT_PIN` — none depends on the zoom, so none grows past `SPOT_PIN.max`.
 * Decides nothing: which marks, their names, words and what a tap does are `spotMarkers` / `spotPress`; the colours
 * are the spot tokens (`palette.spot`, gated in theme.test.ts). One button, which says what a tap does.
 */
import { Pressable, Text, View } from "react-native";
import { SPOT_PIN, spotFace, type SpotMarker } from "@app/core";
import { FACE } from "./type";
import type { Palette } from "./theme";

export function SpotPin({ m, palette, press }: { m: SpotMarker; palette: Palette; press: () => void }) {
  const { mark, ground } = palette.spot;
  const { face, extent } = spotFace(m);
  return (
    <Pressable onPress={press} accessibilityRole="button" aria-label={m.a11y}
               style={{ width: SPOT_PIN.hit, height: SPOT_PIN.hit, alignItems: "center", justifyContent: "center" }}>
      {m.label !== null && (
        <View pointerEvents="none"
              style={{ position: "absolute", bottom: SPOT_PIN.hit / 2 + extent / 2 + SPOT_PIN.labelGap,
                       left: (SPOT_PIN.hit - SPOT_PIN.labelMaxWidth) / 2, width: SPOT_PIN.labelMaxWidth,
                       alignItems: "center" }}>
          <Text testID="spot-name" numberOfLines={1} ellipsizeMode="tail"
                style={{ maxWidth: SPOT_PIN.labelMaxWidth, height: SPOT_PIN.labelHeight, paddingHorizontal: 4,
                         paddingVertical: 1, backgroundColor: ground, color: mark, fontFamily: FACE.textBold,
                         fontSize: 11, lineHeight: 13, textAlign: "center" }}>
            {m.label}
          </Text>
        </View>
      )}
      <View testID="spot-ring" pointerEvents="none"
            style={{ width: extent, height: extent, alignItems: "center", justifyContent: "center",
                     borderRadius: palette.r.chip, borderWidth: m.selected ? SPOT_PIN.frame : 0, borderColor: mark }}>
        <View testID="spot-face" pointerEvents="none"
              style={{ width: face, height: face, borderWidth: SPOT_PIN.frame, borderColor: ground,
                       borderRadius: palette.r.chip, backgroundColor: mark, alignItems: "center",
                       justifyContent: "center" }}>
          {m.count > 1 && (
            <Text testID="spot-count" style={{ fontFamily: FACE.textHeavy, fontSize: 10, lineHeight: 12, color: ground }}>
              {String(m.count)}
            </Text>
          )}
        </View>
      </View>
    </Pressable>
  );
}
