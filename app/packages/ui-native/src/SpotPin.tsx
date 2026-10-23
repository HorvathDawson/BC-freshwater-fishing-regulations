/**
 * T131 — ONE SAVED SPOT ON THE MAP (owner 2026-10-22: "make spots on the map look better and clickable to open the
 * spot"), drawn as the map asks (`MapSpots.face`) inside its 44 px target:
 *
 *   the face     a square of the spot ground framed in the spot mark, the Spots tab's own icon (`spots`, Lucide
 *                map-pin) inside it, on the app's hard offset slab — the app's own object on the map, like its bar
 *   selected     the spot last opened or shown: larger, and inverted (mark fill, ground icon)
 *   a group      its count where the icon is, over a second face offset behind it: "several here"
 *   the name     above the face from the name zoom in (`spotMarkers` decides, and drops a name that would cross
 *                another); card ground, ink frame — the boundary marks' label. Never pressable: the face is the target
 *
 * Decides nothing: which marks, their names, words and what a tap does are `@app/core` `spotMarkers` / `spotPress`;
 * the colours are the spot tokens (`palette.spot`, gated in theme.test.ts). One button, which says what a tap does.
 */
import { Pressable, Text, View } from "react-native";
import { SPOT_PIN, type SpotMarker } from "@app/core";
import { Icon } from "./icons";
import { FACE } from "./type";
import type { Palette } from "./theme";

const FRAME = 2;

export function SpotPin({ m, palette, press }: { m: SpotMarker; palette: Palette; press: () => void }) {
  const { mark, ground } = palette.spot;
  const size = m.selected ? SPOT_PIN.selectedFace : SPOT_PIN.face;
  const fill = m.selected ? mark : ground, ink = m.selected ? ground : mark;
  const square = { width: size, height: size, borderWidth: FRAME, borderColor: mark, borderRadius: palette.r.chip };
  return (
    <Pressable onPress={press} accessibilityRole="button" aria-label={m.a11y}
               style={{ width: SPOT_PIN.hit, height: SPOT_PIN.hit, alignItems: "center", justifyContent: "center" }}>
      {m.label !== null && (
        <View pointerEvents="none"
              style={{ position: "absolute", bottom: SPOT_PIN.hit / 2 + size / 2 + SPOT_PIN.labelGap,
                       left: (SPOT_PIN.hit - SPOT_PIN.labelMaxWidth) / 2, width: SPOT_PIN.labelMaxWidth,
                       alignItems: "center" }}>
          <Text testID="spot-name" numberOfLines={2}
                style={{ maxWidth: SPOT_PIN.labelMaxWidth, paddingHorizontal: 7, paddingVertical: 3,
                         backgroundColor: ground, color: mark, borderWidth: 1.5, borderColor: mark,
                         fontFamily: FACE.textBold, fontSize: 12, lineHeight: 15, textAlign: "center" }}>
            {m.label}
          </Text>
        </View>
      )}
      {m.count > 1 && (
        <View testID="spot-stack" pointerEvents="none"
              style={{ ...square, position: "absolute", backgroundColor: ground,
                       transform: [{ translateX: 4 }, { translateY: -4 }] }} />
      )}
      <View testID="spot-face" pointerEvents="none"
            style={{ ...square, backgroundColor: fill, alignItems: "center", justifyContent: "center",
                     ...palette.lift }}>
        {m.count > 1
          ? <Text testID="spot-count" style={{ fontFamily: FACE.textHeavy, fontSize: 14, lineHeight: 16, color: ink }}>
              {String(m.count)}
            </Text>
          : <Icon name="spots" colour={ink} size={SPOT_PIN.icon} />}
      </View>
    </Pressable>
  );
}
