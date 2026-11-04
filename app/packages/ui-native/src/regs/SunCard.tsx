/**
 * (T382) THE DAY'S SUN, the first small card of the page: sunrise and sunset on the chosen day at the water, on the
 * B.C. clock ("Sunrise 07:44 PT · sunset 17:41 PT"). A card, not loose text (the map UI rule). No logic: it draws the
 * line `useSunLine` (@app/ui) built from core's `sunLine`; none, nothing.
 */
import { View } from "react-native";
import { Text } from "../Text";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { WaterInset } from "../WaterFrame";

export function SunCard({ line, palette }: { line: string | null; palette: Palette }) {
  if (!line) return null;
  return (
    <WaterInset style={{ paddingTop: 10 }}>
      <View testID="regs-sun" style={{ backgroundColor: palette.tint, paddingVertical: 6, paddingHorizontal: 10, alignSelf: "flex-start" }}>
        <Text style={[TYPE.meta, { color: palette.ink }]}>{line}</Text>
      </View>
    </WaterInset>
  );
}
