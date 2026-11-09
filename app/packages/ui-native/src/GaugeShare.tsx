/**
 * (T530) THE GAUGE CARD'S SHARE: the same control as the water header's (the share icon, 32 drawn / 44 to the touch, what it
 * said beside it), on the gauge card's title row. Draws `useGaugeShare`'s control and decides nothing (AGENTS 25).
 */
import { View } from "react-native";
import type { ShareControl } from "@app/ui";
import { Pressable } from "./Pressable";
import { Text } from "./Text";
import { Icon } from "./icons";
import { hitProps } from "./regs/hit";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function GaugeShare({ share, palette }: { share: ShareControl; palette: Palette }) {
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: 4, flexShrink: 1 }}>
      {share.said && (
        <Text testID="gauge-share-said" accessibilityLiveRegion="polite" numberOfLines={2}
              style={{ ...TYPE.meta, flexShrink: 1, textAlign: "right", color: palette.sub }}>{share.said}</Text>
      )}
      <Pressable onPress={share.press} accessibilityRole="button" accessibilityLabel={share.label}
                 aria-disabled={share.busy} disabled={share.busy}
                 testID="gauge-share" {...hitProps({ w: 32, h: 32 })}
                 style={{ width: 32, height: 32, alignItems: "center", justifyContent: "center" }}>
        <Icon name="share" colour={palette.accent} size={18} />
      </Pressable>
    </View>
  );
}
