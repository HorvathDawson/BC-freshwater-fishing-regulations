/**
 * (T254) THE LINE IS POOR, SAID ONCE: a slim bar over the app while the device is offline ("Offline — showing what's
 * saved on this device") or while low-bandwidth mode is on because the app found the line slow ("Slow connection —
 * showing what we have"). The reader's own "On" in More → Data is not announced (they chose it). Pressing it opens
 * More, where the Data setting is. The words are core's (`netBanner`); the app's tokens only.
 */
import { Pressable, Text, View } from "react-native";
import { NET_WORDS, netBanner } from "@app/core";
import { useNet } from "@app/ui";
import { SPACE, type Palette } from "./theme";
import { TYPE } from "./type";

export function NetBanner({ palette, onPress }: { palette: Palette; onPress?: () => void }) {
  const { mode } = useNet();
  const text = netBanner(mode);
  if (!text) return null;
  return (
    <Pressable testID="net-status" onPress={onPress} accessibilityRole="button" accessibilityLabel={text}
               accessibilityHint={NET_WORDS.banner.a11ySettings} accessibilityLiveRegion="polite"
               style={({ pressed }) => ({ opacity: pressed ? 0.7 : 1 })}>
      <View style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s2, minHeight: 30, paddingVertical: 5,
                     paddingHorizontal: SPACE.s3, backgroundColor: palette.tint, borderLeftWidth: 3,
                     borderLeftColor: palette.attention, borderBottomWidth: 1, borderBottomColor: palette.line }}>
        <Text numberOfLines={2} style={{ ...TYPE.small, color: palette.ink, flex: 1 }}>{text}</Text>
        {onPress && <Text style={{ ...TYPE.metaBold, fontSize: 12.5, color: palette.accent }}>{NET_WORDS.banner.settingsLink}</Text>}
      </View>
    </Pressable>
  );
}
