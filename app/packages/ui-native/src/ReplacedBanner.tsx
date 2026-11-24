/**
 * (T1016) THIS PAGE IS OLDER THAN WHAT THE SERVER SERVES, said once: a slim bar over the app (the slow / offline bar's
 * look, `NetBanner`) with the line and its one action (core `REPLACED_WORDS`, picked by `@app/ui` `useReplacedNotice`).
 * Pressing it does the action: the data notice puts the live release in use (the app's in-session apply), the code
 * notice reloads the page. Nothing when no notice is raised.
 */
import { View } from "react-native";
import type { ReplacedNotice } from "@app/ui";
import { Text } from "./Text";
import { Pressable } from "./Pressable";
import { SPACE, type Palette } from "./theme";
import { TYPE } from "./type";

export function ReplacedBanner({ palette, notice }: { palette: Palette; notice: ReplacedNotice | null }) {
  if (!notice) return null;
  return (
    <Pressable testID={`replaced-${notice.kind}`} onPress={notice.act} accessibilityRole="button"
               accessibilityLabel={notice.line} accessibilityHint={notice.a11y} accessibilityLiveRegion="polite"
               style={({ pressed }) => ({ opacity: pressed ? 0.7 : 1 })}>
      <View style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s2, minHeight: 30, paddingVertical: 5,
                     paddingHorizontal: SPACE.s3, backgroundColor: palette.tint, borderLeftWidth: 3,
                     borderLeftColor: palette.attention, borderBottomWidth: 1, borderBottomColor: palette.line }}>
        <Text numberOfLines={2} style={{ ...TYPE.small, color: palette.ink, flex: 1 }}>{notice.line}</Text>
        <Text style={{ ...TYPE.metaBold, fontSize: 12.5, color: palette.accent }}>{notice.action}</Text>
      </View>
    </Pressable>
  );
}
