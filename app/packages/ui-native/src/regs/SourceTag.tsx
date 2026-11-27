/**
 * (T879, owner 2026-11-18 ~20:53 "Two headers, one table", approved ~22:58) WHOSE RULE: a small square tag in the scope
 * kicker's type ("REGION 2-WIDE", GroupCard `kicker:scope`) — the sub colour inside a line2 outline, no status colour —
 * worn by each status block's name and by each row of the one species list, so a row is matched to its block by eye.
 * The words are core's (`SOURCE_WORDS`).
 */
import { View } from "react-native";
import { Text } from "../Text";
import { LABEL_PROPS, TYPE } from "../type";
import type { Palette } from "../theme";

export function SourceTag({ text, palette, testID }: { text: string; palette: Palette; testID?: string }) {
  return (
    <View testID={testID ?? "source-tag"} style={{ borderWidth: 1, borderColor: palette.line2, borderRadius: palette.r.chip,
                                                 paddingHorizontal: 5, paddingVertical: 1, alignSelf: "center" }}>
      <Text {...LABEL_PROPS} style={[TYPE.kicker, { fontSize: 10.5, lineHeight: 14, color: palette.sub }]}>{text}</Text>
    </View>
  );
}

/** A block's name with its tag after it ("SALMON  [DFO]"), the block's header. */
export function TaggedTitle({ text, tag, palette, testID }: { text: string; tag: string; palette: Palette; testID: string }) {
  return (
    <View style={{ flexDirection: "row", flexWrap: "wrap", alignItems: "center", columnGap: 8, rowGap: 2, marginBottom: 2 }}>
      {/* (T1207) the one block's name, "Salmon and game fish", wraps whole and the tag drops under it at a large text size */}
      <Text testID={testID} accessibilityRole="header" style={[TYPE.kicker, { color: palette.ink, flexShrink: 1 }]}>{text}</Text>
      <SourceTag text={tag} palette={palette} testID={`${testID}:tag`} />
    </View>
  );
}
