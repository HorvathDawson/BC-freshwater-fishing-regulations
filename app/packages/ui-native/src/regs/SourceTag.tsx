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

/** `oneLine` (T1207): the tag is one line, whole, at a larger system text size (the T1201 rule); only the merged block asks, every other tag is as it was. */
export function SourceTag({ text, palette, testID, oneLine = false }: { text: string; palette: Palette; testID?: string; oneLine?: boolean }) {
  return (
    <View testID={testID ?? "source-tag"} style={{ borderWidth: 1, borderColor: palette.line2, borderRadius: palette.r.chip,
                                                 paddingHorizontal: 5, paddingVertical: 1, alignSelf: "center" }}>
      <Text {...(oneLine ? LABEL_PROPS : {})} style={[TYPE.kicker, { fontSize: 10.5, lineHeight: 14, color: palette.sub }]}>{text}</Text>
    </View>
  );
}

/** A block's name with its tag after it ("SALMON  [DFO]"), the block's header. */
export function TaggedTitle({ text, tag, palette, testID, wrap = false, label }: { text: string; tag: string; palette: Palette; testID: string;
  /** (T1207) the merged block's long name ("Salmon and game fish"): the row wraps (the tag drops under the name at a large
   *  text size, kept whole). Every other block's title is as it was. */
  wrap?: boolean;
  /** (T1207) what a screen reader says for the header (the merged block: both fish and the status), in place of the name */
  label?: string }) {
  return (
    <View style={{ flexDirection: "row", ...(wrap ? { flexWrap: "wrap", rowGap: 2 } : {}), alignItems: "center", columnGap: 8, marginBottom: 2 }}>
      <Text testID={testID} accessibilityRole="header" accessibilityLabel={label} style={[TYPE.kicker, { color: palette.ink }, wrap ? { flexShrink: 1 } : null]}>{text}</Text>
      <SourceTag text={tag} palette={palette} testID={`${testID}:tag`} oneLine={wrap} />
    </View>
  );
}
