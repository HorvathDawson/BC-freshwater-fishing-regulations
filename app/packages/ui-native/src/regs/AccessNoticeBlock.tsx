/**
 * (T54) THE ACCESS NOTICE at the top of a section's Regulations page: the no-access banner (a section inside land the
 * public may not enter, ruled in the map's fourth colour) or the gentle note (private land, a reserve, permit-only
 * land). No logic and no words of its own: core's `accessNotice` says what, `@app/ui` `useAccessNotice` says for which
 * section; the link shows the area on the water's map.
 */
import { Text, View } from "react-native";
import type { AccessNotice } from "@app/core";
import { TYPE } from "../type";
import { GUTTER, type Palette } from "../theme";
import { RegsPress } from "./RegsPress";

export function AccessNoticeBlock({ notice, palette, onMap }: { notice: AccessNotice; palette: Palette; onMap?: () => void }) {
  const strong = notice.tier === "no_access";
  const link = onMap && (
    <RegsPress accessibilityRole="link" onPress={onMap} box={{ w: 44, h: 44 }} style={{ minHeight: 44, justifyContent: "center" }}>
      <Text testID="access-map" style={[strong ? TYPE.regsBodyBold : TYPE.metaBold, { color: palette.accent }]}>{notice.link}</Text>
    </RegsPress>
  );
  return strong ? (
    <View testID="access-banner" style={{ marginTop: 8, marginHorizontal: GUTTER, paddingTop: 10, paddingBottom: onMap ? 0 : 8,
                                          borderTopWidth: 1.5, borderTopColor: palette.waterStatus.no_access }}>
      <Text accessibilityRole="alert" style={[TYPE.regsBody, { color: palette.ink }]}>{notice.text}</Text>
      {link}
    </View>
  ) : (
    <View testID="access-note" style={{ marginTop: 8, marginHorizontal: GUTTER }}>
      <Text style={[TYPE.meta, { color: palette.sub }]}>{notice.text}</Text>
      {link}
    </View>
  );
}
