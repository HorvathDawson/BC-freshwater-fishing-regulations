/**
 * (T54, T147) THE ACCESS NOTICE at the top of a Regulations page: the no-access banner or the gentle note (private land,
 * a reserve, permit-only land). No logic and no words of its own: core's `accessNotice` / `waterAccessNotice` say what,
 * `@app/ui` `useAccessNotice` says for which section or part; the link shows the area on the water's map.
 *
 * (T147, owner 2026-10-23: "make the no public access a bigger part of the regulations at the top and more obvious")
 * The banner LEADS the page as a block in the fourth map colour: a thick bar of it down the left, its tint as the ground
 * (`palette.noAccessBand`), the legend's line swatch beside a large title ("No public access", the legend's own words),
 * then the plain-words body and the link — every word in ink, which holds 7:1 on the tint. The gentle note stays one
 * small grey line (T54 Q1: permit-only land is this tier).
 */
import { Text, View } from "react-native";
import type { AccessNotice } from "@app/core";
import { TYPE } from "../type";
import { GUTTER, type Palette } from "../theme";
import { RegsPress } from "./RegsPress";

export function AccessNoticeBlock({ notice, palette, onMap }: { notice: AccessNotice; palette: Palette; onMap?: () => void }) {
  if (notice.tier === "no_access") {
    const colour = palette.waterStatus.no_access;
    return (
      <View testID="access-banner" style={{ marginTop: 10, marginBottom: 4, marginHorizontal: GUTTER, borderRadius: 6,
                                            backgroundColor: palette.noAccessBand, borderLeftWidth: 6, borderLeftColor: colour,
                                            paddingTop: 12, paddingBottom: onMap ? 2 : 12, paddingLeft: 12, paddingRight: 14 }}>
        <View style={{ flexDirection: "row", alignItems: "center", gap: 10 }}>
          <View testID="access-swatch" style={{ width: 22, height: 5, borderRadius: 2.5, backgroundColor: colour }} />
          <Text testID="access-title" accessibilityRole="header" style={[TYPE.tier, { color: palette.ink, flex: 1 }]}>
            {notice.title}
          </Text>
        </View>
        <Text accessibilityRole="alert" style={[TYPE.regsBody, { color: palette.ink, marginTop: 6 }]}>{notice.text}</Text>
        {onMap && (
          <RegsPress accessibilityRole="link" onPress={onMap} box={{ w: 44, h: 44 }} style={{ minHeight: 44, justifyContent: "center" }}>
            <Text testID="access-map" style={[TYPE.regsBodyBold, { color: palette.ink, textDecorationLine: "underline" }]}>
              {notice.link}
            </Text>
          </RegsPress>
        )}
      </View>
    );
  }
  return (
    <View testID="access-note" style={{ marginTop: 8, marginHorizontal: GUTTER }}>
      <Text style={[TYPE.meta, { color: palette.sub }]}>{notice.text}</Text>
      {onMap && (
        <RegsPress accessibilityRole="link" onPress={onMap} box={{ w: 44, h: 44 }} style={{ minHeight: 44, justifyContent: "center" }}>
          <Text testID="access-map" style={[TYPE.metaBold, { color: palette.accent }]}>{notice.link}</Text>
        </RegsPress>
      )}
    </View>
  );
}
