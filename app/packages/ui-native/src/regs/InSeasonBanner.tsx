/**
 * (T230) THE IN-SEASON BANNERS on a Regulations page: each provincial in-season change matched to this water (or part)
 * and in effect on the page's day — "In-season change (Oct 15–Nov 30): <verbatim text> — source ›". No words of its own
 * (core `inSeasonBanner`); which changes, from `@app/ui` `useInSeasonBanners`. INFORMATIONAL: drawn above the rules,
 * never changing them; the attention bar is not a water-status colour.
 */
import { View } from "react-native";
import { Text } from "../Text";
import { INSEASON_WORDS, type InSeasonBannerVM } from "@app/core";
import { TYPE } from "../type";
import { GUTTER, RADIUS, type Palette } from "../theme";
import { RegsPress } from "./RegsPress";
import { linkTo } from "../openLink";

export function InSeasonBanners({ banners: all, palette }: { banners: readonly InSeasonBannerVM[]; palette: Palette }) {
  // (T318) a change with no words of its own is never drawn as an empty card with only its link (owner's phone, 0.3.0)
  const banners = all.filter((b) => b.text.trim());
  if (!banners.length) return null;
  return (
    <View testID="inseason-banners">
      {banners.map((b) => (
        <View key={b.id} testID="inseason-banner"
              style={{ marginTop: 10, marginBottom: 2, marginHorizontal: GUTTER, borderRadius: RADIUS.box,
                       backgroundColor: palette.tint, borderLeftWidth: 6, borderLeftColor: palette.attention,
                       paddingTop: 10, paddingBottom: 4, paddingLeft: 12, paddingRight: 14 }}>
          <Text accessibilityRole="alert" style={[TYPE.regsBody, { color: palette.ink }]}>
            <Text style={TYPE.regsBodyBold}>{b.head}: </Text>
            {b.text}
          </Text>
          {b.partial && <Text style={[TYPE.meta, { color: palette.sub, marginTop: 4 }]}>{b.partial}</Text>}
          <View style={{ flexDirection: "row", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
            <Text style={[TYPE.meta, { color: palette.sub, flexShrink: 1 }]}>{b.note}</Text>
            {!!b.url && (
              <RegsPress accessibilityRole="link" accessibilityLabel={INSEASON_WORDS.sourceLabel}
                         onPress={linkTo(b.url)} box={{ w: 44, h: 44 }}
                         style={{ minHeight: 44, justifyContent: "center" }}>
                <Text testID="inseason-source" style={[TYPE.metaBold, { color: palette.accent }]}>{b.source}</Text>
              </RegsPress>
            )}
          </View>
        </View>
      ))}
    </View>
  );
}
