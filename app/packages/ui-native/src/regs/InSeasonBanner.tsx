/**
 * (T230) THE IN-SEASON BANNERS on a Regulations page: each provincial in-season change matched to this water (or part)
 * and in effect on the page's day — "In-season change (Oct 15–Nov 30): <verbatim text> — source ›". No words of its own
 * (core `inSeasonBanner`); which changes, from `@app/ui` `useInSeasonBanners`. INFORMATIONAL: drawn above the rules,
 * never changing them; the attention bar is not a water-status colour.
 *
 * (T472, owner 2026-11-07) One card line just above the species table ("In-season change (since May 16, 2025) · see ›", or
 * "3 in-season changes · see ›"), the Access notice strip's pattern — a press on it folds or opens the notices under it.
 * Never hidden.
 *
 * (T694) An unread notice says so: "New · …" / "Updated · …" on the line (core `inSeasonSummary`) and a mark on its
 * card. (T700) Opened from the bell's inbox at a notice, the notices are open.
 *
 * (T899, owner 2026-11-19: "in season notices should default open") OPEN BY DEFAULT: every open of the page shows each
 * notice under the line; a press on the line still folds them, a second opens them again.
 */
import { useEffect, useState } from "react";
import { View } from "react-native";
import { Text } from "../Text";
import { Pressable } from "../Pressable";
import { Icon } from "../icons";
import { inSeasonSourcesOf, inSeasonSummary, type InSeasonBannerVM } from "@app/core";
import { useInSeasonFocus } from "@app/ui";
import { TYPE } from "../type";
import { GUTTER, RADIUS, type Palette } from "../theme";
import { SourceRow } from "../SourceRow";

export function InSeasonBanners({ banners: all, palette }: { banners: readonly InSeasonBannerVM[]; palette: Palette }) {
  // (T318) a change with no words of its own is never drawn as an empty card with only its link (owner's phone, 0.3.0)
  const banners = all.filter((b) => b.text.trim());
  const [open, setOpen] = useState(true);   // (T899) open on every open of the page
  const focused = useInSeasonFocus(banners.map((b) => b.id));
  useEffect(() => { if (focused) setOpen(true); }, [focused]);
  const summary = inSeasonSummary(banners);
  if (!summary) return null;
  return (
    <View testID="inseason-banners">
      <Pressable testID="inseason-line" onPress={() => setOpen((o) => !o)} accessibilityRole="button" aria-expanded={open}
                 accessibilityLabel={summary}
                 style={{ marginTop: 10, marginBottom: 2, marginHorizontal: GUTTER, borderRadius: RADIUS.box,
                          backgroundColor: palette.tint, borderLeftWidth: 6, borderLeftColor: palette.attention,
                          minHeight: 44, paddingVertical: 8, paddingLeft: 12, paddingRight: 14, justifyContent: "center" }}>
        <View style={{ flexDirection: "row", alignItems: "center", gap: 8 }}>
          <Text testID="inseason-summary" style={[TYPE.regsBodyBold, { color: palette.ink, flex: 1 }]}>{summary}</Text>
          <Icon name={open ? "collapse" : "expand"} colour={palette.sub} size={14} />
        </View>
      </Pressable>
      {open && banners.map((b) => (
        <View key={b.id} testID="inseason-banner"
              style={{ marginTop: 10, marginBottom: 2, marginHorizontal: GUTTER, borderRadius: RADIUS.box,
                       backgroundColor: palette.tint, borderLeftWidth: 6, borderLeftColor: palette.attention,
                       paddingTop: 10, paddingBottom: 4, paddingLeft: 12, paddingRight: 14 }}>
          {b.mark && (
            <Text testID="inseason-mark" style={[TYPE.metaBold, { color: palette.ink, marginBottom: 2 }]}>{b.mark}</Text>
          )}
          <Text accessibilityRole="alert" style={[TYPE.regsBody, { color: palette.ink }]}>
            <Text style={TYPE.regsBodyBold}>{b.head}: </Text>
            {b.text}
          </Text>
          {/* (T518) where on this water, in the change's own ends, when it covers only part of the page */}
          {b.where && <Text testID="inseason-where" style={[TYPE.metaBold, { color: palette.ink, marginTop: 4 }]}>{b.where}</Text>}
          <Text style={[TYPE.meta, { color: palette.sub, marginTop: 4 }]}>{b.note}</Text>
          {/* (T1031 item 3) the notice's page as the one Source line: "Source  B.C. in-season changes ↗" */}
          <SourceRow vm={inSeasonSourcesOf(b)} palette={palette} testID="inseason-source" />
        </View>
      ))}
    </View>
  );
}
