/**
 * (T230) THE IN-SEASON NOTICE: the provincial in-season changes in effect that are not placed on the map yet, as the
 * province printed them — region and water, the dates, the change verbatim, and its source. It opens by itself once per
 * row per device and from the top bar's bell (`@app/ui` `useInSeasonNotice`); words are core's (`INSEASON_WORDS`,
 * `inSeasonNoticeRows`). A scrolling sheet at every width (a centred dialog on a wide screen).
 */
import { Linking, Text, View } from "react-native";
import { INSEASON_WORDS, type InSeasonNoticeRow } from "@app/core";
import { Button } from "./Button";
import { RegsPress } from "./regs/RegsPress";
import { Sheet } from "./Sheet";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function InSeasonNotice({ open, rows, palette, onClose }: {
  open: boolean; rows: readonly InSeasonNoticeRow[]; palette: Palette; onClose: () => void;
}) {
  return (
    <Sheet open={open} onClose={onClose} title={INSEASON_WORDS.noticeTitle} sub={null} palette={palette} scroll dialog>
      <View testID="inseason-notice" style={{ paddingHorizontal: 20, paddingTop: 8, paddingBottom: 12, gap: 14 }}>
        <Text style={{ ...TYPE.body, color: palette.ink }}>{INSEASON_WORDS.noticeIntro}</Text>
        {rows.map((r) => (
          <View key={r.id} testID="inseason-notice-row"
                style={{ borderLeftWidth: 4, borderLeftColor: palette.attention, paddingLeft: 10, gap: 2 }}>
            <Text style={{ ...TYPE.metaBold, color: palette.ink }}>{r.where}{r.dates ? ` · ${r.dates}` : ""}</Text>
            <Text style={{ ...TYPE.body, color: palette.ink }}>{r.text}</Text>
            {!!r.url && (
              <RegsPress accessibilityRole="link" accessibilityLabel={INSEASON_WORDS.sourceLabel}
                         onPress={() => { void Linking.openURL(r.url); }} box={{ w: 44, h: 32 }}
                         style={{ minHeight: 32, justifyContent: "center", alignSelf: "flex-start" }}>
                <Text style={{ ...TYPE.metaBold, color: palette.accent }}>{INSEASON_WORDS.source}</Text>
              </RegsPress>
            )}
          </View>
        ))}
        <Button palette={palette} label={INSEASON_WORDS.noticeClose} onPress={onClose} />
      </View>
    </Sheet>
  );
}
