/**
 * The Regulations view of one tap (T21 slice of T23): the header's date and place, the four tabs, and the selected tab.
 * "What you can keep" draws the Keep tab; Gear / Methods / Licence keep a placeholder until T30–T35 build them. No logic:
 * every word is the RegsPage's (or words.ts).
 */
import { useState } from "react";
import { Pressable, Text, View } from "react-native";
import { WORDS, type RegsPage, type Tab } from "@app/core";
import { TYPE } from "../type";
import { GUTTER, type Palette } from "../theme";
import { KeepTab } from "./KeepTab";
import type { KeepActions } from "./LineText";
import type { FishImages } from "./types";

export function RegsView({ page, palette, actions, images }: {
  page: RegsPage; palette: Palette; actions: Omit<KeepActions, "onTab">; images?: FishImages;
}) {
  const [tab, setTab] = useState<Tab>("keep");
  const h = page.header;
  const dot = h.dot === "open" ? palette.open : h.dot === "closed" ? palette.closed : palette.line2;
  const label = h.tabs.find((t) => t.tab === tab)?.label ?? "";
  return (
    <View testID="regs-view">
      <View style={{ paddingHorizontal: GUTTER, flexDirection: "row", alignItems: "center", gap: 8 }}>
        <View style={{ flex: 1 }}>
          {h.place && <Text testID="regs-place" numberOfLines={2} style={[TYPE.metaSemi, { color: palette.sub }]}>{h.place.full}</Text>}
        </View>
        <View accessibilityLabel={h.dateAria} style={{ flexDirection: "row", alignItems: "center", gap: 6, minHeight: 44 }}>
          <View style={{ width: 8, height: 8, backgroundColor: dot }} />
          <View>
            <Text style={[TYPE.metaHeavy, { color: palette.accent }]}>{h.dayWord}</Text>
            <Text style={[TYPE.metaSemi, { lineHeight: 14.98, color: palette.sub }]}>{h.date}</Text>
          </View>
        </View>
      </View>
      <View accessibilityRole="tablist" style={{ flexDirection: "row", gap: 16, paddingHorizontal: GUTTER, borderBottomWidth: 1.5, borderBottomColor: palette.ink }}>
        {h.tabs.map((t) => (
          <Pressable key={t.tab} accessibilityRole="tab" aria-selected={t.tab === tab} onPress={() => setTab(t.tab)}
                     style={{ minHeight: 34, paddingTop: 2, justifyContent: "center", borderBottomWidth: 3, borderBottomColor: t.tab === tab ? palette.ink : "transparent" }}>
            <Text style={[TYPE.regsTab, { color: t.tab === tab ? palette.ink : palette.sub }]}>{t.label}</Text>
          </Pressable>
        ))}
      </View>
      {tab === "keep"
        ? <KeepTab keep={page.keep} dateLabel={h.date} palette={palette} actions={{ ...actions, onTab: setTab }} images={images} />
        : (
          <View testID="regs-tab-pending" style={{ margin: GUTTER, padding: 14, borderWidth: 1, borderStyle: "dashed", borderColor: palette.line2 }}>
            <Text style={[TYPE.regsBody, { color: palette.sub }]}>{WORDS.TAB_PENDING(label)}</Text>
          </View>
        )}
    </View>
  );
}
