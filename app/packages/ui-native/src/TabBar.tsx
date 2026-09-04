/**
 * Four tabs, and the map is home.
 *
 * That ordering is the design's central claim (`design/riffle.html`): you arrive at a map
 * and everything else is a way back to one. Search and Spots both hand you a water; the
 * map is where a water means something.
 */
import { Pressable, Text, View } from "react-native";
import { Icon, type IconName } from "./icons";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export const TABS = [
  { k: "map", t: "Map" },
  { k: "search", t: "Search" },
  { k: "conditions", t: "Conditions" },
  { k: "spots", t: "Spots" },
] as const satisfies readonly { k: IconName; t: string }[];

export type TabKey = (typeof TABS)[number]["k"];

export function TabBar({ active, onChange, palette }:
  { active: TabKey; onChange: (k: TabKey) => void; palette: Palette }) {
  return (
    <View accessibilityRole="tablist"
          style={{ flexDirection: "row", borderTopWidth: 1, borderTopColor: palette.line,
                   backgroundColor: palette.card, paddingTop: 8, paddingBottom: 10 }}>
      {TABS.map(({ k, t }) => {
        const on = k === active;
        return (
          <Pressable key={k} onPress={() => onChange(k)}
                     accessibilityRole="tab" aria-selected={on}
                     accessibilityLabel={t}
                     style={{ flex: 1, alignItems: "center", gap: 5, paddingVertical: 2 }}>
            <Icon name={k} colour={on ? palette.accent : palette.sub} />
            {/* the label is always there: an icon alone is a guess */}
            <Text style={{ ...TYPE.tab, color: on ? palette.accent : palette.sub }}>{t}</Text>
          </Pressable>
        );
      })}
    </View>
  );
}
