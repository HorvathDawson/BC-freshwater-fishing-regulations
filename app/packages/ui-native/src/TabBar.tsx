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
          // A HARD RULE, in ink. The bar sat on a pale hairline, which on a white card is
          // barely a line at all — the tabs floated. `ink` at 1.5 makes the bar a printed
          // edge, which is the whole language: cut corners, hairlines of ink, offset slabs.
          style={{ flexDirection: "row", borderTopWidth: 1.5, borderTopColor: palette.ink,
                   backgroundColor: palette.card, paddingTop: 9, paddingBottom: 10 }}>
      {TABS.map(({ k, t }) => {
        const on = k === active;
        return (
          <Pressable key={k} onPress={() => onChange(k)}
                     accessibilityRole="tab" aria-selected={on}
                     accessibilityLabel={t}
                     style={{ flex: 1, alignItems: "center", gap: 6, paddingVertical: 2 }}>
            <Icon name={k} colour={on ? palette.ink : palette.sub} />
            {/* the label is always there: an icon alone is a guess */}
            <Text style={{ ...TYPE.tab, color: on ? palette.ink : palette.sub }}>{t}</Text>
            {/* THE SELECTED TAB IS UNDERSCORED, not just tinted. Colour alone carries the
                state nowhere a reader is colour-blind or the screen is in sunlight, and a
                solid bar under the label is how a printed index marks its page. */}
            <View style={{ height: 2.5, width: 22, marginTop: 1,
                           backgroundColor: on ? palette.ink : "transparent" }} />
          </Pressable>
        );
      })}
    </View>
  );
}
