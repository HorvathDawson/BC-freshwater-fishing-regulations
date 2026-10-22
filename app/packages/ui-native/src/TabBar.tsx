/**
 * The bottom bar: Map · Conditions · Spots · More, and the map is home.
 *
 * (T48, owner 2026-10-22, Claude's recommendation) ONE PLACE FOR EACH KIND OF THING: the top bar is search alone, the
 * side rail is Layers alone, and everything you go TO is a tab here — the two maps, your spots (a tab again: its list,
 * open, show on the map, add) and More (what the top-left hamburger held: Glossary, Data sources & dates, Display,
 * About, Report a problem, Contact — moved, not copied). Search left the bar for the top bar's always-open field.
 *
 * Smaller than it was (owner: "we could make the bar smaller"): a 20 px icon over its label with tighter padding — four
 * targets still 44 px tall and a quarter of the width each. Hidden in water mode and while a spot is added (`route.ts`
 * `bottomBarShown`).
 */
import { Pressable, Text, View } from "react-native";
import { Icon, type IconName } from "./icons";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export const TABS = [
  { k: "map", t: "Map" },
  { k: "conditions", t: "Conditions" },
  { k: "spots", t: "Spots" },
  { k: "more", t: "More" },
] as const satisfies readonly { k: IconName; t: string }[];

export type TabKey = (typeof TABS)[number]["k"];

/** The bar's measures: 20 px icons (22 before T48), 3 px to the label, 5 / 4 px above and below (9 / 10 before). */
export const TAB_BAR = { icon: 20, gap: 3, padTop: 5, padBottom: 4, minHit: 44 } as const;

export function TabBar({ active, onChange, palette }:
  { active: TabKey; onChange: (k: TabKey) => void; palette: Palette }) {
  return (
    <View accessibilityRole="tablist"
          // A HARD RULE, in ink. The bar sat on a pale hairline, which on a white card is
          // barely a line at all — the tabs floated. `ink` at 1.5 makes the bar a printed
          // edge, which is the whole language: cut corners, hairlines of ink, offset slabs.
          style={{ flexDirection: "row", borderTopWidth: 1.5, borderTopColor: palette.ink,
                   backgroundColor: palette.card, paddingTop: TAB_BAR.padTop, paddingBottom: TAB_BAR.padBottom }}>
      {TABS.map(({ k, t }) => {
        const on = k === active;
        return (
          <Pressable key={k} onPress={() => onChange(k)}
                     accessibilityRole="tab" aria-selected={on}
                     accessibilityLabel={t}
                     style={{ flex: 1, alignItems: "center", gap: TAB_BAR.gap, paddingVertical: 2,
                              minHeight: TAB_BAR.minHit, justifyContent: "center" }}>
            <Icon name={k} colour={on ? palette.ink : palette.sub} size={TAB_BAR.icon} />
            {/* the label is always there: an icon alone is a guess */}
            <Text style={{ ...TYPE.tab, color: on ? palette.ink : palette.sub }}>{t}</Text>
            {/* THE SELECTED TAB IS UNDERSCORED, not just tinted. Colour alone carries the
                state nowhere a reader is colour-blind or the screen is in sunlight, and a
                solid bar under the label is how a printed index marks its page. */}
            <View style={{ height: 2, width: 22,
                           backgroundColor: on ? palette.ink : "transparent" }} />
          </Pressable>
        );
      })}
    </View>
  );
}
