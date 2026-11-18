/**
 * (T839) THE NAVIGATION RAIL: Map · Conditions · Spots · More down the LEFT edge on a desktop-width window, in place of
 * the bottom bar (owner 2026-11-17, asked Claude to choose the standard spot: Material 3 turns a bottom bar into a rail at
 * wider widths, and desktop map apps keep their tabs on the left). The same four tabs, names, icons and press as
 * `TabBar` — one list (`TABS`), one handler — drawn as a column: icon over label, the current tab FILLED (ink ground,
 * card colour on it: a shape as well as a colour, so it holds in sunlight and for a colour-blind reader). Square corners,
 * an ink hairline on its right edge (the bar's hard rule turned a quarter). Drawn only where the bottom bar would be
 * (`bottomBarShown`), so a water's page (regs mode) and the add-a-spot flow have none, exactly as on a phone.
 */
import { Platform, View } from "react-native";
import { Text } from "./Text";
import { Pressable } from "./Pressable";
import { Icon } from "./icons";
import { TYPE } from "./type";
import type { Palette } from "./theme";
import { hitProps } from "./regs/hit";
import { useCoachRefs } from "./CoachMarks";
import { TABS, TAB_BAR, type TabKey } from "./TabBar";

/** the rail's width and each tab's box: 92 wide holds "CONDITIONS" at the tab type; 64 tall is well over 44 */
export const NAV_RAIL = { width: 92, tabH: 64 } as const;

/** (T856) the paper at partial opacity (#rrggbb -> rgba); anything else is left as it is */
const seeThrough = (hex: string, a: number): string => {
  const m = /^#([0-9a-f]{6})$/i.exec(hex);
  if (!m) return hex;
  const n = parseInt(m[1]!, 16);
  return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${a})`;
};
/** (T856) how much of the map shows through the rail: the paper at 85 %, blurred on the web */
export const NAV_RAIL_OPACITY = 0.85;

export function NavRail({ active, onChange, palette }:
  { active: TabKey; onChange: (k: TabKey) => void; palette: Palette }) {
  const coach = useCoachRefs();   // the first-run tour points at the tabs (`tab:<key>`), here as on the bar
  return (
    <View accessibilityRole="tablist" testID="nav-rail"
          style={{ width: NAV_RAIL.width, borderRightWidth: 1.5, borderRightColor: palette.ink,
                   backgroundColor: seeThrough(palette.card, NAV_RAIL_OPACITY), paddingTop: 8,
                   // the web blurs what is under it, so the type keeps its contrast over a busy map
                   ...(Platform.OS === "web" ? { backdropFilter: "blur(10px)" } as object : {}) }}>
      {TABS.map(({ k, t }) => {
        const on = k === active;
        const fg = on ? palette.card : palette.sub;
        return (
          <Pressable {...hitProps({ w: NAV_RAIL.width, h: NAV_RAIL.tabH })} key={k} onPress={() => onChange(k)} ref={coach(`tab:${k}`)}
                     accessibilityRole="tab" aria-selected={on} accessibilityLabel={t}
                     style={{ alignItems: "center", justifyContent: "center", gap: TAB_BAR.gap, height: NAV_RAIL.tabH,
                              backgroundColor: on ? palette.ink : "transparent" }}>
            <Icon name={k} colour={fg} size={TAB_BAR.icon} />
            <Text numberOfLines={1} style={{ ...TYPE.tab, color: fg }}>{t}</Text>
          </Pressable>
        );
      })}
    </View>
  );
}
