/**
 * WHERE A WATER'S REGULATIONS GO. Deliberately empty.
 *
 * Regulations are not integrated: the app reads no rules, rule sets, licensing, gear or
 * closures, and colours no water by open/closed. This panel holds the place on the water
 * sheet so the screen's layout does not change when they arrive — swap its body for the
 * real panel, fed a `WaterRegulations` (see `regulations.ts` in @app/core, which names the
 * bundle tables and the export it will be built from).
 *
 * It states that the regulations are not here rather than saying nothing. A blank space on
 * a fishing app's water sheet reads as "no rules apply", which is never true: every water
 * in BC is covered by its region's rules at least.
 */
import { Text, View } from "react-native";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function RegulationsPlaceholder({ palette }: { palette: Palette }) {
  return (
    <View accessibilityLabel="Regulations are coming"
          style={{ gap: 6, padding: 14, borderWidth: 1, borderStyle: "dashed",
                   borderColor: palette.line2, borderRadius: palette.r.box }}>
      <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>Regulations are coming</Text>
      <Text style={{ ...TYPE.small, color: palette.sub }}>
        This app does not show fishing regulations yet. Check the current BC Freshwater
        Fishing Regulations Synopsis before you fish — the general rules for this region
        apply to every water in it.
      </Text>
    </View>
  );
}
