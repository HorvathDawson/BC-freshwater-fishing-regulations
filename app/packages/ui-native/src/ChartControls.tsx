/**
 * The three questions a reader can change about a hydrograph.
 *
 * They are NOT view options. Each one changes what the chart claims:
 *
 *   discharge / level   the whole river, or one cross-section. A stage percentile moves
 *                       when the channel does; a discharge percentile does not. Offered
 *                       only where the bundle actually holds an envelope in that quantity,
 *                       so the control can never ask for a comparison that does not exist.
 *   72 hours / year     what the water is doing, or where today sits in its season.
 *
 * Drawn as pill rows rather than a segmented control so a single available option still
 * reads as a statement of fact ("this station measures level") instead of as a broken
 * switch.
 */
import { Pressable, Text, View } from "react-native";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function ChartControls<T extends string>({ palette, value, options, onPick, label }: {
  palette: Palette; value: T; options: readonly (readonly [T, string])[];
  onPick: (v: T) => void; label: string;
}) {
  // One choice is not a choice. Showing it as the current state, unpressable, is honest —
  // and it tells the reader something true: this station only measures the one thing.
  if (options.length < 2) return null;
  return (
    <View style={{ flexDirection: "row", gap: 6, flexWrap: "wrap" }}
          accessibilityRole="radiogroup" accessibilityLabel={label}>
      {options.map(([id, text]) => {
        const on = id === value;
        return (
          /*
           * IT HAS TO BE FINDABLE OVER A MAP.
           *
           * These were transparent with a hairline border and 11px grey text — fine inside
           * a sheet, where they sit on a flat card, and nearly invisible over landcover,
           * roads and rivers, which is where the Conditions view puts them. The control a
           * reader needs in order to ask the other question was the hardest thing on the
           * screen to see.
           *
           * So: an opaque card behind every option rather than only the chosen one, ink
           * instead of grey, and `palette.lift` — the theme's own hard offset shadow, the
           * same slab the map's zoom buttons use, so the app's chrome and MapLibre's read
           * as one set rather than two.
           */
          <Pressable key={id} onPress={() => onPick(id)} accessibilityRole="radio"
                     aria-selected={on} accessibilityLabel={text}
                     style={{ paddingVertical: 7, paddingHorizontal: 13,
                              borderRadius: palette.r.pill, borderWidth: 1,
                              borderColor: on ? palette.accent : palette.line2,
                              backgroundColor: on ? palette.tint : palette.card,
                              ...palette.lift }}>
            <Text style={{ ...TYPE.micro, fontSize: 11.5, fontWeight: on ? "700" : "600",
                           color: on ? palette.accent : palette.ink }}>{text}</Text>
          </Pressable>
        );
      })}
    </View>
  );
}
