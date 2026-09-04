/**
 * A row of choices that carry their own colours.
 *
 * From `design/riffle.html`: the swatches are the point. "Stocked" is a word that means
 * nothing until you see the ramp, and a reader deciding what to put on a map is deciding
 * what the map will LOOK like — so the control shows it before they commit.
 */
import { Pressable, Text, View } from "react-native";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export interface Option {
  k: string;
  t: string;
  /** Three colours, drawn as stacked lines or a row of squares. */
  swatch: readonly [string, string, string];
}

export function OptionRow({ palette, options, value, onChange, shape, label }: {
  palette: Palette; options: readonly Option[]; value: string;
  onChange: (k: string) => void; shape: "line" | "square"; label: string;
}) {
  return (
    <View accessibilityRole="radiogroup" accessibilityLabel={label}
          style={{ flexDirection: "row", gap: 8 }}>
      {options.map((o) => {
        const on = o.k === value;
        return (
          <Pressable key={o.k} onPress={() => onChange(o.k)} accessibilityRole="radio"
                     aria-checked={on} accessibilityLabel={o.t}
                     style={{ flex: 1, alignItems: "center", gap: 7, borderRadius: 12,
                              paddingTop: 11, paddingBottom: 9, borderWidth: 1,
                              borderColor: on ? palette.accent : palette.line2,
                              backgroundColor: on ? palette.tint : "transparent" }}>
            {shape === "line" ? (
              <View style={{ width: 26, gap: 3 }}>
                {o.swatch.map((c, i) => (
                  <View key={i} style={{ height: 3, borderRadius: 2, backgroundColor: c }} />
                ))}
              </View>
            ) : (
              <View style={{ flexDirection: "row", gap: 3 }}>
                {o.swatch.map((c, i) => (
                  <View key={i} style={{ width: 7, height: 7, borderRadius: 2,
                                         backgroundColor: c }} />
                ))}
              </View>
            )}
            <Text style={{ ...TYPE.micro, fontSize: 11.5, fontWeight: "600",
                           color: on ? palette.ink : palette.sub }}>{o.t}</Text>
          </Pressable>
        );
      })}
    </View>
  );
}
