/**
 * A row of choices that carry their own colours.
 *
 * The swatches are the point. "Stocked" is a word that means
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
  /** Greyed and not pressable; said to a screen reader (`aria-disabled`, AGENTS 34). The row says why beside it. */
  disabled?: boolean;
  /** A small line under the label ("Drawn · works offline"). */
  note?: string;
  /** An upper-case state in the tile ("No data yet", "Coming soon", "Showing") — drawn at full strength even greyed. */
  tag?: string;
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
        const off = o.disabled === true;
        return (
          <Pressable key={o.k} onPress={off ? undefined : () => onChange(o.k)} disabled={off}
                     accessibilityRole="radio" aria-checked={on} aria-disabled={off} accessibilityLabel={o.t}
                     style={{ flex: 1, alignItems: "center", gap: 7, borderRadius: palette.r.box,
                              // greyed as the app's other disabled controls are (Button: 0.4)
                              opacity: off && !o.tag ? 0.4 : 1,
                              paddingTop: 11, paddingBottom: 9,
                              // 1.5 in ink: a choice you have NOT made still has to look
                              // like a choice. In `line2` these read as empty space with a
                              // word in it.
                              borderWidth: 1.5,
                              borderColor: on ? palette.accent : off ? palette.line2 : palette.ink,
                              // chosen but unavailable (satellite offline): the choice is kept, drawn as a dashed frame
                              borderStyle: on && off ? "dashed" : "solid",
                              backgroundColor: on ? palette.tint : "transparent" }}>
            {shape === "line" ? (
              <View style={{ width: 26, gap: 3, opacity: off ? 0.45 : 1 }}>
                {o.swatch.map((c, i) => (
                  <View key={i} style={{ height: 3, backgroundColor: c }} />
                ))}
              </View>
            ) : (
              <View style={{ flexDirection: "row", gap: 3, opacity: off ? 0.45 : 1 }}>
                {o.swatch.map((c, i) => (
                  <View key={i} style={{ width: 7, height: 7, backgroundColor: c }} />
                ))}
              </View>
            )}
            <Text style={{ ...TYPE.micro, fontSize: 11.5, fontWeight: "600", opacity: off ? 0.55 : 1,
                           color: on ? palette.ink : palette.sub }}>{o.t}</Text>
            {o.note && (
              <Text style={{ ...TYPE.small, fontSize: 10, lineHeight: 13, color: palette.sub, textAlign: "center",
                             paddingHorizontal: 4, opacity: off ? 0.55 : 1 }}>{o.note}</Text>
            )}
            {o.tag && (
              <Text style={{ ...TYPE.section, fontSize: 9, letterSpacing: 1, color: on ? palette.accent : palette.ink,
                             textAlign: "center" }}>{o.tag.toUpperCase()}</Text>
            )}
          </Pressable>
        );
      })}
    </View>
  );
}
