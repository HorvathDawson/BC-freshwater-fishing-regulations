/**
 * Which day the map is about. Drawn to `design/riffle.html` ("Which day?").
 *
 * WHY THIS EXISTS AT ALL. Half of BC's freshwater regulations are seasonal, so a map of
 * "open" and "closed" is only true for one date. The map has always shown a date pill
 * saying which — but the pill was inert: `MapScreen` declared an `onDate` prop and `Shell`
 * never passed one, so the app displayed the qualifier on every answer it gave and offered
 * no way to change it. A control that looks pressable and does nothing is worse than no
 * control, because the reader concludes the date is fixed rather than that it is broken.
 *
 * A MONTH GRID AND A DAY STEPPER, not a calendar. The question is "roughly when am I
 * going", and season boundaries land on month ends; a full calendar would add weekday
 * columns nobody is choosing by, and a native picker looks different on each platform,
 * which is the one thing a shared component set exists to avoid.
 */
import { Pressable, Text, View } from "react-native";
import type { PlainDate } from "@app/core";
import { Sheet } from "./Sheet";
import { TYPE } from "./type";
import type { Palette } from "./theme";

const MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
                "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"] as const;

/** Days in a month, Gregorian. February needs the year, so the year is required. */
export function daysInMonth(year: number, month: number): number {
  return new Date(Date.UTC(year, month, 0)).getUTCDate();
}

export function DateSheet({ open, onClose, value, onChange, palette }: {
  open: boolean; onClose: () => void;
  value: PlainDate; onChange: (d: PlainDate) => void; palette: Palette;
}) {
  /**
   * Changing month must never produce a date that does not exist. Moving from 31 March to
   * February has to land on the 28th (or 29th), not on a "31 February" that every downstream
   * window comparison would answer nonsense about.
   */
  const set = (month: number, day: number) => {
    const last = daysInMonth(value.year, month);
    onChange({ year: value.year, month, day: Math.min(Math.max(1, day), last) });
  };
  const last = daysInMonth(value.year, value.month);

  return (
    <Sheet open={open} onClose={onClose} title="Which day?" palette={palette}>
      <View style={{ paddingHorizontal: 18, paddingTop: 14, gap: 12 }}>
        <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.6,
                       color: palette.faint }}>MONTH</Text>
        <View accessibilityRole="radiogroup" accessibilityLabel="Month"
              style={{ flexDirection: "row", flexWrap: "wrap", gap: 8 }}>
          {MONTHS.map((label, i) => {
            const month = i + 1;
            const on = month === value.month;
            return (
              <Pressable key={label} onPress={() => set(month, value.day)}
                         accessibilityRole="radio" aria-checked={on}
                         accessibilityLabel={label}
                         // Four to a row at any phone width: the grid is fixed at 12, so a
                         // percentage basis beats a fixed width that wraps differently on a
                         // narrow device.
                         style={{ flexBasis: "22%", flexGrow: 1, alignItems: "center",
                                  paddingVertical: 11, borderRadius: 11,
                                  backgroundColor: on ? palette.tint : "transparent",
                                  borderWidth: 1,
                                  borderColor: on ? palette.accent : palette.line2 }}>
                <Text style={{ ...TYPE.micro, fontSize: 13, fontWeight: on ? "700" : "500",
                               color: on ? palette.accent : palette.ink }}>{label}</Text>
              </Pressable>
            );
          })}
        </View>

        <View style={{ flexDirection: "row", alignItems: "center", borderRadius: 12,
                       borderWidth: 1, borderColor: palette.line2, overflow: "hidden" }}>
          <Step palette={palette} label="Previous day" glyph="−"
                onPress={() => set(value.month, value.day - 1)}
                disabled={value.day <= 1} />
          <Text accessibilityLabel={`Day ${value.day}`}
                style={{ ...TYPE.figureBig, flex: 1, textAlign: "center",
                         color: palette.ink }}>{value.day}</Text>
          <Step palette={palette} label="Next day" glyph="+"
                onPress={() => set(value.month, value.day + 1)}
                disabled={value.day >= last} />
        </View>

        <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.faint }}>
          Half of these regulations are seasonal, so the map is only right for one day at
          a time.
        </Text>
      </View>
    </Sheet>
  );
}

function Step({ palette, label, glyph, onPress, disabled }: {
  palette: Palette; label: string; glyph: string; onPress: () => void; disabled: boolean;
}) {
  return (
    <Pressable onPress={onPress} disabled={disabled} accessibilityRole="button"
               accessibilityLabel={label} aria-disabled={disabled}
               style={{ width: 56, paddingVertical: 13, alignItems: "center",
                        opacity: disabled ? 0.35 : 1 }}>
      <Text style={{ ...TYPE.body, fontSize: 20, color: palette.ink }}>{glyph}</Text>
    </Pressable>
  );
}
