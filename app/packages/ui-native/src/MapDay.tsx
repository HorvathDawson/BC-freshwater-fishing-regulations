/**
 * THE DAY THE MAP IS COLOURED FOR — a stepper in the map's legend strip.
 *
 * The status colours (closed / own / base) are an answer about ONE day, read through
 * `statusOn` in @app/core; this picks which. `null` is today, and is what the map opens on:
 * a person looking at the map without touching this sees today, as before.
 *
 * Steppers, not a calendar sheet: « » a month, ‹ › a day, and the label goes back to today.
 * Every control is a 44 px target with its own accessible label, and the day is said in words
 * ("Oct 10"), never a bare number.
 */
import { dayLabel, shiftDay } from "@app/core";
import { Pressable, Text, View } from "react-native";
import type { Palette } from "./theme";
import { TYPE } from "./type";

// The day arithmetic and its words are `core/day.ts` (P12b); this only draws them.

export function MapDay({ palette, day, onDay }:
  { palette: Palette; day: Date | null; onDay: (d: Date | null) => void }) {
  const shown = day ?? new Date();
  const step = (n: number, months = false) => {
    const next = shiftDay(shown, n, months);
    const now = new Date();
    onDay(next.toDateString() === now.toDateString() ? null : next);
  };
  const Btn = ({ glyph, label, onPress }: { glyph: string; label: string; onPress: () => void }) => (
    <Pressable onPress={onPress} accessibilityRole="button" accessibilityLabel={label}
               hitSlop={6}
               style={{ minWidth: 32, height: 32, alignItems: "center", justifyContent: "center" }}>
      <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{glyph}</Text>
    </Pressable>
  );
  return (
    <View style={{ flexDirection: "row", alignItems: "center" }}
          accessibilityLabel={`Map coloured for ${day ? dayLabel(shown) : "today"}`}>
      <Btn glyph="«" label="One month earlier" onPress={() => step(-1, true)} />
      <Btn glyph="‹" label="One day earlier" onPress={() => step(-1)} />
      <Pressable onPress={() => onDay(null)} accessibilityRole="button"
                 accessibilityLabel={day ? `Showing ${dayLabel(shown)}; back to today` : "Showing today"}
                 style={{ minWidth: 64, height: 32, alignItems: "center", justifyContent: "center",
                          paddingHorizontal: 6, borderRadius: 6,
                          backgroundColor: day ? palette.accent : "transparent" }}>
        <Text style={{ ...TYPE.bodyStrong, color: day ? palette.onAccent : palette.ink }}>
          {day ? dayLabel(shown) : "Today"}
        </Text>
      </Pressable>
      <Btn glyph="›" label="One day later" onPress={() => step(1)} />
      <Btn glyph="»" label="One month later" onPress={() => step(1, true)} />
    </View>
  );
}
