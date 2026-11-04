/**
 * (T367, owner 2026-11-04: "a drop down is better for the days with now and change outside the dropdown") THE HORIZON
 * CONTROL, for the Conditions map's corner and the panel's header alike: one joined strip (`CornerStrip`) of
 * Now · <the forecast day> ▾ · Change. Ten dated chips in a row outgrew the width and pushed Change out of sight; now
 * the middle segment says "Forecast" (or the day chosen, filled) and opens the forecast days under the strip, two rows of
 * dated chips. (owner 2026-11-04: closing on a pick made stepping through the days clunky) The list STAYS OPEN after a
 * pick — the day lights, the map repaints — and closes on the middle segment again, or on Now or Change. The chips are `horizonChips` (@app/ui), so both controls list the same days and set
 * ONE horizon state.
 */
import { useState } from "react";
import { Pressable, ScrollView, Text, View } from "react-native";
import { HORIZON_PICK } from "@app/core";
import { useBackLayer, type Horizon, type HorizonChipVM } from "@app/ui";
import { CornerStrip, type CornerSegment } from "./CornerStrip";
import { hitProps } from "./regs/hit";
import { TYPE } from "./type";
import type { Palette } from "./theme";

const DAY_HIT = hitProps({ w: 52, h: 34 });

export function HorizonPicker({ palette, chips, onPick, testID, coach, changeIcon = false }: {
  palette: Palette; chips: readonly HorizonChipVM[]; onPick: (d: Horizon) => void; testID: string; coach?: string;
  /** (T367) the panel's header draws Change with the trend glyph beside its word (no heavier divider: it read as a break) */
  changeIcon?: boolean;
}) {
  const [open, setOpen] = useState(false);
  // the open day list is a layer: back closes it first
  useBackLayer(open, () => setOpen(false));
  const days = chips.filter((c) => typeof c.day === "number" && c.day > 0);
  const lit = days.find((c) => c.on) ?? null;
  const seg = (h: HorizonChipVM): CornerSegment => ({
    key: String(h.day), chip: h.chip, label: h.label, on: h.on,
    ...(h.note ? { note: h.note } : {}), ...(changeIcon && h.day === "change" ? { icon: "delta" as const } : {}),
    onPress: () => { setOpen(false); onPick(h.day); },
  });
  const now = chips.find((c) => c.day === 0), change = chips.find((c) => c.day === "change");
  const segments: CornerSegment[] = [
    ...(now ? [seg(now)] : []),
    ...(days.length ? [{
      key: "days", chip: lit ? lit.chip : HORIZON_PICK.forecast, on: !!lit, expanded: open,
      label: lit ? lit.label : HORIZON_PICK.open,
      trail: open ? "collapse" as const : "expand" as const, onPress: () => setOpen((o) => !o),
    }] : []),
    ...(change ? [seg(change)] : []),
  ];
  return (
    <View style={{ alignItems: "flex-start", gap: 6, maxWidth: "100%" }} pointerEvents="box-none">
      <CornerStrip palette={palette} testID={testID} coach={coach} segments={segments} />
      {/* (T367, owner 2026-11-04: "big shift here") ONE ROW of days under the strip, as wide as the strip, scrolling
          sideways — the three-row grid pushed the page down and pulled the heading beside it off its line */}
      {open && (
        <ScrollView horizontal showsHorizontalScrollIndicator={false} style={{ alignSelf: "stretch", flexGrow: 0 }}
                    contentContainerStyle={{ gap: 4 }}>
        <View testID={`${testID}-list`} accessibilityRole="menu" accessibilityLabel={HORIZON_PICK.list}
              style={{ flexDirection: "row", gap: 4 }}>
          {days.map((d) => (
            <Pressable key={String(d.day)} testID={`${testID}-day-${d.day}`} onPress={() => onPick(d.day)}
                       accessibilityRole="menuitem" accessibilityLabel={d.label} aria-selected={d.on} {...DAY_HIT}>
              <View style={{ width: 58, paddingVertical: 6, alignItems: "center", borderRadius: palette.r.pill,
                             borderWidth: 1, borderColor: d.on ? palette.accent : palette.line2,
                             backgroundColor: d.on ? palette.accent : palette.card }}>
                <Text style={{ ...TYPE.micro, fontSize: 13, lineHeight: 17, fontWeight: "600",
                               color: d.on ? palette.onAccent : palette.ink }}>{d.chip}</Text>
              </View>
            </Pressable>
          ))}
        </View>
        </ScrollView>
      )}
    </View>
  );
}
