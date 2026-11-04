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
import { Pressable, Text, View } from "react-native";
import { HORIZON_PICK } from "@app/core";
import type { Horizon, HorizonChipVM } from "@app/ui";
import { CornerStrip, type CornerSegment } from "./CornerStrip";
import { hitProps } from "./regs/hit";
import { TYPE } from "./type";
import type { Palette } from "./theme";

const DAY_HIT = hitProps({ w: 52, h: 34 });

export function HorizonPicker({ palette, chips, onPick, testID, coach, changeIcon = false }: {
  palette: Palette; chips: readonly HorizonChipVM[]; onPick: (d: Horizon) => void; testID: string; coach?: string;
  /** the panel's header draws Change as the trend glyph, a mode set off by a heavier divider */
  changeIcon?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const days = chips.filter((c) => typeof c.day === "number" && c.day > 0);
  const lit = days.find((c) => c.on) ?? null;
  const seg = (h: HorizonChipVM): CornerSegment => ({
    key: String(h.day), chip: changeIcon && h.day === "change" ? "" : h.chip, label: h.label, on: h.on,
    ...(h.note ? { note: h.note } : {}), ...(changeIcon && h.day === "change" ? { icon: "delta" as const, mode: true } : {}),
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
    <View style={{ alignItems: "flex-start", gap: 6 }} pointerEvents="box-none">
      <CornerStrip palette={palette} testID={testID} coach={coach} segments={segments} />
      {open && (
        <View testID={`${testID}-list`} accessibilityRole="menu" accessibilityLabel={HORIZON_PICK.list}
              style={{ flexDirection: "row", flexWrap: "wrap", gap: 6, maxWidth: 5 * 58 + 4 * 6 + 16, padding: 8,
                       backgroundColor: palette.card, borderWidth: 1, borderColor: palette.line2,
                       borderRadius: palette.r.box, ...palette.lift }}>
          {days.map((d) => (
            <Pressable key={String(d.day)} testID={`${testID}-day-${d.day}`} onPress={() => onPick(d.day)}
                       accessibilityRole="menuitem" accessibilityLabel={d.label} aria-selected={d.on} {...DAY_HIT}>
              <View style={{ width: 58, paddingVertical: 8, alignItems: "center", borderRadius: palette.r.pill,
                             backgroundColor: d.on ? palette.accent : palette.wash }}>
                <Text style={{ ...TYPE.micro, fontSize: 13, lineHeight: 17, fontWeight: "600",
                               color: d.on ? palette.onAccent : palette.ink }}>{d.chip}</Text>
              </View>
            </Pressable>
          ))}
        </View>
      )}
    </View>
  );
}
