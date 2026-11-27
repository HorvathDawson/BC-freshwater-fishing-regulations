/**
 * (T1211, owner 2026-11-26: "Floating \"Map key\" over the map, near the bottom") THE MAP TAB'S KEY, FLOATING. No bar under
 * the map: a chip near the map's bottom-left corner, the map showing round it, holding the three colours a reader decides by
 * as samples and the words "Map key". A press opens the whole key in a card above the chip (every entry, the same words,
 * swatches and isolate-on-tap as the bar had); a second press closes it. Prototyped and measured in T1200 (round 3: the bar
 * hid 66 px of map at 412 wide; the chip hides none of the map's height, 758 px of 915 shown against 692).
 *
 * WHERE IT SITS. It is the Shell's map foot (`map-foot`), so the scale bar and the attribution, which sit above the foot's
 * height (T275), are never under it, and the card never reaches the rail: its width stops `RAIL_CLEAR` short of the right
 * edge. The chip is at least 44 x 44 (a thumb's target) and is named aloud with what a press does (`MAP_KEY.chip`). Its
 * word is one line and grows with the system text up to `LABEL_SCALE` (type.ts, T1201): the chip has no fixed width, so it
 * grows round its word, never cuts it.
 */
import { useState, type ReactNode } from "react";
import { useWindowDimensions, View } from "react-native";
import { MAP_KEY } from "@app/core";
import { Pressable } from "../Pressable";
import { Text } from "../Text";
import { LegendRows } from "../Chrome";
import { LABEL_PROPS, LEGEND_TEXT, TYPE } from "../type";
import type { Palette } from "../theme";

/** room kept clear at the right for the map's rail (locate, layers, Trout | Salmon), px */
export const RAIL_CLEAR = 76;
/** the chip's least height and width: a thumb's target */
export const KEY_TARGET = 44;

export function MapKey({ palette, rows, samples }: {
  palette: Palette;
  /** the opened key's rows (`LegendRows`): the colours a reader decides by first, then the rest, as the bar had them */
  rows: readonly (readonly ReactNode[])[];
  /** the chip's samples: the lead colours, drawn as short lines */
  samples: readonly string[];
}) {
  const [open, setOpen] = useState(false);
  const { width } = useWindowDimensions();
  return (
    <View testID="map-key" pointerEvents="box-none" style={{ paddingHorizontal: 10, paddingBottom: 10, rowGap: 8, alignItems: "flex-start" }}>
      {open && (
        <View testID="map-key-card" accessibilityLabel={MAP_KEY.chip.label}
              style={{ backgroundColor: palette.card, borderWidth: 1, borderColor: palette.line, borderRadius: 10, padding: 10,
                       rowGap: 6, maxWidth: Math.max(200, width - RAIL_CLEAR - 10), ...palette.lift }}>
          <LegendRows rows={rows.filter((r) => r.length > 0)} />
        </View>
      )}
      <Pressable testID="map-key-chip" accessibilityRole="button" accessibilityLabel={MAP_KEY.chip.label}
                 accessibilityHint={open ? MAP_KEY.chip.hintClose : MAP_KEY.chip.hintOpen} aria-expanded={open}
                 onPress={() => setOpen(!open)}
                 style={{ minHeight: KEY_TARGET, minWidth: KEY_TARGET, flexDirection: "row", alignItems: "center", columnGap: 6,
                          paddingHorizontal: 12, borderRadius: 999, borderWidth: 1, borderColor: palette.line,
                          backgroundColor: palette.card, ...palette.lift }}>
        {samples.map((c, i) => <View key={`${c}-${i}`} style={{ width: 14, height: 4, borderRadius: 2, backgroundColor: c }} />)}
        <Text {...LABEL_PROPS} style={{ ...TYPE.small, fontSize: LEGEND_TEXT - 1, lineHeight: 20, color: palette.accent }}>
          {MAP_KEY.chip.word} {open ? "▾" : "▴"}
        </Text>
      </Pressable>
    </View>
  );
}
