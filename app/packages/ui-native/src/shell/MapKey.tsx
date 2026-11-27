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
 *
 * A CLASS SHOWN ALONE IS NEVER HIDDEN (T377's isolate, the review of T1211). A tap on an entry greys every other class on
 * the map; with the key closed nothing else on screen would say why. So the chip itself says it, open or closed: the one
 * class's sample and "<class> only", with "Show all" beside it, one tap to every class again. Closing the key KEEPS the
 * class alone: the isolate exists to look at the map, which the open card partly covers, so clearing it on close would make
 * it useless on a phone; the chip's words are what keep it honest.
 *
 * CLOSING. A second press on the chip, the one back (Android's back, the browser's: `useBackLayer`, app/BACK.md), or a tap
 * on the map (`noteMapTap`, called by the map's own tap: a tap anywhere on the map, never only inside the foot, which on
 * Android cannot be hit outside its own bounds).
 */
import { useCallback, useEffect, useState, type ReactNode } from "react";
import { useWindowDimensions, View } from "react-native";
import { MAP_KEY } from "@app/core";
import { useBackLayer } from "@app/ui";
import { Pressable } from "../Pressable";
import { Text } from "../Text";
import { LegendRows } from "../Chrome";
import { LABEL_PROPS, LEGEND_TEXT, TYPE } from "../type";
import type { Palette } from "../theme";

/** room kept clear at the right for the map's rail (locate, layers, Trout | Salmon), px */
export const RAIL_CLEAR = 76;
/** the chip's least height and width: a thumb's target */
export const KEY_TARGET = 44;

/** (T1211) a tap on the map: the open key closes (the map's tap still does what it does) */
const mapTapped = new Set<() => void>();
export function noteMapTap(): void { for (const f of [...mapTapped]) f(); }

export function MapKey({ palette, rows, lines, samples, isolated = null, onShowAll }: {
  palette: Palette;
  /** the opened key's rows (`LegendRows`): the colours a reader decides by first, then the rest, as the bar had them */
  rows: readonly (readonly ReactNode[])[];
  /** under the rows: the map's other marks (the region boundary, the fish obstacles while on), not regulations classes */
  lines?: ReactNode;
  /** the chip's samples: the lead colours, drawn as short lines */
  samples: readonly string[];
  /** (T377) the class shown alone on the map, its key words and colour; null while every class is drawn */
  isolated?: { label: string; colour: string } | null;
  /** every class again (the isolate cleared) */
  onShowAll?: () => void;
}) {
  const [keyOpen, setKeyOpen] = useState(false);
  const close = useCallback(() => setKeyOpen(false), []);
  useBackLayer(keyOpen, close);
  useEffect(() => {
    if (!keyOpen) return;
    mapTapped.add(close);
    return () => { mapTapped.delete(close); };
  }, [keyOpen, close]);
  const { width } = useWindowDimensions();
  const word = isolated ? MAP_KEY.chip.only(isolated.label) : MAP_KEY.chip.word;
  const chipSamples = isolated ? [isolated.colour] : samples;
  return (
    <View testID="map-key" pointerEvents="box-none" style={{ paddingHorizontal: 10, paddingBottom: 10, rowGap: 8, alignItems: "flex-start" }}>
      {keyOpen && (
        <View testID="map-key-card" accessibilityLabel={MAP_KEY.chip.label}
              style={{ backgroundColor: palette.card, borderWidth: 1, borderColor: palette.line, borderRadius: 10, padding: 10,
                       rowGap: 6, maxWidth: Math.max(200, width - RAIL_CLEAR - 10), ...palette.lift }}>
          <LegendRows rows={rows.filter((r) => r.length > 0)} />
          {lines != null && <View testID="map-key-lines" style={{ rowGap: 6 }}>{lines}</View>}
        </View>
      )}
      <View pointerEvents="box-none" style={{ flexDirection: "row", flexWrap: "wrap", alignItems: "center", columnGap: 8, rowGap: 8,
                                              maxWidth: Math.max(200, width - RAIL_CLEAR - 10) }}>
        <Pressable testID="map-key-chip" accessibilityRole="button"
                   accessibilityLabel={isolated ? `${MAP_KEY.chip.label}, ${word}` : MAP_KEY.chip.label}
                   accessibilityHint={keyOpen ? MAP_KEY.chip.hintClose : MAP_KEY.chip.hintOpen} aria-expanded={keyOpen}
                   onPress={() => setKeyOpen(!keyOpen)}
                   style={{ minHeight: KEY_TARGET, minWidth: KEY_TARGET, flexDirection: "row", alignItems: "center", columnGap: 6,
                            paddingHorizontal: 12, borderRadius: 999, borderWidth: 1, borderColor: palette.line,
                            backgroundColor: palette.card, ...palette.lift }}>
          {chipSamples.map((c, i) => <View key={`${c}-${i}`} style={{ width: 14, height: 4, borderRadius: 2, backgroundColor: c }} />)}
          <Text testID="map-key-word" {...LABEL_PROPS}
                style={{ ...TYPE.small, fontSize: LEGEND_TEXT - 1, lineHeight: 20, color: isolated ? palette.ink : palette.accent }}>
            {word} {keyOpen ? "▾" : "▴"}
          </Text>
        </Pressable>
        {isolated && onShowAll && (
          <Pressable testID="map-key-show-all" accessibilityRole="button" accessibilityLabel={MAP_KEY.chip.showAll}
                     accessibilityHint={MAP_KEY.chip.showAllHint} onPress={onShowAll}
                     style={{ minHeight: KEY_TARGET, minWidth: KEY_TARGET, justifyContent: "center", paddingHorizontal: 14,
                              borderRadius: 999, borderWidth: 1, borderColor: palette.accent, backgroundColor: palette.card, ...palette.lift }}>
            <Text {...LABEL_PROPS} style={{ ...TYPE.small, fontSize: LEGEND_TEXT - 1, lineHeight: 20, color: palette.accent }}>
              {MAP_KEY.chip.showAll}
            </Text>
          </Pressable>
        )}
      </View>
    </View>
  );
}
