/**
 * (T1167, owner 2026-11-25 "Banner only, but louder") WHAT THE KEEP BLOCKS SAY OF THE PAGE'S IN-SEASON NOTICES: the
 * status line's pointer while one is in force ("See the in-season notice above", core `inSeasonKeepOf`: on the game-fish
 * strip when the page draws it, else at the top of the Keep tab, so every page that answers carries it) and, on
 * every whole-year bar (the game-fish strip, the salmon bar, the trout table's year), a band over a dated notice's range
 * with its legend. The answer and the bars' own colours stay the book's: the band sits above
 * the bar, never over a day's fill. No words or logic here: the regulations page hands core's values down.
 */
import { createContext, useContext } from "react";
import { View } from "react-native";
import type { InSeasonKeepVM } from "@app/core";
import { Text } from "../Text";
import { TYPE } from "../type";
import { GUTTER, type Palette } from "../theme";

/** core `inSeasonKeepOf`: the pointer, where it is drawn, and the year bars' marks */
export const InSeasonKeepContext = createContext<InSeasonKeepVM | null>(null);
const useInSeasonKeep = (): InSeasonKeepVM | null => useContext(InSeasonKeepContext);

const BAND = 4;

/** the bands over a bar (the bar's own box: drawn just above it, on its days) */
export function InSeasonBand({ palette }: { palette: Palette }) {
  const marks = useInSeasonKeep()?.marks;
  if (!marks) return null;
  return (
    <>
      {marks.runs.map((r, i) => (
        <View key={i} testID="inseason-year-band" pointerEvents="none" aria-hidden importantForAccessibility="no-hide-descendants"
              style={{ position: "absolute", top: -(BAND + 2), height: BAND, borderRadius: palette.r.chip, backgroundColor: palette.attention,
                       left: `${r.at * 100}%` as const, width: `${r.len * 100}%` as const }} />
      ))}
    </>
  );
}

/** the band's legend entry: its swatch and core's words ("In-season notice, Jul 30–Sep 15") */
export function InSeasonLegend({ palette }: { palette: Palette }) {
  const marks = useInSeasonKeep()?.marks;
  if (!marks) return null;
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: 5 }}>
      <View style={{ width: 14, height: BAND, borderRadius: palette.r.chip, backgroundColor: palette.attention }} />
      <Text testID="inseason-year-legend" style={[TYPE.legend, { color: palette.ink }]}>{marks.label}</Text>
    </View>
  );
}

/** the status line's pointer, drawn where core says (`on`: the strip's status line, or the top of the Keep tab) */
export function InSeasonPointer({ palette, on }: { palette: Palette; on: InSeasonKeepVM["pointerOn"] }) {
  const k = useInSeasonKeep();
  if (!k?.pointer || k.pointerOn !== on) return null;
  // (T1167, owner 2026-11-26) the banner's warning look: the amber bar and band, the caution mark (hidden from a screen
  // reader), the words ink
  return (
    <View style={on === "tab" ? { paddingTop: 10, paddingHorizontal: GUTTER } : { marginTop: 4 }}>
      <View testID="inseason-pointer" style={{ flexDirection: "row", alignItems: "center", gap: 6, alignSelf: "flex-start",
                                               backgroundColor: palette.noAccessBand, borderLeftWidth: 4, borderLeftColor: palette.warning,
                                               paddingVertical: 2, paddingLeft: 6, paddingRight: 8 }}>
        {k.pointerWarn && <Text testID="st:inseason-warn" aria-hidden style={[TYPE.metaBold, { color: palette.ink }]}>{k.pointerWarn}</Text>}
        <Text testID="st:inseason" style={[TYPE.metaBold, { color: palette.ink }]}>{k.pointer}</Text>
      </View>
    </View>
  );
}
