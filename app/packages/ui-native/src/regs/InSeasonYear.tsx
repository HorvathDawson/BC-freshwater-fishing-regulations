/**
 * (T1167, owner 2026-11-25 "Banner only, but louder") WHAT THE KEEP BLOCKS SAY OF THE PAGE'S IN-SEASON NOTICES: the
 * status line's pointer while one is in force ("See the in-season notice above", core `inSeasonStatusLine`) and, on
 * every whole-year bar (the game-fish strip, the salmon bar, the trout table's year), a band over a dated notice's range
 * with its legend (core `inSeasonYearMarks`). The answer and the bars' own colours stay the book's: the band sits above
 * the bar, never over a day's fill. No words or logic here: the regulations page hands core's values down.
 */
import { createContext, useContext } from "react";
import { View } from "react-native";
import type { InSeasonYearVM } from "@app/core";
import { Text } from "../Text";
import { TYPE } from "../type";
import type { Palette } from "../theme";

export interface InSeasonKeep {
  /** the status line's pointer (core `inSeasonStatusLine`); null: no notice in force */
  readonly pointer: string | null;
  /** the dated notices' ranges on the book year (core `inSeasonYearMarks`); null: none */
  readonly marks: InSeasonYearVM | null;
}
export const InSeasonKeepContext = createContext<InSeasonKeep | null>(null);
export const useInSeasonKeep = (): InSeasonKeep | null => useContext(InSeasonKeepContext);

const BAND = 4;

/** the bands over a bar (the bar's own box: drawn just above it, on its days) */
export function InSeasonBand({ palette }: { palette: Palette }) {
  const marks = useInSeasonKeep()?.marks;
  if (!marks) return null;
  return (
    <>
      {marks.runs.map((r, i) => (
        <View key={i} testID="inseason-year-band" pointerEvents="none" aria-hidden importantForAccessibility="no-hide-descendants"
              style={{ position: "absolute", top: -(BAND + 2), height: BAND, borderRadius: 1, backgroundColor: palette.attention,
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
      <View style={{ width: 14, height: BAND, borderRadius: 1, backgroundColor: palette.attention }} />
      <Text testID="inseason-year-legend" style={[TYPE.legend, { color: palette.ink }]}>{marks.label}</Text>
    </View>
  );
}

/** the status line's pointer, under the day's answer */
export function InSeasonPointer({ palette }: { palette: Palette }) {
  const pointer = useInSeasonKeep()?.pointer;
  if (!pointer) return null;
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: 6, marginTop: 4 }}>
      <View style={{ width: 4, alignSelf: "stretch", borderRadius: 1, backgroundColor: palette.attention }} />
      <Text testID="st:inseason" style={[TYPE.metaBold, { color: palette.ink }]}>{pointer}</Text>
    </View>
  );
}
