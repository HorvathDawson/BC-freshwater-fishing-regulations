/**
 * (T810 O, contract 2.3 `YearBar`) THE TROUT TABLE'S YEAR BAR: "Trout and char through the year" under the table, drawn
 * only when Python set `table.year` (every trout and char fish shares one state today on an open water, owner 12:42).
 * The app decides nothing: `runs` are used only as lengths, each drawn with the page strip's own fills (`YearStrip`'s
 * `RunRect`, T935: stripes drawn as shapes): keep = the open fill, release = the release stripes, closed = the closed hatch; a `none` run
 * (no trout or char rule that day) a plain neutral fill, as the salmon bar's `unknown`. The day's marker and the month
 * initials are the page strip's own (`StripVM.todayAt` / `months`), passed in; none: not drawn.
 */
import { View } from "react-native";
import Svg, { Rect } from "react-native-svg";
import type { TroutVM } from "@app/core";
import { Text } from "../Text";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { GAP, H, RunRect, useBarWidth } from "./YearStrip";

/** a run's state as the strip's bits: the same fill as the page's game-fish bar (bit 1 closed, bit 2 release, 0 open) */
const BITS = { keep: 0, release: 2, closed: 1 } as const;

export function TroutYearBar({ year, palette, todayAt = null, months = [] }: {
  year: NonNullable<TroutVM["year"]>; palette: Palette; todayAt?: number | null; months?: readonly string[] }) {
  const bar = useBarWidth(), w = bar.at;
  const total = year.runs.reduce((a, [, d]) => a + d, 0) || 365;
  const room = Math.max(0, w - GAP * (year.runs.length - 1));
  let x = 0;
  const rects = year.runs.map(([state, days], i) => { const rw = (room * days) / total, at = x; x += rw + GAP; return { at, rw, state, i }; });
  return (
    <View testID="trout-year" style={{ marginTop: 14 }}>
      <Text testID="trout-year-title" style={[TYPE.kicker, { color: palette.sub }]}>{year.title}</Text>
      <View testID="trout-year-bar" style={{ marginTop: todayAt === null ? 8 : 14, height: H }} onLayout={(e) => bar.setW(e.nativeEvent.layout.width)}
            accessible accessibilityRole="image" accessibilityLabel={year.title}>
        {w > 0 && (
          <Svg {...bar.svg}>
            {rects.map((r) => r.state === "none"
              ? <Rect key={r.i} testID="trout-year-run:none" x={r.at} y={0} width={r.rw} height={H} fill={palette.line2} />
              : <RunRect key={r.i} x={r.at} w={r.rw} bits={BITS[r.state]} palette={palette} />)}
          </Svg>
        )}
        {w > 0 && todayAt !== null && (
          <View pointerEvents="none" aria-hidden importantForAccessibility="no-hide-descendants"
                style={{ position: "absolute", top: -5, ...bar.marker(todayAt), width: 2, height: H + 10, backgroundColor: palette.ink }} />
        )}
      </View>
      {months.length > 0 && (
        <View testID="trout-year-months" aria-hidden style={{ flexDirection: "row", justifyContent: "space-between", marginTop: 5 }}>
          {months.map((m, i) => <Text key={i} style={[TYPE.month, { color: palette.sub }]}>{m}</Text>)}
        </View>
      )}
    </View>
  );
}
