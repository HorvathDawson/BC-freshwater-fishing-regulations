/**
 * One `Line` (core regs/line.ts) drawn as ONE outer Text with nested runs (tokens.md §2.4: a big numeral is a nested Text
 * with no lineHeight of its own, so it never makes its line taller). No logic: every word is the VM's; a link only says
 * where it goes (`KeepActions`).
 */
import { Text } from "react-native";
import { WORDS, type Line, type Run, type SheetKey, type Day, type Tab } from "@app/core";
import { TYPE, type TextStyle } from "../type";
import type { Palette } from "../theme";

/** What a tap on a Keep link does. Sheets (T36/T37) are not built yet: `onSheet` may be a no-op. */
export interface KeepActions {
  onSheet(key: SheetKey): void;
  onDay(day: Day): void;
  onTab(tab: Tab): void;
  onBook(pdf: number): void;
}
export const NO_ACTIONS: KeepActions = { onSheet() {}, onDay() {}, onTab() {}, onBook() {} };

const bookWords = (r: Extract<Run, { t: "book" }>): string =>
  r.pdf === null ? `${WORDS.PAGE}${r.printed}` : `${r.bare ? "" : WORDS.BOOK + " "}${WORDS.PAGE}${r.printed} ${WORDS.LINK_OUT}`;

export function LineText({ line, style, color, palette, actions, num = TYPE.numeral, numColor, linkStyle, testID, numberOfLines, inlineBig = false }: {
  line: Line; style: TextStyle; color: string; palette: Palette; actions: KeepActions;
  /** the face of a "big" num run (tier / head: TYPE.numeral; the group's total line: numeralMid) */
  num?: TextStyle; numColor?: string; linkStyle?: TextStyle; testID?: string; numberOfLines?: number;
  /** a kind sentence: its "inline" number is set big too (prototype .kt .ks .kn: --t1 800) */
  inlineBig?: boolean;
}) {
  return (
    <Text style={[style, { color }]} testID={testID} numberOfLines={numberOfLines}>
      {line.map((r, i) => {
        switch (r.t) {
          case "text": return <Text key={i}>{r.s}</Text>;
          case "num":
            return r.size === "inline" && !inlineBig
              ? <Text key={i} style={{ fontFamily: TYPE.regsBodyBold.fontFamily, color: numColor ?? color }}>{r.s}</Text>
              : <Text key={i} style={{ fontFamily: num.fontFamily, fontSize: num.fontSize, color: numColor ?? color }}>{r.s}</Text>;
          case "term":
            return (
              <Text key={i} accessibilityRole="button" accessibilityLabel={WORDS.TERMS_MANY}
                    onPress={() => actions.onSheet(`gl:${r.ids.join(",")}`)} style={{ color: palette.accent }}>
                {r.ids.length >= 2 ? WORDS.TERMS_MANY : WORDS.TERM_ONE}
              </Text>);
          case "book":
            return (
              <Text key={i} accessibilityRole={r.pdf === null ? undefined : "link"} onPress={r.pdf === null ? undefined : () => actions.onBook(r.pdf!)}
                    style={[linkStyle ?? { fontFamily: TYPE.metaBold.fontFamily, fontSize: TYPE.metaBold.fontSize }, { color: palette.accent }]}>
                {bookWords(r)}
              </Text>);
          case "link": {
            const o = r.open;
            const go = () => (typeof o === "string" ? actions.onSheet(o) : "day" in o ? actions.onDay(o.day) : actions.onTab(o.tab));
            return <Text key={i} accessibilityRole="link" onPress={go} style={[linkStyle ?? {}, { color: palette.accent }]}>{r.s}</Text>;
          }
        }
      })}
    </Text>
  );
}
