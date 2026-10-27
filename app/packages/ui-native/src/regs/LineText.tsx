/**
 * One `Line` (core regs/line.ts) drawn as ONE outer Text with nested runs (tokens.md §2.4: a big numeral is a nested Text
 * with no lineHeight of its own, so it never makes its line taller). No logic: every word is the VM's; a link only says
 * where it goes (`KeepActions`).
 */
import { createContext, useContext } from "react";
import { Text } from "react-native";
import { WORDS, type Line, type SourceBox, type SheetKey, type Day, type Tab } from "@app/core";
import { CAPPED_SCALE, TYPE, type TextStyle } from "../type";
import type { Palette } from "../theme";
import { lineRun } from "./hit";
import { RuleBookLink } from "./RuleBookLink";

/** What a tap on a Keep link does. Sheets (T36/T37) are not built yet: `onSheet` may be a no-op. */
export interface KeepActions {
  onSheet(key: SheetKey): void;
  onDay(day: Day): void;
  onTab(tab: Tab): void;
  onBook(pdf: number, box?: readonly SourceBox[]): void;
}
export const NO_ACTIONS: KeepActions = { onSheet() {}, onDay() {}, onTab() {}, onBook() {} };


/** (T40) the page's glossary titles (`RegsPage.terms`), provided by `RegsScreen` for every ⓘ under it, its sheets included */
export const TermTitles = createContext<Readonly<Record<string, string>>>({});

/** (T40) what an ⓘ says to a screen reader: "What does <title> mean?" (one id), "What these words mean" (two or more) */
export function termLabel(ids: readonly string[], titles: Readonly<Record<string, string>>): string {
  if (ids.length >= 2) return WORDS.TERMS_MANY;
  const t = titles[ids[0] ?? ""];
  return t ? WORDS.TERM_A11Y(t) : WORDS.TERM_A11Y_UNTITLED;
}

/** A term run's ⓘ (one id) or "What these words mean" (two or more), opening `gl:<ids>` — nested inside a line's Text. */
export function TermText({ ids, palette, actions }: { ids: readonly string[]; palette: Palette; actions: Pick<KeepActions, "onSheet"> }) {
  const titles = useContext(TermTitles);
  return (
    <Text accessibilityRole="button" accessibilityLabel={termLabel(ids, titles)} {...lineRun}
          onPress={() => actions.onSheet(`gl:${ids.join(",")}`)} style={{ color: palette.accent }}>
      {ids.length >= 2 ? WORDS.TERMS_MANY : WORDS.TERM_ONE}
    </Text>);
}

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
          // (T40) a number stops growing at 2× the reader's text size (a 26 px numeral at 3× would outgrow its line)
          case "num":
            return r.size === "inline" && !inlineBig
              ? <Text key={i} testID={`n:${r.size}`} maxFontSizeMultiplier={CAPPED_SCALE}
                      style={{ fontFamily: TYPE.regsBodyBold.fontFamily, color: numColor ?? color }}>{r.s}</Text>
              : <Text key={i} testID={`n:${r.size}`} maxFontSizeMultiplier={CAPPED_SCALE}
                      style={{ fontFamily: num.fontFamily, fontSize: num.fontSize, color: numColor ?? color }}>{r.s}</Text>;
          case "term":
            return <TermText key={i} ids={r.ids} palette={palette} actions={actions} />;
          case "book":   // (T43) the one book link, nested in this line's Text
            return <RuleBookLink key={i} printed={r.printed} pdf={r.pdf} bare={r.bare} box={r.box} palette={palette} onBook={actions.onBook}
                                 style={linkStyle ?? { fontFamily: TYPE.metaBold.fontFamily, fontSize: TYPE.metaBold.fontSize }} />;
          case "link": {
            const o = r.open;
            const go = () => (typeof o === "string" ? actions.onSheet(o) : "day" in o ? actions.onDay(o.day) : actions.onTab(o.tab));
            return <Text key={i} accessibilityRole="link" onPress={go} {...lineRun} style={[linkStyle ?? {}, { color: palette.accent }]}>{r.s}</Text>;
          }
        }
      })}
    </Text>
  );
}
