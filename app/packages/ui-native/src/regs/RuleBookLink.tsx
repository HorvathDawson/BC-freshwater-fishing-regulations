/**
 * T34 + T43 — THE one book link (prototype `bookLinks` / `pageLink`): "Rule book p.21 ↗" on cards, tiers, gear lines,
 * sources and sheets; bare "p.21 ↗" after a "Rule book" label (glossary sheets); a page the PDF map lacks (pdf null) is
 * plain "p.21" — no arrow, no link role, no press. The label is core's `bookText` (the golden's spelling); a press opens the
 * synopsis through `onBook` (default `openBook`: web at `#page=<pdf>`, native the bare PDF — decisions D6 a). No logic.
 * Drawn alone (SourceLine) or nested inside a line's Text (LineText's book run): it is a Text either way.
 */
import { Text } from "react-native";
import { bookText, WORDS } from "@app/core";
import { TYPE, type TextStyle } from "../type";
import type { Palette } from "../theme";
import { MIN_HIT } from "./hit";
import { openBook } from "./openBook";
import { RegsPress } from "./RegsPress";

export function RuleBookLink({ printed, pdf, bare, palette, onBook = openBook, style, alone = false }: {
  printed: number; pdf: number | null; bare?: boolean; palette: Palette;
  /** what a press does with the PDF page (the screen's `actions.onBook`); default: open the synopsis there */
  onBook?: (pdf: number) => void;
  /** the face (a line's link style when nested); default the meta-bold link face */
  style?: TextStyle;
  /**
   * (T40) drawn on its own, not inside a line's Text (a source line's pages): the link is a press (`RegsPress`) grown to
   * 44 × 44 around its words. Inside a sentence it stays a nested Text — a run of the line, hit on its own glyphs.
   */
  alone?: boolean;
}) {
  const words = bookText({ printed, pdf, bare });
  const face = style ?? { fontFamily: TYPE.metaBold.fontFamily, fontSize: TYPE.metaBold.fontSize };
  // unlinked: not the link colour — inside a line it takes the line's colour (prototype `<span>`), alone the quiet one
  if (pdf === null) return <Text style={style ? face : [face, { color: palette.sub }]}>{words}</Text>;
  if (alone) return (
    <RegsPress accessibilityRole="link" accessibilityLabel={WORDS.BOOK_A11Y(printed)} onPress={() => { void onBook(pdf); }}
               box={{ w: bare ? 24 : MIN_HIT, h: TYPE.metaBold.lineHeight }}>
      <Text style={[face, { color: palette.accent }]}>{words}</Text>
    </RegsPress>
  );
  return (
    <Text accessibilityRole="link" accessibilityLabel={WORDS.BOOK_A11Y(printed)} onPress={() => { void onBook(pdf); }}
          style={[face, { color: palette.accent }]}>
      {words}
    </Text>
  );
}
