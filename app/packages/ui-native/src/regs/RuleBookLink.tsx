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
import { openBook } from "./openBook";

export function RuleBookLink({ printed, pdf, bare, palette, onBook = openBook, style }: {
  printed: number; pdf: number | null; bare?: boolean; palette: Palette;
  /** what a press does with the PDF page (the screen's `actions.onBook`); default: open the synopsis there */
  onBook?: (pdf: number) => void;
  /** the face (a line's link style when nested); default the meta-bold link face */
  style?: TextStyle;
}) {
  const words = bookText({ printed, pdf, bare });
  const face = style ?? { fontFamily: TYPE.metaBold.fontFamily, fontSize: TYPE.metaBold.fontSize };
  // unlinked: not the link colour — inside a line it takes the line's colour (prototype `<span>`), alone the quiet one
  if (pdf === null) return <Text style={style ? face : [face, { color: palette.sub }]}>{words}</Text>;
  return (
    <Text accessibilityRole="link" accessibilityLabel={WORDS.BOOK_A11Y(printed)} onPress={() => { void onBook(pdf); }}
          style={[face, { color: palette.accent }]}>
      {words}
    </Text>
  );
}
