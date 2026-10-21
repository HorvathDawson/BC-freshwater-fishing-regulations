/**
 * T34 — one "Rule book p.8 ↗" link (prototype `a.book`): the label is LineText's `bookWords`, the tap says which PDF page
 * (`actions.onBook`; the screen opens `bookUrl(pdf, Platform.OS)`, decisions D6 a). No logic.
 */
import { Text } from "react-native";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { bookWords, type KeepActions } from "./LineText";

export function RuleBookLink({ printed, pdf, bare, palette, actions }: {
  printed: number; pdf: number | null; bare?: boolean; palette: Palette; actions: Pick<KeepActions, "onBook">;
}) {
  return (
    <Text accessibilityRole={pdf === null ? undefined : "link"} onPress={pdf === null ? undefined : () => actions.onBook(pdf)}
          style={[TYPE.metaBold, { color: palette.accent }]}>
      {bookWords({ printed, pdf, bare })}
    </Text>
  );
}
