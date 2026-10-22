/**
 * T34 — an item's source line (prototype `itemSrc` → `.isrc`): who says so ("Province · Region 2 · This water’s ›", which
 * opens the Sources sheet of exactly those rules), then one "Rule book p.N ↗" per page, wrapping. The Licence tab's line
 * has no pages. No logic: every word and key is the VM's.
 */
import { Text, View } from "react-native";
import type { SourceLineVM } from "@app/core";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";
import type { KeepActions } from "./LineText";
import { MIN_HIT } from "./hit";
import { RegsPress } from "./RegsPress";
import { RuleBookLink } from "./RuleBookLink";

export function SourceLine({ src, palette, actions, pages = true, spaced = false }: {
  src: Pick<SourceLineVM, "who" | "aria" | "open"> & { readonly pages?: SourceLineVM["pages"] };
  palette: Palette; actions: Pick<KeepActions, "onSheet" | "onBook">;
  /** the Licence tab's line draws no pages (its VM has none) */
  pages?: boolean;
  /** a cell's line (.gxc > .isrc): --s2 above it instead of --s1 */
  spaced?: boolean;
}) {
  return (
    <View testID="regs-src" style={{ flexDirection: "row", flexWrap: "wrap", alignItems: "baseline", columnGap: 10, marginTop: spaced ? SPACE.s2 : SPACE.s1 }}>
      <RegsPress accessibilityRole="button" accessibilityLabel={src.aria} onPress={() => actions.onSheet(src.open)}
                 // (T40) it may wrap: at 2× text "Province · Region 2 · This water’s ›" is wider than a phone
                 box={{ w: MIN_HIT, h: TYPE.metaBold.lineHeight }} style={{ flexShrink: 1, maxWidth: "100%" }}>
        <Text style={[TYPE.metaBold, { color: palette.accent }]}>{src.who}</Text>
      </RegsPress>
      {pages && (src.pages ?? []).map((p) => <RuleBookLink key={p.printed} printed={p.printed} pdf={p.pdf} palette={palette} onBook={actions.onBook} alone />)}
    </View>
  );
}
