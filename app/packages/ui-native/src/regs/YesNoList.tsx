/**
 * T34 — ONE ✓ Allowed / ✕ Not allowed list (prototype `yesNo`, round 20g): per column a small head, then rows — the mark
 * (✓ open, ✕ closed; hidden from a screen reader, the head says it), the name, an ⓘ when the item carries terms (T35: the
 * set_line ⓘ), and its note small under the name. No logic.
 */
import { Text, View } from "react-native";
import type { YesNoVM } from "@app/core";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";
import { TermText, type KeepActions } from "./LineText";

export function YesNoList({ list, palette, actions }: { list: YesNoVM; palette: Palette; actions: Pick<KeepActions, "onSheet"> }) {
  return (
    <View testID="regs-yesno" style={{ marginTop: SPACE.s1 }}>
      {list.map((c, ci) => (
        <View key={c.col} style={{ marginTop: ci ? SPACE.s2 : 0 }}>
          <Text style={[TYPE.metaBold, { color: palette.sub, marginBottom: 2 }]}>{c.head}</Text>
          {c.items.map((it, i) => (
            // (T40) no label on the row: it would hide the name, the note and the ⓘ inside it from a screen reader. The column's
            // head ("Allowed" / "Not allowed") is read first, as text; the ✓ / ✕ only repeats it, so it is hidden. Its column is
            // at LEAST 20 wide: at 2× text the mark is 30, and a fixed 20 drew it over the name
            <View key={i} style={{ flexDirection: "row", alignItems: "baseline", paddingVertical: 3 }}>
              <Text aria-hidden testID={c.col === "ok" ? "st:open" : "st:closed"} style={[TYPE.regsChip, { minWidth: 20, color: c.col === "ok" ? palette.open : palette.closed }]}>{it.mark}</Text>
              <Text style={[TYPE.regsBodySemi, { color: palette.ink, flex: 1 }]}>
                {it.name}
                {it.terms.length > 0 && <> <TermText ids={it.terms} palette={palette} actions={actions} /></>}
                {it.note !== null && <Text style={[TYPE.meta, { color: palette.sub }]}>{"\n"}{it.note}</Text>}
              </Text>
            </View>
          ))}
        </View>
      ))}
    </View>
  );
}
