/**
 * T35 — the Reference fold that closes the Methods tab (prototype `details.gref`): a summary row — "Reference" small, the
 * bold title ("Handling fish"), the count ("21 rules that apply everywhere"), a chevron — folded by default; open, each
 * group's head and its items, then the source line. The fold's state is local (it changes no answer). No logic.
 */
import { useState } from "react";
import { Text, View } from "react-native";
import type { MethodsVM } from "@app/core";
import { Icon } from "../icons";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";
import type { KeepActions } from "./LineText";
import { RegsPress } from "./RegsPress";
import { SourceLine } from "./SourceLine";

export function ReferenceFold({ vm, palette, actions, initiallyOpen = false }: {
  vm: NonNullable<MethodsVM["reference"]>; palette: Palette; actions: KeepActions;
  /** closed, as the prototype (golden `open:false`); a render test may mount it open to read the folded text */
  initiallyOpen?: boolean;
}) {
  const [open, setOpen] = useState(initiallyOpen);
  return (
    <View testID="methods-reference" style={{ marginTop: SPACE.s3, borderTopWidth: 1.5, borderTopColor: palette.ink, paddingBottom: SPACE.s2 }}>
      <RegsPress testID="methods-reference-summary" accessibilityRole="button" aria-expanded={open} onPress={() => setOpen(!open)} box={{ w: 44, h: 56 }}
                 style={{ minHeight: 56, flexDirection: "row", flexWrap: "wrap", alignItems: "baseline", columnGap: 8, rowGap: 2, paddingVertical: 10 }}>
        <Text testID="kicker:reference" style={[TYPE.kicker, { width: "100%", color: palette.sub }]}>{vm.label}</Text>
        <Text style={[TYPE.regsChip, { color: palette.ink }]}>{vm.title}</Text>
        <Text style={[TYPE.metaSemi, { flex: 1, color: palette.sub }]}>{vm.sub}</Text>
        <View style={{ alignSelf: "center" }}><Icon name={open ? "collapse" : "expand"} colour={palette.sub} size={16} /></View>
      </RegsPress>
      {open && (
        <View testID="methods-reference-body">
          {vm.groups.map((g, i) => (
            <View key={i}>
              <Text style={[TYPE.metaBold, { color: palette.sub, marginTop: 10 }]}>{g.head}</Text>
              <View style={{ marginBottom: 6 }}>
                {g.items.map((t, j) => <Text key={j} style={[TYPE.regsBody, { color: palette.ink, paddingVertical: 2 }]}>{t}</Text>)}
              </View>
            </View>
          ))}
          {vm.src && <SourceLine src={vm.src} palette={palette} actions={actions} />}
        </View>
      )}
    </View>
  );
}
