/**
 * T35 — Boats (prototype `.gxc[data-k=boats]`): its label, the water's boat rules as a plain list (`ul.gxi.plain`, --t4),
 * its source line. No logic.
 */
import { Text, View } from "react-native";
import type { MethodsVM } from "@app/core";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";
import type { KeepActions } from "./LineText";
import { SourceLine } from "./SourceLine";
import { Cell } from "./TabSection";

export function Boats({ vm, palette, actions }: { vm: NonNullable<MethodsVM["boats"]>; palette: Palette; actions: KeepActions }) {
  return (
    <Cell label={vm.label} palette={palette} testID="methods-boats">
      <View style={{ marginTop: SPACE.s1 }}>
        {vm.items.map((t, i) => <Text key={i} style={[TYPE.meta, { color: palette.ink }]}>{t}</Text>)}
      </View>
      {vm.src && <SourceLine src={vm.src} palette={palette} actions={actions} spaced />}
    </Cell>
  );
}
