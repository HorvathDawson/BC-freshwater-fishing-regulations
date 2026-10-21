/**
 * T34 — the Bait item (prototype `.gxc[data-k=bait]`): its label, its notes (a Line each — the ⓘ is a term run), the dated
 * bans as changes lines, the ✓/✕ list, its source line. `also_allowed` is not drawn (D18 a, parity). No logic.
 */
import { View } from "react-native";
import type { BaitVM } from "@app/core";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { ChangesLine } from "./ChangesLine";
import { LineText, type KeepActions } from "./LineText";
import { SourceLine } from "./SourceLine";
import { Cell } from "./TabSection";
import { YesNoList } from "./YesNoList";

export function BaitList({ vm, palette, actions }: { vm: BaitVM; palette: Palette; actions: KeepActions }) {
  return (
    <Cell label={vm.label} palette={palette} testID="gear-bait">
      {vm.notes.map((n, i) => (
        <View key={i} style={{ marginTop: 2 }}>
          <LineText line={n} style={TYPE.metaSemi} color={palette.sub} palette={palette} actions={actions} />
        </View>
      ))}
      {vm.dated.map((c, i) => <ChangesLine key={i} change={c} palette={palette} />)}
      <YesNoList list={vm.list} palette={palette} actions={actions} />
      {vm.src && <SourceLine src={vm.src} palette={palette} actions={actions} spaced />}
    </Cell>
  );
}
