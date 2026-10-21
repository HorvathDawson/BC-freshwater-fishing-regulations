/**
 * T35 — Ways to fish (prototype `WAYS` → `.gxc[data-k=methods]`): its label, ONE ✓/✕ list (conditions small under each
 * allowed way; the set_line ⓘ on an allowed "Set lines"), its source line. No logic.
 */
import type { MethodsVM } from "@app/core";
import type { Palette } from "../theme";
import type { KeepActions } from "./LineText";
import { SourceLine } from "./SourceLine";
import { Cell } from "./TabSection";
import { YesNoList } from "./YesNoList";

export function WaysList({ vm, palette, actions }: { vm: NonNullable<MethodsVM["ways"]>; palette: Palette; actions: KeepActions }) {
  return (
    <Cell label={vm.label} palette={palette} testID="methods-ways">
      <YesNoList list={vm.list} palette={palette} actions={actions} />
      {vm.src && <SourceLine src={vm.src} palette={palette} actions={actions} spaced />}
    </Cell>
  );
}
