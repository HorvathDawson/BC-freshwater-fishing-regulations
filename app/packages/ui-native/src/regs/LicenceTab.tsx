/**
 * T35 — the Licence tab drawn from `LicenceVM` (prototype `licence()`): the "Licence" section — who is fishing (two
 * choices), the profile's label, the answer's blocks, and the source line without pages (it opens `licsrc:<profile>`).
 * On a closed day (D16 a) only the heading: the VM carries no axes, blocks or source. No logic.
 */
import { Text } from "react-native";
import { WORDS, type LicenceVM, type Who } from "@app/core";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { LicenceAnswer } from "./LicenceAnswer";
import type { KeepActions } from "./LineText";
import { SourceLine } from "./SourceLine";
import { TabSection } from "./TabSection";
import { WhoSelector } from "./WhoSelector";

export function LicenceTab({ vm, who, palette, actions, onWho }: {
  vm: LicenceVM; who: Who; palette: Palette; actions: KeepActions; onWho: (w: Who) => void;
}) {
  return (
    <TabSection title={WORDS.TAB_LICENCE} palette={palette} testID="licence-tab">
      <WhoSelector vm={vm} who={who} palette={palette} onWho={onWho} actions={actions} />
      {vm.label !== "" && <Text style={[TYPE.metaBold, { color: palette.sub, marginTop: 12, marginBottom: 2 }]}>{vm.label}</Text>}
      <LicenceAnswer blocks={vm.blocks} palette={palette} actions={actions} />
      {vm.src && <SourceLine src={vm.src} palette={palette} actions={actions} pages={false} spaced />}
    </TabSection>
  );
}
