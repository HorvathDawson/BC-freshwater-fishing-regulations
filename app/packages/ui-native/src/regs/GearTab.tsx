/**
 * T34 — the Gear tab drawn from `GearVM` (prototype buildGear): on a day with no fishing, only its sentence (and, on tidal
 * water, the Keep banner's paragraphs); otherwise the "Gear" section — the day line, the gear lines, the Bait item. No
 * logic: which blocks and every word are core's (gear.ts, bait.ts).
 */
import { Text, View } from "react-native";
import { WORDS, type GearVM } from "@app/core";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";
import { BaitList } from "./BaitList";
import { ClosedNote } from "./ClosedNote";
import { GearLines } from "./GearLines";
import type { KeepActions } from "./LineText";
import { TabSection } from "./TabSection";

export function GearTab({ vm, palette, actions }: { vm: GearVM; palette: Palette; actions: KeepActions }) {
  if (vm.closed !== null || vm.closedMore.length)
    return (
      <View testID="gear-tab" style={{ marginTop: SPACE.s2 }}>
        {[vm.closed, ...vm.closedMore].map((t, i) => (t ? <ClosedNote key={i} text={t} palette={palette} /> : null))}
      </View>
    );
  return (
    <View testID="gear-tab" style={{ paddingBottom: SPACE.s2 }}>
      <TabSection title={WORDS.TAB_GEAR} palette={palette}>
        {vm.day !== null && <Text style={[TYPE.metaBold, { color: palette.sub, marginBottom: 4 }]}>{vm.day}</Text>}
        <GearLines mode={vm.mode} lines={vm.lines} palette={palette} actions={actions} />
        {vm.bait && <View style={{ marginTop: SPACE.s2, borderTopWidth: 1, borderTopColor: palette.line2 }}><BaitList vm={vm.bait} palette={palette} actions={actions} /></View>}
      </TabSection>
    </View>
  );
}
