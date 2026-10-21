/**
 * T35 — the Methods tab drawn from `MethodsVM` (prototype buildGear's methods half): on a day with no fishing, only its
 * sentence; otherwise the "Methods" section (Ways to fish, Boats) and the Reference fold that closes the tab. No logic:
 * which blocks and every word are core's (methods.ts).
 */
import { View } from "react-native";
import { WORDS, type MethodsVM } from "@app/core";
import { GUTTER, SPACE, type Palette } from "../theme";
import { Boats } from "./Boats";
import { ClosedNote } from "./ClosedNote";
import type { KeepActions } from "./LineText";
import { ReferenceFold } from "./ReferenceFold";
import { TabSection } from "./TabSection";
import { WaysList } from "./WaysList";

export function MethodsTab({ vm, palette, actions, referenceOpen = false }: {
  vm: MethodsVM; palette: Palette; actions: KeepActions;
  /** the Reference fold's first state: closed (the prototype); a render test opens it to read the folded lines */
  referenceOpen?: boolean;
}) {
  if (vm.closed !== null || vm.closedMore.length)
    return (
      <View testID="methods-tab" style={{ marginTop: SPACE.s2 }}>
        {[vm.closed, ...vm.closedMore].map((t, i) => (t ? <ClosedNote key={i} text={t} palette={palette} /> : null))}
      </View>
    );
  return (
    <View testID="methods-tab" style={{ paddingBottom: 24 }}>
      {(vm.ways || vm.boats) && (
        <TabSection title={WORDS.TAB_METHODS} palette={palette}>
          <View style={{ borderTopWidth: 1, borderTopColor: palette.line2 }}>
            {vm.ways && <WaysList vm={vm.ways} palette={palette} actions={actions} />}
            {vm.boats && <Boats vm={vm.boats} palette={palette} actions={actions} />}
          </View>
        </TabSection>
      )}
      {vm.reference && (
        <View style={{ marginHorizontal: GUTTER }}>
          <ReferenceFold vm={vm.reference} palette={palette} actions={actions} initiallyOpen={referenceOpen} />
        </View>
      )}
    </View>
  );
}
