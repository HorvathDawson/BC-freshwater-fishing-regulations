/**
 * T35 — the Methods tab drawn from `MethodsVM` (prototype buildGear's methods half): on a day with no fishing, only its
 * sentence; otherwise the "Methods" section (Ways to fish), (T395) the "Vessel restrictions" section — the vessel rules one
 * row per kind, each opening its rule (they sat as chips at the top of the page), then Boats, the full list with its
 * sources — and the Reference fold that closes the tab. No logic: which blocks and every word are core's (methods.ts,
 * boats.ts).
 */
import { View } from "react-native";
import { METHODS, WORDS, type MethodsVM, type RegsPage } from "@app/core";
import { GUTTER, SPACE, type Palette } from "../theme";
import { Boats } from "./Boats";
import { BoatsLine } from "./BoatsLine";
import { ClosedNote } from "./ClosedNote";
import type { KeepActions } from "./LineText";
import { ReferenceFold } from "./ReferenceFold";
import { TabSection } from "./TabSection";
import { WaysList } from "./WaysList";

export function MethodsTab({ vm, palette, actions, referenceOpen = false, boats }: {
  vm: MethodsVM; palette: Palette; actions: KeepActions;
  /** (T395) the page's vessel restrictions by kind (`page.boats`), drawn in the Vessel restrictions section */
  boats?: RegsPage["boats"];
  /** the Reference fold's first state: closed (the prototype); a render test opens it to read the folded lines */
  referenceOpen?: boolean;
}) {
  const closed = vm.closed !== null || vm.closedMore.length > 0;
  const kinds = boats?.length ? boats : null;
  const listed = closed ? null : vm.boats;
  // (T395) the vessel restrictions' own section, under Methods: also on a closed day, where they were shown before
  const vessels = (kinds || listed) && (
    <TabSection title={METHODS.VESSELS} palette={palette} testID="methods-vessels">
      <View style={{ borderTopWidth: 1, borderTopColor: palette.line2 }}>
        {kinds && <BoatsLine boats={kinds} palette={palette} onSheet={actions.onSheet} />}
        {listed && <Boats vm={listed} palette={palette} actions={actions} />}
      </View>
    </TabSection>
  );
  if (closed)
    return (
      <View testID="methods-tab" style={{ marginTop: SPACE.s2 }}>
        {[vm.closed, ...vm.closedMore].map((t, i) => (t ? <ClosedNote key={i} text={t} palette={palette} /> : null))}
        {vessels}
      </View>
    );
  return (
    <View testID="methods-tab" style={{ paddingBottom: SPACE.s2 }}>
      {vm.ways && (
        <TabSection title={WORDS.TAB_METHODS} palette={palette}>
          <View style={{ borderTopWidth: 1, borderTopColor: palette.line2 }}>
            <WaysList vm={vm.ways} palette={palette} actions={actions} />
          </View>
        </TabSection>
      )}
      {vessels}
      {vm.reference && (
        <View style={{ marginHorizontal: GUTTER }}>
          <ReferenceFold vm={vm.reference} palette={palette} actions={actions} initiallyOpen={referenceOpen} />
        </View>
      )}
    </View>
  );
}
