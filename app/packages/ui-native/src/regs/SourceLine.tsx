/**
 * T34 — a section's source line (Gear, Ways to fish, Boats, Reference). (T1031 item 3, owner 2026-11-22: "clean up our
 * inline sources") drawn as THE ONE SOURCE LINE (`SourceRow`): "Source  B.C. Synopsis p.8 ↗  p.49 ↗  Who says so ›" —
 * the section's rule-book pages, each opening the book with its rules' passages marked, then the section's Sources sheet,
 * which names who made each rule (the level words "Province · Region 2 · This water’s" it printed before fold in there,
 * T991 round 2). No logic: core's `sectionSourcesOf` composes it from the VM.
 */
import { View } from "react-native";
import { sectionSourcesOf, type SourceLineVM } from "@app/core";
import { SPACE, type Palette } from "../theme";
import { SourceRow } from "../SourceRow";
import type { KeepActions } from "./LineText";

export function SourceLine({ src, what, palette, actions, spaced = false }: {
  src: Pick<SourceLineVM, "open"> & { readonly pages?: SourceLineVM["pages"] };
  /** the section's own heading ("Gear", "Ways to fish"): what its sheet link and pages are named for (T60) */
  what: string;
  palette: Palette; actions: Pick<KeepActions, "onSheet" | "onBook">;
  /** a cell's line (.gxc > .isrc): --s2 above it instead of --s1 */
  spaced?: boolean;
}) {
  return (
    <View style={{ marginTop: spaced ? SPACE.s2 : SPACE.s1 }}>
      <SourceRow vm={sectionSourcesOf(src, what)} palette={palette} onBook={actions.onBook} onSheet={actions.onSheet}
                 testID="regs-src" sheetTestID="regs-src-sheet" />
    </View>
  );
}
