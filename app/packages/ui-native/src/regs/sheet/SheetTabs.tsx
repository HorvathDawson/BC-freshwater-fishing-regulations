/**
 * (T1031) A sheet's tab strip — the species sheet's "Identification | Sources" (core `SheetVM.tabs`), drawn as the
 * Regulations page's own tab row (RegsHeader `TabRow`: one tablist, each tab a role=tab with `aria-selected` (AGENTS 34),
 * the drawn one underlined in ink). A press opens that tab's key in this sheet's place (`onOpen`, as any press inside a
 * sheet does), so the sheet is the same however it is reached. No logic: the words and keys are core's.
 */
import { View } from "react-native";
import { SPECIES_SHEET, type SheetKey, type SheetTabVM } from "@app/core";
import { Text } from "../../Text";
import { CAPPED_SCALE, TYPE } from "../../type";
import type { Palette } from "../../theme";
import { RegsPress } from "../RegsPress";
import { TAB_BOX } from "../RegsHeader";

export function SheetTabs({ tabs, palette, onOpen }: { tabs: readonly SheetTabVM[]; palette: Palette; onOpen: (k: SheetKey) => void }) {
  return (
    <View testID="sheet-tabs" accessibilityRole="tablist" accessibilityLabel={SPECIES_SHEET.TABS_LABEL}
          style={{ paddingHorizontal: 18, paddingTop: 8, columnGap: 16, flexDirection: "row", flexWrap: "wrap" }}>
      {tabs.map((t) => (
        <RegsPress key={t.open} testID={`sheet-tab:${t.label}`} onPress={() => { if (!t.on) onOpen(t.open); }} accessibilityRole="tab" aria-selected={t.on}
                   box={TAB_BOX} style={{ minHeight: 34, paddingTop: 2, justifyContent: "center", borderBottomWidth: 3,
                                          borderBottomColor: t.on ? palette.ink : "transparent" }}>
          <Text maxFontSizeMultiplier={CAPPED_SCALE} style={[TYPE.regsTab, { color: t.on ? palette.ink : palette.sub }]}>{t.label}</Text>
        </RegsPress>
      ))}
    </View>
  );
}
