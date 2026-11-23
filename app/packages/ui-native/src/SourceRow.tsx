/**
 * (T991 round 3) A TAB'S SOURCE ROW: the quiet label "Source", then each authority as a link, wrapping — core's
 * `TabSourcesVM` (`regsSourcesOf` / `conditionsSourcesOf`) drawn as is. A synopsis page opens the rule book at that page
 * with its highlights (`RuleBookLink`, T215); a web page leaves the app (`openLink`); "All sources ›" opens a sheet.
 * Each press is grown to 44 × 44 (T40). No logic: every word, page and box is core's.
 */
import { View } from "react-native";
import { Text } from "./Text";
import { type SheetKey, type SourceBox, type TabSourcesVM } from "@app/core";
import { TYPE } from "./type";
import type { Palette } from "./theme";
import { openLink } from "./openLink";
import { MIN_HIT } from "./regs/hit";
import { RegsPress } from "./regs/RegsPress";
import { RuleBookLink } from "./regs/RuleBookLink";

export function SourceRow({ vm, palette, onBook, onSheet, testID = "source-row", sheetTestID = `${testID}-sheet` }: {
  vm: TabSourcesVM; palette: Palette;
  /** what a synopsis page's press does (the screen's `actions.onBook`); default: open the synopsis there */
  onBook?: (pdf: number, box?: readonly SourceBox[]) => void;
  /** what "All sources ›" does; required where the row carries a sheet link */
  onSheet?: (k: SheetKey) => void;
  testID?: string;
  /** the "All sources ›" press's test id */
  sheetTestID?: string;
}) {
  const face = [TYPE.metaBold, { color: palette.accent }];
  return (
    <View testID={testID}
          style={{ flexDirection: "row", flexWrap: "wrap", alignItems: "baseline", columnGap: 12, rowGap: 6, minHeight: 44, paddingVertical: 10 }}>
      <Text style={[TYPE.metaBold, { color: palette.sub }]}>{vm.label}</Text>
      {vm.links.map((l) => {
        switch (l.k) {
          case "book":
            return <RuleBookLink key={`b${l.printed}`} printed={l.printed} pdf={l.pdf} label={l.label} a11y={l.aria} box={l.box} palette={palette} onBook={onBook} alone />;
          case "web":
            return (
              <RegsPress key={l.url} testID={`${testID}-web`} accessibilityRole="link" onPress={() => { void openLink(l.url); }}
                         box={{ w: MIN_HIT, h: TYPE.metaBold.lineHeight }}>
                <Text style={face}>{l.label}</Text>
              </RegsPress>
            );
          case "sheet":
            return (
              <RegsPress key={l.open} testID={sheetTestID} accessibilityRole="button" accessibilityLabel={l.aria}
                         onPress={() => {
                           if (!onSheet) throw new Error(`SourceRow: "${l.label}" opens ${l.open}, but this screen gave no onSheet`);
                           onSheet(l.open);
                         }}
                         box={{ w: MIN_HIT, h: TYPE.metaBold.lineHeight }}>
                <Text style={face}>{l.label}</Text>
              </RegsPress>
            );
        }
      })}
    </View>
  );
}
