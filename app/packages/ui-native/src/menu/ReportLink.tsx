/**
 * "Report a problem with this page" — the quiet link at the foot of the Regulations page (owner 2026-10-21 put one under
 * each tab; T41 Q3, Claude 2026-10-22 under the owner's delegation: ONCE, at the page's foot — four identical links cost
 * ~52 px per section and read as part of each section's rules). It opens the menu's Report a problem page prefilled with
 * where the reader was: the water, its part, the day and the tab IN VIEW WHEN PRESSED (`tab`, read at the press — the
 * tab row's own selection), in the page's own words (`regsReportWhere`), which the reader still sees and can switch off
 * before anything is sent.
 *
 * Reads the Shell's menu from `MenuContext`; with none above it (a test, a screen mounted alone) it draws nothing, so
 * the Regs screens drop it in with one line and nothing else about them changes.
 *
 * (T40) A link on the Regulations view like any other: the one link colour (the accent, no underline — r20c-style-tokens)
 * and a `RegsPress` grown to 44 × 44 around its 32-tall box (the a11y gate reads its `data-hitslop`).
 */
import { useContext } from "react";
import { Text } from "react-native";
import { MENU_WORDS, regsReportWhere, type RegsPage, type Tab } from "@app/core";
import { TYPE } from "../type";
import { GUTTER, SPACE, type Palette } from "../theme";
import { MIN_HIT } from "../regs/hit";
import { RegsPress } from "../regs/RegsPress";
import { MenuContext } from "./MenuContext";

export function ReportLink({ page, tab, palette }: {
  page: Pick<RegsPage, "target" | "header">;
  /** the tab the report names, read when the link is pressed (the page's tab in view) */
  tab: () => Tab;
  palette: Palette;
}) {
  const menu = useContext(MenuContext);
  if (!menu) return null;
  return (
    <RegsPress testID="report-link" onPress={() => menu.report(regsReportWhere(page, tab()))}
               accessibilityRole="link" accessibilityLabel={MENU_WORDS.report.fromRegs} box={{ w: MIN_HIT, h: 32 }}
               style={{ alignSelf: "flex-start", marginHorizontal: GUTTER, marginTop: SPACE.s2, marginBottom: SPACE.s3,
                        minHeight: 32, minWidth: MIN_HIT, justifyContent: "center" }}>
      <Text style={{ ...TYPE.small, color: palette.accent }}>
        {MENU_WORDS.report.fromRegs}
      </Text>
    </RegsPress>
  );
}
