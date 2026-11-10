/**
 * "Report a problem with this page" — the quiet link at the foot of the Regulations page (owner 2026-10-21 put one under
 * each tab; T41 Q3, Claude 2026-10-22 under the owner's delegation: ONCE, at the page's foot — four identical links cost
 * ~52 px per section and read as part of each section's rules). It opens the menu's Report a problem page prefilled with
 * where the reader was: the water, its part and the day, in the page's own words (`regsReportWhere`), which the reader
 * still sees and can switch off before anything is sent. It names NO tab: the link sits below Licence, so the tab in
 * view at the press was always Licence (T41 review, measured on vedder_1010 / klwa_1010 / bc1_501) — the report is
 * about the whole page and the reader says which rule in their own words.
 *
 * Reads the Shell's menu from `MenuContext`; with none above it (a test, a screen mounted alone) it draws nothing, so
 * the Regs screens drop it in with one line and nothing else about them changes.
 *
 * (T40) A link on the Regulations view like any other: the one link colour (the accent, no underline — r20c-style-tokens)
 * and a `RegsPress` grown to 44 × 44 around its 32-tall box (the a11y gate reads its `data-hitslop`).
 */
import { useContext } from "react";
import { Text } from "../Text";
import { MENU_WORDS, regsReportWhere, reportFailure, type RegsPage, type ReportWhere } from "@app/core";
import { TYPE } from "../type";
import { GUTTER, SPACE, type Palette } from "../theme";
import { MIN_HIT } from "../regs/hit";
import { RegsPress } from "../regs/RegsPress";
import { MenuContext } from "./MenuContext";

export function ReportLink({ page, palette }: {
  page: Pick<RegsPage, "target" | "header">;
  palette: Palette;
}) {
  const menu = useContext(MenuContext);
  if (!menu) return null;
  return (
    <RegsPress testID="report-link" onPress={() => menu.report(regsReportWhere(page, null))}
               accessibilityRole="link" accessibilityLabel={MENU_WORDS.report.fromRegs} box={{ w: MIN_HIT, h: 32 }}
               style={{ alignSelf: "flex-start", marginHorizontal: GUTTER, marginTop: SPACE.s2, marginBottom: SPACE.s3,
                        minHeight: 32, minWidth: MIN_HIT, justifyContent: "center" }}>
      <Text style={{ ...TYPE.small, color: palette.accent }}>
        {MENU_WORDS.report.fromRegs}
      </Text>
    </RegsPress>
  );
}

/**
 * (T569, owner 2026-11-09: "there is no report an issue button on the conditions tab") "Report a problem with this water"
 * on a water's Conditions and Map views, the same quiet link as the Regulations page's. It opens the same form with `where`
 * filled in (`waterReportWhere`: the water, the tab, and from Conditions the gauge, quantity and span), and `feed` — the
 * gauge feed's version, which is read when it is pressed (the feed's index): if that read fails the failure is reported and
 * the form opens without the line, never silently.
 */
export function WaterReportLink({ where, feed, palette, testID = "report-link-water" }: {
  where: ReportWhere; palette: Palette; testID?: string;
  feed?: () => Promise<string | null>;
}) {
  const menu = useContext(MenuContext);
  if (!menu) return null;
  const open = () => {
    if (!feed) { menu.report(where); return; }
    feed().then((f) => menu.report(f ? { ...where, feed: f } : where),
                (e: unknown) => { reportFailure(e); menu.report(where); });
  };
  return (
    <RegsPress testID={testID} onPress={open} accessibilityRole="link" accessibilityLabel={MENU_WORDS.report.fromWater}
               box={{ w: MIN_HIT, h: 32 }}
               style={{ alignSelf: "flex-start", marginTop: SPACE.s2, marginBottom: SPACE.s3, minHeight: 32,
                        minWidth: MIN_HIT, justifyContent: "center" }}>
      <Text style={{ ...TYPE.small, color: palette.accent }}>{MENU_WORDS.report.fromWater}</Text>
    </RegsPress>
  );
}
