/**
 * "Which day?" — the date sheet (T23, prototype sheetHtml 'date'; T49 the month calendar). ONE surface for both entry
 * points: the Regulations page's date pill and the map's day strip's calendar segment (`MapDayStrip`) open this, holding
 * the one `DayCalendar`. ONE day state — the Shell's `mapDay`, null = today (D7) — so the page and the map's colours
 * never disagree: a day picked here from either colours the map and moves the page. Back closes it first (G1).
 *
 * (T915) Split by platform (AGENTS 34a). `.native.tsx`: the bottom sheet at every width. `.web.tsx`: the same sheet on a
 * phone or a narrow window; at the desktop layout class (`tabsAsRail`, >= 1280 px) a popover card hung from the button
 * that opened it (owner 2026-11-19: "desktop doesnt like mobile popup").
 */
import type { ReactNode } from "react";
import type { DayRange } from "@app/core";
import type { Palette } from "../theme";

export interface DaySheetProps {
  open: boolean; onClose: () => void; palette: Palette;
  day: Date | null; onDay: (d: Date | null) => void;
  /** the days the data covers (`@app/core` `dayRange`) */
  range: DayRange;
  /**
   * (T915) the testID of the button that opens it (the map's `map-day-pick`, the page's `regs-date`): on a desktop
   * browser the popover hangs from it and focus goes back to it; the bottom sheet does not use it
   */
  anchor: string;
  /** for tests; the device's clock otherwise */
  now?: Date;
}

export declare function DaySheet(props: DaySheetProps): ReactNode;
