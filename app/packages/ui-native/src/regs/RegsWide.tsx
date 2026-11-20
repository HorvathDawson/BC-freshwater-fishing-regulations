/**
 * T45 — THE REGULATIONS VIEW ON A WIDE SCREEN: the pieces `RegsScreen` arranges when the layout class (@app/ui
 * `useLayoutClass`) is not a phone. A phone draws none of them (the design, golden parity).
 *
 *   tablet (≥ 900)                              desktop (≥ 1280)
 *   ┌ frame column (WATER_FRAME, 1280 max) ┐    ┌ frame column ─────────────────────────────────┐
 *   │ name row · date · Map                │    │ name row · Map                                │
 *   │ place · the four tabs                │    │ place                                         │
 *   │     ┌ reading column ┐               │    │ ┌ side ──────┐    ┌ reading column ┐         │
 *   │     │ the page        │               │    │ │ date pill  │    │ the page       │         │
 *   │     │                 │               │    │ │ sections   │    │ (one scroll)   │         │
 *   │     └─────────────────┘               │    │ │            │    └────────────────┘         │
 *   └──────────────────────────────────────┘    │ └────────────┘                                │
 *                                                └───────────────────────────────────────────────┘
 *
 * THE READING COLUMN is a measure INSIDE the view, not the frame's: the frame (T44) still owns the gutters, the 1280
 * column and the header, so the back, the strip, the name row and the content's left edge (the place label, the side
 * column) stay where the Conditions and Map views have them. Only the page's own lines are held to about 70 characters
 * (REGS_WIDE.measure), centred in the room the page has.
 *
 * No words and no logic: the date pill and the section list are the header's and the tab store's. (T810 cutover, owner
 * 2026-11-17 23:29 "Remove it (Recommended)") The pin bar, over the column and in the side, is gone.
 */
import { View } from "react-native";
import type { HeaderVM, SunDay, Tab } from "@app/core";
import type { LayoutClass } from "@app/ui";
import { REGS_WIDE, type Palette } from "../theme";
import { WaterInset } from "../WaterFrame";
import type { TabStore } from "./tabStore";
import { DatePill, SectionList, SunLine } from "./RegsHeader";

/** The page's lines held to the reading measure, centred; a phone: the children as they are (nothing wraps them). */
export function ReadingColumn({ layout, testID, children }: { layout: LayoutClass; testID?: string; children: React.ReactNode }) {
  if (layout === "phone") return <>{children}</>;
  return <View testID={testID} style={{ width: "100%", maxWidth: REGS_WIDE.measure, alignSelf: "center" }}>{children}</View>;
}

/**
 * The desktop side column, at the frame's left (its words at the frame's gutter, where the Conditions view's start):
 * the day control and the four sections as a list (a press scrolls the page to one; the one in view is marked) — what a
 * reader reaches for while reading, so the main column is just the page. Outside the page's
 * scroll, so it stays put while the page moves.
 */
export function RegsSide({ header, store, palette, onDate, onTab, sun = null }: {
  header: HeaderVM; store: TabStore; palette: Palette; onDate: () => void; onTab: (t: Tab) => void;
  /** (T878) the picked day's sun at the water, under the date pill; null: the pill alone */
  sun?: SunDay | null;
}) {
  return (
    <View testID="regs-side" style={{ width: REGS_WIDE.side, borderRightWidth: 1, borderRightColor: palette.line }}>
      <WaterInset style={{ paddingTop: 12, paddingBottom: 12, alignItems: "flex-start" }}>
        <DatePill h={header} palette={palette} onDate={onDate} />
        {!!sun && <SunLine sun={sun} palette={palette} align="start" />}
      </WaterInset>
      <WaterInset style={{ paddingBottom: 12 }}>
        <SectionList h={header} tabs={store} palette={palette} onTab={onTab} />
      </WaterInset>
    </View>
  );
}
