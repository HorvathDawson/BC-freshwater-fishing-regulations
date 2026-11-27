/**
 * (T915) The phone's and tablet's "Which day?": the bottom sheet at every width, as it always was. A `<Sheet>`, so back
 * closes it first (G1). See `DaySheet.d.ts`; the browser's is `DaySheet.web.tsx` (the same sheet below the desktop class).
 */
import { View } from "react-native";
import { WORDS } from "@app/core";
import { DayCalendar } from "../DayCalendar";
import { Sheet } from "../Sheet";
import { GUTTER } from "../theme";
import { useCappedScale } from "../textScale";
import type { DaySheetProps } from "./DaySheet";

export function DaySheet({ open, onClose, palette, day, onDay, range, now }: DaySheetProps) {
  // (T1203) taller than the window at a large text size: the body scrolls under the title and Done (a scrolling body brings the gutter
  // itself); at the default size the sheet is 0.9.0's
  const larger = useCappedScale() > 1;
  return (
    <Sheet open={open} onClose={onClose} title={WORDS.DAY_SHEET_TITLE} palette={palette} scroll={larger}>
      <View style={{ paddingHorizontal: larger ? 0 : GUTTER, paddingTop: 8 }}>
        <DayCalendar palette={palette} day={day} onDay={onDay} range={range} now={now} />
      </View>
    </Sheet>
  );
}
