/**
 * (T915) The phone's and tablet's "Which day?": the bottom sheet at every width, as it always was. A `<Sheet>`, so back
 * closes it first (G1). See `DaySheet.d.ts`; the browser's is `DaySheet.web.tsx` (the same sheet below the desktop class).
 */
import { View } from "react-native";
import { WORDS } from "@app/core";
import { DayCalendar } from "../DayCalendar";
import { Sheet } from "../Sheet";
import { GUTTER } from "../theme";
import type { DaySheetProps } from "./DaySheet";

export function DaySheet({ open, onClose, palette, day, onDay, range, now }: DaySheetProps) {
  return (
    <Sheet open={open} onClose={onClose} title={WORDS.DAY_SHEET_TITLE} palette={palette}>
      <View style={{ paddingHorizontal: GUTTER, paddingTop: 8 }}>
        <DayCalendar palette={palette} day={day} onDay={onDay} range={range} now={now} />
      </View>
    </Sheet>
  );
}
