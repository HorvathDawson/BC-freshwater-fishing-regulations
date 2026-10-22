/**
 * "Which day?" — the date sheet (T23, prototype sheetHtml 'date'; T49 the month calendar). ONE sheet for both entry
 * points: the Regulations page's date pill and the map's calendar button (`MapDayButton`) open this, holding the one
 * `DayCalendar`. ONE day state — the Shell's `mapDay`, null = today (D7) — so the page and the map's colours never
 * disagree: a day picked here from either colours the map and moves the page. A `<Sheet>`, so back closes it first (G1).
 */
import { View } from "react-native";
import { WORDS, type DayRange } from "@app/core";
import { DayCalendar } from "../DayCalendar";
import { Sheet } from "../Sheet";
import { GUTTER, type Palette } from "../theme";

export function DaySheet({ open, onClose, palette, day, onDay, range, now }: {
  open: boolean; onClose: () => void; palette: Palette;
  day: Date | null; onDay: (d: Date | null) => void;
  /** the days the data covers (`@app/core` `dayRange`) */
  range: DayRange;
  /** for tests; the device's clock otherwise */
  now?: Date;
}) {
  return (
    <Sheet open={open} onClose={onClose} title={WORDS.DAY_SHEET_TITLE} palette={palette}>
      <View style={{ paddingHorizontal: GUTTER, paddingTop: 8 }}>
        <DayCalendar palette={palette} day={day} onDay={onDay} range={range} now={now} />
      </View>
    </Sheet>
  );
}
