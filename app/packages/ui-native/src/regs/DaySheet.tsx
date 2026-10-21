/**
 * T23 — "Which day?" (prototype sheetHtml 'date', the header pill's sheet; decisions D7 a): the app's own day stepper
 * (`MapDay`) in a sheet. ONE day state — the Shell's `mapDay`, null = today — so the page and the map's colours never
 * disagree: a day picked here colours the map, and the map's stepper moves the page. The prototype's list of snapshot
 * days does not carry over (the app reads any day).
 */
import { View } from "react-native";
import { WORDS } from "@app/core";
import { MapDay } from "../MapDay";
import { Sheet } from "../Sheet";
import { GUTTER, type Palette } from "../theme";

export function DaySheet({ open, onClose, palette, day, onDay }: {
  open: boolean; onClose: () => void; palette: Palette;
  day: Date | null; onDay: (d: Date | null) => void;
}) {
  return (
    <Sheet open={open} onClose={onClose} title={WORDS.DAY_SHEET_TITLE} palette={palette}>
      <View style={{ paddingHorizontal: GUTTER, alignItems: "center" }}>
        <MapDay palette={palette} day={day} onDay={onDay} />
      </View>
    </Sheet>
  );
}
