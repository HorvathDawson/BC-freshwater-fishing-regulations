/**
 * (T272) A LOADING STAGE'S BAR: a first install's two stages ("Downloading …", then "Setting up on this device") each
 * draw one, under the one waiting state (`LoadingState`'s fish). A thin track and its fill, in the palette's own colours;
 * `value` 0..1. Said to a screen reader as a progress bar with its percentage.
 */
import { View } from "react-native";
import { round, type Palette } from "./theme";

/** the bar's height (its ends are round: `round(BAR)`) */
const BAR = 6;

export function StageBar({ palette, value, width = 220, testID }: {
  palette: Palette; value: number; width?: number; testID?: string;
}) {
  const v = Math.min(1, Math.max(0, Number.isFinite(value) ? value : 0));
  return (
    <View testID={testID} accessibilityRole="progressbar"
          accessibilityValue={{ min: 0, max: 100, now: Math.floor(v * 100) }}
          style={{ width, height: BAR, borderRadius: round(BAR), backgroundColor: palette.line, overflow: "hidden" }}>
      <View style={{ width: `${v * 100}%`, height: "100%", borderRadius: round(BAR), backgroundColor: palette.accent }} />
    </View>
  );
}
