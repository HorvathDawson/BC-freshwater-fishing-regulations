/**
 * The three questions a reader can change about a hydrograph.
 *
 * They are NOT view options. Each one changes what the chart claims:
 *
 *   discharge / level   the whole river, or one cross-section. A stage percentile moves
 *                       when the channel does; a discharge percentile does not. Offered
 *                       only where the bundle actually holds an envelope in that quantity,
 *                       so the control can never ask for a comparison that does not exist.
 *   72 hours / year     what the water is doing, or where today sits in its season.
 *
 * (T367, owner 2026-11-04: the four outlined buttons were "really wrong … ametuer") Drawn as a compact JOINED segmented
 * control — the map corner's own shape (`CornerStrip`, radio) — on the gauge card's title row. A single option is
 * not drawn: one choice is not a choice.
 */
import { CornerStrip } from "./CornerStrip";
import type { Palette } from "./theme";

export function ChartControls<T extends string>({ palette, value, options, onPick, label, testID }: {
  palette: Palette; value: T;
  /** each option: its id, its word on the toggle, and (optionally) its spoken name */
  options: readonly (readonly [T, string] | readonly [T, string, string])[];
  onPick: (v: T) => void; label: string; testID: string;
}) {
  // One choice is not a choice. Showing it as the current state, unpressable, is honest —
  // and it tells the reader something true: this station only measures the one thing.
  if (options.length < 2) return null;
  return (
    <CornerStrip palette={palette} testID={testID} label={label} radio
                 segments={options.map(([id, text, said]) => ({ key: id, chip: text, label: said ?? text, on: id === value,
                                                                onPress: () => onPick(id) }))} />
  );
}
