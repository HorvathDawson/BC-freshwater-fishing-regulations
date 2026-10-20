/**
 * The water-temperature legend (P12), worded from the publisher's bands.
 *
 * The publisher writes a class onto every temperature reading (`_TEMP_BANDS` in pipeline/gauges/feed/publish.py) and
 * the map colours the gauge dots by it. The Conditions legend used to type "under 18 °C / 18–20 °C / 20 °C and over"
 * into `Shell.tsx` — a second copy of the thresholds that a curated change would have left behind. It now reads the
 * same bands, emitted by `python -m pipeline.tools.emit_gauge_policy`.
 */
import { TEMP_BANDS, type TempBand } from "./gauge-policy.generated";

export { TEMP_BANDS, type TempBand };

export interface TempLegendRow<B extends string = TempBand> { band: B; label: string }

/** One label per band: "under 18 °C", "18–20 °C", "20 °C and over"; a lone open band is "any temperature". */
export function tempLegend<B extends string>(
  bands: readonly (readonly [B, number | null])[],
): TempLegendRow<B>[] {
  return bands.map(([band, ceiling], i) => {
    const floor = i > 0 ? bands[i - 1]![1] : null;
    const label = floor === null
      ? (ceiling === null ? "any temperature" : `under ${ceiling} °C`)
      : ceiling === null ? `${floor} °C and over` : `${floor}–${ceiling} °C`;
    return { band, label };
  });
}

/** The shipped legend, in band order. */
export const TEMP_LEGEND: readonly TempLegendRow[] = tempLegend(TEMP_BANDS);
