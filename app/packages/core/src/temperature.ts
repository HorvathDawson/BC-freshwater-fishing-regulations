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

/**
 * THE TOKEN EACH BAND IS PAINTED WITH — the gauge dot on the map and the legend swatch beside it, one table (the hex is
 * the map's: `tempBandColour` in @app/map). Cool and critical are the map's "fishable" and "don't" (`status.open`,
 * `status.closed`). Warm has a hue of its own, `color.temp.warm` (T83): it was `status.restricted`, the permit-only
 * plum, and a warm river's dot read as a permit area.
 */
export const TEMP_BAND_TOKEN: Readonly<Record<TempBand, string>> = {
  cool: "color.status.open",
  warm: "color.temp.warm",
  critical: "color.status.closed",
};

/** The shipped legend, in band order. */
export const TEMP_LEGEND: readonly TempLegendRow[] = tempLegend(TEMP_BANDS);
