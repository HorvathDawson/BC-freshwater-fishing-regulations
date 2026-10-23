import { describe, expect, it } from "vitest";
import { TEMP_BAND_TOKEN, TEMP_BANDS, TEMP_LEGEND, tempLegend } from "./temperature";

describe("the temperature legend (P12): the publisher's bands, worded once", () => {
  it("the shipped bands read as the Conditions legend reads them", () => {
    expect(TEMP_BANDS).toEqual([["cool", 18], ["warm", 20], ["critical", null]]);
    expect(TEMP_LEGEND).toEqual([
      { band: "cool", label: "under 18 °C" },
      { band: "warm", label: "18–20 °C" },
      { band: "critical", label: "20 °C and over" },
    ]);
  });

  it("every branch: first, middle, open-ended, a fractional ceiling, a lone band", () => {
    expect(tempLegend([["cool", 12.5], ["warm", 16], ["hot", 21], ["critical", null]]).map((b) => b.label))
      .toEqual(["under 12.5 °C", "12.5–16 °C", "16–21 °C", "21 °C and over"]);
    expect(tempLegend([["cool", null]]).map((b) => b.label)).toEqual(["any temperature"]);
  });

  /*
   * THE DOT'S COLOUR, ONCE (T83). Cool and critical are the map's "fishable" and "don't" (open, closed); warm has a hue
   * of its own — it was `color.status.restricted`, the permit-only plum, so a warm river read as a permit area.
   */
  it("each band names the token its gauge dot and legend swatch are painted with; warm is not a status", () => {
    expect(TEMP_BAND_TOKEN).toEqual({ cool: "color.status.open", warm: "color.temp.warm", critical: "color.status.closed" });
    expect(Object.keys(TEMP_BAND_TOKEN)).toEqual(TEMP_BANDS.map(([b]) => b));
  });
});
