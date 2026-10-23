/**
 * THE MAP TAB'S LEGEND STRIP (layers menu B): it WRAPS at a phone's width instead of scrolling a caption sideways (it
 * measured 613 px in 390 and hid two of the three statuses), and under a lake measure it is the measure's key — ending
 * with the line that says streams and wetlands draw neutral, since one colouring covers all water.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { waterLegend } from "@app/core";
import { LegendCount, LegendStrip } from "./Chrome";
import { legendColour } from "./LayersSheet";
import { LIGHT } from "./theme";

afterEach(cleanup);

const strip = (w: "regulations" | "stocked" | "depth", wrap: boolean) =>
  render(
    <LegendStrip palette={LIGHT} wrap={wrap}>
      {waterLegend(w).map((r) => (
        <LegendCount key={r.label} palette={LIGHT} colour={legendColour(LIGHT, r.swatch)} label={r.label} />
      ))}
    </LegendStrip>);

describe("the Map tab's legend strip", () => {
  it("wraps (no sideways scroller), so every entry is on screen at 390 px", () => {
    strip("regulations", true);
    const el = screen.getByTestId("legend-strip");
    expect([el.style.flexWrap, el.style.flexDirection, el.textContent])
      .toEqual(["wrap", "row", "ClosedHas its own regulationsBase regulations onlyNo public access"]);
  });

  it("MUTATION PIN: without `wrap` it is the old one-line scroller (so the Shell must ask for it)", () => {
    strip("regulations", false);
    expect(screen.queryByTestId("legend-strip")).toBeNull();
  });

  it("under Depth: the survey key, then what draws neutral and why — each swatch the map's own colour", () => {
    strip("depth", true);
    expect(screen.getByTestId("legend-strip").textContent).toBe(
      "Contours tracedSurveyed, sheet onlyNot surveyed. Streams and wetlands draw plain: depth surveys cover lakes only.");
    expect(waterLegend("depth").map((r) => legendColour(LIGHT, r.swatch)))
      .toEqual([LIGHT.survey[0], LIGHT.survey[1], LIGHT.water[0]]);
  });
});
