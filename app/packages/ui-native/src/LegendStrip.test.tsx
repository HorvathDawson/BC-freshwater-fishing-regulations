/**
 * THE MAP TAB'S LEGEND STRIP (layers menu B): it WRAPS at a phone's width instead of scrolling a caption sideways (it
 * measured 613 px in 390 and hid two of the three statuses), and under a lake measure it is the measure's key — ending
 * with the line that says streams and wetlands draw neutral, since one colouring covers all water.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { waterLegend, WATER_STATUS, WATER_STATUS_LINE, WATER_STATUSES } from "@app/core";
import { LegendCount, LegendStrip } from "./Chrome";
import { legendColour } from "./LayersSheet";
import { LIGHT } from "./theme";

afterEach(cleanup);

const rgb = (hex: string) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
};

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

  it("(T165) the regulations key draws no public access as the map does — a dotted grey line; the rest solid", () => {
    render(
      <LegendStrip palette={LIGHT} wrap>
        {WATER_STATUSES.map((s) => (
          <LegendCount key={s} palette={LIGHT} colour={LIGHT.waterStatus[s]} line dotted={WATER_STATUS_LINE[s] === "dotted"}
                       label={WATER_STATUS[s].label} />
        ))}
      </LegendStrip>);
    const dotted = screen.getAllByTestId("swatch-dotted");
    expect([dotted.length, [...new Set([...dotted[0]!.children].map((d) => (d as HTMLElement).style.backgroundColor))]])
      .toEqual([1, [rgb(LIGHT.waterStatus.no_access)]]);
    expect(dotted[0]!.parentElement!.textContent).toBe("No public access");
    expect(WATER_STATUS_LINE).toEqual({ closed: "solid", own: "solid", base: "solid", no_access: "dotted" });
  });

  it("MUTATION PIN: a line swatch not asked to be dotted is solid (so the Shell must ask for it)", () => {
    render(<LegendCount palette={LIGHT} colour={LIGHT.waterStatus.no_access} line label="No public access" />);
    expect(screen.queryByTestId("swatch-dotted")).toBeNull();
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
