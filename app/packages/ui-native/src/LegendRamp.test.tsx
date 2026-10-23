/**
 * THE CONDITIONS LEGEND (refinement pass, owner 2026-10-22: "the histogram bar at bottom of condition map could be made a
 * little better"): the ramp is the map's own, stop for stop; it says what the scale is (a title, Low · Normal · High, a
 * bracket over the middle half where core's `standing` says normal); it counts the gauges its histogram is of; its words
 * meet the text contrast floor (sub, never faint); it is capped in width so a desktop does not stretch it into a stripe.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { STYLE_META } from "@app/map";
import { CHANGE_LEGEND, CHANGE_STOP_RATIOS, changeMarkWords, codedChange } from "@app/core";
import { LEGEND_RAMP_MAX, LegendRamp } from "./Chrome";
import { DARK, LIGHT, changeRampStops, flowRampStops, type Palette } from "./theme";

afterEach(cleanup);

const ramp = (palette: Palette, theme: "light" | "dark", marks?: number[]) => render(
  <LegendRamp palette={palette} stops={flowRampStops(theme)} title="Flow vs. past years, this date"
              low="Low" mid="Normal" high="High" marks={marks} />);

describe("the Conditions legend", () => {
  it("draws the map's ramp: each colour at the percentile its stop names, as the map interpolates it", () => {
    ramp(LIGHT, "light");
    const stops = [...screen.getByTestId("legend-bar").querySelectorAll("stop")]
      .map((s) => `${s.getAttribute("offset")} ${s.getAttribute("stop-color")}`);
    const meta = (STYLE_META.colorModes.stream!.standing as { stops: [number, unknown][] }).stops.map(([at]) => at);
    // -1 is "no record", not on the ramp; 200 and up are the Change view's own scale (T52), not this one
    expect(meta.filter((at) => at <= 100)).toEqual([-1, 0, 10, 25, 50, 75, 90, 100]);
    expect(stops).toEqual(flowRampStops("light").map((s) => `${s.at} ${s.colour}`));
    expect(flowRampStops("light").map((s) => s.at)).toEqual([0, 0.1, 0.25, 0.5, 0.75, 0.9, 1]);
    expect(new Set(flowRampStops("dark").map((s) => s.colour)).size).toBe(7);
  });

  it("says what the scale is and how many gauges it shows, in that order, in words", () => {
    ramp(LIGHT, "light", [0.05, 0.06, 0.2, 0.55]);
    expect(screen.getByTestId("legend-ramp").textContent)
      .toBe("Flow vs. past years, this date4 gauges in viewLowNormalHigh");
    expect(screen.getByTestId("legend-bar").getAttribute("aria-label"))
      .toBe("Flow vs. past years, this date: colours from low to high; normal is the middle half of past years");
    cleanup();
    ramp(LIGHT, "light", [0.5]);
    expect(screen.getByTestId("legend-count").textContent).toBe("1 gauge in view");
    cleanup();
    ramp(LIGHT, "light", []);
    expect([screen.queryByTestId("legend-count"), screen.getByTestId("legend-ramp").textContent])
      .toEqual([null, "Flow vs. past years, this dateLowNormalHigh"]);
  });

  it("brackets the middle half — where core's `standing` says NORMAL — under the bar", () => {
    ramp(LIGHT, "light");
    const b = screen.getByTestId("legend-normal").style;
    expect([b.left, b.width]).toEqual(["25%", "50%"]);
  });

  it("the histogram: one tick per occupied fortieth, taller where more gauges stand, each named for a screen reader", () => {
    ramp(LIGHT, "light", [0.05, 0.06, 0.2, 0.55]);
    const ticks = [...screen.getByTestId("legend-ramp").querySelectorAll("[aria-label$='percentile']")];
    expect(ticks.map((t) => [t.getAttribute("aria-label"), (t as HTMLElement).style.left,
                             ((t as HTMLElement).firstElementChild as HTMLElement).style.height]))
      .toEqual([["2 gauges near the 6th percentile", "5%", "14px"], ["1 gauge near the 21st percentile", "20%", "7px"],
                ["1 gauge near the 56th percentile", "55%", "7px"]]);
  });

  it("its words are `sub` (the T40 contrast floor), never `faint`, in light and dark; the title is ink", () => {
    for (const [p, t] of [[LIGHT, "light"], [DARK, "dark"]] as const) {
      ramp(p, t, [0.5]);
      const colours = [...screen.getByTestId("legend-ramp").querySelectorAll("div[dir='auto']")]
        .map((e) => `${e.textContent}:${getComputedStyle(e).color}`);
      const rgb = (hex: string) => { const d = document.createElement("div"); d.style.color = hex; return d.style.color; };
      expect(colours).toEqual([`Flow vs. past years, this date:${rgb(p.ink)}`, `1 gauge in view:${rgb(p.sub)}`,
                               `Low:${rgb(p.sub)}`, `Normal:${rgb(p.sub)}`, `High:${rgb(p.sub)}`]);
      cleanup();
    }
  });

  it("is capped and centred: a desktop window gets a scale, not a 1250 px stripe", () => {
    ramp(LIGHT, "light");
    const s = screen.getByTestId("legend-ramp").style;
    expect([s.maxWidth, s.alignSelf, s.width, LEGEND_RAMP_MAX]).toEqual(["560px", "center", "100%", 560]);
  });

  it("THE CHANGE LEGEND (T52): the same map mode's change stops, each at its coded ratio, in core's words", () => {
    const meta = (STYLE_META.colorModes.stream!.standing as { stops: [number, unknown][] }).stops.map(([at]) => at);
    expect(meta.filter((at) => at >= 200)).toEqual([215.87, 230.85, 250, 269.15, 284.13]);
    // the style's stops ARE core's ratios, coded the publisher's way: a quarter, half, steady, twice, four times
    expect(changeRampStops("light").map((s) => s.at)).toEqual(
      CHANGE_STOP_RATIOS.map((r) => expect.closeTo(codedChange(r), 4)));
    expect(new Set(changeRampStops("dark").map((s) => s.colour)).size).toBe(5);
    render(<LegendRamp palette={LIGHT} stops={changeRampStops("light")} title={CHANGE_LEGEND.title}
                       low={CHANGE_LEGEND.low} mid={CHANGE_LEGEND.mid} high={CHANGE_LEGEND.high}
                       band={CHANGE_LEGEND.steady} markWords={changeMarkWords}
                       describe="Expected change, next 10 days: colours from falling to rising; steady is little change"
                       marks={[0.5, 0.84]} />);
    expect(screen.getByTestId("legend-ramp").textContent)
      .toBe("Expected change, next 10 days2 gauges in viewFallingSteadyRising");
    const b = screen.getByTestId("legend-normal").style;
    expect([b.left, b.width]).toEqual(["43.6%", "12.8%"]);
    expect([...screen.getByTestId("legend-ramp").querySelectorAll("[aria-label$='flow']")]
      .map((t) => t.getAttribute("aria-label"))).toEqual(["1 gauge near 1.0× today's flow", "1 gauge near 4× today's flow"]);
  });
});
