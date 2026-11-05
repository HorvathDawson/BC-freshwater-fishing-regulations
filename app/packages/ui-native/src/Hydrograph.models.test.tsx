/**
 * T377 — several forecast models on one chart: the other models' traces are drawn beside the chosen one's ribbon, each
 * in its own dash (`forecastDash`, in the order drawn), and each model's legend entry is a switch (checkbox) whose
 * swatch wears that dash. "Real-time only" (T398) stays beside them.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { CONDITIONS_WORDS } from "@app/core";
import { buildHydrograph, forecastDash } from "@app/ui";
import { Hydrograph } from "./Hydrograph";
import { LIGHT } from "./theme";

afterEach(cleanup);

const at = Array.from({ length: 6 }, (_, i) => new Date(Date.UTC(2026, 8, 1, i)).toISOString());
const ahead = (n: number, v: number) => ({
  at: Array.from({ length: n }, (_, i) => new Date(Date.UTC(2026, 8, 1, 6 + i)).toISOString()),
  mid: Array.from({ length: n }, () => v),
});
const shape = buildHydrograph({
  values: [10, 11, 12, 12, 13, 13], bands: Array.from({ length: 6 }, () => null), xLabels: [], at,
  forecast: { ...ahead(8, 14), lo: [], hi: [] },
  alsoForecasts: [{ key: "GEM", ...ahead(8, 16) }, { key: "ELF", ...ahead(20, 9) }],
});

describe("(T377) several models at once", () => {
  it("draws each other model's trace in its own dash, after the chosen one's", () => {
    const { container } = render(<Hydrograph shape={shape} palette={LIGHT} colour={LIGHT.live} />);
    const dashOf = (d: string) => [...container.querySelectorAll("path")].find((p) => p.getAttribute("d") === d)!
      .getAttribute("stroke-dasharray");
    expect(dashOf(shape.forecast!.line)).toBe(forecastDash(0));
    expect(dashOf(shape.alsoForecasts[0]!.line)).toBe(forecastDash(1));
    expect(dashOf(shape.alsoForecasts[1]!.line)).toBe(forecastDash(2));
    expect(new Set([0, 1, 2].map(forecastDash)).size).toBe(3);
  });

  it("each model's entry is a switch in the legend, its swatch in its dash; Real-time only beside them", () => {
    const press = vi.fn();
    render(<Hydrograph shape={shape} palette={LIGHT} colour={LIGHT.live}
                       models={[{ key: "CLEVER", label: "CLEVER", on: true, dash: forecastDash(0), onPress: press },
                                { key: "GEM", label: "GEM", on: true, dash: forecastDash(1), onPress: press },
                                { key: "COFFEE", label: "COFFEE", on: false, dash: forecastDash(0), onPress: press }]}
                       realtime={{ on: false, onPress: () => {} }} />);
    const gem = screen.getByTestId("chart-model-GEM");
    expect(gem.getAttribute("role")).toBe("checkbox");
    expect(gem.getAttribute("aria-checked")).toBe("true");
    expect(gem.getAttribute("aria-label")).toBe(CONDITIONS_WORDS.key.model("GEM", true));
    expect(gem.querySelector("line")!.getAttribute("stroke-dasharray")).toBe(forecastDash(1));
    expect(screen.getByTestId("chart-model-COFFEE").getAttribute("aria-checked")).toBe("false");
    expect(screen.getByTestId("chart-model-off").getAttribute("aria-checked")).toBe("false");
    fireEvent.click(gem);
    expect(press).toHaveBeenCalledTimes(1);
  });
});
