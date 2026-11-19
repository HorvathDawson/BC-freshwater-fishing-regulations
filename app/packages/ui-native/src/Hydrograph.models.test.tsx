/**
 * T377 — several forecast models on one chart: the other models' traces are drawn beside the chosen one's ribbon, and
 * each model's legend entry is a switch (checkbox). "Real-time only" (T398) stays beside them.
 *
 * T895 (owner 2026-11-19: "make the different models forecast have different colors so when they are toggled on or off
 * you can tell which are which") — each model is drawn in ITS OWN colour token and ITS OWN dash, keyed by the model, so
 * switching one off takes its line away and greys its switch while every other model keeps exactly its colour and dash
 * (T377 handed dashes out in the order drawn, all in one colour: ELF's pattern changed when COFFEE was switched off).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { CONDITIONS_WORDS } from "@app/core";
import { buildHydrograph, FORECAST_DASH, OTHER_MODEL_DASH, pickModel, type BuildInput, type ModelPick } from "@app/ui";
import { Hydrograph, type LegendModel } from "./Hydrograph";
import { CVD, DARK, LIGHT, type Palette } from "./theme";

afterEach(cleanup);

const at = Array.from({ length: 6 }, (_, i) => new Date(Date.UTC(2026, 8, 1, i)).toISOString());
const ahead = (n: number, v: number) => ({
  at: Array.from({ length: n }, (_, i) => new Date(Date.UTC(2026, 8, 1, 6 + i)).toISOString()),
  mid: Array.from({ length: n }, () => v),
});
const build = (also: BuildInput["alsoForecasts"]) => buildHydrograph({
  values: [10, 11, 12, 12, 13, 13], bands: Array.from({ length: 6 }, () => null), xLabels: [], at,
  forecast: { key: "CLEVER", ...ahead(8, 14), lo: ahead(8, 12).mid, hi: ahead(8, 16).mid },
  alsoForecasts: also,
});
const ALL = build([{ key: "COFFEE", ...ahead(8, 16) }, { key: "ELF", ...ahead(20, 9) }]);
const NO_COFFEE = build([{ key: "ELF", ...ahead(20, 9) }]);

const legend = (on: Record<string, boolean>, press = () => {}): LegendModel[] =>
  ["CLEVER", "COFFEE", "ELF"].map((m) => ({ key: m, label: m, on: on[m]!, onPress: press }));

/** The stroke and dash of the path drawn with this `d`. */
const strokeOf = (container: HTMLElement, d: string) => {
  const p = [...container.querySelectorAll("path")].find((q) => q.getAttribute("d") === d)!;
  return { stroke: p.getAttribute("stroke"), dash: p.getAttribute("stroke-dasharray") };
};
/** A legend switch's border and its swatch's stroke and dash. */
const chipOf = (m: string) => {
  const chip = screen.getByTestId(`chart-model-${m}`);
  const line = chip.querySelector("line")!;
  return { checked: chip.getAttribute("aria-checked"), border: getComputedStyle(chip).borderTopColor,
           stroke: line.getAttribute("stroke"), dash: line.getAttribute("stroke-dasharray") };
};
/** `#RRGGBB` as the browser reports a computed colour. */
const rgb = (hex: string) => `rgb(${[1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16)).join(", ")})`;

describe.each([["light", LIGHT], ["dark", DARK], ["cvd", CVD]] as const)("(T895) %s: each model in its own colour and dash",
  (_name, p: Palette) => {
    it("draws every model's trace, and its switch, in the model's own token and dash", () => {
      const { container } = render(<Hydrograph shape={ALL} palette={p} colour={p.live}
                                               models={legend({ CLEVER: true, COFFEE: true, ELF: true })}
                                               realtime={{ on: false, onPress: () => {} }} />);
      const lines = { CLEVER: ALL.forecast!.line, COFFEE: ALL.alsoForecasts[0]!.line, ELF: ALL.alsoForecasts[1]!.line };
      for (const [m, d] of Object.entries(lines) as [keyof typeof lines, string][]) {
        expect(strokeOf(container, d), `${m}'s trace`).toEqual({ stroke: p.forecast[m], dash: FORECAST_DASH[m] });
        expect(chipOf(m), `${m}'s switch`).toEqual({ checked: "true", border: rgb(p.forecast[m]), stroke: p.forecast[m],
                                                    dash: FORECAST_DASH[m] });
      }
      // the chosen model's ribbon is its own colour too, never the measured line's
      const band = [...container.querySelectorAll("path")].find((q) => q.getAttribute("d") === ALL.forecast!.band)!;
      expect(band.getAttribute("fill")).toBe(p.forecast.CLEVER);
      // and the measured line keeps the chart's colour, solid
      expect(strokeOf(container, ALL.line)).toEqual({ stroke: p.live, dash: null });
    });

    it("a model switched off loses its line and its colour; the others keep theirs exactly", () => {
      const { container } = render(<Hydrograph shape={NO_COFFEE} palette={p} colour={p.live}
                                               models={legend({ CLEVER: true, COFFEE: false, ELF: true })}
                                               realtime={{ on: false, onPress: () => {} }} />);
      expect(screen.queryByTestId("chart-also-COFFEE")).toBeNull();
      expect(container.querySelectorAll("path[stroke-dasharray]").length).toBe(2);
      // the switch stays (a tap brings the model back), grey, still in its own dash so it is still told apart
      expect(chipOf("COFFEE")).toEqual({ checked: "false", border: rgb(p.line2), stroke: p.sub, dash: FORECAST_DASH.COFFEE });
      expect(strokeOf(container, NO_COFFEE.forecast!.line)).toEqual({ stroke: p.forecast.CLEVER, dash: FORECAST_DASH.CLEVER });
      expect(strokeOf(container, NO_COFFEE.alsoForecasts[0]!.line)).toEqual({ stroke: p.forecast.ELF, dash: FORECAST_DASH.ELF });
      expect(chipOf("ELF")).toEqual({ checked: "true", border: rgb(p.forecast.ELF), stroke: p.forecast.ELF, dash: FORECAST_DASH.ELF });
      expect(chipOf("CLEVER").stroke).toBe(p.forecast.CLEVER);
    });
  });

/**
 * (T895 review) THE SWITCHES DRIVEN THROUGH `pickModel`, as the Conditions panel drives them: the chart drawn from each
 * pick the way `useChartView` draws it (the chosen model with its ribbon, the others' traces beside it). Switching off the
 * CHOSEN model hands the ribbon to the next model drawn; the last one off is the measured line alone. At every step each
 * drawn model wears exactly its own colour and dash, the ribbon wears the chosen model's colour, and a model off is grey.
 */
describe.each([["light", LIGHT], ["dark", DARK], ["cvd", CVD]] as const)("(T895) %s: switching off the chosen model",
  (_name, p: Palette) => {
    const RUNS: Record<string, { at: string[]; mid: number[] }> = { CLEVER: ahead(8, 14), COFFEE: ahead(8, 16), ELF: ahead(20, 9) };
    const shapeOf = (pick: ModelPick) => buildHydrograph({
      values: [10, 11, 12, 12, 13, 13], bands: Array.from({ length: 6 }, () => null), xLabels: [], at,
      forecast: pick.on && pick.model
        ? { key: pick.model, ...RUNS[pick.model]!, lo: RUNS[pick.model]!.mid.map((v) => v - 2), hi: RUNS[pick.model]!.mid.map((v) => v + 2) }
        : null,
      alsoForecasts: pick.on ? pick.also.map((m) => ({ key: m, ...RUNS[m]! })) : [],
    });
    /** What the chart says about each model, and where the ribbon is. */
    const drawn = (pick: ModelPick) => {
      cleanup();
      const shape = shapeOf(pick);
      const on = (m: string) => !!pick.on && (m === pick.model || pick.also.includes(m));
      const { container } = render(<Hydrograph shape={shape} palette={p} colour={p.live}
                                               models={["CLEVER", "COFFEE", "ELF"].map((m) => ({ key: m, label: m, on: on(m),
                                                                                                onPress: () => {} }))}
                                               realtime={{ on: !pick.on, onPress: () => {} }} />);
      const lines = [...(shape.forecast ? [{ key: shape.forecast.key!, line: shape.forecast.line }] : []), ...shape.alsoForecasts];
      const band = shape.forecast?.band
        ? [...container.querySelectorAll("path")].find((q) => q.getAttribute("d") === shape.forecast!.band)!.getAttribute("fill") : null;
      return { band, ribbon: shape.forecast?.key ?? null,
               traces: Object.fromEntries(lines.map((l) => [l.key, strokeOf(container, l.line)])),
               chips: Object.fromEntries(["CLEVER", "COFFEE", "ELF"].map((m) => [m, chipOf(m)])) };
    };
    const own = (m: "CLEVER" | "COFFEE" | "ELF") => ({ stroke: p.forecast[m], dash: FORECAST_DASH[m] });
    const offChip = (m: "CLEVER" | "COFFEE" | "ELF") => ({ checked: "false", border: rgb(p.line2), stroke: p.sub, dash: FORECAST_DASH[m] });
    const onChip = (m: "CLEVER" | "COFFEE" | "ELF") => ({ checked: "true", border: rgb(p.forecast[m]), stroke: p.forecast[m],
                                                           dash: FORECAST_DASH[m] });

    it("hands the ribbon to the next model drawn, then to none, every model keeping its colour and dash", () => {
      let pick: ModelPick = { model: "CLEVER", also: ["COFFEE", "ELF"], on: true };
      let d = drawn(pick);
      expect(d).toEqual({ band: p.forecast.CLEVER, ribbon: "CLEVER",
                          traces: { CLEVER: own("CLEVER"), COFFEE: own("COFFEE"), ELF: own("ELF") },
                          chips: { CLEVER: onChip("CLEVER"), COFFEE: onChip("COFFEE"), ELF: onChip("ELF") } });

      // CLEVER, the chosen one, off: COFFEE takes the ribbon in its own colour; ELF is untouched
      pick = pickModel(pick, "CLEVER", "CLEVER", true);
      expect(pick).toEqual({ model: "COFFEE", also: ["ELF"], on: true });
      d = drawn(pick);
      expect(d).toEqual({ band: p.forecast.COFFEE, ribbon: "COFFEE", traces: { COFFEE: own("COFFEE"), ELF: own("ELF") },
                          chips: { CLEVER: offChip("CLEVER"), COFFEE: onChip("COFFEE"), ELF: onChip("ELF") } });

      // COFFEE, now chosen, off: ELF alone, with the ribbon
      pick = pickModel(pick, "COFFEE", "COFFEE", true);
      expect(pick).toEqual({ model: "ELF", also: [], on: true });
      d = drawn(pick);
      expect(d).toEqual({ band: p.forecast.ELF, ribbon: "ELF", traces: { ELF: own("ELF") },
                          chips: { CLEVER: offChip("CLEVER"), COFFEE: offChip("COFFEE"), ELF: onChip("ELF") } });

      // the last one off: the measured line alone, no ribbon, no broken line, every switch grey in its own dash
      pick = pickModel(pick, "ELF", "ELF", true);
      expect(pick.on).toBe(false);
      d = drawn(pick);
      expect(d).toEqual({ band: null, ribbon: null, traces: {},
                          chips: { CLEVER: offChip("CLEVER"), COFFEE: offChip("COFFEE"), ELF: offChip("ELF") } });
      expect(screen.getByTestId("chart-model-off").getAttribute("aria-checked")).toBe("true");

      // and back on: CLEVER alone, in its own colour again
      pick = pickModel(pick, "CLEVER", "ELF", false);
      d = drawn(pick);
      expect(d).toMatchObject({ band: p.forecast.CLEVER, ribbon: "CLEVER", traces: { CLEVER: own("CLEVER") } });
    });
  });

describe("(T377) several models at once", () => {
  it("each model's entry is a switch in the legend; Real-time only beside them", () => {
    const press = vi.fn();
    render(<Hydrograph shape={ALL} palette={LIGHT} colour={LIGHT.live}
                       models={legend({ CLEVER: true, COFFEE: true, ELF: false }, press)}
                       realtime={{ on: false, onPress: () => {} }} />);
    const coffee = screen.getByTestId("chart-model-COFFEE");
    expect(coffee.getAttribute("role")).toBe("checkbox");
    expect(coffee.getAttribute("aria-checked")).toBe("true");
    expect(coffee.getAttribute("aria-label")).toBe(CONDITIONS_WORDS.key.model("COFFEE", true));
    expect(screen.getByTestId("chart-model-ELF").getAttribute("aria-checked")).toBe("false");
    expect(screen.getByTestId("chart-model-off").getAttribute("aria-checked")).toBe("false");
    fireEvent.click(coffee);
    expect(press).toHaveBeenCalledTimes(1);
  });

  it("a model with no token of its own is drawn in the chart's colour, in the other dash", () => {
    const shape = build([{ key: "GEM", ...ahead(8, 16) }]);
    const { container } = render(<Hydrograph shape={shape} palette={LIGHT} colour={LIGHT.live} />);
    expect(strokeOf(container, shape.alsoForecasts[0]!.line)).toEqual({ stroke: LIGHT.live, dash: OTHER_MODEL_DASH });
    expect(Object.values(FORECAST_DASH)).not.toContain(OTHER_MODEL_DASH);
  });
});
