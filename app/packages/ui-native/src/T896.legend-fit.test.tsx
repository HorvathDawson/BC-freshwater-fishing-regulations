/**
 * T896 — the map key's fold button is useless where the whole key fits the bar (owner: "this button is useless?").
 * Fit is measured (`useKeyFit`: the bar's width against the key laid on one row), not a breakpoint; jsdom lays nothing out,
 * so the measurement is stood in for here and `keyFits` (the decision) is tested on its own.
 */
import { afterEach, describe, expect, it, vi } from "vitest";

const fit = vi.hoisted(() => ({ fits: null as boolean | null }));
vi.mock("./shell/useKeyFit", async (orig) => ({
  ...(await orig<Record<string, unknown>>()), useKeyFit: () => ({ fits: fit.fits, onBar() {}, onKey() {} }),
}));
vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: () => null }));
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MAP_KEY } from "@app/core";
import { MapLegend } from "./shell/MapLegend";
import { keyFits } from "./shell/useKeyFit";
import { LIGHT } from "./theme";

afterEach(cleanup);

const col = (regsLayer: "standard" | "salmon") => ({
  flowParam: "discharge", horizon: 0, mm: { onConditions: false }, cond: { marks: [] }, gearMarked: true, regsLayer,
  salmonOffered: true, statusOk: true, salmonFacts: null, setRegsLayer: () => {}, isolated: null, isolate: () => {},
  layers: { water: { drawn: "regulations" }, effective: { groups: { obstacles: false } } },
}) as never;
const show = (mode: "standard" | "salmon", fits: boolean | null) => {
  fit.fits = fits;
  return render(<MapLegend col={col(mode)} conditions={false} palette={LIGHT} theme="light" mapDay={null} />);
};

describe("(T896) the key fits the bar", () => {
  for (const mode of ["standard", "salmon"] as const) {
    it(`${mode}: the key is shown whole on one row and there is no fold button`, () => {
      show(mode, true);
      expect(screen.queryByTestId("legend-fold")).toBeNull();
      expect(screen.getAllByTestId("legend-row")).toHaveLength(1);
      expect(screen.getAllByTestId("legend-entry")).toHaveLength(mode === "standard" ? 7 : 6);
    });
    it(`${mode}: where it does not fit, the fold stays; folded is never an empty bar`, () => {
      show(mode, false);
      const fold = screen.getByTestId("legend-fold");
      expect(screen.getAllByTestId("legend-row")).toHaveLength(2);
      fireEvent.click(fold);
      expect(screen.getByTestId("legend-fold").getAttribute("aria-label")).toBe(MAP_KEY.fold.show);
      expect(screen.getAllByTestId("legend-row")).toHaveLength(1);
      expect(screen.getAllByTestId("legend-entry").length).toBeGreaterThan(0);
    });
  }
  it("not measured yet counts as not fitting (the phone layout), never a bar without its key", () => {
    show("standard", null);
    expect(screen.getByTestId("legend-fold")).toBeTruthy();
    expect(screen.getAllByTestId("legend-entry").length).toBeGreaterThan(0);
  });
  it("the measuring copy is not an entry: seven entries, hidden from a screen reader", () => {
    show("standard", true);
    expect(screen.getAllByTestId("legend-entry")).toHaveLength(7);
    expect(screen.getByTestId("legend-measure").closest("[aria-hidden]")).toBeTruthy();
  });
});

describe("(T896) keyFits decides by measured widths", () => {
  it("fits when the key plus the side padding is within the bar; unknown until both are measured", () => {
    expect(keyFits(null, 400, 12)).toBeNull();
    expect(keyFits(400, null, 12)).toBeNull();
    expect(keyFits(700, 1440, 12)).toBe(true);
    expect(keyFits(700, 724, 12)).toBe(true);
    expect(keyFits(700, 723, 12)).toBe(false);
    expect(keyFits(660, 390, 12)).toBe(false);
  });
});
