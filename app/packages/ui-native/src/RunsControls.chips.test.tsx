/**
 * T902 — the Runs species chips: at the desktop layout every chip is drawn with no sideways-scrolling container (the row
 * wraps inside the panel); a phone keeps the scroll row, with the same chips in the same order.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { LayoutWidthContext } from "@app/ui";
import type { RunSpecies } from "@app/core";
import { RunSpeciesChips } from "./RunsControls";
import { LIGHT } from "./theme";

afterEach(cleanup);

const NAMES = ["Steelhead", "Chinook", "Chum", "Coho", "Pink (even)", "Pink (odd)", "Sockeye"];
const species = NAMES.map((name, slot) => ({ name, slot })) as unknown as RunSpecies[];
const draw = (width: number) => render(
  <LayoutWidthContext.Provider value={width}>
    <RunSpeciesChips species={species} chosen={species.slice(0, 1)} onToggle={() => {}} onAll={() => {}} palette={LIGHT} />
  </LayoutWidthContext.Provider>);
/** a scroll container: an ancestor of the row that scrolls sideways */
const scrolls = () => {
  for (let e = screen.getByTestId("run-species").parentElement; e; e = e.parentElement)
    if (getComputedStyle(e).overflowX === "auto" || getComputedStyle(e).overflowX === "scroll") return true;
  return false;
};
const order = () => [...screen.getByTestId("run-species").querySelectorAll('[data-testid^="run-species-"]')]
  .map((e) => e.getAttribute("data-testid")).filter((t) => !t!.startsWith("run-species-swatch"));

describe("(T902) the species chips by layout", () => {
  it("desktop: all eight chips, wrapped in the panel, no scroll container", () => {
    draw(1440);
    expect(order()).toEqual(["run-species-all", ...NAMES.map((n) => `run-species-${n}`)]);
    expect(scrolls()).toBe(false);
    expect(screen.getByTestId("run-species").style.flexWrap).toBe("wrap");
  });
  it("390: the scroll row, the same chips in the same order", () => {
    draw(390);
    expect(scrolls()).toBe(true);
    expect(order()).toEqual(["run-species-all", ...NAMES.map((n) => `run-species-${n}`)]);
    expect(screen.getByTestId("run-species").style.flexWrap).not.toBe("wrap");
  });
});
