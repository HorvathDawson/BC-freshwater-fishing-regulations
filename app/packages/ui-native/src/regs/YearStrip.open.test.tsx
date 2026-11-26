/**
 * (owner 2026-11-25, after T1028: "also the calendar bar colors still do not match for open") the GAME FISH block's open is
 * the SALMON block's open: `palette.salmon.open` on the bar's run, the status word, the "opens soon" pill and the legend
 * swatch, in light, dark and colour-blind. The map's colours are `palette.open`'s tokens and are not read here.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import type { StripVM } from "@app/core";
import { StripWidthHint, YearStrip } from "./YearStrip";
import { CVD, DARK, LIGHT } from "../theme";

afterEach(cleanup);

const STRIP: StripVM = {
  status: "open", todayAt: 0.2, months: ["J", "F", "M"],
  line: [{ kind: "status", text: "Open today" }, { kind: "soonOpen", text: "Opens Mar 1" }],
  runs: [{ bits: 0, days: 200 }, { bits: 1, days: 100 }, { bits: 4, days: 65 }],
  legend: [{ bit: 0, mark: "✓", label: "Open" }, { bit: 1, mark: "✕", label: "Closed" }],
};

describe("<YearStrip> open wears the salmon open colour", () => {
  it("the premise: light is where the two opens differ", () => { expect(LIGHT.open).not.toBe(LIGHT.salmon.open); });
  for (const [name, pal] of [["light", LIGHT], ["dark", DARK], ["colour-blind", CVD]] as const) {
    it(`${name}: bar run, word, soon pill and legend swatch`, () => {
      render(<StripWidthHint.Provider value={300}><YearStrip strip={STRIP} dateLabel="Jan 15" palette={pal} /></StripWidthHint.Provider>);
      const bar = screen.getByTestId("strip-bar").querySelector("svg")!;
      const plain = [...bar.querySelectorAll(":scope > rect")].filter((r) => r.getAttribute("fill") === pal.salmon.open);
      expect(plain.length).toBe(1);   // the one plain open run (the closed run is hatch; the fly-only run, stripes)
      expect([...bar.querySelectorAll(":scope > polygon")].some((p) => p.getAttribute("fill") === pal.salmon.open)).toBe(true);
      expect(screen.getAllByTestId("st:open").map((e) => e.style.color)).toEqual(Array(2).fill(rgbOf(pal.salmon.open)));
      const swatch = screen.getByTestId("strip-legend").querySelector("svg rect")!;
      expect(swatch.getAttribute("fill")).toBe(pal.salmon.open);
      // only light has two different greens/blues today (dark and colour-blind already agree): the old token must be gone there
      if (pal.open !== pal.salmon.open) expect(bar.innerHTML).not.toContain(`fill="${pal.open}"`);
    });
  }
});

/** a CSS colour as react-native-web writes it back (rgb()), from a #rrggbb */
function rgbOf(hex: string): string {
  const n = parseInt(hex.slice(1, 7), 16);
  return `rgb(${n >> 16}, ${(n >> 8) & 255}, ${n & 255})`;
}
