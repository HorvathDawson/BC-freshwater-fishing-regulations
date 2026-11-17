/**
 * (T820, owner 2026-11-16 21:37, iPhone build 36: "also the calendar bar doesnt have a full red line for salmon when closed
 * but instead it is empty with no color on the calendar bar..") A salmon year closed every day draws the FULL closed bar:
 * one run across the whole track, filled with the game-fish bar's own closed hatch (the closed colour, solid stripes over
 * its tint) — never an empty track, never the bare word. Core gives every card that year (`salmon.test.ts`,
 * `salmonBook.test.ts`); this pins the drawing.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen, within } from "@testing-library/react";
import type { KeepVM, RegsPage, SalmonStripVM } from "@app/core";
import { SalmonYearStrip, StripWidthHint } from "./YearStrip";
import { KeepTab } from "./KeepTab";
import { DARK, LIGHT, type Palette } from "../theme";
import type { KeepActions } from "./LineText";
import vedder1010 from "./fixtures/vedder_1010.json";

afterEach(cleanup);

/** core `salmonStripOf` over a region-closed / unlisted card (pinned there): every day closed */
const ALL_CLOSED: SalmonStripVM = {
  title: "Salmon (DFO)", status: "closed", mark: "✕", word: "Closed", when: "today", soon: null,
  runs: [{ drawn: "closed", days: 365 }], generalTitle: "Game fish (B.C. rules)", legend: [{ drawn: "closed", label: "✕ Closed" }],
};

function draw(strip: SalmonStripVM, palette: Palette) {
  // the app measures the bar; jsdom never lays out, so the width is handed in as the SEO prerender does (T269)
  return render(
    <StripWidthHint.Provider value={300}>
      <SalmonYearStrip strip={strip} todayAt={0.04} months={["J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D"]}
                       dateLabel="Jan 15" palette={palette} />
    </StripWidthHint.Provider>,
  );
}

describe("(T820) <SalmonYearStrip> closed all year: the full closed bar", () => {
  for (const [name, palette] of [["light", LIGHT], ["dark", DARK]] as const) {
    it(`${name}: one closed run over the whole track, the closed hatch in the closed colour`, () => {
      draw(ALL_CLOSED, palette);
      const bar = screen.getByTestId("salmon-strip-bar");
      const svg = bar.querySelector("svg")!;
      const runs = [...svg.querySelectorAll(":scope > rect")];
      expect(runs.map((r) => [r.getAttribute("x"), r.getAttribute("width"), r.getAttribute("fill")]))
        .toEqual([["0", "300", "url(#sb-closed)"]]);
      // the fill it names is defined in the same picture: the card under a tint of the closed colour, and solid stripes
      // of it (half of every tile) — red on every platform, never a colourless track
      const pat = svg.querySelector("pattern#sb-closed")!;
      expect(pat).not.toBeNull();
      const tiles = [...pat.querySelectorAll("rect")].map((r) => [r.getAttribute("fill"), r.getAttribute("opacity"), Number(r.getAttribute("width"))]);
      expect(tiles).toEqual([[palette.card, null, 6 * Math.SQRT2], [palette.closed, "0.3", 6 * Math.SQRT2], [palette.closed, null, 3 * Math.SQRT2]]);
      expect(pat.getAttribute("patternTransform")).toBe("rotate(135)");
      // the words and the legend say it too
      expect([screen.getByTestId("salmon-strip-st:closed").textContent, screen.getByTestId("salmon-strip-legend").textContent])
        .toEqual(["✕Closed", "✕ Closed"]);
    });
  }

  it("a strip with no days draws no track at all (never an empty, colourless bar)", () => {
    draw({ ...ALL_CLOSED, runs: [], legend: [] }, LIGHT);
    expect(screen.queryByTestId("salmon-strip-bar")).toBeNull();
    expect(screen.getByTestId("salmon-strip-st:closed").textContent).toBe("✕Closed");
  });

  it("on the Keep tab: the Salmon (DFO) block draws the closed bar, never the bare word", () => {
    const actions: KeepActions = { onSheet() {}, onDay() {}, onTab() {}, onBook() {} };
    const salmon = { row: { source: "dfo" as const, thumb: "CO", name: "Salmon", value: "closed", tone: "closed" as const, fold: false },
                     card: () => null };
    render(
      <StripWidthHint.Provider value={300}>
        <KeepTab keep={(vedder1010 as unknown as RegsPage).keep as KeepVM} dateLabel="Jan 15" palette={LIGHT} actions={actions}
                 salmonStrip={ALL_CLOSED} glance={{ salmon }} />
      </StripWidthHint.Provider>,
    );
    const block = screen.getByTestId("salmon-block");
    expect(within(block).queryByTestId("salmon-bare")).toBeNull();
    const fills = [...within(block).getByTestId("salmon-strip-bar").querySelectorAll("svg > rect")].map((r) => r.getAttribute("fill"));
    expect(fills).toEqual(["url(#sb-closed)"]);
  });
});
