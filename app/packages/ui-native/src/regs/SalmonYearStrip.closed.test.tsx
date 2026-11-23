/**
 * (T820, owner 2026-11-16 21:37, iPhone build 36: "also the calendar bar doesnt have a full red line for salmon when closed
 * but instead it is empty with no color on the calendar bar..") A salmon year closed every day draws the FULL closed bar:
 * one run across the whole track, filled with the game-fish bar's own closed hatch (the closed colour, solid stripes over
 * its tint; T935: drawn as shapes, iOS drew the rotated pattern as nothing) — never an empty track, never the bare word. Core gives every card that year (`salmon.test.ts`,
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
import { TEST_TROUT_SOURCES } from "./testing/sources";

afterEach(cleanup);

/** core `salmonStripOf` over a region-closed / unlisted card (pinned there): every day closed */
const ALL_CLOSED: SalmonStripVM = {
  title: "Salmon (DFO)", by: "dfo", status: "closed", mark: "✕", word: "Closed", when: "today", soon: null,
  runs: [{ drawn: "closed", days: 365, gear: 0 }], generalTitle: "Game fish (B.C. rules)", legend: [{ drawn: "closed", label: "✕ Closed" }],
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
      // (T935) drawn as shapes, never an SVG pattern (iOS drew the rotated one as nothing): the card, a tint of the closed
      // colour, then solid stripes of it cut to the run — red on every platform, never a colourless track
      expect(svg.innerHTML).not.toMatch(/<pattern|url\(#/);
      const runs = [...svg.querySelectorAll(":scope > rect")];
      expect(runs.map((r) => [r.getAttribute("x"), r.getAttribute("width"), r.getAttribute("fill"), r.getAttribute("opacity")]))
        .toEqual([["0", "300", palette.card, null], ["0", "300", palette.closed, "0.3"]]);
      const stripes = [...svg.querySelectorAll(":scope > polygon")];
      expect(stripes.length).toBe(Math.ceil((300 + 10) / 12));
      expect(new Set(stripes.map((p) => p.getAttribute("fill")))).toEqual(new Set([palette.closed]));
      for (const p of stripes) {
        for (const xy of p.getAttribute("points")!.split(" ")) {
          const [x, y] = xy.split(",").map(Number);
          expect(x! >= 0 && x! <= 300 && y! >= 0 && y! <= 10, xy).toBe(true);
        }
      }
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
        <KeepTab troutSources={TEST_TROUT_SOURCES} keep={(vedder1010 as unknown as RegsPage).keep as KeepVM} dateLabel="Jan 15" palette={LIGHT} actions={actions}
                 salmonStrip={ALL_CLOSED} glance={{ salmon }} />
      </StripWidthHint.Provider>,
    );
    const block = screen.getByTestId("salmon-block");
    expect(within(block).queryByTestId("salmon-bare")).toBeNull();
    const svg = within(block).getByTestId("salmon-strip-bar").querySelector("svg")!;
    expect([...svg.querySelectorAll(":scope > rect")].map((r) => r.getAttribute("fill"))).toEqual([LIGHT.card, LIGHT.closed]);
    expect(svg.querySelectorAll(":scope > polygon").length).toBeGreaterThan(0);
  });
});

// (T934, owner 2026-11-20 "Bait ban needs to apply to salmon [too]. So would fly fish only")
describe("(T934) <SalmonYearStrip> the B.C. gear marks on salmon days", () => {
  const GEAR: SalmonStripVM = {
    ...ALL_CLOSED, status: "open", mark: "✓ ", word: "Open",
    runs: [{ drawn: "closed", days: 200, gear: 0 }, { drawn: "open", days: 100, gear: 16 }, { drawn: "open", days: 65, gear: 20 }],
    legend: [{ drawn: "open", label: "✓ Open" }, { drawn: "closed", label: "✕ Closed" },
             { drawn: "open", bit: 4, label: "Fly fishing only" }, { drawn: "open", bit: 16, label: "Bait ban" }],
  };
  it("a bait-ban run wears the bait band; a fly-only run the upright stripes in the salmon colour; the key names both", () => {
    draw(GEAR, DARK);
    const svg = screen.getByTestId("salmon-strip-bar").querySelector("svg")!;
    expect(svg.innerHTML).not.toMatch(/<pattern|url\(#/);
    const rects = [...svg.querySelectorAll(":scope > rect")].map((r) => [r.getAttribute("fill"), r.getAttribute("height")]);
    // closed: card + tint; bait run: salmon green + the bait band; fly + bait run: card under the stripes + the bait band
    expect(rects).toEqual([[DARK.card, "10"], [DARK.closed, "10"], [DARK.salmon.open, "10"], [DARK.restricted, "5"],
                           [DARK.card, "10"], [DARK.restricted, "5"]]);
    const fills = new Set([...svg.querySelectorAll(":scope > polygon")].map((p) => p.getAttribute("fill")));
    expect(fills).toEqual(new Set([DARK.closed, DARK.salmon.open]));
    expect(screen.getByTestId("salmon-strip-legend").textContent).toBe("✓ Open✕ ClosedFly fishing onlyBait ban");
  });
});
