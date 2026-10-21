/**
 * P16 — the Conditions panel's LAYOUT, and the "same content" guard.
 *
 * Owner Q10: the content was right and the formatting was not. So this file pins two
 * things and nothing else:
 *
 *   1. THE ORDER: horizon control (the panel header) → headline → the lead gauge's reading
 *      → the chart → the donor list → one fold holding the explanations, closed.
 *   2. THE SAME WORDS: every line of text the panel drew before P16 is still drawn after it
 *      (with the fold opened). `ConditionsLayout.before.ts` was RECORDED from the pre-P16
 *      panel against these fixtures (`P16_RECORD=1`), not written by hand — so it cannot
 *      agree with the new layout by construction. A dropped sentence fails here.
 *
 * Whole strings only (AGENTS 33): each recorded line is matched as an entire text node.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { CONDITIONS_WORDS, donorReason, noGaugeSentence, readingHeading } from "@app/core";
import type { Panel, RegsSource, SectionId } from "@app/data";
import { clearPanelCache } from "@app/ui";
import { ConditionsPanel } from "./ConditionsPanel";
import { BEFORE_P16 } from "./ConditionsLayout.before";
import { LIGHT } from "./theme";

afterEach(cleanup);
beforeEach(clearPanelCache);

const SEC = 7 as SectionId;
const DAY = new Date(2026, 8, 4, 12);

/** Three donors: a long-record lead, a short-record dam, and one that is not reporting. */
const PANEL: Panel = { areaKm2: 50, members: [
  { station: "LEAD" as never, role: "up", areaKm2: 60, years: 40, regulated: false,
    sameRiver: true },
  { station: "DAM" as never, role: "down", areaKm2: 55, years: 6, regulated: true,
    sameRiver: true },
  { station: "QUIET" as never, role: "up", areaKm2: 300, years: 30, regulated: false,
    sameRiver: true },
] };

const ROUTES = [
  { station: "LEAD", name: "Lead Creek near Town", lat: 49, lon: -122, role: "up", path: [SEC] },
  { station: "DAM", name: "Dam Creek below the Dam", lat: 49.1, lon: -122.1, role: "down",
    path: [SEC] },
  { station: "QUIET", name: "Quiet River", lat: 49.2, lon: -122.2, role: "up", path: [SEC] },
];

const sourceWith = (panel: Panel | null, now: Record<string, unknown> | null) => ({
  panelsFor: async () => new Map(panel ? [[SEC, panel]] : []),
  panelRoutes: async () => (panel ? ROUTES : []),
  lakeStationsFor: async () => new Map(),
  gaugeForSection: async () => null,
  traceToGauge: async () => [],
  gaugeSeries: async () => null,
  gaugeParameters: async () => ["discharge", "level"],
  // A FIXED clock-free age: the reading's own time is pinned, the fetch is "5 min ago".
  gaugeNow: async () => (now ? { fetchedAt: Date.now() - 5 * 60_000, value: now } : null),
}) as unknown as RegsSource;

/** The two donors disagree by far more than 25 points, so the disagreement note shows. */
const feed = { index: async () => ({ stations: {
  LEAD: { percentile: 0.15, parameter: "discharge" as const, discharge: 0.15 },
  DAM: { percentile: 0.8, parameter: "discharge" as const, discharge: 0.8 },
} }) };

const NOW = { discharge: 12.5, level: 1.2, percentile: 0.15, standing: "below",
              at: "2026-09-04T06:00:00Z" };
const CREDITS = ["Hydrometric data: Environment and Climate Change Canada",
                 "Forecast data provided by the BC River Forecast Centre"];

const SCENES = {
  estimate: () => sourceWith(PANEL, NOW),
  none: () => sourceWith(null, null),
} as const;
type Scene = keyof typeof SCENES;

async function draw(scene: Scene) {
  const onHorizon = vi.fn();
  render(<ConditionsPanel source={SCENES[scene]()} section={SEC} palette={LIGHT} feed={feed}
                          scroll={false} day={DAY} credits={CREDITS}
                          onHorizon={onHorizon} />);
  await waitFor(() => expect(screen.queryByText(scene === "estimate"
    ? readingHeading("Lead Creek near Town") : noGaugeSentence())).toBeTruthy());
  return { onHorizon };
}

/** Every element's OWN text (its direct text nodes), in document order. */
function lines(): string[] {
  const out: string[] = [];
  for (const el of Array.from(document.body.querySelectorAll("*"))) {
    const own = Array.from(el.childNodes)
      .filter((n) => n.nodeType === 3).map((n) => n.textContent ?? "").join("").trim();
    if (own) out.push(own);
  }
  return out;
}

/** Open the explanations fold — every scene has exactly one. */
function openFold() {
  fireEvent.click(screen.getByRole("button", { name: CONDITIONS_WORDS.fold }));
}

/** True when `a` comes before `b` in the document. */
const precedes = (a: Element, b: Element) =>
  (a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING) !== 0;


describe("P16: the same content", () => {
  // RECORD MODE ran once, on the pre-P16 panel; it is left here so the record can be
  // re-made from a checkout of that commit (`P16_RECORD=1 pnpm -C app test ConditionsLayout`).
  it.runIf(process.env.P16_RECORD === "1")("records the pre-P16 text set", async () => {
    const rec: Record<string, string[]> = {};
    for (const s of Object.keys(SCENES) as Scene[]) {
      await draw(s); rec[s] = [...new Set(lines())]; cleanup(); clearPanelCache();
    }
    // Printed, not written: ui-native may not touch the filesystem (layers.json).
    console.log(JSON.stringify(rec, null, 2));
  });

  for (const s of Object.keys(SCENES) as Scene[]) {
    it.skipIf(process.env.P16_RECORD === "1")(
      `every line drawn before P16 is still drawn (${s})`, async () => {
        const before = BEFORE_P16[s];
        // NOT EMPTY, or the guard passes vacuously.
        expect(before.length).toBeGreaterThan(s === "estimate" ? 30 : 3);
        await draw(s);
        openFold();
        const after = new Set(lines());
        expect(before.filter((l) => !after.has(l))).toEqual([]);
      });
  }

  it.skipIf(process.env.P16_RECORD === "1")(
    "is not vacuous: with the fold shut it reports the small print missing", async () => {
      // MUTATION PIN. The guard opens the fold; skip that and the explainer and the credits
      // must be reported missing — so a layout that dropped them could not pass.
      const recorded = BEFORE_P16.estimate;
      await draw("estimate");
      const shut = new Set(lines());
      expect(recorded.filter((l) => !shut.has(l))).toEqual(expect.arrayContaining([
        CONDITIONS_WORDS.explainer, "SOURCES", CREDITS[0]]));
    });
});

describe("P16: one reading order", () => {
  it("runs horizon → headline → reading → chart → donors → fold", async () => {
    await draw("estimate");
    const order = [
      screen.getByLabelText("Forecast 1 day ahead"),               // the panel header
      screen.getByText("ESTIMATE FOR THIS SPOT"),                   // the headline
      screen.getByText(readingHeading("Lead Creek near Town")),     // its detail rows
      screen.getByLabelText(CONDITIONS_WORDS.controls.span),        // the chart
      screen.getByText(CONDITIONS_WORDS.tapToChart),                // the donor list
      screen.getByRole("button", { name: CONDITIONS_WORDS.fold }),  // the fold
    ];
    for (let i = 1; i < order.length; i++)
      expect(precedes(order[i - 1]!, order[i]!), `block ${i} after block ${i - 1}`).toBe(true);
  });

  it("puts the caveats with the headline they qualify, above the reading", async () => {
    await draw("estimate");
    const head = screen.getByText("ESTIMATE FOR THIS SPOT");
    const disagree = screen.getByText("These gauges disagree by 46 points. That is a "
      + "catchment doing more than one thing, and the range above is the honest answer "
      + "rather than their average.");
    const dam = screen.getByText("This water is controlled by a dam. The reading is real, "
      + "but it follows a release schedule as much as the weather — a low river here can "
      + "mean the gate is shut rather than that the country is dry.");
    const reading = screen.getByText(readingHeading("Lead Creek near Town"));
    for (const [a, b] of [[head, disagree], [disagree, dam], [dam, reading]] as const)
      expect(precedes(a, b)).toBe(true);
  });

  it("has exactly one horizon control", async () => {
    await draw("estimate");
    for (const chip of ["Now", "+1d", "+3d", "+5d"])
      expect(screen.getAllByText(chip)).toHaveLength(1);
  });

  it("hands the tapped horizon to the caller from the header", async () => {
    const { onHorizon } = await draw("estimate");
    fireEvent.click(screen.getByLabelText("Forecast 3 days ahead"));
    expect(onHorizon).toHaveBeenCalledWith(3);
  });

  it("keeps the fold closed by default; opening it adds the small print", async () => {
    await draw("estimate");
    const fold = screen.getByRole("button", { name: CONDITIONS_WORDS.fold });
    expect(fold.getAttribute("aria-expanded")).toBe("false");
    expect(screen.queryByText(CONDITIONS_WORDS.explainer)).toBeNull();
    expect(screen.queryByText("SOURCES")).toBeNull();
    expect(screen.queryByText("Counts this much because it drains almost exactly the same "
                              + "country.")).toBeNull();
    fireEvent.click(fold);
    expect(fold.getAttribute("aria-expanded")).toBe("true");
    expect(screen.getByText(CONDITIONS_WORDS.explainer)).toBeTruthy();
    expect(screen.getByText("Counts this much because it drains almost exactly the same "
                            + "country.")).toBeTruthy();
    expect(screen.getByText("SOURCES")).toBeTruthy();
    expect(screen.getByText(CREDITS[0]!)).toBeTruthy();
  });

  it("gives the no-gauge case the same order: headline, its detail, the fold", async () => {
    await draw("none");
    const order = [screen.getByText(CONDITIONS_WORDS.noEstimate),
                   screen.getByText(donorReason("no-station")),
                   screen.getByText(noGaugeSentence()),
                   screen.getByRole("button", { name: CONDITIONS_WORDS.fold })];
    for (let i = 1; i < order.length; i++) expect(precedes(order[i - 1]!, order[i]!)).toBe(true);
    // No horizon control where there is no estimate to move.
    expect(screen.queryByText("+1d")).toBeNull();
  });
});
