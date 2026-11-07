/**
 * P16 — the Conditions panel's LAYOUT, and the "same content" guard.
 *
 * Owner Q10: the content was right and the formatting was not. So this file pins two
 * things and nothing else:
 *
 *   1. THE ORDER: headline → its horizon control (T367: both in the estimate tile) → the lead
 *      gauge's card (title, toggles, reading, chart) → the donor list → one fold holding the explanations, closed.
 *   2. THE SAME WORDS: every line of text the panel draws (with the fold opened) is named
 *      below, whole, per scene — the set P16 kept from the panel before it (once a recorded
 *      snapshot, T62 3.10: now asserted directly). A dropped sentence fails here.
 *
 * Whole strings only (AGENTS 33): each recorded line is matched as an entire text node.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { CONDITIONS_WORDS, donorReason, forecastRunLine, horizonDateChip, bcNow, localStamp, noGaugeSentence,
         type Horizon } from "@app/core";

/** (T367) a forecast day's chip: its date, from the device's today */
const chipOf = (d: number) => horizonDateChip(d as Horizon, bcNow());
/** (T367) the forecast days sit in the header's drop-down: open it */
const openDays = () => fireEvent.click(screen.getByLabelText("Choose a forecast day"));
import type { Panel, WaterSource, SectionId } from "@app/data";
import { makeFixtureSource } from "@app/data/fixture";
import { clearPanelCache } from "@app/ui";
import { ConditionsPanel } from "./ConditionsPanel";
import { LIGHT } from "./theme";

afterEach(cleanup);
beforeEach(clearPanelCache);

const SEC = 7 as SectionId;
const DAY = new Date(2026, 8, 4, 12);

/** Three donors: a long-record lead, a short-record dam, and one that is not reporting. */
const PANEL: Panel = { areaKm2: 50, members: [
  { station: "LEAD" as never, role: "up", areaKm2: 60, years: { discharge: 40, level: 40 }, regulated: false,
    sameRiver: true },
  { station: "DAM" as never, role: "down", areaKm2: 55, years: { discharge: 6, level: 6 }, regulated: true,
    sameRiver: true },
  { station: "QUIET" as never, role: "up", areaKm2: 300, years: { discharge: 30, level: 30 }, regulated: false,
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
}) as unknown as WaterSource;

/** The two donors disagree by far more than 25 points, so the disagreement note shows. */
const feed = { index: async () => ({ stations: {
  LEAD: { percentile: 0.15, parameter: "discharge" as const, discharge: 0.15 },
  DAM: { percentile: 0.8, parameter: "discharge" as const, discharge: 0.8 },
} }) };

const NOW = { discharge: 12.5, level: 1.2, percentile: 0.15, standing: "below",
              at: "2026-09-04T06:00:00Z" };
const CREDITS = ["Hydrometric data: Environment and Climate Change Canada",
                 "Forecast data provided by the BC River Forecast Centre"];

/**
 * A third scene, NOT in `SAYS`: the lead gauge with a forecast running, so the chart
 * carries the Centre's disclaimer. The other scenes have no series (the chart says so),
 * and adding one there would change what they draw.
 */
const FIXTURE = makeFixtureSource();
const forecasting = () => ({
  ...(sourceWith(PANEL, NOW) as unknown as Record<string, unknown>),
  gaugeSeries: (_st: string, span: "72h" | "year", p?: never, m?: string) =>
    FIXTURE.gaugeSeries("08MH001" as never, span, p, m),
}) as unknown as WaterSource;

const SCENES = {
  estimate: () => sourceWith(PANEL, NOW),
  none: () => sourceWith(null, null),
  forecast: forecasting,
} as const;
type Scene = keyof typeof SCENES;

async function draw(scene: Scene) {
  const onHorizon = vi.fn();
  render(<ConditionsPanel source={SCENES[scene]()} section={SEC} palette={LIGHT} feed={feed}
                          scroll={false} day={DAY} credits={CREDITS}
                          onHorizon={onHorizon} />);
  await waitFor(() => expect(scene === "none" ? screen.queryByText(noGaugeSentence())
    : screen.queryByTestId("reading-figure")).toBeTruthy());
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


/**
 * WHAT THE PANEL SAYS, per scene, with the fold open: every line, whole (AGENTS 33). The reading's time is said in the
 * reader's clock with its zone named (P16 item 5, core's `localStamp`). A silent donor (QUIET) draws no row; one line
 * names it instead (owner 2026-10-22, "yes they should be hidden but mentioned with a last data time").
 */
const SILENT_ROW = ["Quiet River", "not counted today", "Not reporting today.",
                    "upstream · drains 300 km² · 6.0× apart · on this reach"];
const SILENT_LINE = "Not reporting: Quiet River (last report not known)";
const SAYS: Readonly<Record<"estimate" | "none", readonly string[]>> = {
  estimate: [
    "ESTIMATE FOR THIS SPOT",
    "About normal for the time of year",
    "Lower than 7 days in 10 at this time of year — fairly confident, from 2 gauges nearby.",
    "3rd–49th percentile for early September · this spot drains 50 km²",
    "Now",
    "Forecast",
    "Change",
    "TAP A GAUGE TO SEE ITS CHART",
    "LEAD",
    "Lead Creek near Town",
    "80% of this estimate",
    "Reads lower than 8 days in 10 at this time of year.",
    "upstream · drains 60 km² · same size · on this reach",
    "Counts this much because it drains almost exactly the same country.",
    "DAM",
    "Dam Creek below the Dam",
    "20% of this estimate",
    "Reads higher than 8 days in 10 at this time of year.",
    "downstream · drains 55 km² · same size · dam-controlled · on this reach",
    "Counts this much because it drains almost exactly the same country; it sits downstream, so it carries extra water; it has only 6 years of record, so it counts 30%.",
    "This water is controlled by a dam. The reading is real, but it follows a release schedule as much as the weather — a low river here can mean the gate is shut rather than that the country is dry.",
    "These gauges disagree by 46 points. That is a catchment doing more than one thing, and the range above is the honest answer rather than their average.",
    "No gauge sits exactly here, so the reading is carried from the nearest ones. A gauge counts for more when it drains a piece of country the same size as this one, when it sits upstream, and when it has a long record. The percentage beside each is how much of the answer above came from it.",
    "12.5",
    "m³/s",
    "· 1.2 m stage",
    "Below normal · 15th percentile for the date",
    `This gauge's own reading · observed ${localStamp(Date.parse("2026-09-04T06:00:00Z"))} · checked 5 min ago`,
    "Flow",
    "Level",
    "72 h",
    "Year",
    "This station published no recent readings.",
    "SOURCES",
    "Hydrometric data: Environment and Climate Change Canada",
    "Forecast data provided by the BC River Forecast Centre",
    SILENT_LINE,
  ],
  none: [
    "NO ESTIMATE",
    "No gauge on this water is close enough in size to speak for it.",
    "No gauge is entitled to speak for this water. A station draining a far larger watershed would have given a number, and the number would have been wrong.",
    "SOURCES",
    "Hydrometric data: Environment and Climate Change Canada",
    "Forecast data provided by the BC River Forecast Centre",
  ],
};

describe("P16: the same content", () => {
  for (const s of ["estimate", "none"] as const) {
    it(`draws every line, whole (${s})`, async () => {
      await draw(s);
      openFold();
      const drawn = new Set(lines());
      expect(SAYS[s].filter((l) => !drawn.has(l))).toEqual([]);
      // the silent donor's row is not drawn
      expect(SILENT_ROW.filter((l) => drawn.has(l))).toEqual([]);
    });
  }

  it("is not vacuous: with the fold shut it reports the small print missing", async () => {
    // MUTATION PIN. The guard opens the fold; skip that and the explainer and the credits
    // must be reported missing — so a layout that dropped them could not pass.
    await draw("estimate");
    const shut = new Set(lines());
    expect(SAYS.estimate.filter((l) => !shut.has(l))).toEqual(expect.arrayContaining([
      CONDITIONS_WORDS.explainer, "SOURCES", CREDITS[0]]));
  });
});

describe("P16: one reading order", () => {
  it("runs headline → horizon → gauge card (title, reading, chart) → donors → fold", async () => {
    await draw("estimate");
    const order = [
      screen.getByText("ESTIMATE FOR THIS SPOT"),                   // the headline's kicker (T367: the tile's top row …
      screen.getByLabelText("Choose a forecast day"),              // … holds the horizon control, after it)
      screen.getByTestId("gauge-title"),                            // the gauge card: its title …
      screen.getByLabelText(CONDITIONS_WORDS.controls.span),        // … and the chart's toggles on its row,
      screen.getByTestId("reading-figure"),                         // the reading (this scene has no series to chart)
      screen.getByText(CONDITIONS_WORDS.tapToChart),                // the donor list
      screen.getByRole("button", { name: CONDITIONS_WORDS.fold }),  // the fold
    ];
    for (let i = 1; i < order.length; i++)
      expect(precedes(order[i - 1]!, order[i]!), `block ${i} after block ${i - 1}`).toBe(true);
  });

  it("folds the notes with the rest of the small print, closed by default", async () => {
    await draw("estimate");
    const DISAGREE = "These gauges disagree by 46 points. That is a catchment doing more than "
      + "one thing, and the range above is the honest answer rather than their average.";
    const DAM = "This water is controlled by a dam. The reading is real, but it follows a "
      + "release schedule as much as the weather — a low river here can mean the gate is "
      + "shut rather than that the country is dry.";
    // SHUT: the notes are not on screen.
    expect(screen.queryByText(DISAGREE)).toBeNull();
    expect(screen.queryByText(DAM)).toBeNull();
    // OPEN: under the donor list, in the order they were said before P16, above the explainer.
    openFold();
    const fold = screen.getByRole("button", { name: CONDITIONS_WORDS.fold });
    const order = [screen.getByText(CONDITIONS_WORDS.tapToChart), fold,
                   screen.getByText(DAM), screen.getByText(DISAGREE),
                   screen.getByText(CONDITIONS_WORDS.explainer), screen.getByText("SOURCES")];
    for (let i = 1; i < order.length; i++)
      expect(precedes(order[i - 1]!, order[i]!), `item ${i} after item ${i - 1}`).toBe(true);
  });

  it("(T300) shuts the model's information and the Centre's disclaimer; ELF alone is said, and both open on a press", async () => {
    await draw("forecast");
    const DISCLAIMER = "USERS OF THIS DATA MUST ACCEPT ALL RESPONSIBILITY FOR THE USE AND "
      + "INTERPRETATION.";
    const run = forecastRunLine({ model: "ELF", issuedAt: "2026-08-30T00:00:00Z", horizonDays: 30 });
    // (T398) ELF is opt-in: the reader draws it; then it is the only model with one here — said, not hidden …
    fireEvent.click(await screen.findByTestId("chart-model-ELF"));
    await waitFor(() => expect(screen.getByTestId("only-elf").textContent).toBe(CONDITIONS_WORDS.onlyModel("ELF")));
    // … but the model's information and the disclaimer are shut: one "About this forecast", one "Sources and notes"
    expect(screen.queryByText(DISCLAIMER)).toBeNull();
    expect(screen.queryByTestId("about-forecast")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: CONDITIONS_WORDS.aboutForecast }));
    // the fixture's run is weeks old on any real clock: past the forecast's 36 h line (core staleness.ts), it says so
    const runLine = screen.getByText(run);
    expect(run).toContain("likely out of date");
    expect(screen.getByTestId("about-forecast").textContent).toContain("ELF is a low-water model");
    fireEvent.click(screen.getByRole("button", { name: CONDITIONS_WORDS.fold }));
    const disclaimer = screen.getByText(DISCLAIMER);
    expect(precedes(runLine, disclaimer)).toBe(true);
    expect(CONDITIONS_WORDS.fold).toBe("Sources and notes");
  });

  it("(T398) an ELF-only gauge opens on the measured line; ELF is drawn on a press and switched fully off again", async () => {
    await draw("forecast");
    const off = await screen.findByTestId("chart-model-off");
    // OPENS REAL-TIME: the off entry is the one checked, ELF is offered, nothing speaks for a forecast
    expect(off.getAttribute("aria-checked")).toBe("true");
    expect(screen.getByTestId("chart-model-ELF").getAttribute("aria-checked")).toBe("false");
    expect(off.getAttribute("aria-label")).toBe(CONDITIONS_WORDS.key.realtimeLabel(true));
    expect(screen.queryByTestId("only-elf")).toBeNull();
    expect(screen.queryByRole("button", { name: CONDITIONS_WORDS.aboutForecast })).toBeNull();
    // ON: the reader draws ELF
    fireEvent.click(screen.getByTestId("chart-model-ELF"));
    await waitFor(() => expect(screen.getByTestId("chart-model-ELF").getAttribute("aria-checked")).toBe("true"));
    expect(screen.getByTestId("chart-model-off").getAttribute("aria-checked")).toBe("false");
    expect(screen.getByTestId("only-elf")).toBeTruthy();
    // OFF AGAIN: always offered
    fireEvent.click(screen.getByTestId("chart-model-off"));
    await waitFor(() => expect(screen.queryByTestId("only-elf")).toBeNull());
    expect(screen.getByTestId("chart-model-off").getAttribute("aria-checked")).toBe("true");
  });

  // ONE SET OF CHIPS IN THE PANEL. The owner's rule (2026-10-20) is not "exactly one control
  // in the app": the Conditions map has its own in the corner (MapScreen.test.tsx), the panel
  // has this one in its header, and both set ONE horizon state (HorizonShared.test.tsx).
  it("has one set of horizon chips in its header, ten days included", async () => {
    await draw("estimate");
    expect(screen.queryByText(chipOf(1))).toBeNull();               // (T367) the days wait in the drop-down
    openDays();
    for (const chip of ["Now", ...[1, 2, 3, 4, 5, 6, 7, 8, 9, 10].map(chipOf)])
      expect(screen.getAllByText(chip)).toHaveLength(1);
  });

  it("hands the tapped horizon to the caller from the header", async () => {
    const { onHorizon } = await draw("estimate");
    openDays();
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
    expect(screen.queryByText(chipOf(1))).toBeNull();
  });
});
