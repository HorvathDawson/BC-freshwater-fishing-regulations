/**
 * Owner 2026-10-21, the Conditions chart, mounted:
 *
 *   - "the way you switch gauges is far away from the plot" — the switch sits in the chart block, above the reading
 *     and the plot it changes, not a screen below in the donor list;
 *   - "zoom and look at data better" — the zoom controls work, and a tap on the plot reads the value in core's words;
 *   - "not reporting today should say when last report was" — the quiet row says it.
 *
 * Whole strings only (AGENTS 33).
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { fireGestureHandler, getByGestureTestId } from "react-native-gesture-handler/jest-utils";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { chartLabel, CONDITIONS_WORDS, donorLastReport, noGaugeSentence, readingHeading } from "@app/core";
import type { Panel, RegsSource, SectionId } from "@app/data";
import { makeFixtureSource } from "@app/data/fixture";
import { clearPanelCache } from "@app/ui";
import { ConditionsPanel } from "./ConditionsPanel";
import { LIGHT } from "./theme";

afterEach(cleanup);
beforeEach(clearPanelCache);

const SEC = 7 as SectionId;
const DAY = new Date(2026, 8, 4, 12);
const PANEL: Panel = { areaKm2: 50, members: [
  { station: "LEAD" as never, role: "up", areaKm2: 60, years: 40, regulated: false, sameRiver: true },
  { station: "DAM" as never, role: "down", areaKm2: 55, years: 6, regulated: true, sameRiver: true },
  { station: "QUIET" as never, role: "up", areaKm2: 300, years: 30, regulated: false, sameRiver: true },
] };
const ROUTES = [
  { station: "LEAD", name: "Lead Creek near Town", lat: 49, lon: -122, role: "up", path: [SEC] },
  { station: "DAM", name: "Dam Creek below the Dam", lat: 49.1, lon: -122.1, role: "down", path: [SEC] },
  { station: "QUIET", name: "Quiet River", lat: 49.2, lon: -122.2, role: "up", path: [SEC] },
];
const FIXTURE = makeFixtureSource();
const source = {
  panelsFor: async () => new Map([[SEC, PANEL]]),
  panelRoutes: async () => ROUTES,
  lakeStationsFor: async () => new Map(),
  gaugeForSection: async () => null,
  traceToGauge: async () => [],
  gaugeSeries: (_st: string, span: "72h" | "year", p?: never, m?: string) =>
    FIXTURE.gaugeSeries("08MH001" as never, span, p, m),
  gaugeParameters: async () => ["discharge", "level"],
  gaugeNow: async () => ({ fetchedAt: Date.now() - 5 * 60_000,
                           value: { discharge: 12.5, level: 1.2, percentile: 0.15, standing: "below",
                                    at: "2026-09-04T06:00:00Z" } }),
} as unknown as RegsSource;
const feed = { index: async () => ({ stations: {
  LEAD: { percentile: 0.15, parameter: "discharge" as const, discharge: 0.15 },
  DAM: { percentile: 0.8, parameter: "discharge" as const, discharge: 0.8 },
  QUIET: { percentile: null, parameter: "discharge" as const, observedAt: "2026-08-30T10:00:00-08:00" },
} }) };

async function draw(src: RegsSource = source) {
  render(<GestureHandlerRootView>
    <ConditionsPanel source={src} section={SEC} palette={LIGHT} feed={feed} scroll={false} day={DAY} />
  </GestureHandlerRootView>);
  await waitFor(() => expect(screen.queryByText(readingHeading("Lead Creek near Town"))).toBeTruthy());
}

const precedes = (a: Element, b: Element) =>
  (a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING) !== 0;

describe("the gauge switch sits by the plot", () => {
  it("runs switch → the gauge's reading → the chart's controls, all above the donor list", async () => {
    await draw();
    const order = [
      screen.getByText(CONDITIONS_WORDS.chartGauge),
      screen.getByRole("radio", { name: "Showing LEAD, Lead Creek near Town, 80% of this estimate" }),
      screen.getByText(readingHeading("Lead Creek near Town")),
      screen.getByLabelText(CONDITIONS_WORDS.controls.span),
      screen.getByText(CONDITIONS_WORDS.tapToChart),
    ];
    for (let i = 1; i < order.length; i++)
      expect(precedes(order[i - 1]!, order[i]!), `item ${i} after item ${i - 1}`).toBe(true);
    // every donor is offered, the quiet one too: it says what it last said (see "charting a quiet gauge")
    expect(screen.getByRole("radio", { name: "Chart QUIET, Quiet River, not counted today" })).toBeTruthy();
  });

  it("charts the gauge picked there, and marks it", async () => {
    await draw();
    fireEvent.click(screen.getByRole("radio", { name: "Chart DAM, Dam Creek below the Dam, 20% of this estimate" }));
    await waitFor(() => expect(screen.getByText(readingHeading("Dam Creek below the Dam"))).toBeTruthy());
    expect(screen.getByRole("radio", { name: "Showing DAM, Dam Creek below the Dam, 20% of this estimate" })
      .getAttribute("aria-checked")).toBe("true");
  });

  it("says when the quiet gauge last reported", async () => {
    await draw();
    expect(screen.getByText("Not reporting today.")).toBeTruthy();
    // core's sentence for that instant, in the device's zone — the whole line, as drawn
    expect(screen.getByText(donorLastReport("2026-08-30T10:00:00-08:00"))).toBeTruthy();
    expect(donorLastReport("2026-08-30T10:00:00-08:00", "America/Vancouver"))
      .toBe("Last reported 2026-08-30 at 11:00 PDT.");
  });
});

/** The review's 08MH055: a quiet gauge with no file — no reading and no series to chart. */
const silent = {
  ...(source as unknown as Record<string, unknown>),
  gaugeNow: async (st: string) => (st === "QUIET" ? null : (source as unknown as {
    gaugeNow: (s: string) => Promise<unknown> }).gaugeNow(st)),
  gaugeSeries: (st: string, span: "72h" | "year", p?: never, m?: string) =>
    (st === "QUIET" ? Promise.resolve(null) : FIXTURE.gaugeSeries("08MH001" as never, span, p, m)),
} as unknown as RegsSource;

describe("charting a quiet gauge (review 2026-10-21)", () => {
  it("says the gauge is quiet and when it last reported, under the switch — never the no-gauge refusal", async () => {
    await draw(silent);
    fireEvent.click(screen.getByRole("radio", { name: "Chart QUIET, Quiet River, not counted today" }));
    await waitFor(() => expect(screen.getByText(readingHeading("Quiet River"))).toBeTruthy());
    // the headline answered from three gauges; picking one to chart does not take that back
    expect(screen.queryByText(noGaugeSentence())).toBeNull();
    const radio = screen.getByRole("radio", { name: "Showing QUIET, Quiet River, not counted today" });
    const said = screen.getAllByText(donorLastReport("2026-08-30T10:00:00-08:00"));
    // nothing new above the switch: the quiet gauge's words come after it, before the chart's controls
    const inBlock = said.find((el) => precedes(radio, el)
      && precedes(el, screen.getByLabelText(CONDITIONS_WORDS.controls.span)));
    expect(inBlock, "the last report sits between the switch and the chart controls").toBeTruthy();
    expect(precedes(screen.getByText(CONDITIONS_WORDS.chartGauge), radio)).toBe(true);
  });
});

/** The plot's own element, for the browser's wheel. */
const plot = () => screen.getByLabelText(chartLabel("72h")).querySelector("svg")!;
const wheel = (deltaY: number) => {
  const e = new WheelEvent("wheel", { deltaY, bubbles: true, cancelable: true });
  plot().dispatchEvent(e);
  return e.defaultPrevented;
};

describe("the chart zooms and reads out", () => {
  const ready = async () => {
    await draw();
    await waitFor(() => expect(screen.getByRole("button", { name: CONDITIONS_WORDS.zoom.in })).toBeTruthy());
  };

  it("opens on the whole chart: zoom out and whole chart have nothing to do", async () => {
    await ready();
    expect(screen.getByRole("button", { name: CONDITIONS_WORDS.zoom.out }).getAttribute("aria-disabled"))
      .toBe("true");
    expect(screen.getByRole("button", { name: CONDITIONS_WORDS.zoom.fit }).getAttribute("aria-disabled"))
      .toBe("true");
    // the hint, whole, and no read-out yet
    expect(screen.getByText(CONDITIONS_WORDS.zoomHint.pointer)).toBeTruthy();
    expect(screen.queryByText(/^\d{4}-\d\d-\d\d at \d\d:\d\d .+ · [\d.]+ m³\/s/)).toBeNull();
  });

  it("zooms in, then the whole chart puts it back", async () => {
    await ready();
    fireEvent.click(screen.getByRole("button", { name: CONDITIONS_WORDS.zoom.in }));
    await waitFor(() => expect(screen.getByRole("button", { name: CONDITIONS_WORDS.zoom.out })
      .getAttribute("aria-disabled")).not.toBe("true"));
    fireEvent.click(screen.getByRole("button", { name: CONDITIONS_WORDS.zoom.fit }));
    await waitFor(() => expect(screen.getByRole("button", { name: CONDITIONS_WORDS.zoom.out })
      .getAttribute("aria-disabled")).toBe("true"));
  });

  it("a tap on the plot reads the value there in core's words; a double-tap clears it", async () => {
    await ready();
    act(() => { fireGestureHandler(getByGestureTestId("chart-tap"), [{ x: 200, y: 80 }, { x: 200, y: 80 }]); });
    const line = await waitFor(() => {
      const el = screen.getByText(/^\d{4}-\d\d-\d\d at \d\d:\d\d .+ · [\d.]+ m³\/s/);
      return el.textContent!;
    });
    expect(line).not.toBe(CONDITIONS_WORDS.zoomHint.pointer);
    // The hint stays, whole, under the plot while a value is read (it was cut to "…" on a phone above it).
    expect(screen.getByText(CONDITIONS_WORDS.zoomHint.pointer)).toBeTruthy();
    act(() => { fireGestureHandler(getByGestureTestId("chart-double-tap"), [{ x: 1, y: 1 }, { x: 1, y: 1 }]); });
    await waitFor(() => expect(screen.queryByText(/^\d{4}-\d\d-\d\d at \d\d:\d\d .+ · [\d.]+ m³\/s/)).toBeNull());
  });

  it("a wheel the chart cannot use scrolls the page: out at the whole chart, in at the narrowest", async () => {
    await ready();
    expect(wheel(400), "zoom out at the whole chart: the page scrolls").toBe(false);
    // a trackpad pinch (ctrl+wheel) over the chart never zooms the PAGE, even with nothing left to zoom
    const pinch = new WheelEvent("wheel", { deltaY: 400, ctrlKey: true, bubbles: true, cancelable: true });
    plot().dispatchEvent(pinch);
    expect(pinch.defaultPrevented).toBe(true);
    expect(screen.getByRole("button", { name: CONDITIONS_WORDS.zoom.out }).getAttribute("aria-disabled"))
      .toBe("true");
    expect(wheel(-100), "zoom in: the chart takes it").toBe(true);
    await waitFor(() => expect(screen.getByRole("button", { name: CONDITIONS_WORDS.zoom.out })
      .getAttribute("aria-disabled")).not.toBe("true"));
    // in, and in, to the narrowest window; then one more notch in is the page's
    for (let i = 0; i < 40; i++) act(() => { wheel(-400); });
    expect(wheel(-400), "zoom in at the narrowest: the page scrolls").toBe(false);
    expect(wheel(100), "zoom out from the narrowest: the chart takes it").toBe(true);
  });

  it("a pinch zooms in", async () => {
    await ready();
    act(() => { fireGestureHandler(getByGestureTestId("chart-pinch"),
      [{ scale: 1, focalX: 180 }, { scale: 1.5, focalX: 180 }, { scale: 3, focalX: 180 }]); });
    await waitFor(() => expect(screen.getByRole("button", { name: CONDITIONS_WORDS.zoom.out })
      .getAttribute("aria-disabled")).not.toBe("true"));
  });
});
