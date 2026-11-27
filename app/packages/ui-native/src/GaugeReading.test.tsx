/**
 * (T1194, acceptance 7) THE SHARED READING PIECE, failure and quiet cases: a reading that cannot be loaded is SHOWN as a failure
 * and REPORTED, with no figure, standing or colour-bar mark; a gauge with no current reading says when it last reported.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { FLOW_LEGEND, GAUGE_PAGE_WORDS, setFailureSink } from "@app/core";
import { FEED_TTL_MS, type StationId } from "@app/data";
import { LIGHT } from "./theme";
import { GaugeReading, type GaugeReadingSource } from "./GaugeReading";

vi.mock("./StationChart", () => ({ StationChart: () => null }));
// (T1205) a reading is current BY THE CLOCK: the clock is pinned at the fixture's own time (20 minutes after its feed was written)
beforeEach(() => { vi.useFakeTimers({ toFake: ["Date", "setInterval", "clearInterval"], shouldAdvanceTime: true }); vi.setSystemTime(FED + 20 * 60_000); });
afterEach(() => { cleanup(); setFailureSink(null); vi.useRealTimers(); });

const FED = Date.parse("2026-10-28T05:01:28Z");
const H = 3_600_000;
const source = (now: () => Promise<unknown>, flows: number[] | null = null): GaugeReadingSource => ({
  gaugeNow: now as GaugeReadingSource["gaugeNow"],
  gaugeSeries: (async () => (flows ? { value: { step: "1h", from: "", parameter: "discharge", values: flows, band: [], now: null, forecast: null, forecasts: [],
                                               at: flows.map((_, i) => new Date(FED - (flows.length - 1 - i) * 12 * H).toISOString()) }, fetchedAt: FED } : null)) as never,
  gaugeParameters: async () => ["discharge"],
});
const ST = "08MH103" as StationId;

describe("(T1194) the shared gauge reading", () => {
  it("a feed that fails: the failure line, reported; no figure, no standing, no colour bar", async () => {
    const sent: string[] = [];
    setFailureSink((e) => { sent.push((e as Error).message); });
    render(<GaugeReading source={source(async () => { throw new Error("feed 500"); })} station={ST} palette={LIGHT} theme="light" />);
    await waitFor(() => expect(screen.getByTestId("gauge-feed-failed").textContent).toBe(GAUGE_PAGE_WORDS.feedFailed));
    expect(sent).toContain("feed 500");
    for (const id of ["gauge-figure", "gauge-standing", "legend-ramp"]) expect(screen.queryByTestId(id)).toBeNull();
  });

  it("a current ranked reading: the figure, its standing and the colour bar", async () => {
    const now = async () => ({ fetchedAt: FED, value: { discharge: 8.01, level: null, parameter: "discharge", percentile: 0.02,
                                                       standing: null, at: "2026-10-27T20:05:00-08:00" } });
    render(<GaugeReading source={source(now)} station={ST} palette={LIGHT} theme="light" />);
    await waitFor(() => expect(screen.getByTestId("gauge-figure").textContent).toBe("8.01 m³/s"));
    expect(screen.getByTestId("legend-ramp")).toBeTruthy();
    expect(screen.queryByTestId("gauge-no-reading")).toBeNull();
    // (review 2026-11-27) the bar's ACCESSIBLE NAMES say THIS GAUGE, in words (percentile 0.02), never "1 reach" and never colour alone
    const near = FLOW_LEGEND.near(0.025);
    const names = [...screen.getByTestId("legend-ramp").querySelectorAll("[aria-label]")].map((e) => e.getAttribute("aria-label"));
    expect(names).toContain(`This gauge: ${near}`);
    expect(names).toContain(`${FLOW_LEGEND.scale(FLOW_LEGEND.title.discharge, FLOW_LEGEND.low, FLOW_LEGEND.high, FLOW_LEGEND.mid)}. This gauge: ${FLOW_LEGEND.near(0.02)}`);
    expect(names.join("|")).not.toMatch(/reach/i);
  });

  it("(T1205) 9 h old by the clock, whatever the feed was written at: when it last reported, no figure, no bar, no trend", async () => {
    vi.setSystemTime(FED + 9 * 3_600_000);
    const now = async () => ({ fetchedAt: FED, value: { discharge: 8.01, level: null, parameter: "discharge", percentile: 0.02,
                                                       standing: null, at: "2026-10-27T20:05:00-08:00" } });
    render(<GaugeReading source={source(now)} station={ST} palette={LIGHT} theme="light" />);
    await waitFor(() => expect(screen.getByTestId("gauge-no-reading").textContent)
      .toBe("No current reading. Last reported 2026-10-27 at 21:05 PT."));
    expect(screen.queryByTestId("gauge-figure")).toBeNull();
    expect(screen.queryByTestId("legend-ramp")).toBeNull();
  });
});

describe("(T1194) the trend word beside the reading", () => {
  const now = async () => ({ fetchedAt: FED, value: { discharge: 8.01, level: null, parameter: "discharge", percentile: 0.02,
                                                     standing: null, at: "2026-10-27T20:05:00-08:00" } });
  it("falling: said beside the figure; with no measured line a day back there is no word", async () => {
    render(<GaugeReading source={source(now, [20, 14, 12, 9, 8.01])} station={ST} palette={LIGHT} theme="light" />);
    await waitFor(() => expect(screen.getByTestId("gauge-trend").textContent).toBe("· falling"));
    expect(screen.getByTestId("gauge-figure").textContent).toBe("8.01 m³/s");
    cleanup();
    render(<GaugeReading source={source(now, [8.01])} station={ST} palette={LIGHT} theme="light" />);
    await waitFor(() => expect(screen.getByTestId("gauge-figure").textContent).toBe("8.01 m³/s"));
    expect(screen.queryByTestId("gauge-trend")).toBeNull();
  });
});

describe("(T1205) the open page goes old as time passes, with no new feed", () => {
  it("current at 20 minutes; the clock passes the 8 h line and the feed's beat comes: the figure, bar and trend give way to the last-reported line", async () => {
    const now = async () => ({ fetchedAt: FED, value: { discharge: 8.01, level: null, parameter: "discharge", percentile: 0.02,
                                                       standing: null, at: "2026-10-27T20:05:00-08:00" } });
    render(<GaugeReading source={source(now, [20, 14, 12, 9, 8.01])} station={ST} palette={LIGHT} theme="light" />);
    await waitFor(() => expect(screen.getByTestId("gauge-figure").textContent).toBe("8.01 m³/s"));
    expect(screen.getByTestId("legend-ramp")).toBeTruthy();
    vi.setSystemTime(FED + 9 * 3_600_000);
    act(() => { vi.advanceTimersByTime(FEED_TTL_MS + 1000); });
    await waitFor(() => expect(screen.getByTestId("gauge-no-reading").textContent).toBe("No current reading. Last reported 2026-10-27 at 21:05 PT."));
    for (const id of ["gauge-figure", "gauge-trend", "gauge-standing", "legend-ramp"]) expect(screen.queryByTestId(id), id).toBeNull();
  });
});

describe("(owner 2026-11-27) a Level reading's trend word, by the change in the stage; none on tidal water", () => {
  const level = async () => ({ fetchedAt: FED, value: { discharge: null, level: 531.25, parameter: "level", percentile: 0.5,
                                                       standing: null, at: "2026-10-27T20:05:00-08:00" } });
  it("a lake: steady inside 2 cm, rising past it; the same line, flagged tidal: no word", async () => {
    render(<GaugeReading source={source(level, [531.24, 531.245, 531.25, 531.25, 531.25])} station={ST} palette={LIGHT} theme="light" />);
    await waitFor(() => expect(screen.getByTestId("gauge-trend").textContent).toBe("· steady"));
    cleanup();
    render(<GaugeReading source={source(level, [531.0, 531.1, 531.2, 531.25, 531.25])} station={ST} palette={LIGHT} theme="light" />);
    await waitFor(() => expect(screen.getByTestId("gauge-trend").textContent).toBe("· rising"));
    cleanup();
    render(<GaugeReading source={source(level, [531.0, 531.1, 531.2, 531.25, 531.25])} station={ST} palette={LIGHT} theme="light" tidal />);
    await waitFor(() => expect(screen.getByTestId("gauge-figure").textContent).toBe("531.25 m"));
    expect(screen.queryByTestId("gauge-trend")).toBeNull();
  });
});
