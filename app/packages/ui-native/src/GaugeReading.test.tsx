/**
 * (T1194, acceptance 7) THE SHARED READING PIECE, failure and quiet cases: a reading that cannot be loaded is SHOWN as a failure
 * and REPORTED, with no figure, standing or colour-bar mark; a gauge with no current reading says when it last reported.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { GAUGE_PAGE_WORDS, setFailureSink } from "@app/core";
import type { StationId } from "@app/data";
import { LIGHT } from "./theme";
import { GaugeReading, type GaugeReadingSource } from "./GaugeReading";

vi.mock("./StationChart", () => ({ StationChart: () => null }));
afterEach(() => { cleanup(); setFailureSink(null); });

const FED = Date.parse("2026-10-28T05:01:28Z");
const source = (now: () => Promise<unknown>): GaugeReadingSource => ({
  gaugeNow: now as GaugeReadingSource["gaugeNow"],
  gaugeSeries: async () => null,
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
  });

  it("a reading too old to be current: when it last reported, no figure and no bar", async () => {
    const now = async () => ({ fetchedAt: FED, value: { discharge: 8.01, level: null, parameter: "discharge", percentile: 0.02,
                                                       standing: null, at: "2026-10-25T10:00:00-08:00" } });
    render(<GaugeReading source={source(now)} station={ST} palette={LIGHT} theme="light" />);
    await waitFor(() => expect(screen.getByTestId("gauge-no-reading").textContent)
      .toBe("No current reading. Last reported 2026-10-25 at 11:00 PT."));
    expect(screen.queryByTestId("gauge-figure")).toBeNull();
    expect(screen.queryByTestId("legend-ramp")).toBeNull();
  });
});
