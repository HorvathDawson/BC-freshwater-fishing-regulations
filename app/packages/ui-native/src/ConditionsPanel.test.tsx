/**
 * The conditions panel, mounted (P12a): it draws `useConditionsView` and decides nothing.
 *
 * Whole strings only (AGENTS 33): the expected lines are core's finished sentences, so a
 * reading line that rendered broken cannot pass on a fragment.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { noGaugeSentence, readingHeading, readingStandingLine, stageLine } from "@app/core";
import type { Panel, RegsSource, SectionId } from "@app/data";
import { clearPanelCache } from "@app/ui";
import { ConditionsPanel } from "./ConditionsPanel";
import { LIGHT } from "./theme";

afterEach(cleanup);
beforeEach(clearPanelCache);

const SEC = 7 as SectionId;
const PANEL: Panel = { areaKm2: 50, members: [
  { station: "LEAD" as never, role: "up", areaKm2: 60, years: 40, regulated: false,
    sameRiver: true },
] };

const sourceWith = (now: Record<string, unknown> | null, panel: Panel | null) => ({
  panelsFor: async () => new Map(panel ? [[SEC, panel]] : []),
  panelRoutes: async () => [{ station: "LEAD", name: "Lead Creek near Town", lat: 49,
                              lon: -122, role: "up", path: [SEC] }],
  lakeStationsFor: async () => new Map(),
  gaugeForSection: async () => null,
  traceToGauge: async () => [],
  gaugeSeries: async () => null,
  gaugeParameters: async () => ["discharge", "level"],
  gaugeNow: async () => (now ? { fetchedAt: Date.now() - 5 * 60_000, value: now } : null),
}) as unknown as RegsSource;

const feed = { index: async () => ({ stations: {
  LEAD: { percentile: 0.15, parameter: "discharge" as const, discharge: 0.15 } } }) };

describe("the conditions panel", () => {
  it("draws the lead donor's reading as the view model finished it", async () => {
    const source = sourceWith({ discharge: 12.5, level: 1.2, percentile: 0.15,
                                standing: "below", at: "2026-09-04T06:00:00Z" }, PANEL);
    render(<ConditionsPanel source={source} section={SEC} palette={LIGHT} feed={feed}
                            scroll={false} day={new Date(2026, 8, 4, 12)} />);
    await waitFor(() =>
      expect(screen.getByText(readingHeading("Lead Creek near Town"))).toBeTruthy());
    expect(screen.getByText("AT LEAD CREEK NEAR TOWN")).toBeTruthy();
    expect(screen.getByText("12.5")).toBeTruthy();
    expect(screen.getByText("m³/s")).toBeTruthy();
    expect(screen.getByText(stageLine(1.2))).toBeTruthy();
    expect(screen.getByText(readingStandingLine(0.15, "below"))).toBeTruthy();
    expect(screen.getByText("BELOW NORMAL — 15th percentile for the date")).toBeTruthy();
    // The donor panel above it is drawn for the CHOSEN day.
    expect(screen.getByText("3rd–27th percentile for early September · this spot drains "
                            + "50 km²")).toBeTruthy();
  });

  it("refuses in words where no gauge may speak", async () => {
    render(<ConditionsPanel source={sourceWith(null, null)} section={SEC} palette={LIGHT}
                            feed={feed} scroll={false} />);
    await waitFor(() => expect(screen.getByText(noGaugeSentence())).toBeTruthy());
  });
});
