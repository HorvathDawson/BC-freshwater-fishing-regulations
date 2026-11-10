/**
 * (T582, F2) A WATER'S CONDITIONS VIEW NAMES ITS OWN DAY AND LINKS TO THE REGULATIONS DAY, never moving it.
 * Whole strings only: the expected lines are core's finished sentences (`DAY_LABELS`, `DAY_LINK_WORDS`).
 *
 * MUTATION PINS: a Conditions view that follows the regulations day (the old "one day"), or one that says nothing of it,
 * fails the first test; a day past the window drawn as a mark (or with no line) fails the second.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { bcNow, DAY_LABELS, DAY_LINK_WORDS, dayWords, FORECAST_WINDOW } from "@app/core";
import type { Panel, WaterSource, SectionId } from "@app/data";
import { clearPanelCache } from "@app/ui";
import { ConditionsPanel } from "./ConditionsPanel";
import { LIGHT } from "./theme";

afterEach(cleanup);
beforeEach(clearPanelCache);

const SEC = 7 as SectionId;
const PANEL: Panel = { areaKm2: 50, members: [
  { station: "LEAD" as never, role: "up", areaKm2: 60, years: { discharge: 40, level: 40 }, regulated: false, sameRiver: true }] };
const source = {
  panelsFor: async () => new Map([[SEC, PANEL]]),
  panelRoutes: async () => [{ station: "LEAD", name: "Lead Creek near Town", lat: 49, lon: -122, role: "up", path: [SEC] }],
  lakeStationsFor: async () => new Map(), gaugeForSection: async () => null, traceToGauge: async () => [],
  gaugeSeries: async () => null, gaugeParameters: async () => ["discharge", "level"],
  gaugeNow: async () => ({ fetchedAt: Date.now() - 5 * 60_000,
                           value: { discharge: 12.5, level: 1.2, percentile: 0.15, standing: "below", at: new Date().toISOString() } }),
} as unknown as WaterSource;
const feed = { index: async () => ({ stations: { LEAD: { percentile: 0.15, parameter: "discharge" as const, discharge: 0.15 } } }) };
const ahead = (n: number) => { const t = bcNow(); return new Date(t.getFullYear(), t.getMonth(), t.getDate() + n, 12); };
const mount = (regsDay: Date | null | undefined, horizon: 0 | 3 = 0) =>
  render(<ConditionsPanel source={source} section={SEC} palette={LIGHT} feed={feed} scroll={false} horizon={horizon} regsDay={regsDay} />);

describe("(T582) Conditions names its own day and links to the regulations day", () => {
  it("says it is on today whatever the regulations day, and marks a day inside the window", async () => {
    mount(ahead(2));
    await waitFor(() => expect(screen.getByTestId("conditions-day-label")).toBeTruthy());
    expect(screen.getByTestId("conditions-day-label").textContent).toBe(DAY_LABELS.conditions(0, bcNow()));
    expect(screen.getByTestId("conditions-day-link").textContent).toBe(DAY_LINK_WORDS.inside(dayWords(ahead(2))));
  });
  it("its own horizon is its own: +3 is named, the regulations day stays what it was", async () => {
    mount(ahead(10), 3);
    await waitFor(() => expect(screen.getByTestId("conditions-day-label")).toBeTruthy());
    expect(screen.getByTestId("conditions-day-label").textContent).toBe(DAY_LABELS.conditions(3, bcNow()));
    expect(screen.getByTestId("conditions-day-link").textContent).toBe(DAY_LINK_WORDS.inside(dayWords(ahead(10))));
  });
  it("a day past the window gets the one line", async () => {
    mount(ahead(FORECAST_WINDOW + 4));
    await waitFor(() => expect(screen.getByTestId("conditions-day-link")).toBeTruthy());
    expect(screen.getByTestId("conditions-day-link").textContent).toBe(`${dayWords(ahead(FORECAST_WINDOW + 4))} is past the forecast.`);
  });
  it("today (or no regulations day) says nothing of a link; a sheet with no regulations day has no day line", async () => {
    mount(null);
    await waitFor(() => expect(screen.getByTestId("conditions-day-label")).toBeTruthy());
    expect(screen.queryByTestId("conditions-day-link")).toBeNull();
    cleanup(); clearPanelCache();
    mount(undefined);
    await waitFor(() => expect(screen.getByTestId("gauge-card")).toBeTruthy());
    expect(screen.queryByTestId("conditions-day")).toBeNull();
  });
});
