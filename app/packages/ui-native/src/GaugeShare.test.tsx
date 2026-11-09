/**
 * (T530) THE GAUGE CARD'S SHARE, on the Conditions panel: drawn on the card's title row when the Shell's share and a water with
 * a page are there, pressed once per tap, what it said beside it; absent without either.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { GAUGE_SHARE_WORDS, SHARE_WORDS, SITE_ORIGIN } from "@app/core";
import type { Panel, WaterSource, SectionId } from "@app/data";
import { clearPanelCache, type SharePort } from "@app/ui";
import { ConditionsPanel } from "./ConditionsPanel";
import { GaugeShare } from "./GaugeShare";
import { SharePortContext } from "./SharePortContext";
import { LIGHT } from "./theme";

afterEach(() => { cleanup(); clearPanelCache(); });

const SEC = 7 as SectionId;
const PANEL: Panel = { areaKm2: 50, members: [
  { station: "LEAD" as never, role: "up", areaKm2: 60, years: { discharge: 40, level: 40 }, regulated: false, sameRiver: true }] };
const source = {
  panelsFor: async () => new Map([[SEC, PANEL]]),
  panelRoutes: async () => [{ station: "LEAD", name: "Lead Creek near Town", lat: 49, lon: -122, role: "up", path: [SEC] }],
  lakeStationsFor: async () => new Map(), gaugeForSection: async () => null, traceToGauge: async () => [],
  gaugeSeries: async () => null, gaugeParameters: async () => ["discharge", "level"],
  gaugeNow: async () => ({ fetchedAt: Date.now() - 5 * 60_000, value: { discharge: 12.5, level: 1.2, percentile: 0.15, standing: "below", at: "2026-09-04T06:00:00Z" } }),
} as unknown as WaterSource;

const feed = { index: async () => ({ stations: { LEAD: { percentile: 0.15, parameter: "discharge" as const, discharge: 0.15 } } }) };
const mount = (port: SharePort | null, water: { item: string | null; name: string } | null) => render(
  <SharePortContext.Provider value={port}>
    <ConditionsPanel source={source} section={SEC} palette={LIGHT} feed={feed} scroll={false} day={new Date(2026, 8, 4, 12)} alertWater={water} />
  </SharePortContext.Provider>);

describe("(T530) Share on the Conditions gauge card", () => {
  it("drawn beside the gauge's title for a water with a page; a press sends the gauge page's address", async () => {
    const send = vi.fn(async () => "copied" as const);
    mount({ send }, { item: "gnis:1", name: "Lead Creek" });
    await waitFor(() => expect(screen.getByTestId("gauge-share")).toBeTruthy());
    fireEvent.click(screen.getByLabelText(GAUGE_SHARE_WORDS.label));
    await waitFor(() => expect(screen.getByTestId("gauge-share-said").textContent).toBe(SHARE_WORDS.copied));
    expect(send).toHaveBeenCalledWith({ url: `${SITE_ORIGIN}/gauge/LEAD/lead-creek-near-town`, title: SHARE_WORDS.title });
  });

  it("not drawn without a water with a page, or without a platform share", async () => {
    mount({ send: async () => "sent" }, { item: null, name: "Lead Creek" });
    await waitFor(() => expect(screen.getByTestId("gauge-title")).toBeTruthy());
    expect(screen.queryByTestId("gauge-share")).toBeNull();
    cleanup();
    mount(null, { item: "gnis:1", name: "Lead Creek" });
    await waitFor(() => expect(screen.getByTestId("gauge-title")).toBeTruthy());
    expect(screen.queryByTestId("gauge-share")).toBeNull();
  });

  it("the control alone: busy is inert", () => {
    const press = vi.fn();
    render(<GaugeShare palette={LIGHT} share={{ label: GAUGE_SHARE_WORDS.label, said: SHARE_WORDS.failed, busy: false, press }} />);
    fireEvent.click(screen.getByTestId("gauge-share"));
    expect([press.mock.calls.length, screen.getByTestId("gauge-share-said").textContent]).toEqual([1, SHARE_WORDS.failed]);
  });
});
