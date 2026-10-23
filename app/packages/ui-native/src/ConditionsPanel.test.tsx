/**
 * The conditions panel, mounted (P12a): it draws `useConditionsView` and decides nothing.
 *
 * Whole strings only (AGENTS 33): the expected lines are core's finished sentences, so a
 * reading line that rendered broken cannot pass on a fragment.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { fireGestureHandler, getByGestureTestId } from "react-native-gesture-handler/jest-utils";
import { CHANGE_LOW_WATER, changeSourceLine, CONDITIONS_WORDS, noGaugeSentence, readingHeading, readingStandingLine,
         stageLine, TIDAL_NOTE } from "@app/core";
import type { Panel, RegsSource, SectionId } from "@app/data";
import { clearPanelCache } from "@app/ui";
import { ConditionsPanel } from "./ConditionsPanel";
import { WaterViewsContext, type WaterViews } from "./ViewSwipe";
import { LIGHT } from "./theme";

afterEach(cleanup);
beforeEach(clearPanelCache);

const SEC = 7 as SectionId;
const PANEL: Panel = { areaKm2: 50, members: [
  { station: "LEAD" as never, role: "up", areaKm2: 60, years: { discharge: 40, level: 40 }, regulated: false,
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

  it("hands the map's quantity to the donor panel, so a tidal lead is disclosed in Level only (T78)", async () => {
    const tidalSource = (base: RegsSource) => ({
      ...base,
      panelRoutes: async () => [{ station: "LEAD", name: "Lead Creek near Town", lat: 49, lon: -122, role: "up",
                                  path: [SEC], tidal: true }],
    }) as unknown as RegsSource;
    const both = { index: async () => ({ stations: {
      LEAD: { percentile: 0.15, parameter: "discharge" as const, discharge: 0.15, level: 0.4 } } }) };
    const source = tidalSource(sourceWith({ discharge: 12.5, level: 1.2, percentile: 0.15, standing: "below",
                                            at: "2026-09-04T06:00:00Z" }, PANEL));
    const fold = () => fireEvent.click(screen.getByRole("button", { name: CONDITIONS_WORDS.fold }));
    render(<ConditionsPanel source={source} section={SEC} palette={LIGHT} feed={both} parameter="level"
                            scroll={false} day={new Date(2026, 8, 4, 12)} />);
    await waitFor(() => expect(screen.getByRole("button", { name: CONDITIONS_WORDS.fold })).toBeTruthy());
    fold();
    await waitFor(() => expect(screen.getByText(TIDAL_NOTE)).toBeTruthy());
    cleanup();
    clearPanelCache();
    render(<ConditionsPanel source={source} section={SEC} palette={LIGHT} feed={both} parameter="discharge"
                            scroll={false} day={new Date(2026, 8, 4, 12)} />);
    await waitFor(() => expect(screen.getByRole("button", { name: CONDITIONS_WORDS.fold })).toBeTruthy());
    fold();
    await waitFor(() => expect(screen.getByText(readingStandingLine(0.15, "below"))).toBeTruthy());
    expect(screen.queryByText(TIDAL_NOTE)).toBeNull();
  });
});

describe("the Change block, mounted (T144)", () => {
  // The station's change as the feed writes it: ELF alone (no flood model running), so the low-water caveat.
  const low = { models: ["ELF"], lowWater: true, reach: { ELF: "2026-09-08" }, dir: "fall", base: 1,
                peak: { day: "2026-09-04", ratio: 1, value: 1, back: null },
                trough: { day: "2026-09-08", ratio: 0.4, value: 0.4, back: null },
                days: [["2026-09-04", 1, 1], ["2026-09-08", 0.4, 0.4]] };
  const elfRun = { model: "ELF", issuedAt: "2026-09-04T09:00:00", horizonDays: 30, value: 5, extreme: "min",
                   min: 5, ave: 8, max: 12, unit: "m3/s", disclaimer: null,
                   series: { step: "1d", at: ["2026-09-04"], mid: [12], lo: [12], hi: [12] },
                   change: { ...low, base: 12, models: undefined } };
  const series = (change: unknown) => ({ fetchedAt: Date.now(), value: {
    step: "1h", from: "2026-09-04T00:00:00Z", parameter: "discharge", at: [], values: [], band: [], now: null,
    forecast: elfRun, forecasts: [elfRun], priorYears: [], change } });

  it("draws the publisher's source line and the low-water caveat it carries", async () => {
    const source = { ...sourceWith({ discharge: 12, level: null, percentile: 0.15, standing: "below",
                                     at: "2026-09-04T06:00:00Z" }, PANEL),
                     gaugeSeries: async () => series(low) } as unknown as RegsSource;
    render(<ConditionsPanel source={source} section={SEC} palette={LIGHT} feed={feed} horizon="change"
                            scroll={false} day={new Date(2026, 8, 4, 12)} />);
    await waitFor(() => expect(screen.getByTestId("change-block")).toBeTruthy());
    expect(screen.getByText(changeSourceLine(low as never, "2026-09-04"))).toBeTruthy();
    expect(screen.getByTestId("change-caveat").textContent).toBe(CHANGE_LOW_WATER);
  });

  it("an older feed with no station change lists the runs and says no caveat", async () => {
    const source = { ...sourceWith({ discharge: 12, level: null, percentile: 0.15, standing: "below",
                                     at: "2026-09-04T06:00:00Z" }, PANEL),
                     gaugeSeries: async () => series(undefined) } as unknown as RegsSource;
    render(<ConditionsPanel source={source} section={SEC} palette={LIGHT} feed={feed} horizon="change"
                            scroll={false} day={new Date(2026, 8, 4, 12)} />);
    await waitFor(() => expect(screen.getByTestId("change-block")).toBeTruthy());
    expect(screen.getByText("ELF: falls to about 40% of today's flow by Tue, still falling on the last day shown."))
      .toBeTruthy();
    expect(screen.queryByTestId("change-caveat")).toBeNull();
  });

  // T157: the day line at the COFFEE -> CLEVER handover is drawn as a handover (a gap and a tick), never a fall.
  // Mutation: the ChangeBlock not drawing `spark.handovers` -> fails.
  it("marks where COFFEE stops on the joined change's day line", async () => {
    const joined = { ...low, models: ["CLEVER", "COFFEE"], lowWater: false, dir: "rise",
                     reach: { CLEVER: "2026-09-08", COFFEE: "2026-09-05" },
                     peak: { day: "2026-09-05", ratio: 3, value: 3, back: null, until: "2026-09-05" },
                     trough: { day: "2026-09-04", ratio: 1, value: 1, back: null },
                     days: [["2026-09-04", 1, 1], ["2026-09-05", 2, 3], ["2026-09-06", 1, 1.1], ["2026-09-08", 1, 1]] };
    const source = { ...sourceWith({ discharge: 12, level: null, percentile: 0.15, standing: "below",
                                     at: "2026-09-04T06:00:00Z" }, PANEL),
                     gaugeSeries: async () => series(joined) } as unknown as RegsSource;
    render(<ConditionsPanel source={source} section={SEC} palette={LIGHT} feed={feed} horizon="change"
                            scroll={false} day={new Date(2026, 8, 4, 12)} />);
    await waitFor(() => expect(screen.getByTestId("change-block")).toBeTruthy());
    expect(screen.getAllByTestId("change-handover").map((e) => e.getAttribute("d"))).toEqual(["M146,-4 L146,48"]);
    expect(screen.getByRole("img", { name: /COFFEE's forecast ends tomorrow$/ })).toBeTruthy();
  });

  // T144 review — owner "no elf": the chart under a CLEVER+COFFEE change drew the freshest run, ELF.
  it("charts the change's own lead model under Change, the freshest run elsewhere", async () => {
    const joined = { ...low, models: ["CLEVER", "COFFEE"], lowWater: false,
                     reach: { CLEVER: "2026-09-08", COFFEE: "2026-09-08" } };
    for (const [horizon, want] of [["change", "CLEVER"], [0, undefined]] as const) {
      const asked: (string | undefined)[] = [];
      clearPanelCache();
      const source = { ...sourceWith({ discharge: 12, level: null, percentile: 0.15, standing: "below",
                                       at: "2026-09-04T06:00:00Z" }, PANEL),
                       gaugeSeries: async (_s: unknown, _sp: unknown, _p: unknown, model?: string) =>
                         (asked.push(model), series(joined)) } as unknown as RegsSource;
      const { unmount } = render(<ConditionsPanel source={source} section={SEC} palette={LIGHT} feed={feed}
                                                  horizon={horizon} scroll={false} day={new Date(2026, 8, 4, 12)} />);
      await waitFor(() => expect(asked.length).toBeGreaterThan(0));
      await waitFor(() => expect(asked.includes(want)).toBe(true));
      // the chart is asked for the lead model and never for another named one
      expect([...new Set(asked)].filter((m) => m !== undefined)).toEqual(want ? [want] : []);
      unmount();
    }
  });
});

describe("the conditions panel in a water's views", () => {
  it("takes the view swipe on its body (inside its ScrollView, as the Regulations view does)", async () => {
    Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
    window.dispatchEvent(new Event("resize"));
    const release = vi.fn(() => null);
    const swipe = { enabled: true, starts: () => true, offset: (dx: number) => dx, release };
    const views: WaterViews = { views: ["regs", "conditions"], on: "conditions", onView: () => {}, onBack: () => {},
                                backLabel: "Conditions", close: () => {}, closeLabel: "Close, back to Conditions",
                                swipe: null, bodySwipe: swipe };
    render(
      <GestureHandlerRootView>
        <WaterViewsContext.Provider value={views}>
          <ConditionsPanel source={sourceWith(null, null)} section={SEC} palette={LIGHT} feed={feed} />
        </WaterViewsContext.Provider>
      </GestureHandlerRootView>);
    await waitFor(() => expect(screen.getByText(noGaugeSentence())).toBeTruthy());
    act(() => {
      fireGestureHandler(getByGestureTestId("view-swipe-conditions"), [
        { absoluteX: 200, translationX: 0, translationY: 0 },
        { absoluteX: 80, translationX: -120, translationY: 0, velocityX: 0 }]);
    });
    expect(release.mock.calls).toEqual([[-120, 0, 0]]);
  });
});
