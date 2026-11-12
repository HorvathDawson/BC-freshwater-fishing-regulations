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
import { CHANGE_LOW_WATER, changeSourceLine, CONDITIONS_WORDS, noGaugeSentence, readingHeading, standingLong,
         stageLine, TIDAL_NOTE, BACKWATER_NOTE } from "@app/core";
import type { Panel, WaterSource, SectionId } from "@app/data";
import { clearPanelCache, DataProvenanceContext } from "@app/ui";
import type { MenuControls } from "@app/ui";
import { ConditionsPanel } from "./ConditionsPanel";
import { MenuContext } from "./menu/MenuContext";
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
}) as unknown as WaterSource;

const feed = { index: async () => ({ stations: {
  LEAD: { percentile: 0.15, parameter: "discharge" as const, discharge: 0.15 } } }) };

describe("the conditions panel", () => {
  it("(T569) offers Report a problem, with the water, the gauge charted, its quantity and span and the feed's version filled in", async () => {
    const source = sourceWith({ discharge: 12.5, level: 1.2, percentile: 0.15,
                                standing: "below", at: "2026-09-04T06:00:00Z" }, PANEL);
    const report = vi.fn();
    const menu = { report } as unknown as MenuControls;
    const seen = { ...feed, observations: async () => ({ fetchedAt: Date.UTC(2026, 8, 4, 6, 5) }) as never };
    render(<MenuContext.Provider value={menu}>
             <ConditionsPanel source={source} section={SEC} palette={LIGHT} feed={seen} scroll={false} day={new Date(2026, 8, 4, 12)}
                              reportAbout={{ water: "gnis:3062", place: "Vedder River", part: 1, partLabel: "Vedder Crossing → Fraser River" }} />
           </MenuContext.Provider>);
    await waitFor(() => expect(screen.getByTestId("report-link-water")).toBeTruthy());
    fireEvent.click(screen.getByTestId("report-link-water"));
    await waitFor(() => expect(report).toHaveBeenCalledTimes(1));
    expect(report.mock.calls[0]![0]).toEqual({
      screen: "water · conditions", tab: "Conditions", section: null, water: "gnis:3062", place: "Vedder River", part: 1,
      partLabel: "Vedder Crossing → Fraser River", day: "Sep 4", station: "LEAD", quantity: "Flow", span: "Last 72 hours",
      feed: "2026-09-04T06:05:00.000Z" });
  });

  it("draws the lead donor's reading as the view model finished it", async () => {
    const source = sourceWith({ discharge: 12.5, level: 1.2, percentile: 0.15,
                                standing: "below", at: "2026-09-04T06:00:00Z" }, PANEL);
    render(<ConditionsPanel source={source} section={SEC} palette={LIGHT} feed={feed}
                            scroll={false} day={new Date(2026, 8, 4, 12)} />);
    await waitFor(() => expect(screen.getByTestId("reading-figure")).toBeTruthy());
    // (T367) the gauge card's title is the gauge's name as sourced — the old "AT …" kicker is not drawn
    expect(screen.getByTestId("gauge-title").textContent).toBe("Lead Creek near Town");
    expect(screen.queryByText(readingHeading("Lead Creek near Town"))).toBeNull();
    expect(screen.getByText("12.5")).toBeTruthy();
    expect(screen.getByText("m³/s")).toBeTruthy();
    expect(screen.getByText(`· ${stageLine(1.2)}`)).toBeTruthy();   // (T367) beside the figure, after a dot
    expect(screen.getByText(standingLong(0.15, "below"))).toBeTruthy();
    expect(screen.getByText("Below normal · lower than 9 days in 10 for the date")).toBeTruthy();
    // The donor panel above it is drawn for the CHOSEN day.
    expect(screen.getByText("Lower than 7 to 9 days in 10 for early September · this spot drains "
                            + "50 km²")).toBeTruthy();
  });

  it("(T585 F8, F14) a figure the index did not rank says both times, a kept one says it is the phone's copy — never 'no record'", async () => {
    // the station file is a run ahead of the index (or, offline, a newer kept row): no rank of its own, `rankedAt` names the ranked one
    const base = sourceWith(null, PANEL);
    const source = { ...base, gaugeNow: async () => ({ fetchedAt: Date.now() - 5 * 60_000, kept: true as const, value: {
      discharge: 12.5, level: 1.2, percentile: null, standing: null, at: "2026-09-04T06:00:00Z", parameter: "discharge",
      rankedAt: "2026-09-04T05:00:00Z" } }) } as unknown as WaterSource;
    render(<ConditionsPanel source={source} section={SEC} palette={LIGHT} feed={feed} scroll={false} day={new Date(2026, 8, 4, 12)} />);
    await waitFor(() => expect(screen.getByTestId("reading-figure")).toBeTruthy());
    expect(screen.getByText("Ranked at 2026-09-03 at 22:00 PT; this reading 2026-09-03 at 23:00 PT.")).toBeTruthy();
    expect(screen.queryByText(standingLong(null))).toBeNull();
    expect(screen.queryByTestId("reading-dot")).toBeNull();              // no rank, no standing dot
    expect(screen.getByTestId("gauge-card-foot").textContent)
      .toBe("Offline — reading saved on this phone · observed 2026-09-03 at 23:00 PT · checked 5 min ago");
  });

  it("(T585 F12, F13) a development or staging build names its data release in the card's foot; a production build draws nothing", async () => {
    const now = { discharge: 12.5, level: 1.2, percentile: 0.15, standing: "below", at: "2026-09-04T06:00:00Z" };
    render(<DataProvenanceContext.Provider value="Data release r-303b4007a70a · Channel test">
             <ConditionsPanel source={sourceWith(now, PANEL)} section={SEC} palette={LIGHT} feed={feed} scroll={false} day={new Date(2026, 8, 4, 12)} />
           </DataProvenanceContext.Provider>);
    await waitFor(() => expect(screen.getByTestId("reading-figure")).toBeTruthy());
    expect(screen.getByTestId("gauge-card-data-release").textContent).toBe("Data release r-303b4007a70a · Channel test");
    cleanup();
    clearPanelCache();
    render(<ConditionsPanel source={sourceWith(now, PANEL)} section={SEC} palette={LIGHT} feed={feed} scroll={false} day={new Date(2026, 8, 4, 12)} />);
    await waitFor(() => expect(screen.getByTestId("reading-figure")).toBeTruthy());
    expect(screen.queryByTestId("gauge-card-data-release")).toBeNull();
  });

  it("never puts one quantity's figure under the other's chart (T567)", async () => {
    // a station that reports both, ranked in flow: under a LEVEL chart the figure is the level, its standing says it was ranked on flow
    const both = sourceWith({ discharge: 197, level: 2.18, percentile: 0.27, standing: "below",
                              at: "2026-09-04T06:00:00Z", parameter: "discharge" }, PANEL);
    render(<ConditionsPanel source={both} section={SEC} palette={LIGHT} feed={feed} parameter="level"
                            scroll={false} day={new Date(2026, 8, 4, 12)} />);
    await waitFor(() => expect(screen.getByTestId("reading-figure")).toBeTruthy());
    expect([screen.getByTestId("reading-figure").textContent, screen.queryByText("m")?.textContent])
      .toEqual(["2.18", "m"]);
    expect(screen.getByText(`${standingLong(0.27, "below")} (ranked on flow)`)).toBeTruthy();
    expect(screen.queryByText("m³/s")).toBeNull();
    cleanup();
    clearPanelCache();
    // a station that reports level only, asked for flow: the words, no figure
    const levelOnly = sourceWith({ discharge: null, level: 1.548, percentile: 0.66, standing: "normal",
                                   at: "2026-09-04T06:00:00Z", parameter: "level" }, PANEL);
    render(<ConditionsPanel source={levelOnly} section={SEC} palette={LIGHT} feed={feed} parameter="discharge"
                            scroll={false} day={new Date(2026, 8, 4, 12)} />);
    await waitFor(() => expect(screen.getByTestId("reading-other-quantity")).toBeTruthy());
    expect(screen.getByTestId("reading-other-quantity").textContent)
      .toBe("Reports water level only today, not flow. Show Level to see it.");
    expect(screen.queryByTestId("reading-figure")).toBeNull();
  });

  it("on a forecast day the card says its figure is today's and gives the day asked its own line (T567)", async () => {
    const ahead = { index: async () => ({ stations: {
      LEAD: { percentile: 0.15, parameter: "discharge" as const, discharge: 0.15,
              ahead: { "3": { percentile: 0.4, discharge: 0.4 } } } } }) };
    const source = sourceWith({ discharge: 12.5, level: 1.2, percentile: 0.15, standing: "below",
                                at: "2026-09-04T06:00:00Z" }, PANEL);
    render(<ConditionsPanel source={source} section={SEC} palette={LIGHT} feed={ahead} parameter="discharge"
                            horizon={3} scroll={false} day={new Date(2026, 8, 4, 12)} />);
    await waitFor(() => expect(screen.getByTestId("reading-scope")).toBeTruthy());
    expect([screen.getByTestId("reading-scope").textContent, screen.getByTestId("reading-figure").textContent,
            screen.getByTestId("reading-forecast").textContent])
      .toEqual(["Today's reading", "12.5", "Mon 7 Sep, forecast: Reads normal · lower than 6 days in 10."]);
  });

  it("refuses in words where no gauge may speak", async () => {
    render(<ConditionsPanel source={sourceWith(null, null)} section={SEC} palette={LIGHT}
                            feed={feed} scroll={false} />);
    await waitFor(() => expect(screen.getByText(noGaugeSentence())).toBeTruthy());
  });

  it("hands the map's quantity to the donor panel, so a tidal lead is disclosed in Level only (T78)", async () => {
    const tidalSource = (base: WaterSource) => ({
      ...base,
      panelRoutes: async () => [{ station: "LEAD", name: "Lead Creek near Town", lat: 49, lon: -122, role: "up",
                                  path: [SEC], tidal: true }],
    }) as unknown as WaterSource;
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
    await waitFor(() => expect(screen.getByText(standingLong(0.15, "below"))).toBeTruthy());
    expect(screen.queryByText(TIDAL_NOTE)).toBeNull();
  });

  it("discloses a lake's backwater on a backed-up lead in Level only, in its own words (T160)", async () => {
    const backedSource = (base: WaterSource) => ({
      ...base,
      panelRoutes: async () => [{ station: "LEAD", name: "Lead Creek near the Mouth", lat: 49, lon: -122, role: "up",
                                  path: [SEC], backwater: true }],
    }) as unknown as WaterSource;
    const both = { index: async () => ({ stations: {
      LEAD: { percentile: 0.15, parameter: "discharge" as const, discharge: 0.15, level: 0.4 } } }) };
    const source = backedSource(sourceWith({ discharge: 12.5, level: 1.2, percentile: 0.15, standing: "below",
                                             at: "2026-09-04T06:00:00Z" }, PANEL));
    const fold = () => fireEvent.click(screen.getByRole("button", { name: CONDITIONS_WORDS.fold }));
    render(<ConditionsPanel source={source} section={SEC} palette={LIGHT} feed={both} parameter="level"
                            scroll={false} day={new Date(2026, 8, 4, 12)} />);
    await waitFor(() => expect(screen.getByRole("button", { name: CONDITIONS_WORDS.fold })).toBeTruthy());
    fold();
    await waitFor(() => expect(screen.getByText(BACKWATER_NOTE)).toBeTruthy());
    expect(screen.queryByText(TIDAL_NOTE)).toBeNull();
    cleanup();
    clearPanelCache();
    render(<ConditionsPanel source={source} section={SEC} palette={LIGHT} feed={both} parameter="discharge"
                            scroll={false} day={new Date(2026, 8, 4, 12)} />);
    await waitFor(() => expect(screen.getByRole("button", { name: CONDITIONS_WORDS.fold })).toBeTruthy());
    fold();
    await waitFor(() => expect(screen.getByText(standingLong(0.15, "below"))).toBeTruthy());
    expect(screen.queryByText(BACKWATER_NOTE)).toBeNull();
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
                     gaugeSeries: async () => series(low) } as unknown as WaterSource;
    render(<ConditionsPanel source={source} section={SEC} palette={LIGHT} feed={feed} horizon="change"
                            scroll={false} day={new Date(2026, 8, 4, 12)} />);
    await waitFor(() => expect(screen.getByTestId("change-block")).toBeTruthy());
    expect(screen.getByText(changeSourceLine(low as never, "2026-09-04"))).toBeTruthy();
    expect(screen.getByTestId("change-caveat").textContent).toBe(CHANGE_LOW_WATER);
  });

  it("an older feed with no station change lists the runs and says no caveat", async () => {
    const source = { ...sourceWith({ discharge: 12, level: null, percentile: 0.15, standing: "below",
                                     at: "2026-09-04T06:00:00Z" }, PANEL),
                     gaugeSeries: async () => series(undefined) } as unknown as WaterSource;
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
                     gaugeSeries: async () => series(joined) } as unknown as WaterSource;
    render(<ConditionsPanel source={source} section={SEC} palette={LIGHT} feed={feed} horizon="change"
                            scroll={false} day={new Date(2026, 8, 4, 12)} />);
    await waitFor(() => expect(screen.getByTestId("change-block")).toBeTruthy());
    expect(screen.getAllByTestId("change-handover").map((e) => e.getAttribute("d"))).toEqual(["M118,0 L118,120"]);   // (T367) the one Change chart, 120 tall: the tick spans it
    expect(screen.getByRole("img", { name: /COFFEE's forecast ends tomorrow$/ })).toBeTruthy();
    // (T157 review: "Peak Sun" under the tick read as the tick's label) (T367) the mark's words are the chart's key
    // now, beside a dot like the mark — never under the line where the tick stands
    expect(screen.getByText("● Peak tomorrow")).toBeTruthy();
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
                         (asked.push(model), series(joined)) } as unknown as WaterSource;
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
    // the detector registers in a passive effect, and `waitFor` runs outside act: it can resolve on the commit that drew
    // the text before that effect has run (CI, 2026-11-12: "Handler with id ... cannot be found"). Wait for the handler.
    await waitFor(() => getByGestureTestId("view-swipe-conditions"));
    act(() => {
      fireGestureHandler(getByGestureTestId("view-swipe-conditions"), [
        { absoluteX: 200, translationX: 0, translationY: 0 },
        { absoluteX: 80, translationX: -120, translationY: 0, velocityX: 0 }]);
    });
    expect(release.mock.calls).toEqual([[-120, 0, 0]]);
  });
});

describe("(T508) the panel says it is drawn whole only once its first answers are in", () => {
  it("held while the reading, the routes and the chart come in late; at its word, what it draws is what it keeps", async () => {
    const { PaneReadyContext } = await import("./shell/paneReveal");
    const late = <T,>(ms: number, v: T) => new Promise<T>((ok) => setTimeout(() => ok(v), ms));
    const base = sourceWith({ discharge: 12.5, level: 1.2, percentile: 0.15, standing: "below", at: "2026-09-04T06:00:00Z" }, PANEL);
    const source = { ...base,
      gaugeNow: (...a: unknown[]) => late(150, null).then(() => (base.gaugeNow as (...x: unknown[]) => unknown)(...a)),
      panelRoutes: (...a: unknown[]) => late(250, null).then(() => (base.panelRoutes as (...x: unknown[]) => unknown)(...a)),
    } as unknown as WaterSource;
    let waits = 0, atWord: string | null = null;
    const hold = { wait: () => { waits += 1; return () => { atWord ??= document.body.textContent ?? ""; }; } };
    render(
      <PaneReadyContext.Provider value={hold}>
        <ConditionsPanel source={source} section={SEC} palette={LIGHT} feed={feed} scroll={false} day={new Date(2026, 8, 4, 12)} />
      </PaneReadyContext.Provider>);
    expect(waits).toBeGreaterThan(0);                  // it waits from its first draw
    await waitFor(() => expect(atWord).not.toBeNull(), { timeout: 10_000 });
    expect(atWord).toContain("12.5");                  // the reading was in at its word
    // settled: the text unchanged for 500 ms after the last late answer (routes at 250 ms)
    let was = "", since = Date.now();
    await waitFor(() => { const now = document.body.textContent ?? "";
                          if (now !== was) { was = now; since = Date.now(); } expect(Date.now() - since).toBeGreaterThan(500); },
                  { timeout: 10_000, interval: 50 });
    expect(atWord).toBe(document.body.textContent);
  }, 20_000);
});
