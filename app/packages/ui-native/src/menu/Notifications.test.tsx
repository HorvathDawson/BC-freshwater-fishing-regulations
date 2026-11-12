/**
 * (T234, T527) More → Notifications and a gauge's "Alert me", stock-style: opt-in, the permission asked only when
 * something is turned on, a refusal shown; a plain list (one line per alert, its own On/Off and Delete); "Add an alert":
 * gauge → condition → value → done; the percentile line for the day (presets + custom; disabled with the reason on a
 * gauge with no record); saved alerts that could not be kept said until dismissed; nothing drawn without the app's
 * notifications (the web).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MENU_WORDS, NOTIFY_WORDS, setFailureSink, type Band, type DataInput, type GaugeAlertRule } from "@app/core";
import { createNotifySettings, NOTIFY_DEFAULT, type NotifyState, type WaterSource } from "@app/data";
import { LayoutWidthContext, NotifyRoot, useNotifications, type NotifyControls, type NotifyPermission } from "@app/ui";
import { LIGHT } from "../theme";
import { MoreScreen } from "./MoreScreen";
import { NotificationsPage } from "./NotificationsPage";
import { AlertMe, type AlertTarget } from "../AlertMe";
import { CoachScrollContext, type CoachScroller } from "../coachScroll";

afterEach(cleanup);
const NOW = Date.now();
const DATA: DataInput = {
  sources: { synopsis: { title: "Synopsis", date: null }, basemap: { date: "2026-03-05" } },
  validUntil: null, tiles: "d768d6e135e62b99.c9f9604f12bf", tilesPaired: true,
  feedFetchedAt: new Date(NOW - 3.6e6).toISOString(), now: NOW, newRelease: null, forecastIssuedAt: new Date(NOW - 3.6e6).toISOString(),
};
const RULE: GaugeAlertRule = {
  id: "r1", station: "08MH001", stationName: "Vedder Crossing", quantity: "discharge", direction: "above",
  threshold: { kind: "value", value: 50 }, on: true, water: { item: "w1", name: "Chilliwack River" }, createdAt: NOW,
};

function controls(state: Partial<NotifyState> = {}, answer: NotifyPermission = "granted", now: NotifyPermission = "undetermined") {
  let text: string | null = JSON.stringify({ ...NOTIFY_DEFAULT, ...state });
  const settings = createNotifySettings({ read: () => text, write: (t) => { text = t; } });
  let perm = now;
  const platform = {
    permission: vi.fn(async () => perm),
    ask: vi.fn(async () => { perm = answer; return answer; }),
    openSettings: vi.fn(),
    schedule: vi.fn(async (_active: boolean) => {}),
  };
  return { c: { settings, platform } satisfies NotifyControls, platform, settings };
}

/** The reading on the card's 72 h chart, "now" on Nov 9. */
const AT = "2026-11-09T07:00:00-08:00";
const ENVELOPE: Band = [10, 20, 40, 60, 80];
/**
 * A fake source for the editor: the 72 h series (the reading now and today's band), the year series (the record, per
 * day) — `record: false` has none, as a gauge with no climatology — the gauge's quantities and the release's gauges.
 */
function fakeSource(o: { record?: boolean; params?: readonly string[] } = {}) {
  const record = o.record ?? true;
  const series = vi.fn(async (_st: string, span: "72h" | "year", parameter = "discharge") => {
    if (span === "year") return record ? { fetchedAt: null, value: { parameter, yearBand: Array.from({ length: 366 }, () => ENVELOPE),
                                                                       at: [], values: [], band: [], now: null, step: "1d", from: "2026-01-01",
                                                                       forecast: null, forecasts: [], priorYears: [] } } : null;
    return { fetchedAt: NOW, value: { parameter, at: [AT], values: [42.5], band: [ENVELOPE], now: { index: 0, value: 42.5 },
                                      step: "1h", from: AT, forecast: null, forecasts: [], priorYears: [] } };
  });
  return {
    source: {
      gaugeSeries: series,
      gaugeParameters: vi.fn(async () => o.params ?? ["discharge", "level"]),
      gaugePoints: vi.fn(async () => [{ station: "08HA002", name: "COWICHAN RIVER AT LAKE COWICHAN", lon: 0, lat: 0, mag: 1 },
                                      { station: "08HA011", name: "COWICHAN RIVER NEAR DUNCAN", lon: 0, lat: 0, mag: 1 },
                                      { station: "08MH001", name: "CHILLIWACK RIVER AT VEDDER CROSSING", lon: 0, lat: 0, mag: 1 }]),
    } as unknown as WaterSource,
    series,
  };
}

function Page({ source = null }: { source?: WaterSource | null }) {
  const n = useNotifications();
  return n ? <NotificationsPage n={n} palette={LIGHT} source={source} /> : null;
}
const flush = () => act(async () => { for (let i = 0; i < 4; i++) await new Promise((r) => setTimeout(r, 0)); });
const line = () => screen.getByTestId("alert-form-line").textContent;

describe("More → Notifications (T234, T527)", () => {
  it("is listed on the More tab only where the app can notify", () => {
    const { c } = controls();
    const more = (ctl: NotifyControls | null) => (
      <LayoutWidthContext.Provider value={null}>
        <NotifyRoot controls={ctl}>
          <MoreScreen palette={LIGHT} data={DATA} back="Map" onBack={() => {}} contact={null} theme="light" onTheme={() => {}} onPick={() => {}} />
        </NotifyRoot>
      </LayoutWidthContext.Provider>
    );
    const { unmount } = render(more(c));
    expect(screen.getByTestId("menu-item-notifications").textContent).toContain(MENU_WORDS.items.notifications.label);
    unmount();
    render(more(null));
    expect(screen.queryByTestId("menu-item-notifications")).toBeNull();
  });

  it("everything is off on a fresh install; opening the page never asks the phone", async () => {
    const { c, platform } = controls();
    render(<NotifyRoot controls={c}><Page /></NotifyRoot>);
    await flush();
    expect(screen.getByRole("switch", { name: NOTIFY_WORDS.master.label }).getAttribute("aria-checked")).toBe("false");
    expect(screen.getByRole("switch", { name: NOTIFY_WORDS.updateReady.label }).getAttribute("aria-checked")).toBe("false");
    expect(screen.getByTestId("notify-no-alerts").textContent).toBe(NOTIFY_WORDS.alerts.none);
    fireEvent.click(screen.getByRole("button", { name: NOTIFY_WORDS.runs.title }));
    expect(screen.getByTestId("notify-no-runs").textContent).toBe(NOTIFY_WORDS.runs.none);
    expect(screen.getByTestId("notify-permission").textContent).toBe(NOTIFY_WORDS.permission.undetermined);
    expect(platform.ask).not.toHaveBeenCalled();
    expect(platform.schedule.mock.calls).toEqual([[false]]);
  });

  it("turning 'new data' on asks the permission once, turns notifications on and schedules the check", async () => {
    const { c, platform, settings } = controls();
    render(<NotifyRoot controls={c}><Page /></NotifyRoot>);
    await flush();
    await act(async () => { fireEvent.click(screen.getByRole("switch", { name: NOTIFY_WORDS.updateReady.label })); });
    await flush();
    expect(platform.ask).toHaveBeenCalledTimes(1);
    expect(settings.read()).toMatchObject({ on: true, updateReady: true });
    expect(platform.schedule.mock.calls.at(-1)).toEqual([true]);
    expect(screen.getByTestId("notify-permission").textContent).toBe(NOTIFY_WORDS.permission.granted);
  });

  it("a refused permission is shown, with the way to the phone's settings", async () => {
    const { c, platform } = controls({}, "denied");
    render(<NotifyRoot controls={c}><Page /></NotifyRoot>);
    await flush();
    await act(async () => { fireEvent.click(screen.getByRole("switch", { name: NOTIFY_WORDS.master.label })); });
    await flush();
    expect(screen.getByTestId("notify-permission").textContent).toBe(NOTIFY_WORDS.permission.denied);
    fireEvent.click(screen.getByTestId("notify-open-settings"));
    expect(platform.openSettings).toHaveBeenCalledTimes(1);
  });

  it("a plain list: one line per alert with when it last fired, its own On/Off and Delete; the runs in a closed fold", async () => {
    const pct: GaugeAlertRule = { ...RULE, id: "r2", stationName: "Lake Cowichan", water: { item: "w2", name: "Cowichan River" },
                                  threshold: { kind: "percentile", pct: 75, byDay: Array(366).fill(60) } };
    const { c, settings } = controls({ on: true, rules: [RULE, pct],
                                       ruleState: { r1: { past: true, firedAt: Date.parse("2026-11-05T14:05:00-07:00") } },
                                       runs: [{ at: NOW - 60_000, ms: 900, result: "nothing to tell" },
                                              { at: NOW, ms: 1200, result: "1 alert" }] }, "granted", "granted");
    render(<NotifyRoot controls={c}><Page /></NotifyRoot>);
    await flush();
    const r1 = "Vedder Crossing · Flow crosses above 50 m³/s · last fired 2026-11-05 at 14:05 PT";
    const r2 = "Lake Cowichan · Flow crosses above the level higher than 8 days in 10 for the day · not fired yet";
    expect(screen.getByTestId("notify-alert-r1").textContent).toContain(r1);
    expect(screen.getByTestId("notify-alert-r2").textContent).toContain(r2);
    // the alert's own switch never moves the master switch; off says so in its line
    await act(async () => { fireEvent.click(screen.getByRole("switch", { name: NOTIFY_WORDS.alerts.switchAloud(r1) })); });
    expect([settings.read().on, settings.read().rules[0]!.on]).toEqual([true, false]);
    expect(screen.getByTestId("notify-alert-r1").textContent).toContain("Vedder Crossing · Flow crosses above 50 m³/s · off");
    // the runs: closed until opened, then newest first
    expect(screen.queryAllByTestId("notify-run")).toEqual([]);
    fireEvent.click(screen.getByRole("button", { name: NOTIFY_WORDS.runs.title }));
    expect(screen.getAllByTestId("notify-run").map((e) => e.textContent?.split(" · ").at(-1))).toEqual(["1 alert", "nothing to tell"]);
    fireEvent.click(screen.getByRole("button", { name: NOTIFY_WORDS.alerts.removeAloud(r2) }));
    expect(settings.read().rules.map((r) => r.id)).toEqual(["r1"]);
  });

  it("'New app version' is offered only where the build has a version file; on asks the permission", async () => {
    const { c, platform, settings } = controls();
    const { unmount } = render(<NotifyRoot controls={c}><Page /></NotifyRoot>);
    await flush();
    expect(screen.queryByRole("switch", { name: NOTIFY_WORDS.appVersion.label })).toBeNull();
    unmount();
    render(<NotifyRoot controls={{ ...c, appVersion: true }}><Page /></NotifyRoot>);
    await flush();
    await act(async () => { fireEvent.click(screen.getByRole("switch", { name: NOTIFY_WORDS.appVersion.label })); });
    await flush();
    expect(settings.read()).toMatchObject({ on: true, appVersion: true });
    expect(platform.ask).toHaveBeenCalledTimes(1);
  });

  it("'Add an alert': gauge → condition → value → done", async () => {
    const { c, settings, platform } = controls();
    const { source } = fakeSource({ params: ["discharge"] });
    render(<NotifyRoot controls={c}><Page source={source} /></NotifyRoot>);
    await flush();
    fireEvent.click(screen.getByTestId("notify-add-open"));
    fireEvent.change(screen.getByTestId("notify-add-search"), { target: { value: "zzz" } });
    await flush();
    expect(screen.getByTestId("notify-add-none").textContent).toBe(NOTIFY_WORDS.add.none);
    fireEvent.change(screen.getByTestId("notify-add-search"), { target: { value: "cowichan lake" } });
    await flush();
    // 1. the gauge
    fireEvent.click(screen.getByRole("button", { name: "COWICHAN RIVER AT LAKE COWICHAN (08HA002)" }));
    await flush();
    expect(screen.getByText("Alert on COWICHAN RIVER AT LAKE COWICHAN")).toBeTruthy();
    expect(screen.queryByTestId("alert-form-q-level")).toBeNull();          // only the quantities the gauge has
    // 2. the condition, 3. the value
    fireEvent.click(screen.getByTestId("alert-form-below"));
    fireEvent.change(screen.getByTestId("alert-form-value"), { target: { value: "12" } });
    expect(line()).toBe("You get one notification each time the flow at COWICHAN RIVER AT LAKE COWICHAN crosses below 12 m³/s. "
      + "It is ready again once the flow is back above the line.");
    await act(async () => { fireEvent.click(screen.getByTestId("alert-form-save")); });
    await flush();
    expect(settings.read().rules).toMatchObject([{ station: "08HA002", stationName: "COWICHAN RIVER AT LAKE COWICHAN",
      quantity: "discharge", direction: "below", threshold: { kind: "value", value: 12 }, on: true, water: null }]);
    expect(settings.read().on).toBe(true);
    expect(platform.ask).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId("alert-me-saved").textContent).toBe(NOTIFY_WORDS.alertMe.saved);
    expect(screen.getByTestId("notify-alert-" + settings.read().rules[0]!.id).textContent)
      .toContain("COWICHAN RIVER AT LAKE COWICHAN · Flow crosses below 12 m³/s · not fired yet");
  });

  it("a gauge with no flow or level record says so and offers no editor", async () => {
    const { c } = controls();
    const { source } = fakeSource({ params: [] });
    render(<NotifyRoot controls={c}><Page source={source} /></NotifyRoot>);
    await flush();
    fireEvent.click(screen.getByTestId("notify-add-open"));
    fireEvent.change(screen.getByTestId("notify-add-search"), { target: { value: "08MH" } });
    await flush();
    fireEvent.click(screen.getByRole("button", { name: "CHILLIWACK RIVER AT VEDDER CROSSING (08MH001)" }));
    await flush();
    expect(screen.getByTestId("notify-add-no-quantity").textContent).toBe(NOTIFY_WORDS.add.noQuantity);
    expect(screen.queryByTestId("alert-form")).toBeNull();
  });

  it("saved alerts that could not be kept are said, with their words, until dismissed", async () => {
    const text = JSON.stringify({ v: 2, on: true, rules: [
      { id: "a", station: "08MH001", stationName: "Vedder Crossing", quantity: "discharge", kind: "above", value: 50, gapH: null,
        on: true, water: { item: "w1", name: "Chilliwack River" }, createdAt: 1 },
      { id: "p", station: "08MH001", stationName: "Vedder Crossing", quantity: "discharge", kind: "rise", value: 50, gapH: 3,
        on: true, water: { item: "w1", name: "Chilliwack River" }, createdAt: 2 }], ruleState: {} });
    let stored = text;
    const settings = createNotifySettings({ read: () => stored, write: (t) => { stored = t; } });
    const c: NotifyControls = { settings, platform: { permission: async () => "granted", ask: async () => "granted",
                                                      openSettings: () => {}, schedule: async () => {} } };
    render(<NotifyRoot controls={c}><Page /></NotifyRoot>);
    await flush();
    expect(screen.getByTestId("notify-alert-a").textContent).toContain("Vedder Crossing · Flow crosses above 50 m³/s · not fired yet");
    expect(screen.getAllByTestId("notify-refused-item").map((e) => e.textContent))
      .toEqual(["Vedder Crossing · Flow rises 50 % or more within 24 h: a change within a day is no longer an alert kind"]);
    fireEvent.click(screen.getByTestId("notify-refused-ok"));
    expect(screen.queryByTestId("notify-refused")).toBeNull();
    expect(settings.read()).toMatchObject({ v: 3, refused: [], rules: [{ id: "a" }] });
  });
});

describe("Alert me on a gauge's card, and the editor (T527)", () => {
  const target: AlertTarget = { station: "08MH001", stationName: "Vedder Crossing", quantity: "discharge", current: 42.5,
                                quantities: ["discharge", "level"], water: { item: "w1", name: "Chilliwack River" } };

  it("is not drawn without the app's notifications (the web)", () => {
    const { source } = fakeSource();
    render(<NotifyRoot controls={null}><AlertMe palette={LIGHT} target={target} source={source} /></NotifyRoot>);
    expect(screen.queryByTestId("alert-me")).toBeNull();
  });

  it("prefills the card's figure, not the raw reading (3.199 m is the card's 3.2)", async () => {
    const { c } = controls();
    const { source } = fakeSource();
    render(<NotifyRoot controls={c}><AlertMe palette={LIGHT} target={{ ...target, quantity: "level", current: 3.199 }} source={source} /></NotifyRoot>);
    fireEvent.click(screen.getByTestId("alert-me-open"));
    await flush();
    expect((screen.getByTestId("alert-form-value") as HTMLInputElement).value).toBe("3.2");
  });

  it("(T620 G11) opened with no card figure (More → Add an alert): the box fills from the series, one 'now' in both lines", async () => {
    const { c } = controls();
    const { source } = fakeSource();
    render(<NotifyRoot controls={c}><AlertMe palette={LIGHT} target={{ ...target, current: null }} source={source} /></NotifyRoot>);
    fireEvent.click(screen.getByTestId("alert-me-open"));
    await flush();
    expect((screen.getByTestId("alert-form-value") as HTMLInputElement).value).toBe("42.5");
    expect(screen.getByTestId("alert-form-context").textContent).toContain("Now 42.5 m³/s");
    fireEvent.change(screen.getByTestId("alert-form-value"), { target: { value: "40" } });
    expect(line()).toContain("It is above that now (42.5 m³/s)");
  });

  it("opens on this gauge, prefilled with the reading now; the summary says what will happen", async () => {
    const { c, platform } = controls();
    const { source } = fakeSource();
    render(<NotifyRoot controls={c}><AlertMe palette={LIGHT} target={target} source={source} /></NotifyRoot>);
    fireEvent.click(screen.getByTestId("alert-me-open"));
    await flush();
    expect(screen.getByText("Alert on Vedder Crossing")).toBeTruthy();
    expect((screen.getByTestId("alert-form-value") as HTMLInputElement).value).toBe("42.5");
    expect(screen.getByRole("radio", { name: "Flow" }).getAttribute("aria-checked")).toBe("true");
    expect(screen.getByRole("radio", { name: "Crosses above" }).getAttribute("aria-checked")).toBe("true");
    expect(screen.getByTestId("alert-form-context").textContent).toBe("Now 42.5 m³/s · normal today 20–60 m³/s");
    fireEvent.change(screen.getByTestId("alert-form-value"), { target: { value: "40" } });
    expect(line()).toBe("You get one notification each time the flow at Vedder Crossing crosses above 40 m³/s. It is ready "
      + "again once the flow is back below the line. It is above that now (42.5 m³/s), so the first notification comes after "
      + "it crosses back and then above again.");
    expect(platform.ask).not.toHaveBeenCalled();
  });

  it("a percentile for the day: presets and custom, the line today said, saved with its line for every day", async () => {
    const { c, settings } = controls();
    const { source } = fakeSource();
    render(<NotifyRoot controls={c}><AlertMe palette={LIGHT} target={target} source={source} /></NotifyRoot>);
    fireEvent.click(screen.getByTestId("alert-me-open"));
    await flush();
    fireEvent.click(screen.getByTestId("alert-form-line-percentile"));
    // 8 in 10 days lower (the 75th) by default; the median preset
    expect(screen.getByRole("radio", { name: "8 in 10 days lower" }).getAttribute("aria-checked")).toBe("true");
    expect(line()).toBe("You get one notification each time the flow at Vedder Crossing crosses above the level higher than 8 days in 10 "
      + "for the day. The line is worked out for each day from the gauge's record, so it moves with the date: 60 m³/s for Nov 9. It is ready again once the flow is back below the line.");
    fireEvent.click(screen.getByTestId("alert-form-p50"));
    expect(line()).toBe("You get one notification each time the flow at Vedder Crossing crosses above the median for the day. "
      + "The line is worked out for each day from the gauge's record, so it moves with the date: 40 m³/s for Nov 9. It is ready again once the flow is back below the line. It is above "
      + "that now (42.5 m³/s), so the first notification comes after it crosses back and then above again.");
    // custom: out of the record's range is refused and said
    fireEvent.click(screen.getByTestId("alert-form-p-custom"));
    fireEvent.change(screen.getByTestId("alert-form-pct"), { target: { value: "95" } });
    expect(line()).toBe(NOTIFY_WORDS.editor.badPct);
    await act(async () => { fireEvent.click(screen.getByTestId("alert-form-save")); });
    expect(settings.read().rules).toEqual([]);
    fireEvent.change(screen.getByTestId("alert-form-pct"), { target: { value: "30" } });
    fireEvent.click(screen.getByTestId("alert-form-below"));
    await act(async () => { fireEvent.click(screen.getByTestId("alert-form-save")); });
    await flush();
    const r = settings.read().rules[0]!;
    expect([r.direction, r.threshold.kind, r.threshold.kind === "percentile" && r.threshold.pct]).toEqual(["below", "percentile", 30]);
    // the 30th of [10, 20, 40, 60, 80]: 20 + (0.05 / 0.25) × 20 = 24, on every day
    expect(r.threshold.kind === "percentile" && [r.threshold.byDay.length, r.threshold.byDay[0], r.threshold.byDay[312]])
      .toEqual([366, 24, 24]);
  });

  it("a gauge with no record: the percentile choice is shown disabled, with the reason — never a silent fallback", async () => {
    const { c } = controls();
    const { source } = fakeSource({ record: false });
    render(<NotifyRoot controls={c}><AlertMe palette={LIGHT} target={target} source={source} /></NotifyRoot>);
    fireEvent.click(screen.getByTestId("alert-me-open"));
    await flush();
    const chip = screen.getByTestId("alert-form-line-percentile");
    expect(chip.getAttribute("aria-disabled")).toBe("true");
    expect(screen.getByTestId("alert-form-no-percentile").textContent)
      .toBe("This gauge has no historical record of its flow, so it has no usual levels to alert on.");
    fireEvent.click(chip);
    expect(screen.getByRole("radio", { name: NOTIFY_WORDS.editor.lineKinds.value }).getAttribute("aria-checked")).toBe("true");
    expect(screen.queryByTestId("alert-form-p75")).toBeNull();
  });

  it("saves a value alert in the new shape, turns notifications on and asks the permission then", async () => {
    const { c, platform, settings } = controls();
    const { source } = fakeSource();
    render(<NotifyRoot controls={c}><AlertMe palette={LIGHT} target={target} source={source} /></NotifyRoot>);
    fireEvent.click(screen.getByTestId("alert-me-open"));
    fireEvent.click(screen.getByTestId("alert-form-q-level"));
    fireEvent.click(screen.getByTestId("alert-form-below"));
    fireEvent.change(screen.getByTestId("alert-form-value"), { target: { value: "1,25" } });
    await act(async () => { fireEvent.click(screen.getByTestId("alert-form-save")); });
    await flush();
    expect(platform.ask).toHaveBeenCalledTimes(1);
    const s = settings.read();
    expect(s.on).toBe(true);
    expect(s.rules).toMatchObject([{ station: "08MH001", quantity: "level", direction: "below",
                                     threshold: { kind: "value", value: 1.25 }, on: true, water: { item: "w1", name: "Chilliwack River" } }]);
    expect(screen.getByTestId("alert-me-saved").textContent).toBe(NOTIFY_WORDS.alertMe.saved);
    expect(screen.getByTestId("alert-me-count").textContent).toBe("1 alert on this gauge");
  });

  /**
   * (T674) THE CONFIRMATION STAYS ON SCREEN. Saving collapses the editor, so with Save low on the screen the page was left
   * showing what is under the card and "Saved. …" / "1 alert on this gauge" sat above the fold (emulator, T603). The
   * card's page (`CoachScrollContext`, the Conditions pane's own scroll view) is moved so the whole block is in its view.
   */
  const boxes = (at: (testId: string | null) => { top: number; height: number } | null) =>
    vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockImplementation(function (this: HTMLElement) {
      const b = at(this.getAttribute("data-testid")) ?? { top: 0, height: 0 };
      return { x: 0, y: b.top, left: 0, top: b.top, width: b.height ? 360 : 0, height: b.height, right: 360,
               bottom: b.top + b.height, toJSON: () => ({}) } as DOMRect;
    });
  const saveOn = async (page: CoachScroller) => {
    const { c } = controls();
    const { source } = fakeSource();
    render(<NotifyRoot controls={c}><CoachScrollContext.Provider value={page}>
      <AlertMe palette={LIGHT} target={target} source={source} /></CoachScrollContext.Provider></NotifyRoot>);
    fireEvent.click(screen.getByTestId("alert-me-open"));
    await flush();
    await act(async () => { fireEvent.click(screen.getByTestId("alert-form-save")); });
    await flush();
    await act(async () => { await new Promise((r) => setTimeout(r, 400)); });
    expect(screen.getByTestId("alert-me-saved").textContent).toBe("Saved. Manage alerts in More → Notifications.");
  };
  const VIEW = { x: 0, y: 120, w: 360, h: 600 };

  it("(T674) after Save the confirmation left above the fold is scrolled back into the page's view", async () => {
    // the block (button, count, "Saved. …") ends up 300 px above the top of the page's view
    const spy = boxes((id) => (id === "alert-me" ? { top: -180, height: 60 } : null));
    const by = vi.fn();
    await saveOn({ by, view: async () => VIEW });
    spy.mockRestore();
    // up by the 300 px, and the page's 12 px margin above the block
    expect(by.mock.calls).toEqual([[-312]]);
  });

  it("(T674) below the fold it is scrolled up into view; already in view the page is left alone", async () => {
    let spy = boxes((id) => (id === "alert-me" ? { top: 700, height: 60 } : null));
    const by = vi.fn();
    await saveOn({ by, view: async () => VIEW });
    spy.mockRestore();
    expect(by.mock.calls).toEqual([[52]]);        // its foot at 760, the view's at 720, less the 12 px margin
    cleanup();
    spy = boxes((id) => (id === "alert-me" ? { top: 300, height: 60 } : null));
    const still = vi.fn();
    await saveOn({ by: still, view: async () => VIEW });
    spy.mockRestore();
    expect(still).not.toHaveBeenCalled();
  });

  it("(T674) a confirmation that cannot be measured is reported, never passed over", async () => {
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    const spy = boxes(() => null);                // nothing laid out: every box is empty
    const by = vi.fn();
    await saveOn({ by, view: async () => VIEW });
    spy.mockRestore(); undo();
    expect([by.mock.calls.length, seen.map((e) => (e as Error).message)])
      .toEqual([0, ["alert saved: the confirmation could not be measured, so it was not brought into view"]]);
  });

  it("refuses a value that is not a number, and says a refused permission after saving", async () => {
    const { c, settings } = controls({}, "denied");
    const { source } = fakeSource();
    render(<NotifyRoot controls={c}><AlertMe palette={LIGHT} target={target} source={source} /></NotifyRoot>);
    fireEvent.click(screen.getByTestId("alert-me-open"));
    fireEvent.change(screen.getByTestId("alert-form-value"), { target: { value: "lots" } });
    await act(async () => { fireEvent.click(screen.getByTestId("alert-form-save")); });
    expect(line()).toBe(NOTIFY_WORDS.editor.bad);
    expect(settings.read().rules).toEqual([]);
    fireEvent.change(screen.getByTestId("alert-form-value"), { target: { value: "30" } });
    await act(async () => { fireEvent.click(screen.getByTestId("alert-form-save")); });
    await flush();
    expect(screen.getByTestId("alert-me-denied").textContent).toBe(NOTIFY_WORDS.permission.denied);
  });
});
