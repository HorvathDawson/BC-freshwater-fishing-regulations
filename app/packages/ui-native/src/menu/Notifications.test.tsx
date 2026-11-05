/**
 * (T234) More → Notifications and a gauge's "Alert me": opt-in, the permission asked only when something is turned on,
 * a refusal shown, (v1.1) alerts as cards grouped by water with their own switch, Edit and Delete, "Add an alert", the
 * new-app-version switch, the background runs in a closed fold; the editor prefilled from the card, its summary line;
 * nothing drawn without the app's notifications (the web).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { MENU_WORDS, NOTIFY_WORDS, type DataInput, type GaugeAlertRule } from "@app/core";
import { createNotifySettings, NOTIFY_DEFAULT, type NotifyState, type WaterSource } from "@app/data";
import { LayoutWidthContext, NotifyRoot, useNotifications, type NotifyControls, type NotifyPermission } from "@app/ui";
import { LIGHT } from "../theme";
import { MoreScreen } from "./MoreScreen";
import { NotificationsPage } from "./NotificationsPage";
import { AlertMe, type AlertTarget } from "../AlertMe";

afterEach(cleanup);
const NOW = Date.now();
const DATA: DataInput = {
  sources: { synopsis: { title: "Synopsis", date: null }, basemap: { date: "2026-03-05" } },
  validUntil: null, tiles: "d768d6e135e62b99.c9f9604f12bf", tilesPaired: true,
  feedFetchedAt: new Date(NOW - 3.6e6).toISOString(), now: NOW, newRelease: null, forecastIssuedAt: new Date(NOW - 3.6e6).toISOString(),
};
const RULE: GaugeAlertRule = {
  id: "r1", station: "08MH001", stationName: "Vedder Crossing", quantity: "discharge", kind: "above", value: 50, gapH: null,
  on: true, water: { item: "w1", name: "Chilliwack River" }, createdAt: NOW,
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

function Page() {
  const n = useNotifications();
  return n ? <NotificationsPage n={n} palette={LIGHT} /> : null;
}
function PageWith({ source, onFind }: { source: WaterSource; onFind: (item: string) => void }) {
  const n = useNotifications();
  return n ? <NotificationsPage n={n} palette={LIGHT} source={source} onFindWater={onFind} /> : null;
}
const flush = () => act(async () => { await new Promise((r) => setTimeout(r, 0)); });

describe("More → Notifications (T234)", () => {
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

  it("alerts as cards grouped by water: gauge, line, own switch, Edit, Delete; the runs in a closed fold", async () => {
    const other: GaugeAlertRule = { ...RULE, id: "r2", station: "08MH024", stationName: "Chilliwack at Outlet", kind: "rise",
                                    value: 50, gapH: 3 };
    const vedder: GaugeAlertRule = { ...RULE, id: "r3", water: { item: "w2", name: "Vedder River" } };
    const { c, settings } = controls({ on: true, rules: [RULE, vedder, other],
                                       ruleState: { r1: { past: true, firedAt: Date.parse("2026-11-05T14:05:00-07:00") } },
                                       runs: [{ at: NOW - 60_000, ms: 900, result: "nothing to tell" },
                                              { at: NOW, ms: 1200, result: "1 alert" }] }, "granted", "granted");
    render(<NotifyRoot controls={c}><Page /></NotifyRoot>);
    await flush();
    // one card per water, its alerts in it
    const w1 = screen.getByTestId("notify-water-w1");
    expect(w1.textContent).toContain("Chilliwack River");
    expect(within(w1).getAllByTestId(/^notify-alert-r\d$/).map((e) => e.getAttribute("data-testid"))).toEqual(["notify-alert-r1", "notify-alert-r2"]);
    expect(screen.getByTestId("notify-water-w2").textContent).toContain("Vedder River");
    const sw = screen.getByRole("switch", { name: "Alert on: Chilliwack River · Vedder Crossing, Flow above 50 m³/s" });
    expect(screen.getByTestId("notify-alert-r1").textContent)
      .toContain("Flow above 50 m³/s · at most once a day for this water · last sent 2026-11-05 at 14:05 PT");
    expect(screen.getByTestId("notify-alert-r2").textContent).toContain("Flow rises 50 % or more within 24 h · at most once every 3 h · not sent yet");
    // the alert's own switch never moves the master switch
    await act(async () => { fireEvent.click(sw); });
    expect([settings.read().on, settings.read().rules[0]!.on]).toEqual([true, false]);
    // the runs: closed until opened, then newest first
    expect(screen.queryAllByTestId("notify-run")).toEqual([]);
    fireEvent.click(screen.getByRole("button", { name: NOTIFY_WORDS.runs.title }));
    expect(screen.getAllByTestId("notify-run").map((e) => e.textContent?.split(" · ").at(-1))).toEqual(["1 alert", "nothing to tell"]);
    // edit in place, then delete
    fireEvent.click(screen.getByRole("button", { name: NOTIFY_WORDS.alerts.editAloud("Chilliwack River · Vedder Crossing, Flow above 50 m³/s") }));
    fireEvent.change(screen.getByTestId("alert-form-value"), { target: { value: "70" } });
    fireEvent.click(screen.getByTestId("alert-form-gap-6"));
    await act(async () => { fireEvent.click(screen.getByTestId("alert-form-save")); });
    expect(settings.read().rules.find((r) => r.id === "r1")).toMatchObject({ value: 70, gapH: 6 });
    fireEvent.click(screen.getByRole("button", { name: NOTIFY_WORDS.alerts.removeAloud("Chilliwack River · Vedder Crossing, Flow above 70 m³/s") }));
    expect(settings.read().rules.map((r) => r.id)).toEqual(["r3", "r2"]);
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

  it("'Add an alert' finds a water by name and opens its Conditions", async () => {
    const { c } = controls({ on: true });
    const source = { searchNames: vi.fn(async (q: string) => (q.startsWith("Coq")
      ? [{ item: "w9", name: "Coquitlam River", kind: "stream", matchedAs: null }] : [])) } as unknown as WaterSource;
    const onFind = vi.fn();
    render(<NotifyRoot controls={c}><PageWith source={source} onFind={onFind} /></NotifyRoot>);
    await flush();
    fireEvent.click(screen.getByTestId("notify-add-open"));
    fireEvent.change(screen.getByTestId("notify-add-search"), { target: { value: "Coq" } });
    await flush();
    fireEvent.click(screen.getByRole("button", { name: "Coquitlam River" }));
    expect(onFind).toHaveBeenCalledWith("w9");
    fireEvent.click(screen.getByTestId("notify-add-open"));
    fireEvent.change(screen.getByTestId("notify-add-search"), { target: { value: "Zzz" } });
    await flush();
    expect(screen.getByTestId("notify-add-none").textContent).toBe(NOTIFY_WORDS.add.none);
  });

  it("a v1 alert (saved before v1.1) is listed and editable", async () => {
    const v1 = { id: "old", station: "08MH001", stationName: "Vedder Crossing", quantity: "discharge", direction: "below",
                 value: 12, water: { item: "w1", name: "Chilliwack River" }, createdAt: 1 };
    const text = JSON.stringify({ v: 1, on: true, updateReady: false, rules: [v1], ruleState: {}, held: null, toldRelease: null, runs: [] });
    const settings = createNotifySettings({ read: () => text, write: () => {} });
    const c: NotifyControls = { settings, platform: { permission: async () => "granted", ask: async () => "granted",
                                                      openSettings: () => {}, schedule: async () => {} } };
    render(<NotifyRoot controls={c}><Page /></NotifyRoot>);
    await flush();
    expect(screen.getByTestId("notify-alert-old").textContent).toContain("Flow below 12 m³/s · at most once a day for this water");
  });
});

describe("Alert me on a gauge's card and the editor (T234 v1.1)", () => {
  const target: AlertTarget = { station: "08MH001", stationName: "Vedder Crossing", quantity: "discharge", current: 42.5,
                                quantities: ["discharge", "level"], water: { item: "w1", name: "Chilliwack River" } };

  it("is not drawn without the app's notifications (the web)", () => {
    render(<NotifyRoot controls={null}><AlertMe palette={LIGHT} target={target} /></NotifyRoot>);
    expect(screen.queryByTestId("alert-me")).toBeNull();
  });

  it("opens prefilled from the card: its gauge, the chart's quantity, the reading now; the summary says what will happen", async () => {
    const { c, platform } = controls();
    render(<NotifyRoot controls={c}><AlertMe palette={LIGHT} target={target} /></NotifyRoot>);
    fireEvent.click(screen.getByTestId("alert-me-open"));
    expect(screen.getByText("Alert on Vedder Crossing")).toBeTruthy();
    expect((screen.getByTestId("alert-form-value") as HTMLInputElement).value).toBe("42.5");
    expect(screen.getByRole("radio", { name: "Flow" }).getAttribute("aria-checked")).toBe("true");
    expect(screen.getByRole("radio", { name: NOTIFY_WORDS.editor.cadenceDefault }).getAttribute("aria-checked")).toBe("true");
    expect(screen.getByTestId("alert-form-line").textContent).toBe("You get a notification when the flow at Vedder Crossing "
      + "rises above 42.5 m³/s, at most once a day for Chilliwack River. Not again until it has dropped back under the line.");
    // a % kind: the number is a percent (opens on 50), the gap the reader's own
    fireEvent.click(screen.getByTestId("alert-form-rise"));
    fireEvent.click(screen.getByTestId("alert-form-gap-12"));
    expect(screen.getByTestId("alert-form-line").textContent).toBe("You get a notification when the flow at Vedder Crossing "
      + "rises by 50 % or more within 24 hours, at most once every 12 h. Not again until the day's change has eased off.");
    expect(platform.ask).not.toHaveBeenCalled();
  });

  it("saves the alert in the new shape, turns notifications on and asks the permission then", async () => {
    const { c, platform, settings } = controls();
    render(<NotifyRoot controls={c}><AlertMe palette={LIGHT} target={target} /></NotifyRoot>);
    fireEvent.click(screen.getByTestId("alert-me-open"));
    fireEvent.click(screen.getByTestId("alert-form-q-level"));
    fireEvent.click(screen.getByTestId("alert-form-below"));
    fireEvent.change(screen.getByTestId("alert-form-value"), { target: { value: "1,25" } });
    await act(async () => { fireEvent.click(screen.getByTestId("alert-form-save")); });
    await flush();
    expect(platform.ask).toHaveBeenCalledTimes(1);
    const s = settings.read();
    expect(s.on).toBe(true);
    expect(s.rules).toMatchObject([{ station: "08MH001", quantity: "level", kind: "below", value: 1.25, gapH: null, on: true,
                                     water: { item: "w1", name: "Chilliwack River" } }]);
    expect(screen.getByTestId("alert-me-saved").textContent).toBe(NOTIFY_WORDS.alertMe.saved);
    expect(screen.getByTestId("alert-me-count").textContent).toBe("1 alert on this gauge");
  });

  it("refuses a value that is not a number (or a % out of range), and says a refused permission after saving", async () => {
    const { c, settings } = controls({}, "denied");
    render(<NotifyRoot controls={c}><AlertMe palette={LIGHT} target={target} /></NotifyRoot>);
    fireEvent.click(screen.getByTestId("alert-me-open"));
    fireEvent.change(screen.getByTestId("alert-form-value"), { target: { value: "lots" } });
    await act(async () => { fireEvent.click(screen.getByTestId("alert-form-save")); });
    expect(screen.getByTestId("alert-form-line").textContent).toBe(NOTIFY_WORDS.editor.bad);
    fireEvent.click(screen.getByTestId("alert-form-fall"));
    fireEvent.change(screen.getByTestId("alert-form-value"), { target: { value: "120" } });
    expect(screen.getByTestId("alert-form-line").textContent).toBe(NOTIFY_WORDS.editor.badPct);
    expect(settings.read().rules).toEqual([]);
    fireEvent.change(screen.getByTestId("alert-form-value"), { target: { value: "30" } });
    await act(async () => { fireEvent.click(screen.getByTestId("alert-form-save")); });
    await flush();
    expect(screen.getByTestId("alert-me-denied").textContent).toBe(NOTIFY_WORDS.permission.denied);
  });
});
