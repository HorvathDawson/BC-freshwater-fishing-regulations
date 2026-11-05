/**
 * (T234) More → Notifications and a gauge's "Alert me": opt-in, the permission asked only when something is turned on,
 * a refusal shown, alerts listed with Change and Delete, the background runs listed; nothing drawn without the app's
 * notifications (the web).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MENU_WORDS, NOTIFY_WORDS, type DataInput, type GaugeAlertRule } from "@app/core";
import { createNotifySettings, NOTIFY_DEFAULT, type NotifyState } from "@app/data";
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
  id: "r1", station: "08MH001", stationName: "Vedder Crossing", quantity: "discharge", direction: "above", value: 50,
  water: { item: "w1", name: "Chilliwack River" }, createdAt: NOW,
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

  it("lists the alerts with Change and Delete, and the last background runs, newest first", async () => {
    const { c, settings } = controls({ on: true, rules: [RULE], runs: [{ at: NOW - 60_000, ms: 900, result: "nothing to tell" },
                                                                         { at: NOW, ms: 1200, result: "1 alert" }] }, "granted", "granted");
    render(<NotifyRoot controls={c}><Page /></NotifyRoot>);
    await flush();
    expect(screen.getByTestId("notify-alert-r1").textContent).toBe("Chilliwack RiverVedder Crossing · Flow rises above 50 m³/sChangeDelete");
    expect(screen.getAllByTestId("notify-run").map((e) => e.textContent?.split(" · ").at(-1))).toEqual(["1 alert", "nothing to tell"]);
    fireEvent.click(screen.getByTestId("notify-edit-r1"));
    fireEvent.change(screen.getByTestId("alert-form-value"), { target: { value: "70" } });
    await act(async () => { fireEvent.click(screen.getByTestId("alert-form-save")); });
    expect(settings.read().rules.map((r) => r.value)).toEqual([70]);
    fireEvent.click(screen.getByRole("button", { name: NOTIFY_WORDS.alerts.removeAloud("Flow rises above 70 m³/s") }));
    expect(settings.read().rules).toEqual([]);
    expect(screen.getByTestId("notify-no-alerts")).toBeTruthy();
  });
});

describe("Alert me on a gauge's card (T234)", () => {
  const target: AlertTarget = { station: "08MH001", stationName: "Vedder Crossing", quantity: "discharge", current: 42.5,
                                water: { item: "w1", name: "Chilliwack River" } };

  it("is not drawn without the app's notifications (the web)", () => {
    render(<NotifyRoot controls={null}><AlertMe palette={LIGHT} target={target} /></NotifyRoot>);
    expect(screen.queryByTestId("alert-me")).toBeNull();
  });

  it("saves a rule in the gauge's quantity, turns notifications on and asks the permission then", async () => {
    const { c, platform, settings } = controls();
    render(<NotifyRoot controls={c}><AlertMe palette={LIGHT} target={target} /></NotifyRoot>);
    fireEvent.click(screen.getByTestId("alert-me-open"));
    expect(screen.getByTestId("alert-form-line").textContent).toBe("Flow rises above 42.5 m³/s. Now 42.5 m³/s.");
    expect(platform.ask).not.toHaveBeenCalled();
    fireEvent.click(screen.getByTestId("alert-form-below"));
    fireEvent.change(screen.getByTestId("alert-form-value"), { target: { value: "12,5" } });
    expect(screen.getByTestId("alert-form-line").textContent).toBe("Flow falls below 12.5 m³/s. Now 42.5 m³/s.");
    await act(async () => { fireEvent.click(screen.getByTestId("alert-form-save")); });
    await flush();
    expect(platform.ask).toHaveBeenCalledTimes(1);
    const s = settings.read();
    expect(s.on).toBe(true);
    expect(s.rules).toMatchObject([{ station: "08MH001", quantity: "discharge", direction: "below", value: 12.5,
                                     water: { item: "w1", name: "Chilliwack River" } }]);
    expect(screen.getByTestId("alert-me-saved").textContent).toBe(NOTIFY_WORDS.alertMe.saved);
    expect(screen.getByTestId("alert-me-count").textContent).toBe("1 alert on this gauge");
  });

  it("refuses a value that is not a number, and says a refused permission after saving", async () => {
    const { c, settings } = controls({}, "denied");
    render(<NotifyRoot controls={c}><AlertMe palette={LIGHT} target={target} /></NotifyRoot>);
    fireEvent.click(screen.getByTestId("alert-me-open"));
    fireEvent.change(screen.getByTestId("alert-form-value"), { target: { value: "lots" } });
    await act(async () => { fireEvent.click(screen.getByTestId("alert-form-save")); });
    expect(screen.getByTestId("alert-form-line").textContent).toBe(NOTIFY_WORDS.alertMe.bad);
    expect(settings.read().rules).toEqual([]);
    fireEvent.change(screen.getByTestId("alert-form-value"), { target: { value: "60" } });
    await act(async () => { fireEvent.click(screen.getByTestId("alert-form-save")); });
    await flush();
    expect(screen.getByTestId("alert-me-denied").textContent).toBe(NOTIFY_WORDS.permission.denied);
  });
});
