/**
 * (T888) THE POPUP ON OPENING, in the real Shell on the fixture (the map renderer stubbed, no WebGL in jsdom): a newer
 * version comes up as one popup — the swimming coho, "Version 0.8.2 is out", its bullets, Update and Not now — but only
 * after the first-run notice and the walkthrough are done; Not now closes it for that version and the bell's App update
 * section keeps the update; without a platform way (the web) there is no popup.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import { HOME, LayoutWidthContext, type AppUpdatePort } from "@app/ui";
import { LIGHT } from "./theme";

vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: () => null }));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");
afterEach(cleanup);

const NOTES = ["The map draws faster", "A swimming coho greets you"];
const memory = (v: string | null = null) => ({ v, read() { return this.v; }, write(t: string) { this.v = t; } });
const port = (version: string | null = "0.8.2", check?: AppUpdatePort["check"]): AppUpdatePort => ({
  channel: "play", installed: "0.8.1",
  check: check ?? (async () => (version ? { available: true, version, notes: NOTES } : { available: false, version: null })),
  start: async () => "downloading",
});

function mount(extra: Record<string, unknown>) {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
  return render(<LayoutWidthContext.Provider value={390}><GestureHandlerRootView>
    <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
           tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={HOME} spots={[]} onSaveSpot={() => {}}
           locate={null} {...extra} />
  </GestureHandlerRootView></LayoutWidthContext.Provider>);
}
/** a closed sheet slides out on react-native-web and is gone at the end of its animation, which jsdom never plays */
const slidOut = () => act(() => {
  for (const el of document.querySelectorAll("div"))
    for (const type of ["animationend", "webkitAnimationEnd"]) el.dispatchEvent(new Event(type, { bubbles: true }));
});
const settle = async () => { await act(async () => { await new Promise((r) => setTimeout(r, 300)); }); slidOut(); };
const press = (id: string) => act(() => { fireEvent.click(screen.getByTestId(id)); });
const popup = () => screen.queryByTestId("app-update-popup");

describe("(T888) the popup on opening", () => {
  it("waits behind the first-run notice and the walkthrough, then comes up once, whole", async () => {
    const store = memory();
    mount({ appUpdater: port(), appUpdateStore: store, firstRun: memory(null), walkthrough: memory(null) });
    await settle();
    expect(screen.getByTestId("first-run-notice")).toBeTruthy();
    expect(popup()).toBeNull();
    press("first-run-accept");
    await settle();
    // the walkthrough is next; the popup still waits
    expect(await screen.findByTestId("guide-skip", {}, { timeout: 4000 })).toBeTruthy();
    expect(popup()).toBeNull();
    press("guide-skip");
    await settle();
    const p = popup()!;
    expect(p).toBeTruthy();
    expect(screen.getByText("Version 0.8.2 is out")).toBeTruthy();
    expect(p.querySelector('[data-testid="coho-sheet"]')).toBeTruthy();   // the swimming coho, on top
    expect(screen.getAllByTestId("app-update-popup-note").map((n) => n.textContent)).toEqual(NOTES.map((n) => `• ${n}`));
    expect(screen.getByTestId("app-update-popup-update").textContent).toBe("Update");
    expect(screen.getByTestId("app-update-popup-later").textContent).toBe("Not now");
  });

  it("Not now: gone for that version (kept on the device), and the bell's App update keeps it", async () => {
    const store = memory();
    const first = mount({ appUpdater: port(), appUpdateStore: store });
    await settle();
    expect(popup()).toBeTruthy();
    press("app-update-popup-later");
    await settle();
    expect(popup()).toBeNull();
    expect(JSON.parse(store.v!).popped).toBe("0.8.2");
    press("inseason-bell");
    await settle();
    expect(screen.getByTestId("notice-app")).toBeTruthy();
    expect(screen.getAllByTestId("notice-app-note").map((n) => n.textContent)).toEqual(NOTES.map((n) => `• ${n}`));
    first.unmount();
    // the next opening: no popup for the same version
    mount({ appUpdater: port(), appUpdateStore: store });
    await settle();
    expect(popup()).toBeNull();
  });

  it("none when the app is current, nor without a platform way (the web)", async () => {
    mount({ appUpdater: port(null), appUpdateStore: memory() });
    await settle();
    expect(popup()).toBeNull();
    cleanup();
    mount({ appUpdater: null, appUpdateStore: memory(JSON.stringify({ day: "2026-11-18", available: true, version: "0.8.2" })) });
    await settle();
    expect(popup()).toBeNull();
  });

  it("(finding 3) opened from the new-version notification: the bell shows it, and closing the bell pops nothing", async () => {
    const store = memory();
    mount({ appUpdater: port(), appUpdateStore: store, openUpdate: 1 });
    await settle();
    // the bell is up on its App update; the popup is held behind it, and the version is read there
    expect(screen.getByTestId("notice-app")).toBeTruthy();
    expect(popup()).toBeNull();
    expect(JSON.parse(store.v!).popped).toBe("0.8.2");
    act(() => { fireEvent.click(screen.getByText("Got it")); });
    await settle();
    expect(screen.queryByTestId("notice-app")).toBeNull();
    expect(popup()).toBeNull();
  });

  it("(finding 4) a late check never stacks the popup over an open sheet (Layers): it waits until the sheet is closed", async () => {
    let answer!: (r: { available: boolean; version: string | null; notes: readonly string[] }) => void;
    mount({ appUpdater: port("0.8.2", () => new Promise((r) => { answer = r; })), appUpdateStore: memory() });
    await settle();
    act(() => { fireEvent.click(screen.getAllByLabelText(/^Choose layers/)[0]!); });
    await settle();
    expect(screen.getByTestId("water-legend")).toBeTruthy();
    await act(async () => { answer({ available: true, version: "0.8.2", notes: NOTES }); });
    await settle();
    expect(popup()).toBeNull();
    // the sheet closed: the popup comes up
    act(() => { fireEvent.click(screen.getByLabelText("Done")); });
    await settle();
    expect(screen.queryByTestId("water-legend")).toBeNull();
    expect(popup()).toBeTruthy();
  });
});
