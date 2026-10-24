/**
 * The Spots TAB and the long-press drop sheet, mounted (T48: Spots is a tab again, owner 2026-10-22). Its promises: the empty state says what a spot is (the only place that does), a spot on unnamed
 * water is an ordinary row, the row dates the visit and shows what was recorded then, the first photo is drawn through
 * the store, stale spots can be refreshed. And the new ones: Show sends the map, the ways to add, a failure said.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import type { Spot } from "@app/data/spots";
import { spotsMenu, type LocatePermission } from "@app/ui";
import { SpotsScreen } from "./SpotsScreen";
import { SpotDropSheet } from "./SpotDropSheet";
import { SpotPickBanner } from "./SpotPickBanner";
import { LIGHT } from "./theme";

afterEach(cleanup);

// LOCAL parts: the list dates a spot by the reader's own day (P12b), so the test must not depend on the zone it runs in.
const VISIT = new Date(2026, 7, 30, 23, 30).getTime();
const spot = (over: Partial<Spot> = {}): Spot => ({
  id: "s1", createdAt: VISIT, visitedAt: VISIT, updatedAt: 0,
  lat: 49.0974, lon: -121.9675, item: null, waterName: null,
  title: "Tamihi run", notes: "", photos: [],
  reading: null, weather: null, trace: null, panel: null, ...over,
});

function mount(spots: Spot[], o: { permission?: LocatePermission | null; error?: string | null; refresh?: boolean;
                                   photoStore?: { uri: (r: string) => Promise<string> }; railError?: string } = {}) {
  const calls: string[] = [];
  const vm = spotsMenu(spots, { error: o.error ?? null, canAdd: true, canRefresh: !!o.refresh,
                                permission: o.permission ?? null });
  render(
    <GestureHandlerRootView>
      <SpotsScreen palette={LIGHT} vm={vm}
                  onOpen={(id) => calls.push(`open ${id}`)} onShow={(id) => calls.push(`show ${id}`)}
                  onAdd={(k) => calls.push(`add ${k}`)} onRefresh={o.refresh ? () => calls.push("refresh") : undefined}
                  photoStore={o.photoStore} error={o.railError ?? null} />
    </GestureHandlerRootView>);
  return calls;
}
/** What the menu offers, in order: every button's spoken name (the scrim, "Close", is a press area, not a button). */
const buttons = () => screen.getAllByRole("button").map((b) => b.getAttribute("aria-label"));

describe("<SpotsScreen>", () => {
  it("is a tab page: its title is a header, and nothing on it closes (back from a tab is the route's)", () => {
    mount([]);
    expect([screen.getByRole("heading").textContent, screen.queryByRole("button", { name: "Done" })]).toEqual(["Spots", null]);
  });

  it("none yet: what a spot is, how to drop one, and the two ways to add", () => {
    mount([], { permission: "prompt" });
    expect(screen.getByText("No spots yet")).toBeTruthy();
    expect(screen.getByText("Pin an exact point on a stream and the app records the gauge, its standing against the " +
                            "record and the weather that day. Notes and photographs go on top. Press and hold the map " +
                            "to drop one.")).toBeTruthy();
    expect(buttons()).toEqual(["At my location", "Pick on the map"]);
    expect([screen.getByText("Asks for your location first").textContent,
            screen.getByText("Go to the map and tap the place").textContent])
      .toEqual(["Asks for your location first", "Go to the map and tap the place"]);
  });

  it("a spot with no photo, no gauge and no water is a normal row; tap opens it, Show sends the map", () => {
    const calls = mount([spot()]);
    expect(screen.getByTestId("spot-row-s1").textContent).toBe("2026-08-30 · no gauge · no weather\n49.0974, -121.9675");
    expect(screen.getByText("NO PHOTO")).toBeTruthy();
    expect(buttons()).toEqual(["Tamihi run", "Show Tamihi run on the map", "Pick on the map"]);
    fireEvent.click(screen.getByRole("button", { name: "Tamihi run" }));
    fireEvent.click(screen.getByRole("button", { name: "Show Tamihi run on the map" }));
    fireEvent.click(screen.getByRole("button", { name: "Pick on the map" }));
    expect(calls).toEqual(["open s1", "show s1", "add pick"]);
  });

  it("shows the reading that was recorded, not a live one", () => {
    mount([spot({ reading: { station: "08MH001", discharge: 15.7, level: null, percentile: 0.038, at: null },
                  weather: { window: null, tempC: 14, windKph: null, windDir: null, rain3h: null, pressureHpa: null,
                             code: null, at: null } })]);
    expect(screen.getByTestId("spot-row-s1").textContent).toBe("2026-08-30 · 15.7 m³/s · p4th · 14°C\n49.0974, -121.9675");
  });

  it("draws a spot's first photo from its reference, through the photo store", async () => {
    const store = { uri: async (ref: string) => `file:///doc/spots/${ref}` };
    mount([spot({ photos: ["photos/s1-1.jpg", "photos/s1-2.jpg"] })], { photoStore: store });
    await act(async () => { for (let i = 0; i < 4; i++) await Promise.resolve(); });
    expect([...document.querySelectorAll("img")].map((i) => i.getAttribute("src")))
      .toEqual(["file:///doc/spots/photos/s1-1.jpg"]);
    expect(screen.queryByText("NO PHOTO")).toBeNull();
  });

  it("counts the spots with something still to fill in, and refreshes them", () => {
    const calls = mount([spot(), spot({ id: "s2", weather: { at: "x" } as never })], { refresh: true });
    expect(screen.getByTestId("spots-stale").textContent)
      .toBe("2 spots are missing the weather or the flow from the day they were pinned, or the week before it.");
    fireEvent.click(screen.getByRole("button", { name: "Refresh spots" }));
    expect(calls).toEqual(["refresh"]);
  });

  it("an unreadable file is said, never an empty list; a location that could not be read is said", () => {
    mount([spot()], { error: "The saved spots could not be read.", permission: "granted",
                      railError: "Your location could not be read, so no spot was started." });
    expect(screen.getAllByRole("alert").map((a) => a.textContent)).toEqual([
      "The saved spots could not be read.", "Your location could not be read, so no spot was started."]);
    expect(screen.queryByText("No spots yet")).toBeNull();
    expect(buttons()).toEqual(["At my location", "Pick on the map"]);
  });

  it("your location refused, or none here: not offered, and the tab says why (whole line); pick on the map stays", () => {
    const seen = (["denied", "unavailable"] as const).map((permission) => {
      const calls = mount([], { permission });
      const out = [buttons(), screen.getByTestId("spots-note").textContent];
      fireEvent.click(screen.getByRole("button", { name: "Pick on the map" }));
      cleanup();
      return [...out, calls];
    });
    expect(seen).toEqual([
      [["Pick on the map"], "Location access is turned off for this app, so At my location is not offered.", ["add pick"]],
      [["Pick on the map"], "Your location is not available here, so At my location is not offered.", ["add pick"]],
    ]);
  });
});

describe("<SpotPickBanner>", () => {
  it("over the map in pick mode: says what to do; Cancel and Escape leave it", () => {
    const calls: string[] = [];
    render(<SpotPickBanner palette={LIGHT} top={10} onCancel={() => calls.push("cancel")} />);
    const said = [screen.getByTestId("spot-pick-banner").textContent, buttons()];
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    act(() => { window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" })); });
    act(() => { window.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter" })); });
    expect([said, calls]).toEqual([["Tap where your spot isCancel", ["Cancel"]], ["cancel", "cancel"]]);
  });
});

describe("<SpotDropSheet>", () => {
  const drop = (at: { lat: number; lon: number } | null) => {
    const calls: string[] = [];
    render(
      <GestureHandlerRootView>
        <SpotDropSheet at={at} palette={LIGHT} onCancel={() => calls.push("cancel")}
                       onContinue={() => calls.push("continue")} />
      </GestureHandlerRootView>);
    return calls;
  };
  it("says where the pin is and what comes next; Cancel, Done and the scrim cancel; Continue continues", () => {
    const calls = drop({ lat: 49.09744, lon: -121.96752 });
    expect([screen.getByText("New spot").textContent, screen.getByTestId("spot-drop-where").textContent,
            screen.getByText("Next you pick the water it is on, then when you were there.").textContent, buttons()])
      .toEqual(["New spot", "49.0974, -121.9675", "Next you pick the water it is on, then when you were there.",
                ["Done", "Cancel", "Continue"]]);
    for (const name of ["Cancel", "Done", "Close", "Continue"])
      fireEvent.click(screen.getByLabelText(name));       // Close: the scrim behind the sheet
    expect(calls).toEqual(["cancel", "cancel", "cancel", "continue"]);
  });

  it("no pin, no sheet", () => {
    drop(null);
    expect(screen.queryByText("New spot")).toBeNull();
  });
});
