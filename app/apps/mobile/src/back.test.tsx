/**
 * BACK, EVERYWHERE — through the app's own wiring: the real `Shell` with the real `systemBack.native.ts`, whose
 * `BackHandler` is replaced by a stand-in that behaves as Android's does (the last listener first; a press no listener
 * handles is Android's own — it leaves the app). One test per surface: what one hardware back does there, and that
 * the press says it was handled (true) everywhere but the Map tab's root (false: Android leaves the app).
 * The table these follow is app/HANDOFF-ui.md "Back, everywhere".
 *
 * The browser's back is the same back through `systemBack.web.ts` (jsdom's real History API) — the last block.
 *
 * The map renderer is replaced by buttons (no WebGL in jsdom): "tap the map" taps a section, "tap a point" a point,
 * "Layers" is the map's Layers button. Everything else is the app's, on the fixture.
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId, SectionId } from "@app/data";
import type { Spot } from "@app/data/spots";
import type { ReleaseLoader } from "@app/data/release";
import { openRegsFixture } from "@app/data/release/node";
import { HOME, type Route, type SystemBack } from "@app/ui";

/** Android's BackHandler, as Android drives it: listeners newest first; a press none handles goes to Android. */
const android = vi.hoisted(() => {
  const listeners: (() => boolean | null | undefined)[] = [];
  const exits: number[] = [];
  return {
    listeners, exits,
    BackHandler: {
      addEventListener: (event: string, h: () => boolean | null | undefined) => {
        if (event !== "hardwareBackPress") throw new Error(`unexpected ${event}`);
        listeners.push(h);
        return { remove: () => { const i = listeners.indexOf(h); if (i >= 0) listeners.splice(i, 1); } };
      },
      exitApp: () => { exits.push(1); },
    },
  };
});
vi.mock("react-native", async (importOriginal) => ({
  ...(await importOriginal<object>()), BackHandler: android.BackHandler,
}));

const TAP = { sid: 0 };
vi.mock("../../../packages/ui-native/src/MapScreen", async () => {
  const { Pressable, Text, View } = await import("react-native");
  return {
    MapScreen: ({ onPressFeature, onMapPoint, onLayers }: {
      onPressFeature?: (l: string, id: number, lat?: number, lon?: number) => void;
      onMapPoint?: (lat: number, lon: number) => void; onLayers?: () => void;
    }) => (
      <View>
        <Pressable accessibilityRole="button" accessibilityLabel="tap the map"
                   onPress={() => { void onPressFeature?.("streams", TAP.sid, 49.1, -121.6); }}><Text>map</Text></Pressable>
        {onMapPoint && <Pressable accessibilityRole="button" accessibilityLabel="tap a point"
                                  onPress={() => onMapPoint(49.1, -121.6)}><Text>point</Text></Pressable>}
        {onLayers && <Pressable accessibilityRole="button" accessibilityLabel="Layers"
                                onPress={onLayers}><Text>Layers</Text></Pressable>}
      </View>
    ),
  };
});
vi.mock("../../../packages/ui-native/src/MiniMap", () => ({ MiniMap: () => null }));

const { Shell, LIGHT } = await import("@app/ui-native");
const { systemBack: nativeBack } = await import("./systemBack.native");
const { systemBack: webBack } = await import("./systemBack.web");

afterEach(() => { cleanup(); android.exits.length = 0; });
beforeAll(() => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
  window.dispatchEvent(new Event("resize"));
});

const CHILLIWACK = "gnis:8634" as ItemId;
const VISIT = new Date(2026, 7, 30, 9).getTime();
const SPOT: Spot = {
  id: "s1", createdAt: VISIT, visitedAt: VISIT, updatedAt: 0, lat: 49.0974, lon: -121.9675, item: null,
  waterName: null, title: "Tamihi run", notes: "", photos: [], reading: null, weather: null, trace: null, panel: null,
};

/** One hardware back press, as Android delivers it: true = the app handled it; false = Android's (the app leaves). */
function hardwareBack(): boolean {
  let handled = false;
  act(() => {
    for (const h of [...android.listeners].reverse()) if (h()) { handled = true; return; }
    android.BackHandler.exitApp();
  });
  return handled;
}

async function mount(initialRoute: Route, system: SystemBack = nativeBack, loader: ReleaseLoader | null = null) {
  const source = makeFixtureSource();
  const water = await source.water(CHILLIWACK);
  TAP.sid = water!.sections[1]!;
  render(
    <GestureHandlerRootView>
      <Shell source={source} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={initialRoute} systemBack={system}
             loader={loader} spots={[SPOT]} onSaveSpot={async () => {}} onDeleteSpot={async () => {}} />
    </GestureHandlerRootView>);
  await act(async () => {});
}

/**
 * Where the reader is, in one line: the selected tab (or "no bar"), the water's title and front view, the spot screen's
 * state, the capture's step, an open sheet and an open question.
 */
function where(): string {
  const parts: string[] = [];
  const bar = screen.queryByRole("tab", { name: "Spots" });   // only the bottom bar has a Spots tab (a water's view strip has Conditions too)
  const tab = bar ? ["Map", "Conditions", "Spots", "More"]
    .find((k) => screen.getByRole("tab", { name: k }).getAttribute("aria-selected") === "true") : null;
  const strip = screen.queryByRole("tablist", { name: "Views of this water" });
  if (strip) {
    const on = [...strip.querySelectorAll("[role=tab]")].find((t) => t.getAttribute("aria-selected") === "true");
    const name = ["Chilliwack River", "Jeperson Side Channel"].find((n) => screen.queryByText(n)) ?? "?";
    parts.push(`water ${name} on ${on?.getAttribute("aria-label")}`);
  }
  if (screen.queryByLabelText("Edit spot") || screen.queryByLabelText("Cancel editing"))
    parts.push(screen.queryByLabelText("Cancel editing") ? "spot, editing" : "spot");
  for (const step of ["Pick the water", "Now the exact spot", "When were you there?"])
    if (screen.queryByText(step)) parts.push(`adding: ${step}`);
  if (screen.queryByPlaceholderText("Name this spot") && !screen.queryByLabelText("Cancel editing"))
    parts.push("adding: details");
  const sheet = openSheet();
  if (sheet !== null) parts.push(`${sheet} sheet`);
  if (screen.queryByLabelText("Back to all results")) parts.push("water near a town");
  if (screen.queryByLabelText("Close search")) parts.push("search");
  const page = menuPage();
  if (page) parts.push(`${page} page`);
  for (const q of ["Unsaved changes", "Confirm delete", "Discard this spot?"])
    if (screen.queryByLabelText(q)) parts.push(`asking: ${q}`);
  parts.push(tab ? `tab ${tab}` : "no bar");
  return parts.join(" · ");
}
/**
 * The open sheet's title, or null. react-native-web's Modal keeps a closed sheet in the page until its slide-out
 * animation ends, which jsdom never runs: so the animation is ended here, and a sheet still drawn after that is open.
 * The title is the text beside the sheet's Done.
 */
function openSheet(): string | null {
  const done = screen.queryByRole("button", { name: "Done" });
  if (!done) return null;
  act(() => {
    for (let e: HTMLElement | null = done; e; e = e.parentElement)
      for (const type of ["animationend", "webkitAnimationEnd"]) e.dispatchEvent(new Event(type, { bubbles: true }));
  });
  const still = screen.queryByRole("button", { name: "Done" });
  return still ? still.previousElementSibling?.textContent ?? "?" : null;
}
/** The open menu page's title (a More page, a Modal: its slide-out ended here as `openSheet` does), or null. */
function menuPage(): string | null {
  const open = () => screen.queryAllByTestId(/^menu-page-/);
  act(() => {
    for (const p of open())
      for (let e: HTMLElement | null = p; e; e = e.parentElement)
        for (const type of ["animationend", "webkitAnimationEnd"]) e.dispatchEvent(new Event(type, { bubbles: true }));
  });
  const p = open()[0];
  return p ? p.querySelector("[role=heading]")?.textContent ?? "?" : null;
}
/** Press back `n` times, writing down what each press returned and where it left the reader. */
function backs(n: number): string[] {
  const out = [where()];
  for (let i = 0; i < n; i++) { const h = hardwareBack(); out.push(`${h ? "handled" : "ANDROID"} -> ${where()}`); }
  return out;
}
const click = async (label: string) => { await act(async () => { fireEvent.click(screen.getByLabelText(label)); }); };
const until = async (f: () => boolean) => waitFor(() => expect(f()).toBe(true));

describe("Android's back, surface by surface (HANDOFF-ui 'Back, everywhere')", () => {
  it("the Map tab's root: not handled — Android leaves the app (no trap)", async () => {
    await mount(HOME);
    expect([...backs(1), `exits ${android.exits.length}`])
      .toEqual(["tab Map", "ANDROID -> tab Map", "exits 1"]);
  });

  it("another tab's root goes to the Map tab first, then Android leaves", async () => {
    const seen = [];
    for (const tab of ["conditions", "spots", "more"] as const) {
      await mount({ ...HOME, tab });
      seen.push(backs(2).join(" | "));
      cleanup();
    }
    expect(seen).toEqual([
      "tab Conditions | handled -> tab Map | ANDROID -> tab Map",
      "tab Spots | handled -> tab Map | ANDROID -> tab Map",
      "tab More | handled -> tab Map | ANDROID -> tab Map",
    ]);
  });

  it("the Layers sheet closes first; then the map is the root again", async () => {
    await mount(HOME);
    await click("Layers");
    expect(backs(2)).toEqual(["Layers sheet · tab Map", "handled -> tab Map", "ANDROID -> tab Map"]);
  });

  it("the water screen, whichever view is in front, leaves for the map it was tapped on (views are lateral)", async () => {
    const seen = [];
    for (const view of ["regs", "conditions", "map"] as const) {
      await mount({ mode: "water", from: { ...HOME, tab: view === "conditions" ? "conditions" : "map" },
                    stack: [{ item: CHILLIWACK, part: null, tapSid: null, at: null, view }] });
      await until(() => screen.queryByText("Chilliwack River") !== null);
      seen.push(backs(1).join(" | "));
      cleanup();
    }
    expect(seen).toEqual([
      "water Chilliwack River on Regulations · no bar | handled -> tab Map",
      "water Chilliwack River on Conditions · no bar | handled -> tab Conditions",
      "water Chilliwack River on Map · no bar | handled -> tab Map",
    ]);
  });

  it("the Regulations page's day sheet closes first; then the water leaves for the map", async () => {
    await mount({ mode: "water", from: HOME,
                  stack: [{ item: CHILLIWACK, part: null, tapSid: null, at: null, view: "regs" }] },
                nativeBack, openRegsFixture());
    await until(() => screen.queryByTestId("regs-date") !== null);
    await act(async () => { fireEvent.click(screen.getByTestId("regs-date")); });
    expect(backs(2)).toEqual([
      "water Chilliwack River on Regulations · Which day? sheet · no bar",
      "handled -> water Chilliwack River on Regulations · no bar",
      "handled -> tab Map",
    ]);
  });

  it("a water flowed into returns to the water it came from, then to the map", async () => {
    const frame = { part: null, tapSid: null, at: null, view: "regs" as const };
    await mount({ mode: "water", from: HOME, stack: [{ ...frame, item: "gnis:11481" as ItemId },
                                                       { ...frame, item: CHILLIWACK }] });
    await until(() => screen.queryByText("Chilliwack River") !== null);
    const first = where();
    const h = hardwareBack();
    await until(() => screen.queryByText("Jeperson Side Channel") !== null);
    expect([first, `${h ? "handled" : "ANDROID"} -> ${where()}`, `${hardwareBack() ? "handled" : "ANDROID"} -> ${where()}`])
      .toEqual([
        "water Chilliwack River on Regulations · no bar",
        "handled -> water Jeperson Side Channel on Regulations · no bar",
        "handled -> tab Map",
      ]);
  });

  it("(T48) search over the Conditions map: a town's list steps back to all results, then search closes, then the tab " +
     "goes to the Map tab, then Android leaves", async () => {
    await mount({ ...HOME, tab: "conditions" });
    act(() => { fireEvent.focus(screen.getByLabelText("Search waters and towns")); });
    fireEvent.change(screen.getByLabelText("Search waters and towns"), { target: { value: "chilliwack" } });
    await until(() => screen.queryByLabelText("Show Chilliwack on the map") !== null);
    await click("Show Chilliwack on the map");
    await until(() => screen.queryByLabelText("Back to all results") !== null);
    expect(backs(4)).toEqual(["water near a town · search · tab Conditions", "handled -> search · tab Conditions",
                              "handled -> tab Conditions", "handled -> tab Map", "ANDROID -> tab Map"]);
  });

  it("(T48) a More page closes first, then the More tab goes to the Map tab", async () => {
    await mount({ ...HOME, tab: "more" });
    await click("About");
    expect(backs(3)).toEqual(["About page · tab More", "handled -> tab More", "handled -> tab Map", "ANDROID -> tab Map"]);
  });

  it("a spot (opened over the map it was picked on): back returns to that map, then Android leaves", async () => {
    await mount({ ...HOME, spot: "s1" });
    expect(backs(2)).toEqual(["spot · tab Map", "handled -> tab Map", "ANDROID -> tab Map"]);
  });

  it("a spot being edited, nothing changed: back ends the edit and stays on the spot", async () => {
    await mount({ ...HOME, spot: "s1" });
    await click("Edit spot");
    expect(backs(2)).toEqual(["spot, editing · tab Map", "handled -> spot · tab Map", "handled -> tab Map"]);
  });

  it("a spot being edited with changes: back ASKS (nothing dropped); back again keeps editing", async () => {
    await mount({ ...HOME, spot: "s1" });
    await click("Edit spot");
    fireEvent.change(screen.getByPlaceholderText("Name this spot"), { target: { value: "Tamihi run, low" } });
    const out = backs(2);
    expect([...out, (screen.getByPlaceholderText("Name this spot") as HTMLInputElement).value]).toEqual([
      "spot, editing · tab Map",
      "handled -> spot, editing · asking: Unsaved changes · tab Map",
      "handled -> spot, editing · tab Map",
      "Tamihi run, low",
    ]);
  });

  it("a delete being asked: back answers 'keep it' — nothing is deleted", async () => {
    const onDelete = vi.fn(async () => {});
    const source = makeFixtureSource();
    render(
      <GestureHandlerRootView>
        <Shell source={source} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
               tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={{ ...HOME, spot: "s1" }}
               systemBack={nativeBack} spots={[SPOT]} onSaveSpot={async () => {}} onDeleteSpot={onDelete} />
      </GestureHandlerRootView>);
    await click("Delete spot");
    expect([...backs(1), `deleted ${onDelete.mock.calls.length}`])
      .toEqual(["spot · asking: Confirm delete · tab Map", "handled -> spot · tab Map", "deleted 0"]);
  });

  it("adding a spot: each back is one step back, the first step's back leaves", async () => {
    await mount({ ...HOME, adding: { at: null } });
    await click("tap the map");
    await until(() => screen.queryAllByText("Chilliwack River").length > 0);
    await click("Next");
    await click("tap a point");
    await click("Next");
    expect(backs(4)).toEqual([
      "adding: When were you there? · no bar",
      "handled -> adding: Now the exact spot · no bar",
      "handled -> adding: Pick the water · no bar",
      "handled -> tab Map",
      "ANDROID -> tab Map",
    ]);
  });

  it("adding a spot with notes typed: back never drops them unasked — it asks, and back again keeps them", async () => {
    await mount({ ...HOME, adding: { at: null } });
    await click("tap the map");
    await until(() => screen.queryAllByText("Chilliwack River").length > 0);
    await click("Next");
    await click("tap a point");
    await click("Next");
    await click("Next");
    await until(() => screen.queryByPlaceholderText("Name this spot") !== null);
    const notes = screen.getAllByRole("textbox").at(-1)!;
    fireEvent.change(notes, { target: { value: "fish holding below the riffle" } });
    expect(backs(5)).toEqual([
      "adding: details · no bar",
      "handled -> adding: When were you there? · no bar",
      "handled -> adding: Now the exact spot · no bar",
      "handled -> adding: Pick the water · no bar",
      "handled -> adding: Pick the water · asking: Discard this spot? · no bar",
      "handled -> adding: Pick the water · no bar",
    ]);
    // and the draft is still there: forward again finds the notes
    await click("Next");
    await click("tap a point");
    await click("Next");
    await click("Next");
    await until(() => screen.queryByPlaceholderText("Name this spot") !== null);
    expect(screen.queryByDisplayValue("fish holding below the riffle")).not.toBeNull();
  });
});

describe("the browser's back (systemBack.web: the History API) does the same", () => {
  /** The browser's back button: a real `history.back()`, whose popstate jsdom delivers later, as browsers do. */
  const browserBack = async () => { await act(async () => { window.history.back(); await new Promise((r) => setTimeout(r, 30)); }); };

  it("Layers sheet, then a water, then a tab: one entry each, closed innermost first", async () => {
    await mount({ ...HOME, tab: "spots" }, webBack);
    const seen = [where()];
    // leaving a tab's root steps the history back; jsdom delivers that popstate later, as a browser does — let it land
    // before the next press (a person is never faster than a few milliseconds)
    await click("Map"); await act(async () => { await new Promise((r) => setTimeout(r, 30)); }); seen.push(where());
    await click("Layers"); seen.push(where());
    await browserBack(); seen.push(where());
    await click("tap the map");
    await until(() => screen.queryByText("Chilliwack River") !== null);
    seen.push(where());
    await browserBack(); seen.push(where());
    await click("Spots"); seen.push(where());
    await browserBack(); seen.push(where());
    expect(seen).toEqual([
      "tab Spots", "tab Map", "Layers sheet · tab Map", "tab Map",
      "water Chilliwack River on Regulations · no bar", "tab Map",
      "tab Spots", "tab Map",
    ]);
  });
});
