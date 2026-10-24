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
import type { ItemId, WaterSource, SectionId } from "@app/data";
import type { Spot } from "@app/data/spots";
import type { ReleaseLoader } from "@app/data/release";
import { openRegsFixture } from "@app/data/release/node";
import { HOME, type RegsStart, type Route, type SystemBack } from "@app/ui";

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

async function mount(initialRoute: Route, system: SystemBack = nativeBack, loader: ReleaseLoader | null = null,
                     more: { source?: WaterSource; regsStart?: RegsStart } = {}) {
  const source = more.source ?? makeFixtureSource();
  const water = await source.water(CHILLIWACK);
  TAP.sid = water!.sections[1]!;
  render(
    <GestureHandlerRootView>
      <Shell source={source} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={initialRoute} systemBack={system}
             loader={loader} spots={[SPOT]} onSaveSpot={async () => {}} onDeleteSpot={async () => {}}
             regsStart={more.regsStart ?? null} />
    </GestureHandlerRootView>);
  await act(async () => {});
}

/**
 * (T58) The fixture source with Kootenay Lake's West Arm closure areas in it (the dev fixture holds two rivers; the regs
 * fixture release holds the lake), and one section the test answers by hand: `held.release()` lets its lookup finish.
 */
const KLWA: Record<string, string> = { "wbk:-21": "Kootenay Lake — Upper West Arm", "wbk:-22": "Kootenay Lake — Lower West Arm" };
const KLWA_SID = 990_022 as SectionId, SLOW_SID = 990_001 as SectionId;
function withLake() {
  const base = makeFixtureSource();
  let release = () => {};
  const held = new Promise<void>((ok) => { release = ok; });
  const source: WaterSource = {
    ...base,
    itemForSection: async (s) => (s === KLWA_SID ? "wbk:-22" as ItemId
      : s === SLOW_SID ? (await held, JEPERSON) : base.itemForSection(s)),
    water: async (i) => (KLWA[i] ? { item: i, name: KLWA[i]!, kind: "lake", sections: [] } : base.water(i)),
  };
  return { source, release: async () => { await act(async () => { release(); await held; }); } };
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
    const name = ["Chilliwack River", "Jeperson Side Channel", "Vedder River", ...Object.values(KLWA)]
      .find((n) => screen.queryByText(n)) ?? "?";
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

/** A water's view strip (the bottom bar is hidden in water mode, so these tabs are the strip's). */
const view = async (name: string) => { await act(async () => { fireEvent.click(screen.getByRole("tab", { name })); }); };
const press = async (name: string) => { await act(async () => { fireEvent.click(screen.getByRole("button", { name })); }); };
const mapWhere = () => screen.queryByTestId("water-map-where")?.textContent ?? "no water map";
const onWaterMap = () => waitFor(() => expect(screen.getByTestId("water-map-where")).toBeTruthy(), { timeout: 20_000 });
const JEPERSON = "gnis:11481" as ItemId;

/**
 * (Owner 2026-10-22 late) Moves inside ONE water, from the Map tab: tap the Chilliwack open (Regulations in front), then
 * Conditions, Map, ▲, ▼, ▼, Regulations, Map — each written down as where the reader is
 * (and the water map's part while it is in front), with `extra` (the browser's history entry) beside it.
 */
async function movesInsideChilliwack(extra: () => string = () => ""): Promise<string[]> {
  const out: string[] = [];
  const note = (s: string) => out.push(`${s}${extra()}`);
  await click("tap the map");
  await until(() => screen.queryByText("Chilliwack River") !== null);
  note(where());
  await view("Conditions"); note(where());
  await view("Map"); await onWaterMap(); note(`${where()} · ${mapWhere()}`);
  await press("▲ Upstream"); note(`${where()} · ${mapWhere()}`);
  await press("▼ Downstream"); note(`${where()} · ${mapWhere()}`);
  await press("▼ Downstream"); note(`${where()} · ${mapWhere()}`);
  await view("Regulations"); note(where());
  await view("Map"); await onWaterMap(); note(`${where()} · ${mapWhere()}`);
  return out;
}
const MOVES_SEEN = [
  "tab Map",
  "water Chilliwack River on Regulations · no bar",
  "water Chilliwack River on Conditions · no bar",
  "water Chilliwack River on Map · no bar · Part 3 of 4 · Chilliwack River",
  "water Chilliwack River on Map · no bar · Part 4 of 4 · Chilliwack River",
  "water Chilliwack River on Map · no bar · Part 3 of 4 · Chilliwack River",
  "water Chilliwack River on Map · no bar · Part 2 of 4 · Chilliwack River",
  "water Chilliwack River on Regulations · no bar",
  // the stepped part is the frame's: the map comes back on it
  "water Chilliwack River on Map · no bar · Part 2 of 4 · Chilliwack River",
];

/** Search "chilliwack river", frame its row, and open it from the row's action (the search result path). */
async function openFromSearch(): Promise<void> {
  act(() => { fireEvent.focus(screen.getByLabelText("Search waters and towns")); });
  fireEvent.change(screen.getByLabelText("Search waters and towns"), { target: { value: "chilliwack river" } });
  await until(() => screen.queryAllByRole("button", { name: /^Show Chilliwack River/ }).length > 0);
  await act(async () => { fireEvent.click(screen.getAllByRole("button", { name: /^Show Chilliwack River/ })[0]!); });
  await until(() => screen.queryAllByRole("button", { name: /^Open Chilliwack River/ }).length > 0);
  await act(async () => { fireEvent.click(screen.getAllByRole("button", { name: /^Open Chilliwack River/ })[0]!); });
  await until(() => screen.queryByText("Chilliwack River") !== null && !screen.queryByLabelText("Close search"));
}

/** The tributary chain: tap the Chilliwack open, its Map, then a tap there on the Jeperson Side Channel (another water). */
async function openTributary(): Promise<void> {
  TAP.sid = (await makeFixtureSource().water(CHILLIWACK))!.sections[1]!;
  await click("tap the map");
  await until(() => screen.queryByText("Chilliwack River") !== null);
  await view("Map"); await onWaterMap();
  TAP.sid = (await makeFixtureSource().water(JEPERSON))!.sections[0]!;
  await click("tap the map");
  await until(() => screen.queryByText("Jeperson Side Channel") !== null);
}

/**
 * (Owner 2026-10-22 late, "a river that changes its name is ONE system") Tap the Chilliwack open, its Map, ▼ ▼ to its
 * lowest part, then ▼ into the Vedder, ▲ into the Chilliwack, ▼ into the Vedder: each cross replaces the water on
 * screen. Written down as `movesInsideChilliwack` does, the map's part beside each.
 */
async function acrossChilliwackVedder(extra: () => string = () => ""): Promise<string[]> {
  const out: string[] = [];
  const note = (s: string) => out.push(`${s}${extra()}`);
  await click("tap the map");
  await until(() => screen.queryByText("Chilliwack River") !== null);
  await view("Map"); await onWaterMap();
  await press("▼ Downstream"); await press("▼ Downstream"); note(`${where()} · ${mapWhere()}`);
  await press("▼ Downstream into Vedder River");
  await until(() => mapWhere().startsWith("Part 1 of 1")); note(`${where()} · ${mapWhere()}`);
  await press("▲ Upstream into Chilliwack River");
  await until(() => mapWhere().startsWith("Part 1 of 4")); note(`${where()} · ${mapWhere()}`);
  await press("▼ Downstream into Vedder River");
  await until(() => mapWhere().startsWith("Part 1 of 1")); note(`${where()} · ${mapWhere()}`);
  return out;
}
const ACROSS_SEEN = [
  "water Chilliwack River on Map · no bar · Part 1 of 4 · Chilliwack River",
  "water Vedder River on Map · no bar · Part 1 of 1",
  "water Chilliwack River on Map · no bar · Part 1 of 4 · Chilliwack River",
  "water Vedder River on Map · no bar · Part 1 of 1",
];

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

  it("(owner 2026-10-22 late) N moves inside one water — its views, ▲ / ▼ along its parts — then ONE back is the " +
     "map it was opened from, and the next is Android's", async () => {
    await mount(HOME, nativeBack, openRegsFixture());
    const seen = [where(), ...(await movesInsideChilliwack())];
    expect([...seen, ...backs(2).slice(1)]).toEqual([...MOVES_SEEN, "handled -> tab Map", "ANDROID -> tab Map"]);
  }, 60_000);

  it("(owner 2026-10-22 late) one system: ▼ / ▲ across the Chilliwack ↔ Vedder name change replaces the water, so " +
     "ONE back from either side is the map it was opened from, and the next is Android's", async () => {
    await mount(HOME, nativeBack, openRegsFixture());
    const seen = await acrossChilliwackVedder();
    expect([...seen, ...backs(2).slice(1)]).toEqual([...ACROSS_SEEN, "handled -> tab Map", "ANDROID -> tab Map"]);
  }, 60_000);

  it("(owner 2026-10-22 late) a tributary chain A -> B: back is A, back is the map; × from B is the map at once", async () => {
    await mount(HOME, nativeBack, openRegsFixture());
    await openTributary();
    const chain = [where()];
    const h = hardwareBack();
    await until(() => screen.queryByText("Chilliwack River") !== null);   // its map reads the release again
    chain.push(`${h ? "handled" : "ANDROID"} -> ${where()}`, ...backs(1).slice(1));
    cleanup();
    await mount(HOME, nativeBack, openRegsFixture());
    await openTributary();
    const before = where();
    await click("Close, back to Map");
    expect({ chain, close: [before, where(), ...backs(1).slice(1)] }).toEqual({
      chain: ["water Jeperson Side Channel on Regulations · no bar",
              "handled -> water Chilliwack River on Map · no bar", "handled -> tab Map"],
      close: ["water Jeperson Side Channel on Regulations · no bar", "tab Map", "ANDROID -> tab Map"],
    });
  }, 60_000);

  it("(T58) a split lake is held once: from a river opened over Kootenay Lake's Upper West Arm, a tap on its Lower West Arm " +
     "goes back to the lake (not a second frame), so ONE back is the map", async () => {
    const lake = withLake();
    const frame = { part: null, tapSid: null, at: null };
    await mount({ mode: "water", from: HOME, stack: [{ ...frame, item: "wbk:-21" as ItemId, view: "regs" },
                                                     { ...frame, item: CHILLIWACK, view: "map" }] },
                nativeBack, openRegsFixture(), { source: lake.source });
    await onWaterMap();
    const seen = [where()];
    TAP.sid = KLWA_SID;
    await click("tap the map");
    await until(() => screen.queryByText(KLWA["wbk:-22"]!) !== null);
    expect([...seen, ...backs(2)]).toEqual([
      "water Chilliwack River on Map · no bar",
      "water Kootenay Lake — Lower West Arm on Regulations · no bar", "handled -> tab Map", "ANDROID -> tab Map"]);
  }, 60_000);

  it("(T58) a Parts sheet row reads that part (the frame's part: the water's map comes back on it), and is no layer: " +
     "one back is the map", async () => {
    await mount({ mode: "water", from: HOME, stack: [{ item: CHILLIWACK, part: 0, tapSid: null, at: null, view: "regs" }] },
                nativeBack, openRegsFixture(), { regsStart: { tab: "keep", sheet: "parts" as RegsStart["sheet"], scroll: 0 } });
    await until(() => screen.queryAllByRole("radio").length > 0);
    const seen = [where()];
    const row = screen.getAllByRole("radio").find((r) => r.getAttribute("aria-checked") === "false"
                                                     && /^Vedder Crossing Bridge → Tamihi Rapids Bridge/.test(r.getAttribute("aria-label") ?? ""))!;
    await act(async () => { fireEvent.click(row); });
    seen.push(where());
    await view("Map"); await onWaterMap();
    seen.push(`${where()} · ${mapWhere()}`);
    expect([...seen, ...backs(1).slice(1)]).toEqual([
      "water Chilliwack River on Regulations · Parts of this water sheet · no bar",
      "water Chilliwack River on Regulations · no bar",
      // opened on part 0 (Part 3 of 4, "100 m below the Slesse → the Ecological Reserve"); the row picked the mouth
      "water Chilliwack River on Map · no bar · Part 1 of 4 · Chilliwack River",
      "handled -> tab Map"]);
  }, 60_000);

  it("(T58) a slow tap on a water's map: back pressed before its lookup finishes — the late answer opens nothing", async () => {
    const lake = withLake();
    await mount({ mode: "water", from: HOME, stack: [{ item: CHILLIWACK, part: null, tapSid: null, at: null, view: "map" }] },
                nativeBack, openRegsFixture(), { source: lake.source });
    await onWaterMap();
    TAP.sid = SLOW_SID;
    await click("tap the map");
    const seen = backs(1);
    await lake.release();
    seen.push(`answered -> ${where()}`);
    expect(seen).toEqual(["water Chilliwack River on Map · no bar", "handled -> tab Map", "answered -> tab Map"]);
  }, 60_000);

  it("(T58 review) \"Flows into\" goes through the opener: it opens the receiving water once its areas are read, and " +
     "back pressed before they are read wins (the water opens nothing late)", async () => {
    const chilliwack = { mode: "water", from: HOME,
                         stack: [{ item: CHILLIWACK, part: 1, tapSid: null, at: null, view: "map" }] } as const;
    const flows = () => screen.getByRole("link", { name: "Flows into Vedder River ›" });
    // the dev fixture source holds no Vedder: its title, so the page says which water opened
    const base = makeFixtureSource();
    const source: WaterSource = { ...base, water: async (i) => (i === "gnis:3062"
      ? { item: i, name: "Vedder River", kind: "river", sections: [] } : base.water(i)) };
    await mount(chilliwack, nativeBack, openRegsFixture(), { source });
    await onWaterMap(); await until(() => screen.queryByRole("link", { name: "Flows into Vedder River ›" }) !== null);
    await act(async () => { fireEvent.click(flows()); });
    await waitFor(() => expect(screen.queryByTestId("water-map") === null
                           && screen.queryAllByText("Vedder River").length > 0).toBe(true), { timeout: 20_000 });
    const opened = [where()];
    hardwareBack();
    await until(() => screen.queryAllByText("Chilliwack River").length > 0);
    opened.push(`back -> ${where()}`);
    cleanup();
    await mount(chilliwack, nativeBack, openRegsFixture(), { source });
    await onWaterMap(); await until(() => screen.queryByRole("link", { name: "Flows into Vedder River ›" }) !== null);
    // the press and back in one go: back lands before the receiving water's areas are read
    act(() => {
      fireEvent.click(flows());
      for (const h of [...android.listeners].reverse()) if (h()) return;
    });
    await act(async () => { await new Promise((r) => setTimeout(r, 50)); });
    expect([...opened, `pressed then back -> ${where()}`]).toEqual([
      "water Vedder River on Regulations · no bar", "back -> water Chilliwack River on Map · no bar",
      "pressed then back -> tab Map"]);
  }, 60_000);

  it("(owner 2026-10-22 late) a water opened from a search RESULT: back is the map, search closed; then Android's", async () => {
    await mount(HOME);
    act(() => { fireEvent.focus(screen.getByLabelText("Search waters and towns")); });
    const searching = where();
    await openFromSearch();
    expect([searching, ...backs(2)]).toEqual([
      "search · tab Map", "water Chilliwack River on Regulations · no bar", "handled -> tab Map", "ANDROID -> tab Map"]);
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

  it("(owner 2026-10-22) More opened from Conditions: its ‹ and the system back return to Conditions, then the Map tab",
     async () => {
    await mount({ ...HOME, tab: "more", prev: "conditions" });
    expect(screen.getByLabelText("Back to Conditions")).toBeTruthy();
    expect(backs(3)).toEqual(["tab More", "handled -> tab Conditions", "handled -> tab Map", "ANDROID -> tab Map"]);
    cleanup();
    await mount({ ...HOME, tab: "more", prev: "conditions" });
    await click("Back to Conditions");
    expect(backs(2)).toEqual(["tab Conditions", "handled -> tab Map", "ANDROID -> tab Map"]);
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
  /**
   * THE POPSTATE AN ACTION CAUSES, awaited — not a fixed sleep (T80 7). jsdom delivers a history step's popstate on a
   * later task, as browsers do; the old 30 ms sleep lost that race under load (the browser-back step flaked when the
   * machine was busy). Armed before the action, so an event delivered at once is not missed; the 2 s ceiling only bounds
   * a walk whose action steps no history (the assertion then says what happened).
   */
  const popstateAfter = async (action: () => unknown): Promise<void> => {
    let off = () => {};
    const landed = new Promise<void>((res) => {
      const t = setTimeout(() => { off(); res(); }, 2000);
      const h = () => { clearTimeout(t); off(); res(); };
      off = () => window.removeEventListener("popstate", h);
      window.addEventListener("popstate", h);
    });
    await action();
    await act(async () => { await landed; await new Promise((r) => setTimeout(r, 0)); });
  };
  /** The browser's back button: a real `history.back()`, and its popstate let land. */
  const browserBack = () => popstateAfter(() => { window.history.back(); });

  it("Layers sheet, then a water, then a tab: one entry each, closed innermost first", async () => {
    await mount({ ...HOME, tab: "spots" }, webBack);
    const seen = [where()];
    // leaving a tab's root steps the history back; jsdom delivers that popstate later, as a browser does — let it land
    // before the next press (a person is never faster than a few milliseconds)
    await popstateAfter(() => click("Map")); seen.push(where());
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

  it("(owner 2026-10-22 late) the same walks: moves inside a water hold ONE entry and one browser back leaves it; " +
     "A -> B is two entries, back A, back the map; × drops every water's entries at once", async () => {
    const entry = () => {
      const s: unknown = window.history.state;
      const v = s && typeof s === "object" ? (s as Record<string, unknown>)["canifishthis.back"] : undefined;
      return ` [entry ${typeof v === "number" ? v : 0}]`;
    };
    await mount(HOME, webBack, openRegsFixture());
    const moves = [`${where()}${entry()}`, ...(await movesInsideChilliwack(entry))];
    await browserBack(); moves.push(`${where()}${entry()}`);

    await openTributary(); const chain = [`${where()}${entry()}`];
    await browserBack();
    await until(() => screen.queryByText("Chilliwack River") !== null); chain.push(`${where()}${entry()}`);
    await browserBack(); chain.push(`${where()}${entry()}`);

    await openTributary(); const close = [`${where()}${entry()}`];
    await popstateAfter(() => click("Close, back to Map")); close.push(`${where()}${entry()}`);
    const e = (n: number) => ` [entry ${n}]`;
    expect({ moves, chain, close }).toEqual({
      moves: [...MOVES_SEEN.map((s, i) => `${s}${e(i === 0 ? 0 : 1)}`), `tab Map${e(0)}`],
      chain: [`water Jeperson Side Channel on Regulations · no bar${e(2)}`, `water Chilliwack River on Map · no bar${e(1)}`,
              `tab Map${e(0)}`],
      close: [`water Jeperson Side Channel on Regulations · no bar${e(2)}`, `tab Map${e(0)}`],
    });
  }, 60_000);

  it("(owner 2026-10-22 late) one system: the Chilliwack ↔ Vedder crossings hold ONE entry; one browser back leaves", async () => {
    const entry = () => {
      const s: unknown = window.history.state;
      const v = s && typeof s === "object" ? (s as Record<string, unknown>)["canifishthis.back"] : undefined;
      return ` [entry ${typeof v === "number" ? v : 0}]`;
    };
    await mount(HOME, webBack, openRegsFixture());
    const seen = await acrossChilliwackVedder(entry);
    await browserBack(); seen.push(`${where()}${entry()}`);
    expect(seen).toEqual([...ACROSS_SEEN.map((s) => `${s} [entry 1]`), "tab Map [entry 0]"]);
  }, 60_000);

  it("(owner 2026-10-22 late) a water opened from a search result holds the SAME one entry search held (no orphan " +
     "entry for the closed search): one browser back is the map, search closed; × the same", async () => {
    const entry = () => {
      const s: unknown = window.history.state;
      const v = s && typeof s === "object" ? (s as Record<string, unknown>)["canifishthis.back"] : undefined;
      return ` [entry ${typeof v === "number" ? v : 0}]`;
    };
    const settle = () => act(async () => { await new Promise((r) => setTimeout(r, 30)); });
    const walk = async (leave: () => Promise<unknown>) => {
      await mount(HOME, webBack);
      const out = [`${where()}${entry()}`];
      act(() => { fireEvent.focus(screen.getByLabelText("Search waters and towns")); });
      await settle(); out.push(`${where()}${entry()}`);
      await openFromSearch(); await settle(); out.push(`${where()}${entry()}`);
      await popstateAfter(leave); out.push(`${where()}${entry()}`);
      cleanup();
      return out;
    };
    const viaBack = await walk(async () => { window.history.back(); });
    const viaClose = await walk(() => click("Close, back to Map"));
    const seen = ["tab Map [entry 0]", "search · tab Map [entry 1]",
                  "water Chilliwack River on Regulations · no bar [entry 1]", "tab Map [entry 0]"];
    expect({ viaBack, viaClose }).toEqual({ viaBack: seen, viaClose: seen });
  }, 60_000);
});
