/**
 * (T48) SEARCH IN THE TOP BAR, THROUGH THE REAL SHELL: the always-open field over the browse maps, the sync icon at
 * its right end (owner 2026-10-22: no status text over the field), the results sheet rising over the SAME map (never a second one), a place shown by its tap (no eye), a water
 * opened through the one opener, and back / Escape / ‹ closing search first and giving the camera back exactly.
 *
 * The map renderer is replaced by a stub that counts its mounts and writes down every framing the Shell sends it (no
 * WebGL in jsdom). Everything else is the app's, on the fixture.
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { useEffect } from "react";
import { Text, View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { SEARCH_BAR } from "@app/core";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId, WaterSource } from "@app/data";
import { HOME, type Route, type SystemBack } from "@app/ui";
import { LIGHT } from "./theme";

type Fit = { key: string; bbox: unknown; camera?: { lon: number; lat: number; zoom: number } } | null | undefined;
type Stub = { fit?: Fit; highlight?: readonly unknown[]; pins?: readonly { tone?: string }[]; insetTop?: number;
               onMapPoint?: (lat: number, lon: number) => void;
               onPressFeature?: (layer: string, id: string, lat?: number, lon?: number) => void };
const seen = vi.hoisted(() => ({ mounts: 0, fits: [] as string[], frames: [] as unknown[] }));
vi.mock("./MapScreen", () => ({
  MapScreen: (p: Stub) => {
    useEffect(() => { seen.mounts += 1; }, []);
    const f = p.fit;
    const said = !f ? "none" : f.camera ? `${f.key} camera ${f.camera.lat},${f.camera.lon},${f.camera.zoom}` : f.key;
    if (seen.fits[seen.fits.length - 1] !== said) { seen.fits.push(said); seen.frames.push(f ?? null); }
    return (
      <View>
        <Text testID="map-state">
          {`fit ${f?.key ?? "none"} · lit ${p.highlight?.length ?? 0} · pins ${(p.pins ?? []).map((x) => x.tone).join(",") || "none"}` +
           ` · inset ${p.insetTop}`}
        </Text>
        {/* a click on the map's ground, and one on a water: what the real map calls for each (both fire on a feature) */}
        <Text testID="stub-map-click" onPress={() => p.onMapPoint?.(49.2, -121.9)}>ground</Text>
        <Text testID="stub-feature-click"
              onPress={() => { p.onMapPoint?.(49.15, -121.85); p.onPressFeature?.("streams", "1", 49.15, -121.85); }}>
          water
        </Text>
      </View>
    );
  },
}));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");

afterEach(() => { cleanup(); seen.mounts = 0; seen.fits.length = 0; seen.frames.length = 0; });
beforeAll(() => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
});

let androidBack: () => boolean = () => false;
const android: SystemBack = (back) => { androidBack = back; return { sync: () => {}, dispose: () => {} }; };
const CHILLIWACK = "gnis:8634" as ItemId;
const FETCHED = new Date(Date.now() - 3 * 3.6e6).toISOString();
const feedAt = (iso: () => string, refreshed?: () => void) =>
  ({ index: async () => ({ fetchedAt: iso(), stations: {} }), refresh: refreshed }) as never;
const FEED = feedAt(() => FETCHED);

function mount(start: Route = HOME, feed: unknown = FEED, source: WaterSource = makeFixtureSource()) {
  render(
    <GestureHandlerRootView>
      <Shell source={source} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={start} systemBack={android} feed={feed as never}
             spots={[]} onSaveSpot={async () => {}} />
    </GestureHandlerRootView>);
}
const syncSaid = () => screen.getByTestId("sync-button").getAttribute("aria-label");
const field = () => screen.getByLabelText(SEARCH_BAR.label) as HTMLInputElement;
const settle = () => act(async () => { await new Promise((r) => setTimeout(r, 260)); });
const typeIn = async (q: string) => { fireEvent.change(field(), { target: { value: q } }); await settle(); await settle(); };
const tabOn = () => screen.queryAllByRole("tab").find((t) => t.getAttribute("aria-selected") === "true")
  ?.getAttribute("aria-label") ?? "no bar";
const sheet = () => {
  return screen.queryByTestId("search-sheet-full") ? "full" : screen.queryByTestId("search-sheet-half") ? "half" : "no sheet";
};
/** The search's state in one line: open or not, the sheet's height, the field's value and the tab. */
const where = () => `${screen.queryByLabelText(SEARCH_BAR.close) ? "search open" : "search closed"} · ${sheet()}` +
  ` · "${field().value}" · tab ${tabOn()}`;
const back = () => { let h = false; act(() => { h = androidBack(); }); return h; };

describe("the top bar (T48): the search field alone, always open, the sync icon at its end", () => {
  it("the field is drawn on both maps, called the same aloud, with ONE placeholder from the list that never cycles", async () => {
    mount();
    await settle();
    const first = field().placeholder;
    await typeIn("ch");
    await typeIn("");
    const onMap = [SEARCH_BAR.placeholders.includes(first as never), field().placeholder === first];
    fireEvent.click(screen.getByRole("tab", { name: "Conditions" }));
    expect([onMap, field().placeholder === first, field().getAttribute("aria-label")])
      .toEqual([[true, true], true, "Search waters and towns"]);
  });

  it("(owner 2026-10-22) NO status text over the field; a feed the server has not published lately is said, not dotted",
     async () => {
    mount();
    await waitFor(() => expect(syncSaid()).not.toBe("Refresh data: checking now"));
    expect([screen.queryByTestId("sync-status"), syncSaid(), !!screen.queryByTestId("sync-dot"),
            screen.queryByText(/Flows checked/)])
      .toEqual([null, "Refresh data: nothing newer to fetch; river flows last published 3h ago", false, null]);
  });

  it("the sync icon's tap re-checks what a tap can fix: an unreached feed dots the icon, the tap clears it; it opens nothing",
     async () => {
    let up = false;
    const forgot: string[] = [];
    const feed = { index: async () => { if (!up) throw new Error("offline"); return { fetchedAt: new Date().toISOString(), stations: {} }; },
                   refresh: () => forgot.push("cache") } as never;
    mount(HOME, feed);
    await waitFor(() => expect(syncSaid()).toBe("Refresh data: river flows could not be reached"));
    const dot = screen.getByTestId("sync-dot").firstElementChild as HTMLElement;
    const dotColour = getComputedStyle(dot).backgroundColor;
    up = true;
    act(() => { fireEvent.click(screen.getByTestId("sync-button")); });
    const during = syncSaid();
    await waitFor(() => expect(syncSaid()).toBe("Refresh data: up to date"), { timeout: 2000 });
    expect([during, forgot, !!screen.queryByTestId("sync-dot"), sheet(), tabOn()])
      .toEqual(["Refresh data: checking now", ["cache"], false, "no sheet", "Map"]);
    // the dot is the attention token, never the closed-water colour beside it on the map
    const rgb = (hex: string) => `rgb(${[1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16)).join(", ")})`;
    expect([dotColour === rgb(LIGHT.attention), dotColour === rgb(LIGHT.closed)]).toEqual([true, false]);
  });

  it("the sync icon gives way to the clear ✕ while search is open", async () => {
    mount();
    await waitFor(() => expect(screen.queryByTestId("sync-button")).not.toBeNull());
    act(() => { fireEvent.focus(field()); });
    expect(screen.queryByTestId("sync-button")).toBeNull();
  });

  it("no top bar on the Spots and More tabs (no map there to search over), and none in water mode", () => {
    const bars: string[] = [];
    for (const tab of ["spots", "more"] as const) {
      mount({ ...HOME, tab });
      bars.push(`${tab}: ${screen.queryByLabelText(SEARCH_BAR.label) ? "bar" : "no bar"}`);
      cleanup();
    }
    mount({ mode: "water", from: HOME, stack: [{ item: "gnis:8634" as never, part: null, tapSid: null, at: null, view: "regs" }] });
    bars.push(`water: ${screen.queryByLabelText(SEARCH_BAR.label) ? "bar" : "no bar"}`);
    expect(bars).toEqual(["spots: no bar", "more: no bar", "water: no bar"]);
  });

  it("on a wide screen the bar and its sheet are a 440 px left panel; on a phone they span the screen", async () => {
    const size = (w: number) => {
      Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: w });
      window.dispatchEvent(new Event("resize"));
    };
    try {
      size(1280);
      mount();
      act(() => { fireEvent.focus(field()); });
      const wide = [screen.getByTestId("top-bar").style.width, screen.getByTestId("search-sheet-full").style.width];
      cleanup();
      size(390);
      mount();
      act(() => { fireEvent.focus(field()); });
      expect([wide, screen.getByTestId("top-bar").style.width, screen.getByTestId("top-bar").style.right])
        .toEqual([["440px", "440px"], "", "14px"]);
    } finally { size(390); }
  });

  it("the map's own chrome starts under the bar: the Shell hands it the bar's bottom edge (no status line: 20 px less)",
     async () => {
    mount();
    await waitFor(() => expect(screen.queryByTestId("sync-button")).not.toBeNull());
    expect(screen.getByTestId("map-state").textContent).toMatch(/ · inset 60$/);
  });
});

describe("opening search: in place, over the SAME map", () => {
  it("the field's frame is the same before and after focus (no jump); the ‹ takes the magnifier's slot; the sheet rises " +
     "FULL over the map, which is not remounted", async () => {
    mount();
    const frame = () => [screen.getByTestId("top-bar").getAttribute("style"),
                         screen.getByTestId("search-field").getAttribute("style")!.replace(/border-[a-z-]*color:[^;]*;?/g, "")];
    const before = [frame(), where(), screen.queryByLabelText(SEARCH_BAR.close)];
    act(() => { fireEvent.focus(field()); });
    expect([before, frame(), where(), seen.mounts]).toEqual([
      [before[0], `search closed · no sheet · "" · tab Map`, null],
      before[0], `search open · full · "" · tab Map`, 1]);
    expect(screen.getByText(SEARCH_BAR.start)).toBeTruthy();
  });

  it("back closes search before anything else, on either map; then the tab rule (Conditions -> Map -> Android leaves)",
     async () => {
    mount({ ...HOME, tab: "conditions" });
    act(() => { fireEvent.focus(field()); });
    await typeIn("chilliwack");
    const open = where();
    const h1 = back();
    const closed = where();
    const h2 = back();
    const map = where();
    expect([open, h1, closed, h2, map, back(), seen.mounts]).toEqual([
      `search open · full · "chilliwack" · tab Conditions`, true,
      `search closed · no sheet · "chilliwack" · tab Conditions`, true,
      `search closed · no sheet · "chilliwack" · tab Map`, false, 1]);
  });

  it("‹ and Escape close it too; a cancel that moved nothing sends the camera nowhere", async () => {
    mount();
    act(() => { fireEvent.focus(field()); });
    act(() => { fireEvent.click(screen.getByLabelText(SEARCH_BAR.close)); });
    const viaClose = where();
    act(() => { fireEvent.focus(field()); });
    act(() => { fireEvent.keyDown(field(), { key: "Escape" }); });
    expect([viaClose, where(), seen.fits]).toEqual([
      `search closed · no sheet · "" · tab Map`, `search closed · no sheet · "" · tab Map`, ["none"]]);
  });
});

describe("a place is SHOWN by its tap; a water is framed by its tap and opened by its action", () => {
  it("tapping a town: the map flies to it, its pin in the town's colour, the waters near it lit, the sheet at HALF — no " +
     "eye pressed; ← All results steps back inside search", async () => {
    mount();
    act(() => { fireEvent.focus(field()); });
    await typeIn("chilliwack");
    fireEvent.click(screen.getByRole("button", { name: /^Show Chilliwack on the map/ }));
    await waitFor(() => expect(screen.queryByLabelText("Back to all results")).not.toBeNull());
    await settle();
    const state = screen.getByTestId("map-state").textContent!;
    expect([where(), state.replace(/lit \d+/, "lit N"), Number(/lit (\d+)/.exec(state)![1]) > 0,
            screen.queryAllByRole("button", { name: /^View / }).length]).toEqual([
      `search open · half · "chilliwack" · tab Map`,
      `fit place:${/fit place:(\S+)/.exec(state)![1]} · lit N · pins ${LIGHT.place} · inset 60`, true, 0]);
    expect(back()).toBe(true);
    expect([where(), screen.queryByLabelText("Back to all results")]).toEqual([`search open · full · "chilliwack" · tab Map`, null]);
  });

  it("a water row: its tap frames it (half); closing search then gives the camera back exactly", async () => {
    mount();
    act(() => { fireEvent.focus(field()); });
    await typeIn("chilliwack river");
    fireEvent.click(screen.getByRole("button", { name: /^Show Chilliwack River/ }));
    await settle();
    const framed = [where(), screen.getByTestId("map-state").textContent!.split(" · ")[0]];
    back();
    await settle();
    expect([framed, where(), seen.fits]).toEqual([
      [`search open · half · "chilliwack river" · tab Map`, "fit item:gnis:8634"],
      `search closed · no sheet · "chilliwack river" · tab Map`,
      // the framing; then, as search closes, one render with no fit (nothing sent), the camera put back, and no fit again
      ["none", "item:gnis:8634", "none", "restore:1 camera 49.15,-121.85,10", "none"],
    ]);
  });

  /**
   * (owner 2026-10-22 late, decisions.md "back from a searched water lands on the map, at the water") A water opened
   * from a result CONSUMES the search: its ‹ and × name the map, and either one lands on the map — search closed, the
   * text gone — with the camera framed on that water: the SAME box search framed it with (its own outline), sent once
   * as the map comes back, never the camera from before search. One back, then nothing more to go back to.
   */
  const searchedWater = async (leave: () => Promise<void>) => {
    mount();
    act(() => { fireEvent.focus(field()); });
    await typeIn("chilliwack river");
    fireEvent.click(screen.getByRole("button", { name: /^Show Chilliwack River/ }));
    await settle();
    fireEvent.click(screen.getByRole("button", { name: /^Open Chilliwack River/ }));
    await waitFor(() => expect(screen.queryByRole("tablist", { name: "Views of this water" })).not.toBeNull());
    const header = [screen.getByRole("button", { name: "Back" }).textContent, screen.getByTestId("water-close")
      .getAttribute("aria-label")];
    await leave();
    await settle();
    return { header, landed: where(), fits: seen.fits.slice(), arrived: seen.frames[seen.frames.length - 1],
             searchedWith: seen.frames[1], more: back(), mounts: seen.mounts };
  };
  const LANDED = async () => {
    const box = await makeFixtureSource().waterBox(CHILLIWACK);
    return {
      header: ["Map", "Close, back to Map"],
      landed: `search closed · no sheet · "" · tab Map`,
      // framed by search, the water screen (no map), then the map back, framed on the water once
      fits: ["none", "item:gnis:8634", "arrive:gnis:8634:1"],
      arrived: { key: "arrive:gnis:8634:1", bbox: box, refine: false },
      searchedWith: { key: "item:gnis:8634", bbox: box, refine: false },
      more: false, mounts: 2,
    };
  };

  it("(owner 2026-10-22 late) back from a water opened from a result lands on the MAP at the water, search closed",
     async () => {
    expect(await searchedWater(async () => { back(); })).toEqual(await LANDED());
  });

  it("(owner 2026-10-22 late) its × does the same", async () => {
    expect(await searchedWater(async () => {
      await act(async () => { fireEvent.click(screen.getByLabelText("Close, back to Map")); });
    })).toEqual(await LANDED());
  });

  it("(owner 2026-10-22 late) on a wide screen too (the panel's row opens it the same way)", async () => {
    Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 1280 });
    window.dispatchEvent(new Event("resize"));
    try {
      expect(await searchedWater(async () => { back(); })).toEqual(await LANDED());
    } finally {
      Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
      window.dispatchEvent(new Event("resize"));
    }
  });

  it("the framing is sent ONCE: a water then tapped on that map and left again brings the map back where it was, " +
     "not dragged to the searched water", async () => {
    await searchedWater(async () => { back(); });
    const before = seen.fits.slice();
    fireEvent.click(screen.getByTestId("stub-feature-click"));
    await waitFor(() => expect(screen.queryByRole("tablist", { name: "Views of this water" })).not.toBeNull());
    back();
    await settle();
    expect([before, seen.fits.slice(before.length), where()]).toEqual([
      ["none", "item:gnis:8634", "arrive:gnis:8634:1"], ["none"], `search closed · no sheet · "" · tab Map`]);
  });

  it("(owner 2026-10-22 late) searched over the CONDITIONS map: the water opens on its Conditions view; moving between " +
     "its views does not forget the framing; back lands on the Conditions map (the map searched over), framed on it",
     async () => {
    mount({ ...HOME, tab: "conditions" });
    act(() => { fireEvent.focus(field()); });
    await typeIn("chilliwack river");
    fireEvent.click(screen.getByRole("button", { name: /^Show Chilliwack River/ }));
    await settle();
    fireEvent.click(screen.getByRole("button", { name: /^Open Chilliwack River/ }));
    await waitFor(() => expect(screen.queryByRole("tablist", { name: "Views of this water" })).not.toBeNull());
    const viewOn = () => screen.getAllByRole("tab").find((t) => t.getAttribute("aria-selected") === "true")
      ?.getAttribute("aria-label") ?? screen.getAllByRole("tab").find((t) => t.getAttribute("aria-selected") === "true")
      ?.textContent;
    const opened = [viewOn(), screen.getByRole("button", { name: "Back" }).textContent];
    fireEvent.click(screen.getAllByRole("tab").find((t) => /Regulations/.test(t.textContent ?? ""))!);
    await settle();
    const moved = viewOn();
    back();
    await settle();
    const box = await makeFixtureSource().waterBox(CHILLIWACK);
    expect([opened, moved, where(), seen.fits, seen.frames[seen.frames.length - 1]]).toEqual([
      ["Conditions", "Conditions"], "Regulations", `search closed · no sheet · "" · tab Conditions`,
      ["none", "item:gnis:8634", "arrive:gnis:8634:1"], { key: "arrive:gnis:8634:1", bbox: box, refine: false }]);
  });

  it("a water opened by a TAP on the map beside the half sheet (a phone) keeps search under it: its ‹ and × say " +
     "Search, and back returns to the results as they were", async () => {
    mount();
    act(() => { fireEvent.focus(field()); });
    await typeIn("chilliwack river");
    fireEvent.click(screen.getByRole("button", { name: /^Show Chilliwack River/ }));
    await settle();
    fireEvent.click(screen.getByTestId("stub-feature-click"));
    await waitFor(() => expect(screen.queryByRole("tablist", { name: "Views of this water" })).not.toBeNull());
    const header = [screen.getByRole("button", { name: "Back" }).textContent,
                    screen.getByTestId("water-close").getAttribute("aria-label")];
    back();
    await settle();
    expect([header, where()]).toEqual([["Search", "Close, back to Search"],
                                       `search open · half · "chilliwack river" · tab Map`]);
  });

  it("(owner 2026-10-22) a GROUP NAME THAT ONLY POINTS: its line in core's words over the waters, and a way to open each " +
     "water it names — straight to that water", async () => {
    // the fixture's valley prints no such row: one is handed in, naming the fixture's river as one of its waters
    const fixture = makeFixtureSource();
    const asked: string[] = [];
    const source: WaterSource = { ...fixture, async searchPointers(q) {
      asked.push(q);
      return /^tee/i.test(q) ? [{ entry: "r8:tee_pee_lakes@8-6", name: "Tee Pee Lakes", see: [
        { name: "Friday Lake", item: "gnis:8634" as ItemId }, { name: "Saturday Lake", item: null },
        { name: "Sunday Lake", item: null }] }] : [];
    } };
    mount(HOME, FEED, source);
    act(() => { fireEvent.focus(field()); });
    await typeIn("tee pee");
    const block = screen.getByTestId("search-see-s:r8:tee_pee_lakes@8-6");
    const shown = [screen.getByRole("heading", { name: SEARCH_BAR.seeHead }) !== null,
                   within(block).getByText(/^Tee Pee Lakes: see/).textContent,
                   within(block).queryAllByRole("button").map((b) => b.getAttribute("aria-label"))];
    fireEvent.click(within(block).getByRole("button", { name: "Open Friday Lake" }));
    await waitFor(() => expect(screen.queryByRole("tablist", { name: "Views of this water" })).not.toBeNull());
    expect([asked, shown]).toEqual([["tee pee"], [
      true, "Tee Pee Lakes: see Friday Lake, Saturday Lake, Sunday Lake",
      // a named row that matched no water is said, never a dead button
      ["Open Friday Lake"]]]);
  });

  it("typing again after a pick raises the sheet to FULL (the pick is forgotten)", async () => {
    mount();
    act(() => { fireEvent.focus(field()); });
    await typeIn("chilliwack river");
    fireEvent.click(screen.getByRole("button", { name: /^Show Chilliwack River/ }));
    const half = where();
    await typeIn("vedder");
    expect([half, where()]).toEqual([`search open · half · "chilliwack river" · tab Map`,
                                     `search open · full · "vedder" · tab Map`]);
  });
});

/**
 * (owner 2026-10-22 late, decisions.md "closing search on desktop") ON A WIDE SCREEN search closes where the eye is:
 * Escape (typed or not, the field focused or not), a click on the map beside the panel, and the panel's own close at
 * its top-right. The field's ✕ only clears what was typed. On a phone nothing changes.
 */
describe("closing search on a wide screen (owner 2026-10-22 late)", () => {
  const size = (w: number) => {
    Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: w });
    window.dispatchEvent(new Event("resize"));
  };
  const wide = async (body: () => Promise<void>) => { size(1280); try { await body(); } finally { size(390); } };
  const open = () => act(() => { fireEvent.focus(field()); });
  const escape = () => act(() => { window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true })); });
  const sheetEl = () => screen.queryByTestId("search-sheet-full") ?? screen.queryByTestId("search-sheet-half");
  /** Every control the bar and the panel offer, by its spoken name: the whole list, in order. */
  const controls = () => {
    const names = (el: HTMLElement | null) =>
      el ? within(el).queryAllByRole("button").map((b) => b.getAttribute("aria-label")) : [];
    return { bar: names(screen.getByTestId("top-bar")), panel: names(sheetEl()) };
  };
  const water = () => !!screen.queryByRole("tablist", { name: "Views of this water" });

  it("Escape closes it with nothing typed, with text typed, and after a pick took the focus out of the field", () =>
    wide(async () => {
      mount();
      open();
      escape();
      const empty = where();
      open();
      await typeIn("chilliwack");
      escape();
      const typed = where();
      open();
      await typeIn("chilliwack river");
      fireEvent.click(screen.getByRole("button", { name: /^Show Chilliwack River/ }));
      await settle();
      const picked = [where(), document.activeElement === field()];
      escape();
      await settle();
      expect([empty, typed, picked, where()]).toEqual([
        `search closed · no sheet · "" · tab Map`,
        `search closed · no sheet · "chilliwack" · tab Map`,
        [`search open · half · "chilliwack river" · tab Map`, false],
        `search closed · no sheet · "chilliwack river" · tab Map`]);
    }));

  it("Escape typed IN THE FIELD closes it: react-native-web's input stops its keys at the field, so the window never " +
     "hears them and the field must close search itself", () =>
    wide(async () => {
      mount();
      open();
      act(() => { fireEvent.keyDown(field(), { key: "Escape" }); });
      const empty = where();
      open();
      await typeIn("chilliwack");
      act(() => { fireEvent.keyDown(field(), { key: "Escape" }); });
      expect([empty, where()]).toEqual([
        `search closed · no sheet · "" · tab Map`, `search closed · no sheet · "chilliwack" · tab Map`]);
    }));

  it("a click on the map beside the panel closes it — on the ground or on a water, which it does not open", () =>
    wide(async () => {
      mount();
      open();
      await typeIn("chilliwack");
      act(() => { fireEvent.click(screen.getByTestId("stub-map-click")); });
      const ground = where();
      open();
      act(() => { fireEvent.click(screen.getByTestId("stub-feature-click")); });
      await settle();
      expect([ground, where(), water()]).toEqual([
        `search closed · no sheet · "chilliwack" · tab Map`, `search closed · no sheet · "chilliwack" · tab Map`, false]);
    }));

  it("the panel carries the one close, at its top-right; the bar keeps only the field's clear", () =>
    wide(async () => {
      mount();
      open();
      await typeIn("zzqx");
      const offered = controls();
      act(() => { fireEvent.click(within(sheetEl()!).getByRole("button", { name: SEARCH_BAR.close })); });
      await settle();
      expect([offered, where()]).toEqual([
        { bar: [SEARCH_BAR.clear], panel: [SEARCH_BAR.close] },
        `search closed · no sheet · "zzqx" · tab Map`]);
    }));

  it("THE FIELD'S ✕ ONLY CLEARS: the text goes, search stays open (wide and phone alike)", async () => {
    const clearOn = async (w: number) => {
      size(w);
      mount();
      open();
      await typeIn("chilliwack");
      act(() => { fireEvent.click(screen.getByRole("button", { name: SEARCH_BAR.clear })); });
      await settle();
      const said = where();
      cleanup();
      return said;
    };
    try {
      expect([await clearOn(1280), await clearOn(390)]).toEqual([
        `search open · full · "" · tab Map`, `search open · full · "" · tab Map`]);
    } finally { size(390); }
  });

  it("back (the system's, the browser's) closes it on a wide screen too", () =>
    wide(async () => {
      mount();
      open();
      await typeIn("chilliwack");
      const h = back();
      expect([h, where()]).toEqual([true, `search closed · no sheet · "chilliwack" · tab Map`]);
    }));

  it("ON A PHONE NOTHING CHANGES: the ‹ in the field closes, the ✕ clears, the sheet has no close of its own; a " +
     "window Escape away from the field leaves it open", async () => {
    mount();
    open();
    await typeIn("zzqx");
    const offered = controls();
    await typeIn("chilliwack river");
    fireEvent.click(screen.getByRole("button", { name: /^Show Chilliwack River/ }));
    await settle();
    escape();                                   // a window Escape is a wide screen's; the phone's is the field's
    expect([offered, where()]).toEqual([
      { bar: [SEARCH_BAR.close, SEARCH_BAR.clear], panel: [] },
      `search open · half · "chilliwack river" · tab Map`]);
  });
});

/*
 * (T69) SEARCH ON A WIDE SCREEN BY KEYBOARD ALONE: open, type, pick, close, Escape — focus asserted at every step.
 * Closing never drops the focus on the page body: it comes back to the field, which stays FOCUSED BUT CLOSED (its
 * accent edge says where the focus is); typing, Enter or ↓ there, or focusing it afresh, opens search again. And the
 * Tab order runs the way the eye does: the bar (field, its clear) before the results panel (its close, then the rows).
 */
describe("(T69) search on a wide screen by keyboard alone", () => {
  const size = (w: number) => {
    Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: w });
    window.dispatchEvent(new Event("resize"));
  };
  const wide = async (body: () => Promise<void>) => { size(1280); try { await body(); } finally { size(390); } };
  const escape = () => act(() => { window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true })); });
  /** Where the keyboard's focus is, by its spoken name ("page body" when it was dropped). */
  const focused = () => {
    const a = document.activeElement;
    return !a || a === document.body ? "page body" : a.getAttribute("aria-label") ?? a.tagName;
  };
  /** What Tab reaches in the bar and the panel, in the order it reaches it (document order: no positive tabindex). */
  const tabOrder = () => {
    const sheet = screen.queryByTestId("search-sheet-full") ?? screen.queryByTestId("search-sheet-half");
    return [...document.querySelectorAll<HTMLElement>('input, [tabindex]:not([tabindex="-1"])')]
      .filter((e) => screen.getByTestId("top-bar").contains(e) || !!sheet?.contains(e))
      .map((e) => e.getAttribute("aria-label"));
  };
  /** The field's edge, by the palette's name for it (the browser hands back rgb; a probe turns the hex the same way). */
  const edge = () => {
    const probe = (hex: string) => { const d = document.createElement("div"); d.style.color = hex; return d.style.color; };
    const at = screen.getByTestId("search-field").style.borderTopColor;
    return at === probe(LIGHT.accent) ? "accent" : at === probe(LIGHT.ink) ? "ink" : at;
  };
  /** The river's row, by its spoken name (the WHERE line in it: `spokenName`). */
  const NAMED = "Chilliwack River, near Chilliwack, 2 km";
  const ROW = `Show ${NAMED} on the map`;

  it("open, type, pick, close, Escape: the focus is where the reader is at each step, and never on the page body", () =>
    wide(async () => {
      mount();
      // Tab into the field: search opens, the focus stays in the field
      act(() => { field().focus(); });
      const opened = [where(), focused(), tabOrder()];
      await typeIn("chilliwack river");
      const order = tabOrder();
      const typed = [where(), focused(), order.slice(0, 3), order.slice(3).every((n) => /^Show /.test(n ?? ""))];
      // Tab down to the row and press Enter: the row is picked and keeps the focus; its open action is next in line
      const row = screen.getByRole("button", { name: (n) => n.startsWith(ROW) });
      act(() => { row.focus(); });
      fireEvent.click(row);
      await settle();
      const at = tabOrder().indexOf(document.activeElement?.getAttribute("aria-label") ?? "");
      const picked = [where(), focused().replace(/\. .*/, ""), tabOrder()[at + 1]];
      // Escape (the focus on the row): search closes, the focus comes back to the field — focused but closed
      escape();
      await settle();
      const closed = [where(), focused(), edge()];
      // Escape again, in that field and on the page: nothing to close; it stays closed and keeps the focus
      act(() => { fireEvent.keyDown(field(), { key: "Escape" }); });
      escape();
      await settle();
      const again = [where(), focused()];
      expect([opened, typed, picked, closed, again]).toEqual([
        [`search open · full · "" · tab Map`, SEARCH_BAR.label, [SEARCH_BAR.label, SEARCH_BAR.close]],
        [`search open · full · "chilliwack river" · tab Map`, SEARCH_BAR.label,
         [SEARCH_BAR.label, SEARCH_BAR.clear, SEARCH_BAR.close], true],
        [`search open · half · "chilliwack river" · tab Map`, ROW, SEARCH_BAR.open(NAMED)],
        [`search closed · no sheet · "chilliwack river" · tab Map`, SEARCH_BAR.label, "accent"],
        [`search closed · no sheet · "chilliwack river" · tab Map`, SEARCH_BAR.label]]);
    }));

  it("the focused-but-closed field opens again by typing, by Enter or ↓, and by focusing it afresh; the panel's ✕ " +
     "hands the focus back to it too", () =>
    wide(async () => {
      mount();
      act(() => { field().focus(); });
      escape();
      await settle();
      const resting = [where(), focused()];
      await typeIn("chilliwack");
      const byTyping = [where(), focused()];
      // the panel's ✕, reached by Tab and pressed: the ✕ goes with the panel, the focus comes back to the field
      const close = within(screen.getByTestId("search-sheet-full")).getByRole("button", { name: SEARCH_BAR.close });
      act(() => { close.focus(); });
      fireEvent.click(close);
      await settle();
      const byClose = [where(), focused()];
      act(() => { fireEvent.keyDown(field(), { key: "Enter" }); });
      await settle();                           // a blur on submit would land after the key: wait it out
      const byEnter = [where(), focused()];
      escape();
      await settle();
      act(() => { fireEvent.keyDown(field(), { key: "ArrowDown" }); });
      const byDown = where();
      escape();
      await settle();
      act(() => { field().blur(); });
      const away = [where(), focused()];
      act(() => { field().focus(); });
      expect([resting, byTyping, byClose, byEnter, byDown, away, [where(), focused()]]).toEqual([
        [`search closed · no sheet · "" · tab Map`, SEARCH_BAR.label],
        [`search open · full · "chilliwack" · tab Map`, SEARCH_BAR.label],
        [`search closed · no sheet · "chilliwack" · tab Map`, SEARCH_BAR.label],
        [`search open · full · "chilliwack" · tab Map`, SEARCH_BAR.label],
        `search open · full · "chilliwack" · tab Map`,
        [`search closed · no sheet · "chilliwack" · tab Map`, "page body"],
        [`search open · full · "chilliwack" · tab Map`, SEARCH_BAR.label]]);
    }));

  it("a mouse click on the focused-but-closed field opens search again (the click gives no fresh focus)", () =>
    wide(async () => {
      mount();
      act(() => { field().focus(); });
      escape();
      await settle();
      const resting = [where(), focused(), edge()];
      fireEvent.click(field());
      await settle();
      expect([resting, [where(), focused(), edge()]]).toEqual([
        [`search closed · no sheet · "" · tab Map`, SEARCH_BAR.label, "accent"],
        [`search open · full · "" · tab Map`, SEARCH_BAR.label, "accent"]]);
    }));

  it("a click on the map beside the panel closes search and leaves the focus where the click put it (no pull back " +
     "to the field); a closed field without the focus wears the ink edge", () =>
    wide(async () => {
      mount();
      const resting = edge();
      act(() => { field().focus(); });
      await typeIn("chilliwack");
      act(() => { field().blur(); });
      fireEvent.click(screen.getByTestId("stub-map-click"));
      await settle();
      expect([resting, where(), focused(), edge()]).toEqual([
        "ink", `search closed · no sheet · "chilliwack" · tab Map`, "page body", "ink"]);
    }));

  it("ON A PHONE closing still drops the focus and the keyboard (no focused-but-closed field holding a keyboard up)",
     async () => {
      mount();
      act(() => { field().focus(); });
      act(() => { fireEvent.click(screen.getByLabelText(SEARCH_BAR.close)); });
      await settle();
      expect([where(), focused()]).toEqual([`search closed · no sheet · "" · tab Map`, "page body"]);
    });
});
