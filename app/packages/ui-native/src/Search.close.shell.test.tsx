/**
 * (T730, owner 2026-11-14 on a phone) CLOSING SEARCH, THROUGH THE REAL SHELL on the fixture:
 *   a. a swipe down closes it — from the grabber, or from the list at its top; a list scrolled down scrolls instead;
 *   b. the recents are one small block (at most RECENT_MAX tags), not a row each;
 *   c. a water row tapped moves the map; closing search leaves the map there (nothing is sent back);
 *   d. every close resets search: focused again, the field is empty and the recents show.
 * The map renderer is a stub that writes down every framing the Shell sends it, as in Search.shell.test.tsx. The pans
 * are delivered with RNGH's own test utility (`fireGestureHandler`), which skips the recognisers' activation offsets
 * (how it feels under a finger is the emulator's check, the ticket's "left").
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { useRef } from "react";
import { Text, View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { fireGestureHandler, getByGestureTestId } from "react-native-gesture-handler/jest-utils";
import { RECENT_MAX, SEARCH_BAR, SEARCH_FIND } from "@app/core";
import { makeFixtureSource } from "@app/data/fixture";
import { HOME, type SystemBack } from "@app/ui";
import { LIGHT } from "./theme";

type Fit = { key: string; camera?: { lon: number; lat: number; zoom: number } } | null | undefined;
const seen = vi.hoisted(() => ({ fits: [] as string[] }));
vi.mock("./MapScreen", () => ({
  MapScreen: (p: { fit?: Fit }) => {
    const f = p.fit;
    const said = !f ? "none" : f.camera ? `${f.key} camera` : f.key;
    const last = useRef<string | null>(null);
    if (last.current !== said) { last.current = said; seen.fits.push(said); }
    return <View><Text testID="map-state">{`fit ${f?.key ?? "none"}`}</Text></View>;
  },
}));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");

afterEach(() => { cleanup(); seen.fits.length = 0; });
beforeAll(() => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
});

let androidBack: () => boolean = () => false;
const android: SystemBack = (back) => { androidBack = back; return { sync: () => {}, dispose: () => {} }; };
const memory = (v: string | null = null) => { const s = { v, read: () => s.v, write: (t: string) => { s.v = t; } }; return s; };
function mount(store = memory()) {
  render(
    <GestureHandlerRootView>
      <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={HOME} systemBack={android}
             feed={{ index: async () => ({ fetchedAt: new Date().toISOString(), stations: {} }) } as never}
             spots={[]} onSaveSpot={async () => {}} recentSearches={store} />
    </GestureHandlerRootView>);
  return store;
}
const field = () => screen.getByLabelText(SEARCH_BAR.label) as HTMLInputElement;
const settle = () => act(async () => { await new Promise((r) => setTimeout(r, 260)); });
const typeIn = async (q: string) => { fireEvent.change(field(), { target: { value: q } }); await settle(); await settle(); };
const sheet = () => screen.queryByTestId("search-sheet-full") ? "full" : screen.queryByTestId("search-sheet-half") ? "half" : "no sheet";
const where = () => `${screen.queryByLabelText(SEARCH_BAR.close) ? "search open" : "search closed"} · ${sheet()} · "${field().value}"`;
const back = () => act(() => { androidBack(); });
/** A downward pan of `dy` px let go at `vy` px/s (the flick rule: jsdom lays nothing out, so the sheet's height is 0). */
const pull = (id: string, dy: number, vy: number) => act(() => {
  fireGestureHandler(getByGestureTestId(id), [
    { translationY: 0 }, { translationY: dy / 2 }, { translationY: dy, velocityY: vy }]);
});
const open = async () => { act(() => { fireEvent.focus(field()); }); await settle(); };
const recentTags = () => screen.queryAllByTestId(/^search-recent-/).map((e) => e.textContent);

describe("(a) a swipe down closes search on a phone", () => {
  it("from the grabber: a flick closes it; a slow short drag springs back and search stays", async () => {
    mount();
    await open();
    await typeIn("chilliwack");
    pull("search-sheet-drag", 40, 300);
    const slow = where();
    pull("search-sheet-drag", 40, 1200);
    await settle();
    expect([slow, where()]).toEqual([`search open · full · "chilliwack"`, `search closed · no sheet · ""`]);
  });

  it("from the list at its top it closes; with the list scrolled down the same drag scrolls the list and search stays",
     async () => {
    mount();
    await open();
    await typeIn("chilliwack");
    const list = screen.getByTestId("search-list");
    list.scrollTop = 180; fireEvent.scroll(list); await settle();
    pull("search-list-drag", 60, 1200);
    const scrolled = where();
    list.scrollTop = 0; fireEvent.scroll(list); await settle();
    pull("search-list-drag", 60, 1200);
    await settle();
    expect([scrolled, where()]).toEqual([`search open · full · "chilliwack"`, `search closed · no sheet · ""`]);
  });

  it("at half (a water row tapped) the swipe closes it too", async () => {
    mount();
    await open();
    await typeIn("chilliwack river");
    fireEvent.click(screen.getByRole("button", { name: /^Show Chilliwack River/ }));
    await settle();
    const half = where();
    pull("search-sheet-drag", 50, 1500);
    await settle();
    expect([half, where()]).toEqual([`search open · half · "chilliwack river"`, `search closed · no sheet · ""`]);
  });
});

describe("(b) the recents are one small block", () => {
  it(`at most ${RECENT_MAX} kept and shown, as tags in one block under the start line, before the hint; Clear empties it`,
     async () => {
    const store = mount(memory(JSON.stringify(["elk river", "hope", "vedder", "thompson", "fraser", "skagit"])));
    await open();
    const block = screen.getByTestId("search-recents");
    const start = screen.getByText(SEARCH_BAR.start), hint = screen.getByText(SEARCH_FIND.hint);
    const order = [start.compareDocumentPosition(block) & Node.DOCUMENT_POSITION_FOLLOWING,
                   block.compareDocumentPosition(hint) & Node.DOCUMENT_POSITION_FOLLOWING].map(Boolean);
    const shown = [RECENT_MAX, recentTags(), order, block.contains(screen.getByLabelText(SEARCH_FIND.clearLabel))];
    await typeIn("chilliwack");
    fireEvent.click(screen.getByRole("button", { name: /^Show Chilliwack River/ }));
    expect([shown, JSON.parse(store.v!)]).toEqual([
      [4, ["elk river", "hope", "vedder", "thompson"], [true, true], true],
      ["chilliwack", "elk river", "hope", "vedder"]]);
  });
});

describe("(c) closing search leaves the map where search moved it; (d) and starts search afresh", () => {
  /** type, tap the water row (the map frames it), close one way; then open search again */
  const closeAfterPick = async (close: () => void | Promise<void>) => {
    mount();
    await open();
    await typeIn("chilliwack river");
    fireEvent.click(screen.getByRole("button", { name: /^Show Chilliwack River/ }));
    await settle();
    const framed = [where(), screen.getByTestId("map-state").textContent];
    await close();
    await settle();
    const closed = [where(), screen.getByTestId("map-state").textContent, seen.fits.slice()];
    await open();
    return { framed, closed, reopened: [where(), recentTags(), screen.queryAllByRole("button", { name: /^Show / }).length] };
  };
  const STAYED = {
    framed: [`search open · half · "chilliwack river"`, "fit item:gnis:8634"],
    // the framing, then no fit: nothing is sent back, the map stays on the river (T48's "restore:1 camera …" is gone)
    closed: [`search closed · no sheet · ""`, "fit none", ["none", "item:gnis:8634", "none"]],
    // a fresh search: empty field, full sheet, the recents (the search just picked from first), no old results
    reopened: [`search open · full · ""`, ["chilliwack river"], 0],
  };

  it("the system back (the owner's Thompson River case)", async () => {
    expect(await closeAfterPick(back)).toEqual(STAYED);
  });
  it("the field's ‹", async () => {
    expect(await closeAfterPick(() => { act(() => { fireEvent.click(screen.getByLabelText(SEARCH_BAR.close)); }); }))
      .toEqual(STAYED);
  });
  it("a swipe down", async () => {
    expect(await closeAfterPick(() => pull("search-sheet-drag", 50, 1500))).toEqual(STAYED);
  });
  it("a typed search closed without a pick reopens empty too (and sends the camera nowhere)", async () => {
    mount();
    await open();
    await typeIn("chilliwack");
    back();
    await settle();
    const closed = where();
    await open();
    expect([closed, where(), seen.fits]).toEqual([`search closed · no sheet · ""`, `search open · full · ""`, ["none"]]);
  });
});
