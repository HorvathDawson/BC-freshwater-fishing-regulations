/**
 * (T858) A PHONE HAS NO KEYBOARD IN SEARCH'S LIST: the real Shell on the fixture with `search/listKeys` swapped for its
 * native twin (listKeys.native.ts, what Metro bundles for a phone). The arrows and Enter in the field are the field's
 * as before T858 — none is taken, nothing is highlighted, nothing pressed — and the field carries no combobox
 * attributes and the rows no ids. (The web's behaviour: Search.keys.shell.test.tsx.)
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { SEARCH_BAR } from "@app/core";
import { makeFixtureSource } from "@app/data/fixture";
import { HOME, type SystemBack } from "@app/ui";
import { LIGHT } from "./theme";

vi.mock("./search/listKeys", () => import("./search/listKeys.native"));
vi.mock("./MapScreen", () => ({ MapScreen: () => <View /> }));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");

const size = (w: number) => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: w });
  window.dispatchEvent(new Event("resize"));
};
afterEach(() => { cleanup(); size(390); });
const android: SystemBack = () => ({ sync: () => {}, dispose: () => {} });
const field = () => screen.getByLabelText(SEARCH_BAR.label) as HTMLInputElement;
const settle = () => act(async () => { await new Promise((r) => setTimeout(r, 260)); });
const key = (k: string) => { let free = true; act(() => { free = fireEvent.keyDown(field(), { key: k }); }); return free; };
const sheet = () => screen.queryByTestId("search-sheet-full") ? "full" : screen.queryByTestId("search-sheet-half") ? "half" : "no sheet";

describe("(T858) the native twin in the Shell", () => {
  it.each([390, 1440])("at %i px: ↓ ↑ Enter are the field's, nothing is highlighted or pressed, no combobox", async (w) => {
    size(w);
    render(
      <GestureHandlerRootView>
        <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
               tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={HOME} systemBack={android}
               feed={{ index: async () => ({ fetchedAt: new Date().toISOString(), stations: {} }) } as never}
               spots={[]} onSaveSpot={async () => {}} />
      </GestureHandlerRootView>);
    act(() => { field().focus(); });
    fireEvent.change(field(), { target: { value: "chilliwack" } });
    await settle(); await settle();
    const keys = [key("ArrowDown"), key("ArrowDown"), key("ArrowUp"), key("Enter")];
    await settle();
    expect([keys, sheet(), field().getAttribute("role"), field().getAttribute("aria-activedescendant"),
            document.querySelectorAll('[id^="search-stop-"]').length, document.getElementById("search-results")])
      .toEqual([[true, true, true, true], "full", null, null, 0, null]);
  });
});
