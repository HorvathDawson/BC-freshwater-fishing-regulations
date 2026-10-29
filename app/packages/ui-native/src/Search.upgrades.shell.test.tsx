/**
 * (T247) SEARCH UPGRADES, THROUGH THE REAL SHELL on the fixture: a typo still finds the water, a management unit and a
 * region list theirs, each row says where its water is, and the searches a reader picked a result from come back as recents
 * (device-only, cleared by one tap). The map renderer is a stub, as in Search.shell.test.tsx.
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Text, View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { SEARCH_BAR, SEARCH_FIND } from "@app/core";
import { makeFixtureSource } from "@app/data/fixture";
import { HOME, type SystemBack } from "@app/ui";
import { LIGHT } from "./theme";

vi.mock("./MapScreen", () => ({ MapScreen: () => <View><Text>map</Text></View> }));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");

afterEach(cleanup);
beforeAll(() => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
});
const android: SystemBack = () => ({ sync: () => {}, dispose: () => {} });
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
const rows = () => screen.queryAllByRole("button").map((b) => b.getAttribute("aria-label") ?? "").filter((l) => l.startsWith("Show "));
const heads = () => screen.queryAllByRole("heading").map((h) => h.textContent);

describe("search upgrades (T247)", () => {
  it("a typo still finds the water, and the group says some matched by close spelling", async () => {
    mount();
    await typeIn("chillwak");
    expect(rows().some((r) => r.startsWith("Show Chilliwack River"))).toBe(true);
    expect(heads().some((h) => h?.includes("close spelling"))).toBe(true);
  });

  it("a management unit lists its waters, the heading says which, and every row says where it is", async () => {
    mount();
    await typeIn("2-8");
    expect(heads()).toContain("2 waters in MU 2-8");
    expect(rows().map((r) => r.includes("Region 2, MU 2-8"))).toEqual([true, true]);
    expect(screen.getAllByText(/Stream · Region 2 · MU 2-8 · near Chilliwack/).length).toBe(2);
  });

  it("a region, written as typed, lists its waters", async () => {
    mount();
    await typeIn("region 2");
    expect(heads()).toContain("2 waters in Region 2");
    await typeIn("region 3");
    expect(rows()).toEqual([]);
  });

  it("a water picked from the results is a recent search, kept in the store; it comes back before two letters are typed", async () => {
    const store = mount();
    await typeIn("chilliwack");
    fireEvent.click(screen.getByRole("button", { name: /^Show Chilliwack River/ }));
    expect(JSON.parse(store.v!)).toEqual(["chilliwack"]);
    await typeIn("");
    expect(screen.getByText(SEARCH_FIND.recent)).toBeTruthy();
    fireEvent.click(screen.getByLabelText(SEARCH_FIND.again("chilliwack")));
    await settle();
    expect(field().value).toBe("chilliwack");
  });

  it("recents survive a restart (they are read from the store) and clear with one tap", async () => {
    const store = mount(memory(JSON.stringify(["elk river", "hope"])));
    fireEvent.focus(field());
    await settle();
    expect(screen.getAllByTestId(/^search-recent-/).map((e) => e.textContent)).toEqual(["elk river", "hope"]);
    fireEvent.click(screen.getByLabelText(SEARCH_FIND.clearLabel));
    expect([screen.queryAllByTestId(/^search-recent-/).length, store.v]).toEqual([0, "[]"]);
  });

  it("the start sheet says what else the field understands", async () => {
    mount();
    fireEvent.focus(field());
    await settle();
    expect(screen.getByText(SEARCH_FIND.hint)).toBeTruthy();
  });
});
