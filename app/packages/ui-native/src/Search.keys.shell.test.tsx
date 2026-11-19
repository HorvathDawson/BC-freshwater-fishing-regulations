/**
 * (T858, owner 2026-11-18: "in the webapp we should have keyboard navigation like in search bar using arrows to move
 * highlight") THE KEYBOARD IN SEARCH'S LIST on the web, THROUGH THE REAL SHELL on the fixture, on a desktop-wide screen:
 *   a. ↓ / ↑ in the field move a highlight from the field into the results and back; the focus never leaves the field,
 *      the caret does not move (the key is taken), and the field names the highlight (`aria-activedescendant`);
 *   b. Enter presses the highlighted stop by its own click: a water row frames it (the sheet drops to half), the
 *      selected water's open action (›) is the next stop and Enter on it opens the water; a town shows its waters;
 *   c. typing forgets the highlight; Escape still closes search;
 *   d. a highlight whose row went away does not come back with it; a key with a modifier is the field's;
 *   e. the stops are pure (`keyStops`), each once. A phone has none of it: Search.keys.native.shell.test.tsx.
 * The map renderer is the stub Search.shell.test.tsx uses (no WebGL in jsdom).
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { useRef } from "react";
import { Text, View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { SEARCH_BAR } from "@app/core";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId, NameHit, PlaceHit } from "@app/data";
import { HOME, NO_SEARCH, type SystemBack } from "@app/ui";
import { LIGHT } from "./theme";
import { goStop, keyStops, recentStop, stepStop, stopDomId } from "./search/keys";
import type { Row } from "./search/rows";

const BACK = /^Back( to .+)?$/;
type Fit = { key: string } | null | undefined;
vi.mock("./MapScreen", () => ({
  MapScreen: (p: { fit?: Fit }) => {
    const last = useRef<string | null>(null);
    last.current = p.fit?.key ?? "none";
    return <View><Text testID="map-state">{`fit ${last.current}`}</Text></View>;
  },
}));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");

const size = (w: number) => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: w });
  window.dispatchEvent(new Event("resize"));
};
afterEach(() => { cleanup(); size(390); });
beforeAll(() => {
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 900 });
});

const android: SystemBack = () => ({ sync: () => {}, dispose: () => {} });
function mount() {
  size(1440);
  render(
    <GestureHandlerRootView>
      <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={HOME} systemBack={android}
             feed={{ index: async () => ({ fetchedAt: new Date().toISOString(), stations: {} }) } as never}
             spots={[]} onSaveSpot={async () => {}} />
    </GestureHandlerRootView>);
}
const field = () => screen.getByLabelText(SEARCH_BAR.label) as HTMLInputElement;
const settle = () => act(async () => { await new Promise((r) => setTimeout(r, 260)); });
const typeIn = async (q: string) => { fireEvent.change(field(), { target: { value: q } }); await settle(); await settle(); };
const sheet = () => screen.queryByTestId("search-sheet-full") ? "full" : screen.queryByTestId("search-sheet-half") ? "half" : "no sheet";
/** A key pressed in the field: whether the field let it do its own thing (false: the list took it). */
const key = (k: string, mods: { shiftKey?: boolean; metaKey?: boolean; ctrlKey?: boolean; altKey?: boolean } = {}) => {
  let free = true;
  act(() => { free = fireEvent.keyDown(field(), { key: k, ...mods }); });
  return free;
};
/** The highlight, by the spoken name of what the field names (`aria-activedescendant`); "field" when none. */
const lit = () => {
  const id = field().getAttribute("aria-activedescendant");
  if (!id) return "field";
  const e = document.getElementById(id);
  return e ? (e.getAttribute("aria-label") ?? "").replace(/\. .*/, "").replace(/, Region .*/, "") : `missing ${id}`;
};
/** The ring the highlighted control wears: the keyboard's mark (an outline), drawn without a keyboard focus on it. */
const ringed = () => {
  const id = field().getAttribute("aria-activedescendant");
  const e = id ? document.getElementById(id) : null;
  return !!e && e.style.outlineStyle === "solid" && e !== document.activeElement;
};
const inField = () => document.activeElement === field();
const PLACE = "Show Chilliwack on the map";
const RIVER = "Show Chilliwack River";

describe("(T858) ↓ / ↑ move a highlight through the results, from the field and back", () => {
  it("the focus stays in the field, the key is taken, and the field names the highlighted row", async () => {
    mount();
    act(() => { field().focus(); });
    await typeIn("chilliwack");
    const combo = [field().getAttribute("role"), field().getAttribute("aria-expanded"),
                   !!document.getElementById(field().getAttribute("aria-controls") ?? "")];
    const start = lit();
    const down1 = [key("ArrowDown"), lit(), ringed(), inField()];
    const down2 = [key("ArrowDown"), lit(), ringed(), inField()];
    const up1 = [key("ArrowUp"), lit()];
    const up2 = [key("ArrowUp"), lit()];
    const up3 = [key("ArrowUp"), lit()];
    expect([combo, start, down1, down2, up1, up2, up3]).toEqual([
      ["combobox", "true", true], "field",
      [false, PLACE, true, true],
      [false, RIVER, true, true],
      [false, PLACE], [false, "field"], [false, "field"]]);
  });

  it("↓ at the last stop stays there; Escape with a highlight still closes search; closed, the field says so", async () => {
    mount();
    act(() => { field().focus(); });
    await typeIn("chilliwack river");
    for (let i = 0; i < 40; i++) key("ArrowDown");
    const last = lit();
    key("ArrowDown");
    const still = lit() === last && last !== "field";
    act(() => { fireEvent.keyDown(field(), { key: "Escape" }); });
    await settle();
    expect([still, sheet(), field().getAttribute("aria-expanded"), field().getAttribute("aria-activedescendant")])
      .toEqual([true, "no sheet", "false", null]);
  });

  it("typing forgets the highlight", async () => {
    mount();
    act(() => { field().focus(); });
    await typeIn("chilliwack");
    key("ArrowDown");
    const before = lit();
    await typeIn("chilliwack r");
    expect([before, lit(), inField()]).toEqual([PLACE, "field", true]);
  });
});

describe("(T858) Enter presses the highlighted stop, by the same path as a click", () => {
  it("a water row: framed, the sheet at half, the focus kept in the field; its › is the next stop, and Enter opens " +
     "the water", async () => {
    mount();
    act(() => { field().focus(); });
    await typeIn("chilliwack");
    key("ArrowDown"); key("ArrowDown");
    const onRow = lit();
    const free = key("Enter");
    await settle();
    const picked = [free, sheet(), screen.getByTestId("map-state").textContent, inField(), lit()];
    key("ArrowDown");
    const onGo = lit();
    key("Enter");
    await waitFor(() => expect(screen.queryAllByRole("button", { name: BACK }).length).toBeGreaterThan(0));
    expect([onRow, picked, onGo.startsWith("Open Chilliwack River")]).toEqual([
      RIVER, [false, "half", "fit item:gnis:8634", true, RIVER], true]);
  });

  it("a town: its waters are listed, the highlight starts again from the field, and ↓ reaches ← All results first",
     async () => {
    mount();
    act(() => { field().focus(); });
    await typeIn("chilliwack");
    key("ArrowDown");
    key("Enter");
    await waitFor(() => expect(screen.queryByLabelText(SEARCH_BAR.backToAll)).not.toBeNull());
    await settle();
    const after = [sheet(), lit(), inField()];
    key("ArrowDown");
    const first = lit();
    key("ArrowDown");
    const second = lit();
    key("ArrowUp"); key("Enter");                 // ← All results: back to the results, the text kept
    await settle();
    expect([after, first, second.startsWith("Show "), sheet(), field().value, screen.queryByLabelText(SEARCH_BAR.backToAll)])
      .toEqual([["half", "field", true], SEARCH_BAR.backToAll, true, "full", "chilliwack", null]);
  });

  it("Enter with nothing highlighted is the field's, as before (nothing is pressed)", async () => {
    mount();
    act(() => { field().focus(); });
    await typeIn("chilliwack");
    const free = key("Enter");
    await settle();
    expect([free, sheet(), lit()]).toEqual([true, "full", "field"]);
  });
});

describe("(T858) review fixes: a stale highlight, modifier keys", () => {
  it("a highlight whose row went away is forgotten: a town picked, then ← All results, starts again from the field",
     async () => {
    mount();
    act(() => { field().focus(); });
    await typeIn("chilliwack");
    key("ArrowDown");
    const town = lit();
    key("Enter");
    await waitFor(() => expect(screen.queryByLabelText(SEARCH_BAR.backToAll)).not.toBeNull());
    await settle();
    fireEvent.click(screen.getByLabelText(SEARCH_BAR.backToAll));   // the mouse: the town's row is drawn again
    await settle();
    expect([town, sheet(), lit(), field().getAttribute("aria-activedescendant")]).toEqual([PLACE, "full", "field", null]);
  });

  it("an arrow or Enter with Shift, Cmd, Ctrl or Alt held is the field's (select to the start, jump to the end)",
     async () => {
    mount();
    act(() => { field().focus(); });
    await typeIn("chilliwack");
    const fromField = (["shiftKey", "metaKey", "ctrlKey", "altKey"] as const).map((m) => [key("ArrowDown", { [m]: true }), lit()]);
    key("ArrowDown"); key("ArrowDown");
    const held = [key("ArrowUp", { shiftKey: true }), lit(), key("Enter", { metaKey: true }), sheet()];
    expect([fromField, held]).toEqual([
      [[true, "field"], [true, "field"], [true, "field"], [true, "field"]],
      [true, RIVER, true, "full"]]);
  });
});

describe("(T858) the stops", () => {
  const water = (item: string, name: string): Row =>
    ({ t: "water", key: `w:${item}`, hit: { item: item as ItemId, name } as NameHit, dup: false });
  const rows: Row[] = [
    { t: "head", key: "ph", label: "Places" },
    { t: "place", key: "p:12", place: { place: "12", name: "Chilliwack" } as unknown as PlaceHit },
    { t: "head", key: "wh", label: "Waters" },
    water("gnis:1", "A River"), water("gnis:2", "B Creek"),
    { t: "see", key: "s:9", line: "see", see: [{ name: "C Lake", item: "gnis:3" as ItemId }, { name: "none", item: null },
                                                { name: "C Lake (upper)", item: "gnis:3" as ItemId }] },
    { t: "note", key: "none", text: "…" },
    { t: "recents", key: "recents", list: ["vedder", "cultus lake"] },
  ];
  it("every control the list draws is a stop, in reading order; the selected water's › follows its row", () => {
    const picked = { ...NO_SEARCH.pick, selected: { kind: "water" as const, item: "gnis:2" as ItemId, name: "B Creek" } };
    expect([keyStops(rows, NO_SEARCH.pick), keyStops(rows, picked)]).toEqual([
      ["p:12", "w:gnis:1", "w:gnis:2", "see:s:9:0", "see:s:9:2", "recent:vedder", "recent:cultus lake"],
      ["p:12", "w:gnis:1", "w:gnis:2", goStop("w:gnis:2"), "see:s:9:0", "see:s:9:2", "recent:vedder", "recent:cultus lake"]]);
  });
  it("each stop once, even where the data repeats (two names in a pointer for one water, a repeated recent): ↓ walks " +
     "past every one", () => {
    const repeated: Row[] = [{ t: "recents", key: "recents", list: ["vedder", "vedder", "cultus"] }, ...rows.slice(5, 6)];
    const s = keyStops(repeated, NO_SEARCH.pick);
    const walk: (string | null)[] = [];
    for (let at: string | null = null, i = 0; i < s.length + 1; i++) walk.push(at = stepStop(s, at, "down"));
    const once = ["recent:vedder", "recent:cultus", "see:s:9:0", "see:s:9:2"];
    expect([s, walk]).toEqual([once, [...once, "see:s:9:2"]]);
  });
  it("↓ from the field to the first, down to the last and no further; ↑ back up and out to the field", () => {
    const s = ["a", "b"];
    expect([stepStop(s, null, "down"), stepStop(s, "a", "down"), stepStop(s, "b", "down"), stepStop(s, "b", "up"),
            stepStop(s, "a", "up"), stepStop(s, null, "up"), stepStop([], null, "down"), stepStop(s, "gone", "down")])
      .toEqual(["a", "b", "b", "a", null, null, null, "a"]);
  });
  it("a stop's element id holds no space (an id reference is one word)", () => {
    expect(stopDomId(recentStop("cultus lake"))).toBe("search-stop-recent%3Acultus%20lake");
  });
});
