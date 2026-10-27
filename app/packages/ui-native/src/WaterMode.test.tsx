/**
 * ONE WATER SCREEN, WHICHEVER MAP YOU TAPPED (owner rule 2026-10-21), through the real Shell: the same tap on the Map
 * tab and on the Conditions map opens the same water screen — the same header, the same views, the same reach and
 * "you are here" — with only the view in front different; a sideways swipe moves Regulations <-> Conditions <-> Map;
 * back leaves the water for exactly the map it was tapped on.
 *
 * The map renderer is replaced by a button that taps a section at a point (no WebGL in jsdom); the Conditions panel
 * by a line saying which reach and which "you are here" it was handed. Everything else is the app's, on the fixture.
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { Pressable, Text } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { fireGestureHandler, getByGestureTestId } from "react-native-gesture-handler/jest-utils";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId, SectionId } from "@app/data";
import { HOME, type Route } from "@app/ui";
import { LIGHT } from "./theme";

/** The section the stub map's button taps, and where on it. */
const TAP: { sid: number; lat: number; lon: number } = { sid: 0, lat: 49.1, lon: -121.6 };

vi.mock("./MapScreen", () => ({
  MapScreen: ({ onPressFeature }: { onPressFeature?: (l: string, id: number, lat?: number, lon?: number) => void }) =>
    <Pressable accessibilityRole="button" accessibilityLabel="tap the map"
               onPress={() => { void onPressFeature?.("streams", TAP.sid, TAP.lat, TAP.lon); }}>
      <Text>map</Text>
    </Pressable>,
}));
// the panel is not under test: it says which reach and which "you are here" it was handed, and it takes the body swipe
// the Shell hands it the way the real panel does (ConditionsPanel.test pins that the real one does)
// T214 item 2: these swipe, and the swipe is for a finger: this is a phone
vi.mock("./touchDevice", () => ({ isTouchDevice: () => true }));
vi.mock("./ConditionsPanel", async (importOriginal) => {
  const real = await importOriginal<typeof import("./ConditionsPanel")>();
  const { useContext } = await import("react");
  const { Text } = await import("react-native");
  const { ViewSwipe, WaterViewsContext } = await import("./ViewSwipe");
  return { ...real, ConditionsPanel: ({ section, from }: { section: number; from?: { lat: number } | null }) => {
    const swipe = useContext(WaterViewsContext)?.bodySwipe ?? null;
    const text = <Text>{`panel for ${section} at ${from?.lat ?? "nowhere"}`}</Text>;
    return swipe ? <ViewSwipe swipe={swipe} testId="view-swipe-conditions">{text}</ViewSwipe> : text;
  } };
});
// the Regulations view is the real one; beside it, the water and part it was handed (what the page opens on)
vi.mock("./WaterScreen", async (importOriginal) => {
  const real = await importOriginal<typeof import("./WaterScreen")>();
  const { Text } = await import("react-native");
  return { ...real, WaterScreen: (p: Parameters<typeof real.WaterScreen>[0]) =>
    <><real.WaterScreen {...p} /><Text testID="regs-on">{`regs on ${p.item} part ${p.part}`}</Text></> };
});
const { Shell } = await import("./Shell");
const { openRegsFixture } = await import("@app/data/release/node");

afterEach(cleanup);
beforeAll(() => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
  window.dispatchEvent(new Event("resize"));
});

const CHILLIWACK = "gnis:8634" as ItemId;
const UNNAMED = 99_999 as SectionId;
const pan = (dx: number) => [
  { absoluteX: 200, x: 200, translationX: 0, translationY: 0 },
  { absoluteX: 200 + dx / 2, x: 200 + dx / 2, translationX: dx / 2, translationY: 0 },
  { absoluteX: 200 + dx, x: 200 + dx, translationX: dx, translationY: 0, velocityX: 0, velocityY: 0 },
];
const swipe = (id: string, dx: number) => act(() => { fireGestureHandler(getByGestureTestId(id), pan(dx)); });

const regsShown = () => screen.queryByText("Regulations unavailable: no data release is open") !== null;
const panel = () => screen.queryByText(/^panel for /)?.textContent ?? "no panel";

/**
 * What a reader sees: the back row's words, the water's view strip (selected marked *), the title, the regs page or
 * the Conditions panel, and the bottom bar's tabs (selected marked *) — or the map.
 */
const look = () => {
  const strip = screen.queryByRole("tablist", { name: "Views of this water" });
  const views = strip ? within(strip).getAllByRole("tab").map((t) =>
    `${t.getAttribute("aria-label")}${t.getAttribute("aria-selected") === "true" ? "*" : ""}`).join(" · ") : "no strip";
  const back = screen.queryAllByLabelText("Back").map((b) => b.textContent).join() || "no back";
  const bar = screen.queryByRole("tab", { name: "Spots" })
    ? ["Map", "Conditions", "Spots", "More"].map((k) =>
      `${k}${screen.getByRole("tab", { name: k }).getAttribute("aria-selected") === "true" ? "*" : ""}`).join(" ")
    : "no bar";
  const title = screen.queryByText("Chilliwack River") ? "Chilliwack River" : "no title";
  const body = regsShown() ? "regs page" : screen.queryByLabelText("tap the map") ? "the map" : panel();
  return `${back} | ${views} | ${title} | ${body} | ${bar}`;
};

async function mount(tab: "map" | "conditions", sid?: SectionId, loader?: ReturnType<typeof openRegsFixture>) {
  const source = makeFixtureSource();
  const water = await source.water(CHILLIWACK);
  TAP.sid = sid ?? water!.sections[1]!;
  expect(TAP.sid).not.toBe(water!.sections[0]);           // a reach that is NOT the water's first
  const start: Route = { ...HOME, tab };
  render(
    <GestureHandlerRootView>
      <Shell source={source} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={start} loader={loader} />
    </GestureHandlerRootView>);
  return TAP.sid;
}
const tapMap = async () => {
  await act(async () => { fireEvent.click(screen.getByLabelText("tap the map")); });
};
const until = async (f: () => boolean) => waitFor(() => expect(f()).toBe(true));
const titled = () => screen.queryByText("Chilliwack River") !== null;

describe("one water screen, whichever map you tapped", () => {
  it("the same tap on the Map tab and on the Conditions map: the same screen, only the front view differs", async () => {
    const seen: Record<string, string> = {};
    const sid = await mount("map");
    await tapMap();
    await until(() => regsShown() && titled());
    seen.mapFront = look();
    fireEvent.click(screen.getByRole("tab", { name: "Conditions" }));
    await until(titled);
    seen.mapConditions = look();
    cleanup();

    await mount("conditions");
    await tapMap();
    await until(() => panel() !== "no panel" && titled());
    seen.condFront = look();
    fireEvent.click(screen.getByRole("tab", { name: "Regulations" }));
    await until(() => regsShown() && titled());
    seen.condRegulations = look();

    expect(seen).toEqual({
      mapFront: "Map | Regulations* · Conditions · Map | Chilliwack River | regs page | no bar",
      mapConditions: `Map | Regulations · Conditions* · Map | Chilliwack River | panel for ${sid} at 49.1 | no bar`,
      condFront: `Conditions | Regulations · Conditions* · Map | Chilliwack River | panel for ${sid} at 49.1 | no bar`,
      condRegulations: "Conditions | Regulations* · Conditions · Map | Chilliwack River | regs page | no bar",
    });
    // the same screen: strip, title and body equal view for view — only where back goes (each map) differs
    const strip = (s: string) => s.replace(/^\w+ \| /, "");
    expect([strip(seen.mapFront!), strip(seen.mapConditions!)])
      .toEqual([strip(seen.condRegulations!), strip(seen.condFront!)]);
  });

  it("from the Conditions map: swipe Conditions -> Regulations -> Conditions -> Map; back is the Conditions map", async () => {
    const sid = await mount("conditions");
    const seen = [look()];
    await tapMap();
    await until(() => panel() !== "no panel" && titled()); seen.push(look());
    swipe("view-swipe-header", 120);                        // finger right: Regulations
    await until(regsShown); seen.push(look());
    swipe("view-swipe-body", -120);                         // anywhere on the Regulations view: Conditions
    await until(() => !regsShown()); seen.push(look());
    swipe("view-swipe-conditions", -120);                   // on the Conditions body: Map
    await until(() => titled() && screen.queryByLabelText("tap the map") !== null); seen.push(look());
    act(() => { fireEvent.click(screen.getByLabelText("Back")); });
    seen.push(look());
    expect(seen).toEqual([
      "no back | no strip | no title | the map | Map Conditions* Spots More",
      `Conditions | Regulations · Conditions* · Map | Chilliwack River | panel for ${sid} at 49.1 | no bar`,
      "Conditions | Regulations* · Conditions · Map | Chilliwack River | regs page | no bar",
      `Conditions | Regulations · Conditions* · Map | Chilliwack River | panel for ${sid} at 49.1 | no bar`,
      "Conditions | Regulations · Conditions · Map* | Chilliwack River | the map | no bar",
      // back from any view leaves the water: the Conditions map, as it was
      "no back | no strip | no title | the map | Map Conditions* Spots More",
    ]);
  });

  it("from the Map tab: back from any view is the Map tab", async () => {
    await mount("map");
    await tapMap();
    await until(() => regsShown() && titled());
    fireEvent.click(screen.getByRole("tab", { name: "Conditions" }));
    act(() => { fireEvent.click(screen.getByLabelText("Back")); });
    expect(look()).toBe("no back | no strip | no title | the map | Map* Conditions Spots More");
  });

  it("(T53) an unnamed reach: Regulations · Conditions (no Map) from either map; the Map tab puts Regulations in front", async () => {
    await mount("conditions", UNNAMED);
    await tapMap();
    await until(() => panel() !== "no panel");
    const opened = look();
    swipe("view-swipe-conditions", 120);                    // its Regulations is to the left of Conditions
    await until(regsShown);
    const swiped = look();
    act(() => { fireEvent.click(screen.getByLabelText("Back")); });
    const backed = look();
    cleanup();
    await mount("map", UNNAMED);
    await tapMap();
    await until(regsShown);
    const mapOpened = look();
    const on = screen.getByTestId("regs-on").textContent;
    act(() => { fireEvent.click(screen.getByLabelText("Back")); });
    expect([opened, swiped, backed, mapOpened, on, look()]).toEqual([
      `Conditions | Regulations · Conditions* | no title | panel for ${UNNAMED} at 49.1 | no bar`,
      "Conditions | Regulations* · Conditions | no title | regs page | no bar",
      // a view move replaced the frame: one back leaves the reach
      "no back | no strip | no title | the map | Map Conditions* Spots More",
      // the Map tab opens the very same screen with Regulations in front; only where back goes differs
      "Map | Regulations* · Conditions | no title | regs page | no bar",
      "regs on null part null",
      "no back | no strip | no title | the map | Map* Conditions Spots More",
    ]);
  });

  it("(T53) on the regs fixture release: an unnamed reach is titled with the store's name on both its views", async () => {
    await mount("conditions", 634 as SectionId, openRegsFixture());
    await tapMap();
    const title = () => screen.queryAllByTestId("water-title").map((t) => t.textContent).join(" | ");
    await until(() => panel() !== "no panel" && title() !== "");
    const cond = [title(), panel()];
    swipe("view-swipe-conditions", 120);
    await waitFor(() => expect(screen.getByTestId("keep-tab")).toBeTruthy(), { timeout: 20_000 });
    // the Regulations view's name row carries its date pill too: the heading is the name
    expect([cond, within(screen.getByTestId("regs-header")).getByRole("heading").textContent,
            screen.getByTestId("regs-on").textContent]).toEqual([
      ["Unnamed stream · Region 1", "panel for 634 at 49.1"], "Unnamed stream · Region 1", "regs on null part null"]);
  }, 40_000);
});

describe("the water's Map view and the rest of the water screen agree (P15 follow-ups, on the regs fixture release)", () => {
  /** The water screen opened straight on a water's Map view, on the release (the map's parts come from it). */
  async function onMap(item: string, part: number) {
    const start: Route = { mode: "water", from: { ...HOME, tab: "map" },
                           stack: [{ item: item as ItemId, part, tapSid: null, at: null, view: "map" }] };
    render(
      <GestureHandlerRootView>
        <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
               tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={start} loader={openRegsFixture()} />
      </GestureHandlerRootView>);
    await waitFor(() => expect(screen.getByTestId("water-map-where")).toBeTruthy(), { timeout: 20_000 });
  }
  const where = () => `${screen.getByTestId("water-map-where").textContent} | ${screen.getByTestId("water-map-name").textContent}`;

  it("ONE WATER SCREEN: ▲ twice on the map, then Regulations — the page is on the STEPPED part, and so is the map again", async () => {
    await onMap(CHILLIWACK, 1);
    fireEvent.click(screen.getByRole("button", { name: "Upstream" }));
    fireEvent.click(screen.getByRole("button", { name: "Upstream" }));
    const stepped = where();
    fireEvent.click(screen.getByRole("tab", { name: "Regulations" }));
    await until(() => screen.queryByTestId("regs-on") !== null);
    const regs = screen.getByTestId("regs-on").textContent;
    fireEvent.click(screen.getByRole("tab", { name: "Map" }));
    await waitFor(() => expect(screen.getByTestId("water-map-where")).toBeTruthy(), { timeout: 20_000 });
    expect([stepped, regs, where()]).toEqual([
      "Part 3 of 4 · Chilliwack River | 100 m below the Slesse Creek confluence → Chilliwack River Ecological Reserve boundary",
      // part_ix 0 is the third part mouth first (rank 2)
      "regs on gnis:8634 part 0",
      "Part 3 of 4 · Chilliwack River | 100 m below the Slesse Creek confluence → Chilliwack River Ecological Reserve boundary"]);
  }, 40_000);

  it("across waters: ▲ on the Vedder opens the Chilliwack's map at its lowest part IN ITS PLACE (owner 2026-10-22 " +
     "late: a river that changes its name is ONE system), so to and fro never stacks and ONE back from the Chilliwack " +
     "is the Map tab", async () => {
    await onMap("gnis:3062", 0);
    const seen = [where()];
    fireEvent.click(screen.getByRole("button", { name: "Upstream into Chilliwack River" }));
    await until(() => where().startsWith("Part 1 of 4 · Chilliwack River"));
    seen.push(`${where()} | ${screen.getByLabelText("Back").textContent}`);
    fireEvent.click(screen.getByRole("button", { name: "Downstream into Vedder River" }));
    await until(() => where().startsWith("Part 1 of 1"));
    seen.push(`${where()} | ${screen.getByLabelText("Back").textContent}`);
    fireEvent.click(screen.getByRole("button", { name: "Upstream into Chilliwack River" }));
    await until(() => where().startsWith("Part 1 of 4"));
    seen.push(`${where()} | ${screen.getByLabelText("Back").textContent}`);
    act(() => { fireEvent.click(screen.getByLabelText("Back")); });
    seen.push(screen.queryByTestId("water-map-where") ? where() : "the Map tab");
    expect(seen).toEqual([
      "Part 1 of 1 | Vedder River",
      // one system: the Chilliwack replaced the Vedder, so its ‹ names the tab the system was opened from
      "Part 1 of 4 · Chilliwack River | Vedder Crossing Bridge → Tamihi Rapids Bridge | Map",
      "Part 1 of 1 | Vedder River | Map",
      "Part 1 of 4 · Chilliwack River | Vedder Crossing Bridge → Tamihi Rapids Bridge | Map",
      "the Map tab"]);
  }, 40_000);
});

describe("ONE MAP ENTRY (T71, owner call 3, 2026-10-22: \"Keep only one Map entry (the view strip; drop the header's Map button)\")", () => {
  /** The water screen opened on a water's Regulations view, through the real Shell, on the regs fixture release. */
  async function onRegs(item: string, part: number) {
    const start: Route = { mode: "water", from: { ...HOME, tab: "map" },
                           stack: [{ item: item as ItemId, part, tapSid: null, at: null, view: "regs" }] };
    render(
      <GestureHandlerRootView>
        <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
               tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={start} loader={openRegsFixture()} />
      </GestureHandlerRootView>);
    await waitFor(() => expect(screen.getByTestId("keep-tab")).toBeTruthy(), { timeout: 20_000 });
  }
  /** Every control on the water screen whose name says Map: [role, name]. A control's name is its aria-label, else its
   *  words. The ‹ (named "Back", its words the tab it returns to) and the × ("Close, back to Map") LEAVE the water for
   *  the Map tab — they are not a way to this water's map, so they are left out by name, exactly. */
  const LEAVING = new Set(["Back", "Close, back to Map"]);
  const mapEntries = () => [...screen.getByTestId("water-frame").querySelectorAll("[role=button],[role=tab],[role=link]")]
    .map((e) => [e.getAttribute("role"), e.getAttribute("aria-label") ?? e.textContent ?? ""])
    .filter(([, name]) => /\bmap\b/i.test(name!) && !LEAVING.has(name!));

  it("on the Regulations view the water's Map is offered once — the view strip's tab — and the header has no action " +
     "(T150: no Parts either); the strip's Map opens the water's Map view on the part being read", async () => {
    await onRegs(CHILLIWACK, 1);
    const leaving = [screen.getByLabelText("Back").textContent, screen.getByTestId("water-close").getAttribute("aria-label")];
    const seen: unknown[] = [leaving, mapEntries(), screen.queryByTestId("regs-action"),
                             within(screen.getByTestId("regs-header")).queryAllByRole("button", { name: /parts/i }).length];
    fireEvent.click(screen.getByRole("tab", { name: "Map" }));
    await waitFor(() => expect(screen.getByTestId("water-map-where")).toBeTruthy(), { timeout: 20_000 });
    seen.push(screen.getByTestId("water-map-where").textContent);
    expect(seen).toEqual([["Map", "Close, back to Map"], [["tab", "Map"]], null, 0, "Part 1 of 4 · Chilliwack River"]);
  }, 40_000);
});

describe("THE PARTS LIST IN THE MAP VIEW (T150, owner 2026-10-23: \"dont we want to remove 'parts' from reg page since map " +
         "is now at the top tabs?\")", () => {
  it("the Map view lists every part by name under the map — closed, then open as a radio group with the part shown " +
     "checked — and a pick moves the frame's part: the map, and the Regulations view's part name", async () => {
    const start: Route = { mode: "water", from: { ...HOME, tab: "map" },
                           stack: [{ item: CHILLIWACK as ItemId, part: 1, tapSid: null, at: null, view: "map" }] };
    render(
      <GestureHandlerRootView>
        <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
               tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={start} loader={openRegsFixture()} />
      </GestureHandlerRootView>);
    await waitFor(() => expect(screen.getByTestId("water-map-parts-toggle")).toBeTruthy(), { timeout: 20_000 });
    const toggle = () => screen.getByTestId("water-map-parts-toggle");
    const rows = () => within(screen.getByTestId("water-map-parts-list")).getAllByRole("radio")
      .map((r) => [r.getAttribute("aria-label"), r.getAttribute("aria-checked")]);
    const closed = [toggle().getAttribute("aria-label"), toggle().getAttribute("aria-expanded"),
                    screen.queryByTestId("water-map-parts-list")];
    fireEvent.click(toggle());
    const open = [toggle().getAttribute("aria-label"), toggle().getAttribute("aria-expanded"),
                  screen.getByTestId("water-map-parts-list").getAttribute("aria-label"), rows()];
    expect([closed, open]).toEqual([
      ["Show all 4 parts", "false", null],
      ["Hide the list of parts", "true", "Parts of this water", [
        ["Vedder Crossing Bridge → Tamihi Rapids Bridge, part 1 of 4 · Chilliwack River", "true"],
        [expect.stringMatching(/, part 2 of 4 · Chilliwack River$/), "false"],
        [expect.stringMatching(/, part 3 of 4 · Chilliwack River$/), "false"],
        [expect.stringMatching(/, part 4 of 4 · Chilliwack River$/), "false"]]]]);
    // the pick: part 2 of the list — the map's bar says so, the list closes, and the Regulations view reads that part
    const name2 = within(screen.getByTestId("water-map-parts-list")).getAllByRole("radio")[1]!.getAttribute("aria-label")!.split(", part ")[0];
    fireEvent.click(screen.getByTestId("water-map-part-1"));
    await waitFor(() => expect(screen.getByTestId("water-map-where").textContent).toBe("Part 2 of 4 · Chilliwack River"));
    const after = [screen.getByTestId("water-map-name").textContent, screen.queryByTestId("water-map-parts-list"),
                   toggle().getAttribute("aria-label")];
    fireEvent.click(screen.getByRole("tab", { name: "Regulations" }));
    await waitFor(() => expect(screen.getByTestId("keep-tab")).toBeTruthy(), { timeout: 20_000 });
    expect([...after, screen.getByTestId("regs-place").getAttribute("aria-label")])
      .toEqual([name2, null, "Show all 4 parts", expect.stringContaining(name2!.split(" → ")[0]!)]);
  }, 60_000);
});
