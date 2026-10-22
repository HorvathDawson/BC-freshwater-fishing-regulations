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

const regsShown = () => screen.queryByRole("heading", { name: "Regulations are coming" }) !== null;
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

async function mount(tab: "map" | "conditions", sid?: SectionId) {
  const source = makeFixtureSource();
  const water = await source.water(CHILLIWACK);
  TAP.sid = sid ?? water!.sections[1]!;
  expect(TAP.sid).not.toBe(water!.sections[0]);           // a reach that is NOT the water's first
  const start: Route = { ...HOME, tab };
  render(
    <GestureHandlerRootView>
      <Shell source={source} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={start} />
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
      mapFront: "‹  Map | Regulations* · Conditions · Map | Chilliwack River | regs page | no bar",
      mapConditions: `‹  Map | Regulations · Conditions* · Map | Chilliwack River | panel for ${sid} at 49.1 | no bar`,
      condFront: `‹  Conditions | Regulations · Conditions* · Map | Chilliwack River | panel for ${sid} at 49.1 | no bar`,
      condRegulations: "‹  Conditions | Regulations* · Conditions · Map | Chilliwack River | regs page | no bar",
    });
    // the same screen: strip, title and body equal view for view — only where back goes (each map) differs
    const strip = (s: string) => s.replace(/^‹ {2}\w+ \| /, "");
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
      `‹  Conditions | Regulations · Conditions* · Map | Chilliwack River | panel for ${sid} at 49.1 | no bar`,
      "‹  Conditions | Regulations* · Conditions · Map | Chilliwack River | regs page | no bar",
      `‹  Conditions | Regulations · Conditions* · Map | Chilliwack River | panel for ${sid} at 49.1 | no bar`,
      "‹  Conditions | Regulations · Conditions · Map* | Chilliwack River | the map | no bar",
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

  it("an unnamed reach: the same Conditions-only screen from the Conditions map and the Map tab", async () => {
    await mount("conditions", UNNAMED);
    await tapMap();
    await until(() => panel() !== "no panel");
    const opened = look();
    swipe("view-swipe-conditions", 120);                    // nothing to the left of Conditions for it
    const swiped = look();
    act(() => { fireEvent.click(screen.getByLabelText("Back")); });
    const backed = look();
    cleanup();
    await mount("map", UNNAMED);
    await tapMap();
    await until(() => panel() !== "no panel");
    const mapOpened = look();
    act(() => { fireEvent.click(screen.getByLabelText("Back")); });
    expect([opened, swiped, backed, mapOpened, look()]).toEqual([
      `‹  Conditions | Conditions* | no title | panel for ${UNNAMED} at 49.1 | no bar`,
      `‹  Conditions | Conditions* | no title | panel for ${UNNAMED} at 49.1 | no bar`,
      "no back | no strip | no title | the map | Map Conditions* Spots More",
      // the Map tab opens the very same screen; only where back goes differs
      `‹  Map | Conditions* | no title | panel for ${UNNAMED} at 49.1 | no bar`,
      "no back | no strip | no title | the map | Map* Conditions Spots More",
    ]);
  });
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
    fireEvent.click(screen.getByRole("button", { name: "▲ Upstream" }));
    fireEvent.click(screen.getByRole("button", { name: "▲ Upstream" }));
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

  it("across waters: ▲ on the Vedder opens the Chilliwack's map at its lowest part; ▼ there opens the Vedder; back returns", async () => {
    await onMap("gnis:3062", 0);
    const seen = [where()];
    fireEvent.click(screen.getByRole("button", { name: "▲ Upstream into Chilliwack River" }));
    await until(() => where().startsWith("Part 1 of 4 · Chilliwack River"));
    seen.push(where());
    fireEvent.click(screen.getByRole("button", { name: "▼ Downstream into Vedder River" }));
    await until(() => where().startsWith("Part 1 of 1"));
    seen.push(where());
    act(() => { fireEvent.click(screen.getByLabelText("Back")); });
    await until(() => where().startsWith("Part 1 of 4"));
    seen.push(where());
    act(() => { fireEvent.click(screen.getByLabelText("Back")); });
    await until(() => where().startsWith("Part 1 of 1"));
    seen.push(where());
    expect(seen).toEqual([
      "Part 1 of 1 | Vedder River",
      "Part 1 of 4 · Chilliwack River | Vedder Crossing Bridge → Tamihi Rapids Bridge",
      "Part 1 of 1 | Vedder River",
      // back walks the waters opened on top, newest first
      "Part 1 of 4 · Chilliwack River | Vedder Crossing Bridge → Tamihi Rapids Bridge",
      "Part 1 of 1 | Vedder River"]);
  }, 40_000);
});
