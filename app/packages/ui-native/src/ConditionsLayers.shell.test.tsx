/**
 * LEVEL AS ITS OWN VIEW IN THE LAYERS MENU, THROUGH THE REAL SHELL (owner 2026-10-22: "we care about flow. so we dont
 * want to mix flow and level for now. we could have a way to separately view level. (a toggle in layer maybe?)").
 *
 *   - the Conditions map's rail has Layers too, and its sheet opens on Show: Flow (default) · Level · Temperature, then
 *     the ground and that map's own switches (gauges, flow arrows); nothing of the Map tab's (Colour the water by,
 *     private land);
 *   - ONE choice: Level recolours through the colouring engine's standing channel and the gauge roster — the map's look
 *     (modes, groups, hidden and shown layers, ground) is handed unchanged, so the map makes no paint call (the map side
 *     of that is `@app/map` one-engine.render.test "A HORIZON CHANGE");
 *   - the legend strip carries no second Flow / Level switch; its ramp names the quantity shown;
 *   - the Conditions panel's own chart quantity is the same choice, both ways;
 *   - the rail's badge counts the Conditions map's changes there, and the Map tab's on the Map tab.
 *
 * The map renderer is replaced by a line saying what the Shell handed it (no WebGL in jsdom); the engine's standing
 * channel and the gauge roster are the app's own hooks, observed (the quantity each was asked for). The panel is a
 * line saying which quantity its chart was handed, with a button that picks another the way its own control does.
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { Pressable, Text, View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId } from "@app/data";
import { HOME, type BrowseTab } from "@app/ui";
import { LIGHT } from "./theme";

/** What the engine's standing channel and the gauge roster were last asked for. */
const asked: { standing: string | null; gauges: string | null } = { standing: null, gauges: null };
vi.mock("@app/ui", async (importOriginal) => {
  const real = await importOriginal<typeof import("@app/ui")>();
  return {
    ...real,
    useConditionsColour: (i: Parameters<typeof real.useConditionsColour>[0]) => {
      asked.standing = i.onConditions ? i.quantity : "off";
      return real.useConditionsColour(i);
    },
    useGaugeGeoJSON: (...a: Parameters<typeof real.useGaugeGeoJSON>) => {
      asked.gauges = a[2] ? a[3] ?? "flow" : "off";
      return real.useGaugeGeoJSON(...a);
    },
  };
});

/** Each `modes` object the map has been handed, numbered: the same number is the same object (no look pass). */
const modesSeen = new Map<object, number>();
const TAP = { sid: 0 };
type Stub = { onLayers?: () => void; layersChanged?: number; modes?: object; groups?: Record<string, boolean>;
              hide?: readonly string[]; show?: readonly string[]; basemap?: string;
              onPressFeature?: (l: string, id: number, lat?: number, lon?: number) => void };
vi.mock("./MapScreen", () => ({
  MapScreen: (p: Stub) => {
    if (p.modes && !modesSeen.has(p.modes)) modesSeen.set(p.modes, modesSeen.size + 1);
    return (
      <View>
        {p.onLayers && <Pressable accessibilityRole="button" accessibilityLabel="Choose layers"
                                  onPress={p.onLayers}><Text>layers</Text></Pressable>}
        <Pressable accessibilityRole="button" accessibilityLabel="tap the map"
                   onPress={() => { void p.onPressFeature?.("streams", TAP.sid, 49.1, -121.6); }}><Text>tap</Text></Pressable>
        <Text testID="map-look">{`modes #${p.modes ? modesSeen.get(p.modes) : "none"}`
          + ` · groups ${JSON.stringify(p.groups ?? null)} · hide ${(p.hide ?? []).join(",") || "-"}`
          + ` · show ${(p.show ?? []).join(",") || "-"} · ground ${p.basemap} · changed ${p.layersChanged ?? 0}`}</Text>
      </View>);
  },
}));
// the panel is not under test: which quantity its chart was handed, and its own quantity control, as a button
vi.mock("./ConditionsPanel", async (importOriginal) => {
  const real = await importOriginal<typeof import("./ConditionsPanel")>();
  const { Pressable, Text, View } = await import("react-native");
  return { ...real, ConditionsPanel: ({ parameter, onParameter }: { parameter?: string;
                                                                    onParameter?: (p: "discharge" | "level") => void }) =>
    <View>
      <Text testID="chart">{`chart ${parameter ?? "its own"}`}</Text>
      <Pressable accessibilityRole="button" accessibilityLabel="chart Flow"
                 onPress={() => onParameter?.("discharge")}><Text>Flow</Text></Pressable>
    </View> };
});
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");

afterEach(() => { cleanup(); modesSeen.clear(); asked.standing = null; asked.gauges = null; });
beforeAll(() => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
});

async function mount(tab: BrowseTab) {
  const source = makeFixtureSource();
  TAP.sid = (await source.water("gnis:8634" as ItemId))!.sections[1]!;
  render(
    <GestureHandlerRootView>
      <Shell source={source} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={{ ...HOME, tab }} />
    </GestureHandlerRootView>);
}
const press = (name: string) => act(() => { fireEvent.click(screen.getByLabelText(name)); });
const pick = (name: string) => act(() => { fireEvent.click(screen.getByRole("radio", { name })); });
const flip = (name: string) => act(() => { fireEvent.click(screen.getByRole("switch", { name })); });
/** The sheet's headings, its radios (chosen *) and switches (on +), whole. */
const sheet = () => [
  screen.getAllByRole("heading").map((h) => h.textContent).filter((h) => h === h?.toUpperCase()).join(" · "),
  screen.getAllByRole("radio").map((r) => `${r.getAttribute("aria-label")}${r.getAttribute("aria-checked") === "true" ? "*" : ""}`).join(" "),
  screen.queryAllByRole("switch")
    .map((r) => `${r.getAttribute("aria-label")}${r.getAttribute("aria-checked") === "true" ? "+" : ""}`).join(" "),
].join(" | ");
const map = () => screen.getByTestId("map-look").textContent;
// the ramp names the quantity in its title (refinement pass: "Flow vs. past years, this date")
const ramp = () => (["Flow vs. past years, this date", "Water level vs. past years, this date"].find((t) => screen.queryByText(t)) ?? "no ramp");

const CONDITIONS_GROUPS = '{"depth":false,"ownership":false}';

describe("Level as its own view, in the Conditions map's Layers", () => {
  it("the Conditions rail has Layers; its sheet is Show · Map · On the map, Flow chosen; no Showing switch in the legend", async () => {
    await mount("conditions");
    const legendSwitch = screen.queryByRole("radiogroup", { name: "Showing" }) !== null;
    press("Choose layers");
    expect([legendSwitch, sheet(), ramp(), asked, map()]).toEqual([
      false,
      "SHOW · MAP · ON THE MAP | Flow* Level Temperature Map* Satellite | Gauges+ Flow arrows+",
      "Flow vs. past years, this date",
      { standing: "discharge", gauges: "flow" },
      `modes #1 · groups ${CONDITIONS_GROUPS} · hide mu,mu__edge,mu__label,region,region__edge,region__label · show stream__arrows · ground map · changed 0`,
    ]);
  });

  it("LEVEL: the standing channel and the gauges ask level; the look is handed unchanged (no paint); the badge counts it", async () => {
    await mount("conditions");
    press("Choose layers");
    const before = map();
    pick("Level");
    expect([sheet(), ramp(), asked, map(), before]).toEqual([
      "SHOW · MAP · ON THE MAP | Flow Level* Temperature Map* Satellite | Gauges+ Flow arrows+",
      "Water level vs. past years, this date",
      { standing: "level", gauges: "level" },
      // the SAME modes object (#1), the same groups, hidden and shown layers and ground: nothing for the map to repaint
      `modes #1 · groups ${CONDITIONS_GROUPS} · hide mu,mu__edge,mu__label,region,region__edge,region__label · show stream__arrows · ground map · changed 1`,
      `modes #1 · groups ${CONDITIONS_GROUPS} · hide mu,mu__edge,mu__label,region,region__edge,region__label · show stream__arrows · ground map · changed 0`,
    ]);
    // and back to Flow: the flow roster again, still no new look
    pick("Flow");
    expect([asked, map()]).toEqual([{ standing: "discharge", gauges: "flow" },
      `modes #1 · groups ${CONDITIONS_GROUPS} · hide mu,mu__edge,mu__label,region,region__edge,region__label · show stream__arrows · ground map · changed 0`]);
  });

  it("the Conditions switches: gauges off stops the roster, arrows off leaves the view's own layer undrawn; each counts", async () => {
    await mount("conditions");
    press("Choose layers");
    flip("Gauges");
    flip("Flow arrows");
    expect([sheet(), asked.gauges, map()]).toEqual([
      "SHOW · MAP · ON THE MAP | Flow* Level Temperature Map* Satellite | Gauges Flow arrows",
      "off",
      `modes #1 · groups ${CONDITIONS_GROUPS} · hide mu,mu__edge,mu__label,region,region__edge,region__label · show - · ground map · changed 2`,
    ]);
  });

  it("THE MAP TAB is unchanged: no Show; a Conditions choice is not counted there, and its private land never "
     + "reaches the Conditions map", async () => {
    await mount("map");
    press("Choose layers");
    const mapSheet = sheet();
    flip("Private land");
    const mapLook = map();
    act(() => { fireEvent.click(screen.getByRole("tab", { name: "Conditions" })); });
    press("Choose layers");
    pick("Level");
    const condLook = map();
    act(() => { fireEvent.click(screen.getByRole("tab", { name: "Map" })); });
    expect([mapSheet, mapLook, condLook, map()]).toEqual([
      "MAP · COLOUR THE WATER BY · ON THE MAP | Map* Satellite Corridors Regulations* Stocked Depth"
        + " | Depth contours Private land Pools and runs Relief",
      'modes #1 · groups {"depth":false,"ownership":true} · hide - · show - · ground map · changed 1',
      // (the fixture is no proven pair, so both tabs' modes are plain — the same object; the private land is the point)
      `modes #1 · groups ${CONDITIONS_GROUPS} · hide mu,mu__edge,mu__label,region,region__edge,region__label · show stream__arrows · ground map · changed 1`,
      'modes #1 · groups {"depth":false,"ownership":true} · hide - · show - · ground map · changed 1',
    ]);
  });

  it("THE PANEL'S CHART IS THE SAME CHOICE: Level on the map opens a Level chart; Flow in the chart is Flow on the map", async () => {
    await mount("conditions");
    press("Choose layers");
    pick("Level");
    press("Done");
    await act(async () => { fireEvent.click(screen.getByLabelText("tap the map")); });
    await waitFor(() => expect(screen.queryByTestId("chart")).not.toBeNull());
    const chart = screen.getByTestId("chart").textContent;
    press("chart Flow");
    const after = screen.getByTestId("chart").textContent;
    act(() => { fireEvent.click(screen.getAllByLabelText("Back")[0]!); });
    await waitFor(() => expect(screen.queryByTestId("map-look")).not.toBeNull());
    press("Choose layers");
    expect([chart, after, asked.standing, sheet()]).toEqual([
      "chart level", "chart discharge", "discharge",
      "SHOW · MAP · ON THE MAP | Flow* Level Temperature Map* Satellite | Gauges+ Flow arrows+",
    ]);
  });
});
