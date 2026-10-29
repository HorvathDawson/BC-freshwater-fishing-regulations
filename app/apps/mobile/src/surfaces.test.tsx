/**
 * ONE SURFACE PER ACTION, AND BACK EVERYWHERE (T60 checks 2 and 6) — through the app's own wiring: the real `Shell` on
 * the fixture, at every place the route can stand (the four tabs, a water's three views, a spot viewed / edited / asked
 * about, the capture's steps, search, the Layers and day sheets, the More tab's pages). The map renderer is replaced by
 * buttons, as in back.test.tsx.
 *
 *   one surface   on one screen an action is offered ONCE: no two controls of one role share an accessible name. "Map"
 *                 was offered twice before (the view strip and the regs header's Map button, owner call 3); the second
 *                 way is always the one a reader misses and a test forgets. A pair that is deliberately two controls
 *                 (e.g. a list whose every row is "Open …") is named in ALLOWED with why.
 *   back          every PUSHED screen — anything that is not a tab's root — draws a way back the reader can see: a ‹,
 *                 a ×, Done, Cancel, Keep. The system back (back.test.tsx) is the same back; this holds that the screen
 *                 also SHOWS one.
 *
 * Mutation-pinned: the duplicate finder reports a planted pair and the back finder refuses a planted screen with no
 * back; and removing FaceBar's ‹ fails every water place here (run recorded in T60's Outcome).
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId } from "@app/data";
import type { Spot } from "@app/data/spots";
import type { ReleaseLoader } from "@app/data/release";
import { openRegsFixture } from "@app/data/release/node";
import { HOME, type Route } from "@app/ui";

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
const { systemBack } = await import("./systemBack.native");

afterEach(() => cleanup());
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
const WATER = (view: "regs" | "conditions" | "map", tab: "map" | "conditions" = "map"): Route =>
  ({ mode: "water", from: { ...HOME, tab }, stack: [{ item: CHILLIWACK, part: null, tapSid: null, at: null, view }] });

async function mount(initialRoute: Route, loader: ReleaseLoader | null = null) {
  const source = makeFixtureSource();
  const water = await source.water(CHILLIWACK);
  TAP.sid = water!.sections[1]!;
  render(
    <GestureHandlerRootView>
      <Shell source={source} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={initialRoute} systemBack={systemBack}
             loader={loader} spots={[SPOT]} onSaveSpot={async () => {}} onDeleteSpot={async () => {}} />
    </GestureHandlerRootView>);
  await act(async () => {});
}
const click = async (label: string) => { await act(async () => { fireEvent.click(screen.getByLabelText(label)); }); };
const until = async (f: () => boolean) => waitFor(() => expect(f()).toBe(true), { timeout: 20_000 });
const named = async (text: string) => until(() => screen.queryAllByText(text).length > 0);

/** The controls on screen, each as `role: name` — what a reader can act on, by what it is called. */
export function controls(root: ParentNode = document): string[] {
  const ROLES = ["button", "tab", "link", "radio", "switch", "checkbox", "menuitem"];
  return [...root.querySelectorAll<HTMLElement>(ROLES.map((r) => `[role="${r}"]`).join(","))]
    .filter((e) => !e.closest('[aria-hidden="true"]'))
    .map((e) => `${e.getAttribute("role")}: ${(e.getAttribute("aria-label") ?? e.textContent ?? "").replace(/\s+/g, " ").trim()}`);
}
/** The controls offered more than once, with how many times. */
export function duplicates(cs: readonly string[]): string[] {
  const n = new Map<string, number>();
  for (const c of cs) n.set(c, (n.get(c) ?? 0) + 1);
  return [...n].filter(([, k]) => k > 1).map(([c, k]) => `${c} ×${k}`).sort();
}
/** The visible ways back on a pushed screen: a ‹ ("Back", "Back to <tab>"), a × ("Close…"), a sheet's Done, a Cancel, a Keep. */
const BACK = /^button: (Back( to .+)?|Close( search|, back to .+)?|Done|Cancel( editing)?|Keep (it|editing)|Discard)$/;
export const waysBack = (cs: readonly string[]): string[] => cs.filter((c) => BACK.test(c));

/**
 * Controls that are offered more than once ON PURPOSE, by place and by name. An entry is a decision said in words, not a
 * way past the scan, and a stale one (no place shows the pair any more) fails.
 */
const ALLOWED: { place: RegExp; control: RegExp; why: string }[] = [
  { place: /^Water: Regulations/, control: /^link: Rule book page \d+, opens the PDF$/,
    why: "every rule carries its own Source link (T43); several rules on one page cite the same printed page — the links are data, one per rule, not a second way to one action" },
];

/** Every place, and how to stand there. `pushed`: not a tab's root, so a way back must show. */
const PLACES: { place: string; route: Route; pushed: boolean; loader?: boolean; go?: () => Promise<void> }[] = [
  { place: "Map tab", route: HOME, pushed: false },
  { place: "Conditions tab", route: { ...HOME, tab: "conditions" }, pushed: false },
  { place: "Spots tab", route: { ...HOME, tab: "spots" }, pushed: false },
  // the More tab is a detour (owner 2026-10-22): its ‹ returns to the tab it came from, so it shows one
  { place: "More tab", route: { ...HOME, tab: "more", prev: "conditions" }, pushed: true },
  { place: "Water: Regulations", route: WATER("regs"), pushed: true, loader: true,
    go: async () => { await until(() => screen.queryByTestId("regs-date") !== null); } },
  { place: "Water: Conditions", route: WATER("conditions", "conditions"), pushed: true, go: () => named("Chilliwack River") },
  { place: "Water: Map", route: WATER("map"), pushed: true, loader: true,
    go: async () => { await until(() => screen.queryByTestId("water-map-where") !== null); } },
  { place: "Water: Regulations, Which day? sheet", route: WATER("regs"), pushed: true, loader: true,
    go: async () => { await until(() => screen.queryByTestId("regs-date") !== null); await act(async () => { fireEvent.click(screen.getByTestId("regs-date")); }); } },
  { place: "Spot", route: { ...HOME, spot: "s1" }, pushed: true },
  { place: "Spot, editing", route: { ...HOME, spot: "s1" }, pushed: true, go: () => click("Edit spot") },
  { place: "Spot, delete asked", route: { ...HOME, spot: "s1" }, pushed: true, go: () => click("Delete spot") },
  { place: "Adding: pick the water", route: { ...HOME, adding: { at: null } }, pushed: true },
  { place: "Adding: the exact spot", route: { ...HOME, adding: { at: null } }, pushed: true,
    go: async () => { await click("tap the map"); await named("Chilliwack River"); await click("Next"); } },
  { place: "Adding: when", route: { ...HOME, adding: { at: null } }, pushed: true,
    go: async () => { await click("tap the map"); await named("Chilliwack River"); await click("Next"); await click("tap a point"); await click("Next"); } },
  { place: "Adding: details", route: { ...HOME, adding: { at: null } }, pushed: true,
    go: async () => {
      await click("tap the map"); await named("Chilliwack River"); await click("Next"); await click("tap a point"); await click("Next"); await click("Next");
      await until(() => screen.queryByPlaceholderText("Name this spot") !== null);
    } },
  { place: "Search open", route: HOME, pushed: true,
    go: async () => { act(() => { fireEvent.focus(screen.getByLabelText("Search waters and towns")); }); await until(() => screen.queryByLabelText("Close search") !== null); } },
  { place: "Layers sheet", route: HOME, pushed: true, go: () => click("Layers") },
  { place: "More › Glossary", route: { ...HOME, tab: "more" }, pushed: true, loader: true, go: () => click("Glossary") },
  { place: "More › Sources and licences", route: { ...HOME, tab: "more" }, pushed: true, loader: true, go: () => click("Sources and licences") },
  { place: "More › About", route: { ...HOME, tab: "more" }, pushed: true, loader: true, go: () => click("About") },
  { place: "More › Report a problem", route: { ...HOME, tab: "more" }, pushed: true, loader: true, go: () => click("Report a problem") },
];

describe("one surface per action, and back everywhere (T60)", () => {
  it("at every place the route can stand: no action is offered twice, and every pushed screen shows a way back", async () => {
    const twice: string[] = [], noBack: string[] = [], stale = new Set(ALLOWED.map((a) => a.why));
    for (const p of PLACES) {
      await mount(p.route, p.loader ? openRegsFixture() : null);
      await p.go?.();
      const cs = controls();
      expect(cs.length, `${p.place}: nothing on screen`).toBeGreaterThan(0);
      for (const d of duplicates(cs)) {
        const allowed = ALLOWED.find((a) => a.place.test(p.place) && a.control.test(d.replace(/ ×\d+$/, "")));
        if (allowed) stale.delete(allowed.why); else twice.push(`${p.place}: ${d}`);
      }
      if (p.pushed && waysBack(cs).length === 0) noBack.push(`${p.place}: no way back among ${cs.join(" · ")}`);
      cleanup();
    }
    expect({ twice, noBack, stale: [...stale] }).toEqual({ twice: [], noBack: [], stale: [] });
  }, 240_000);

  it("MUTATION PIN: a planted second surface is reported; a planted screen with no way back is refused", () => {
    expect(duplicates(["button: Open map", "tab: Map", "button: Open map", "button: Back"])).toEqual(["button: Open map ×2"]);
    expect(duplicates(["button: Open map", "link: Open map"])).toEqual([]);          // another role is another thing
    expect(waysBack(["button: Save", "tab: Conditions", "button: Open map"])).toEqual([]);
    expect(waysBack(["button: Back to Conditions", "button: Close, back to Map", "button: Done"]).length).toBe(3);
  });
});
