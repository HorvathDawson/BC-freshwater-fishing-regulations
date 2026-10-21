/**
 * THE WATER'S VIEWS, through the real Shell (owner 2026-10-21): from a Conditions-tab reach, one labelled action — the
 * header strip's "Regulations" — or a swipe opens that water's regulations; a sideways swipe then moves
 * Regulations <-> Conditions <-> Map; back walks the one back rule home to the reach it started from.
 *
 * The map renderer is stubbed (no WebGL in jsdom); everything else is the app's, on the fixture bundle.
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { fireGestureHandler, getByGestureTestId } from "react-native-gesture-handler/jest-utils";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId } from "@app/data";
import type { Route } from "@app/ui";
import { LIGHT } from "./theme";

vi.mock("@app/map", async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  Map: () => null,
}));
// the panel is not under test: it says which reach and which "you are here" it was handed, and it takes the body swipe
// the Shell hands it the way the real panel does (ConditionsPanel.test pins that the real one does)
vi.mock("./ConditionsPanel", async () => {
  const { useContext } = await import("react");
  const { Text } = await import("react-native");
  const { ViewSwipe, WaterViewsContext } = await import("./ViewSwipe");
  return { ConditionsPanel: ({ section, from }: { section: number; from?: { lat: number } | null }) => {
    const swipe = useContext(WaterViewsContext)?.bodySwipe ?? null;
    const text = <Text>{`panel for ${section} at ${from?.lat ?? "nowhere"}`}</Text>;
    return swipe ? <ViewSwipe swipe={swipe} testId="view-swipe-conditions">{text}</ViewSwipe> : text;
  } };
});
const { Shell } = await import("./Shell");

afterEach(cleanup);
beforeAll(() => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
  window.dispatchEvent(new Event("resize"));
});

const CHILLIWACK = "gnis:8634" as ItemId;
const AT = { lat: 49.1, lon: -121.6 };
const pan = (dx: number) => [
  { absoluteX: 200, x: 200, translationX: 0, translationY: 0 },
  { absoluteX: 200 + dx / 2, x: 200 + dx / 2, translationX: dx / 2, translationY: 0 },
  { absoluteX: 200 + dx, x: 200 + dx, translationX: dx, translationY: 0, velocityX: 0, velocityY: 0 },
];
const swipe = (id: string, dx: number) => act(() => { fireGestureHandler(getByGestureTestId(id), pan(dx)); });

/** What a reader sees of the navigation: the back row's words, the strip (selected view marked) and the bottom bar. */
const chrome = () => {
  const strip = screen.queryByRole("tablist", { name: "Views of this water" });
  const tabs = strip ? within(strip).getAllByRole("tab").map((t) =>
    `${t.getAttribute("aria-label")}${t.getAttribute("aria-selected") === "true" ? "*" : ""}`) : [];
  const back = screen.queryAllByLabelText("Back").map((b) => b.textContent);
  return { back, tabs: tabs.join(" · "), bar: screen.queryByRole("tab", { name: "Spots" }) !== null };
};

async function mount() {
  const source = makeFixtureSource();
  const water = await source.water(CHILLIWACK);
  const sid = water!.sections[1] ?? water!.sections[0]!;
  const start: Route = { mode: "browse", tab: "conditions", cond: { section: sid, at: AT }, spot: null, adding: false };
  render(
    <GestureHandlerRootView>
      <Shell source={source} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={start} />
    </GestureHandlerRootView>);
  await waitFor(() => expect(screen.getByText("Chilliwack River")).toBeTruthy());
  expect(sid).not.toBe(water!.sections[0]);            // a reach that is NOT the water's first
  return sid;
}

const regsShown = () => screen.queryByRole("heading", { name: "Regulations are coming" }) !== null;

describe("water mode's views", () => {
  it("a Conditions reach: the strip offers Regulations beside it; pressing it opens the water's regulations", async () => {
    await mount();
    expect(chrome()).toEqual({ back: ["‹  Conditions"], tabs: "Regulations · Conditions*", bar: true });
    fireEvent.click(screen.getByRole("tab", { name: "Regulations" }));
    await waitFor(() => expect(regsShown()).toBe(true));
    expect(chrome()).toEqual({ back: ["‹  Conditions"], tabs: "Regulations* · Conditions · Map", bar: false });
  });

  it("swipe: reach -> Regulations -> Conditions -> Map -> Conditions; back -> Regulations -> the reach", async () => {
    await mount();
    const seen: string[] = [];
    const note = () => seen.push(`${chrome().tabs} | regs page ${regsShown()} | back ${chrome().back.join()}`);
    swipe("view-swipe-header", 120);                       // finger right on the reach's header: its regulations
    await waitFor(() => expect(regsShown()).toBe(true)); note();
    swipe("view-swipe-body", -120);                        // anywhere on the Regulations view
    await waitFor(() => expect(regsShown()).toBe(false)); note();
    swipe("view-swipe-header", -120);                      // the Conditions view swipes from its header
    note();
    swipe("view-swipe-header", 120);                       // the Map view too: its canvas pans
    note();
    act(() => { fireEvent.click(screen.getByLabelText("Back")); });
    await waitFor(() => expect(regsShown()).toBe(true)); note();
    act(() => { fireEvent.click(screen.getByLabelText("Back")); }); note();
    expect(seen).toEqual([
      "Regulations* · Conditions · Map | regs page true | back ‹  Conditions",
      "Regulations · Conditions* · Map | regs page false | back ‹  Regulations",
      "Regulations · Conditions · Map* | regs page false | back ‹  Regulations",
      "Regulations · Conditions* · Map | regs page false | back ‹  Regulations",
      "Regulations* · Conditions · Map | regs page true | back ‹  Conditions",
      "Regulations · Conditions* | regs page false | back ‹  Conditions",
    ]);
    // home: the same reach, its "you are here" kept (the Conditions tab's bar is back)
    expect(chrome().bar).toBe(true);
  });

  it("the Conditions body swipes too, on the reach and on the water's Conditions view (reviewer 2026-10-21)", async () => {
    await mount();
    const seen: string[] = [];
    const note = () => seen.push(`${chrome().tabs} | regs page ${regsShown()}`);
    swipe("view-swipe-conditions", 120);                   // finger right on the reach's body: its regulations
    await waitFor(() => expect(regsShown()).toBe(true)); note();
    swipe("view-swipe-body", -120);                        // Regulations -> Conditions
    await waitFor(() => expect(regsShown()).toBe(false)); note();
    swipe("view-swipe-conditions", -120);                  // on the Conditions body: -> Map
    note();
    swipe("view-swipe-header", 120);                       // Map -> Conditions (header only there)
    note();
    swipe("view-swipe-conditions", 120);                   // the Conditions body: -> Regulations
    await waitFor(() => expect(regsShown()).toBe(true)); note();
    expect(seen).toEqual([
      "Regulations* · Conditions · Map | regs page true",
      "Regulations · Conditions* · Map | regs page false",
      "Regulations · Conditions · Map* | regs page false",
      "Regulations · Conditions* · Map | regs page false",
      "Regulations* · Conditions · Map | regs page true",
    ]);
  });

  it("the Conditions view of a water opened from a reach is that reach, its \"you are here\" kept", async () => {
    const sid = await mount();
    const panel = () => screen.queryByText(/^panel for /)?.textContent ?? null;
    const seen = [panel()];
    fireEvent.click(screen.getByRole("tab", { name: "Regulations" }));
    await waitFor(() => expect(regsShown()).toBe(true));
    seen.push(panel());
    fireEvent.click(screen.getByRole("tab", { name: "Conditions" }));
    seen.push(panel());
    expect(seen).toEqual([`panel for ${sid} at 49.1`, null, `panel for ${sid} at 49.1`]);
  });
});
