/**
 * (T716) THE TAP'S CUE THROUGH THE REAL SHELL (owner on 0.7.8: "When you click on a stream it is taking a very long time
 * to open with no feedback"). The map says a tap at the finger's lift (`onPressStart`) and the click after it
 * (`onPressFeature`); the Shell lights the tapped section on the map at the lift — before the click, before any lookup —
 * and opens its water there, the pane built hidden behind the map and held until the click, then shown; the map is back
 * unlit after Back. A lift with no click (a double tap: a zoom) is taken back: the cue out, the pane never shown.
 *
 * MUTATION PIN: a cue lit only at the click (the lift ignored), or one never put out, fails the sequence below.
 *
 * The map renderer is replaced by a line naming what it was asked to light, and two buttons (the lift, the click). The
 * regulations are the fixture release, as in MapDay.shell.test.tsx.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { Pressable, Text, View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import type { StatusIndex } from "@app/core";
import { parseManifest, type ItemId } from "@app/data";
import { makeFixtureSource } from "@app/data/fixture";
import { CUE_WAIT_MS, HOME, type PairingInput, type StatusSource, type SystemBack } from "@app/ui";
import { LIGHT } from "./theme";

const TAP = { sid: 0 };
vi.mock("./MapScreen", () => ({
  MapScreen: (p: { highlight?: readonly (string | number)[]; onPressFeature?: (l: string, id: number) => void;
                   onPressStart?: (l: string, id: number) => void }) =>
    <View>
      <Text testID="map-lit">{`lit [${(p.highlight ?? []).join(",")}]`}</Text>
      <Pressable accessibilityRole="button" accessibilityLabel="lift"
                 onPress={() => { p.onPressStart?.("stream", TAP.sid); }}><Text>lift</Text></Pressable>
      <Pressable accessibilityRole="button" accessibilityLabel="click"
                 onPress={() => { void p.onPressFeature?.("stream", TAP.sid); }}><Text>click</Text></Pressable>
    </View>,
}));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");
const { openRegsFixture } = await import("@app/data/release/node");
const { openRegsStore } = await import("@app/data/regs");

const H = "d768d6e135e62b99";
const INDEX = { handles: H, reachDigest: "", sections: 0, waters: 0, profiles: 0, codeOn: () => "base",
                profileOf: () => null, profileCodeOn: () => "base", waterCodeOn: () => "base" } as unknown as StatusIndex;
const STATUS: StatusSource = { key: "t716-status", load: async () => INDEX };
const PAIRING: PairingInput = {
  kind: "release",
  manifest: parseManifest({
    format: "app-manifest/2", data_release: "r-t716",
    tiles: { compatible: [`${H}.*`], assets: null, mask: null,
             current: { id: `${H}.aaa`, url: `tiles/${H}.aaa/atlas.pmtiles`, bytes: 1, sha256: "x" } },
    basemap: null, files: {}, regulations: { valid_until: null, versions: null },
  }),
  held: { tiles: null, basemap: null, release: "r-1", files: {} },
};
let androidBack: () => boolean = () => false;
const android: SystemBack = (back) => { androidBack = back; return { sync: () => {}, dispose: () => {} }; };

afterEach(() => { vi.useRealTimers(); cleanup(); });

const lit = () => screen.queryByTestId("map-lit")?.textContent ?? "no map";
/** The water's page: none, built hidden behind the map (the held pane), or shown. */
const pane = () => {
  const h = screen.queryByTestId("regs-header");
  return !h ? "no pane" : h.closest('[aria-hidden="true"]') ? "pane hidden" : "pane shown";
};
/** FAKE time, advanced only here (setTimeout/clearTimeout faked once the Shell is up): the cue's CUE_WAIT_MS timer fires
 *  only when the test moves the clock past it, never because a slow runner took long (CI, 2026-11-14: a real 400 ms wait
 *  outlasted the 700 ms cue). A real event-loop turn (setImmediate, not faked) after each step lets the fixture store's
 *  I/O and the pane's build go on. */
const wait = (ms: number) => act(async () => {
  await vi.advanceTimersByTimeAsync(ms);
  await new Promise((r) => setImmediate(r));
});
/** Fake time moved 25 ms at a step until `ok`, or throws naming `what` (waitFor would wait on the faked clock). */
async function until(ok: () => boolean, what: string, steps = 400) {
  for (let i = 0; i < steps; i++) { if (ok()) return; await wait(25); }
  throw new Error(`never: ${what}`);
}
const press = (name: string) => act(() => { fireEvent.click(screen.getByRole("button", { name })); });

describe("(T716) the tap's cue", () => {
  it("lit at the lift, held while the water opens, unlit when the map is back; a lift with no click goes out", async () => {
    // the Chilliwack's first section in the regs fixture, which the source maps to the Chilliwack (MapDay.shell.test.tsx)
    const fx = makeFixtureSource();
    const st = await openRegsStore(openRegsFixture());
    TAP.sid = (await st.navRows((await st.itemOrd("gnis:8634"))!)).sections[0]!.sid;
    const source = { ...fx, itemForSection: async (s: number) => (s === TAP.sid ? "gnis:8634" as ItemId : fx.itemForSection(s as never)) };
    render(
      <GestureHandlerRootView>
        <Shell source={source} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
               tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={HOME} systemBack={android}
               status={STATUS} pairing={PAIRING} loader={openRegsFixture()} />
      </GestureHandlerRootView>);
    await waitFor(() => expect(screen.getByTestId("map-lit")).toBeTruthy());
    const seen: string[] = [lit()];
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });

    // a double tap's first lift: lit at once, the water built behind the map; no click: taken back, nothing shown
    press("lift");
    seen.push(lit());
    // watched through the wait: built hidden (on a loaded machine maybe not yet), never shown
    const states = new Set<string>();
    for (let t = 0; t < CUE_WAIT_MS + 100; t += 25) { states.add(pane()); await wait(25); }
    seen.push(states.has("pane shown") ? "SHOWN during the wait" : "never shown");
    seen.push(`${lit()} ${pane()}`);

    // a tap: lit at the lift, the pane built hidden meanwhile and held until the click; shown at the click
    press("lift");
    expect(400).toBeLessThan(CUE_WAIT_MS);
    await wait(400);   // the click comes ~300 ms after the finger went down: before the cue's own timeout
    seen.push(`${lit()} ${pane() === "pane shown" ? "SHOWN before the click" : "not shown before the click"}`);
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "click" })); });
    await until(() => pane() === "pane shown", "the pane shown after the click");
    seen.push(pane());
    // back: the map again, nothing lit
    act(() => { androidBack(); });
    await until(() => screen.queryByTestId("map-lit") !== null, "the map back after Back");
    await wait(50);
    seen.push(`${lit()} ${pane()}`);

    const sid = String(TAP.sid);
    expect(seen).toEqual(["lit []", `lit [${sid}]`, "never shown", "lit [] no pane",
                          `lit [${sid}] not shown before the click`, "pane shown", "lit [] no pane"]);
  }, 40_000);
});
