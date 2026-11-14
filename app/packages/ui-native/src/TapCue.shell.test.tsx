/**
 * (T716) THE TAP'S CUE THROUGH THE REAL SHELL (owner on 0.7.8: "When you click on a stream it is taking a very long time
 * to open with no feedback"). The map says a tap at the finger's lift (`onPressStart`) and the click after it
 * (`onPressFeature`); the Shell lights the tapped section on the map at the lift — before the click, before any lookup —
 * keeps it lit while the water opens over the map, and the map is back unlit after Back. A lift with no click (a double
 * tap: a zoom) goes out by itself.
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

afterEach(cleanup);

const lit = () => screen.queryByTestId("map-lit")?.textContent ?? "no map";
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

    // a double tap's first lift: lit at once, out by itself when no click follows
    press("lift");
    seen.push(lit());
    await act(async () => { await new Promise((r) => setTimeout(r, CUE_WAIT_MS + 50)); });
    seen.push(lit());

    // a tap: lit at the lift, still lit at the click while the water is looked up and built (the map is held over it)
    press("lift");
    seen.push(lit());
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "click" })); });
    seen.push(lit());
    // the water opens (the pane shown whole over the map): its header is up
    await waitFor(() => expect(screen.getByTestId("regs-header")).toBeTruthy(), { timeout: 20_000 });
    // back: the map again, nothing lit
    act(() => { androidBack(); });
    await waitFor(() => expect(screen.getByTestId("map-lit")).toBeTruthy());
    await act(async () => { await new Promise((r) => setTimeout(r, 50)); });
    seen.push(lit());

    const sid = String(TAP.sid);
    expect(seen).toEqual(["lit []", `lit [${sid}]`, "lit []", `lit [${sid}]`, `lit [${sid}]`, "lit []"]);
  }, 40_000);
});
