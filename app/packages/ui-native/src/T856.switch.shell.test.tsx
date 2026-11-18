/**
 * (T856) THE MAP'S TROUT / SALMON SWITCH: on a desktop-width window it is in the left nav rail directly under the Map
 * button and not on the map; on a phone or narrow window it is on the map's rail as before. Flipping it still changes the
 * key. Through the real Shell on the fixture; the map renderer is a stub that draws the rail's extra (`railExtra`), so
 * "on the map" is observable.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { Text, View } from "react-native";
import type { ReactNode } from "react";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import type { SalmonFacts, StatusIndex } from "@app/core";
import { parseManifest } from "@app/data";
import { HOME, LayoutWidthContext, type PairingInput, type StatusSource } from "@app/ui";
import { LIGHT } from "./theme";

vi.mock("./MapScreen", async () => {
  // (T566) a map says the look it was handed is drawn: the key under it follows that word
  const { useEffect } = await import("react");
  const Drawn = ({ k, on }: { k?: string; on?: (k: string) => void }) => { useEffect(() => { if (k !== undefined) on?.(k); }, [k, on]); return null; };
  return {
    MapScreen: (p: { railExtra?: ReactNode; lookKey?: string; onLookDrawn?: (k: string) => void }) =>
      <View testID="map-stub"><Drawn k={p.lookKey} on={p.onLookDrawn} /><Text>map</Text>{p.railExtra}</View>,
  };
});
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");
const { openRegsFixtureWithLines } = await import("@app/data/release/node");
afterEach(cleanup);

const H = "d768d6e135e62b99";
const INDEX = { handles: H, reachDigest: "", sections: 0, waters: 0, profiles: 0, codeOn: () => "base",
                profileOf: () => null, profileCodeOn: () => "base", waterCodeOn: () => "base" } as unknown as StatusIndex;
const FACTS = { handles: H, chains: 1, chainOf: () => 0, payload: { chains: [{ m: [0, 3] }] } } as unknown as SalmonFacts;
const SOURCE = () => ({ ...makeFixtureSource(), salmonFacts: async () => FACTS });
const STATUS: StatusSource = { key: "t856-status", load: async () => INDEX };
const PAIRING: PairingInput = {
  kind: "release",
  manifest: parseManifest({
    format: "app-manifest/2", data_release: "r-t856",
    tiles: { compatible: [`${H}.*`], assets: null, mask: null,
             current: { id: `${H}.aaa`, url: `tiles/${H}.aaa/atlas.pmtiles`, bytes: 1, sha256: "x" } },
    basemap: null, files: {}, regulations: { valid_until: null, versions: null },
  }),
  held: { tiles: null, basemap: null, release: "r-1", files: {} },
};

function mount(width: number) {
  render(
    <LayoutWidthContext.Provider value={width}><GestureHandlerRootView>
      <Shell source={SOURCE()} loader={openRegsFixtureWithLines()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={HOME} systemBack={() => ({ sync: () => {}, dispose: () => {} })}
             feed={{ index: async () => ({ fetchedAt: new Date().toISOString(), stations: {} }) } as never}
             status={STATUS} pairing={PAIRING} spots={[]} onSaveSpot={async () => {}} />
    </GestureHandlerRootView></LayoutWidthContext.Provider>);
}
const settle = () => act(async () => { await new Promise((r) => setTimeout(r, 260)); });
const key = () => screen.queryAllByTestId("legend-entry").map((e) => e.textContent).join("|");

describe("(T856) where the Trout / Salmon switch is", () => {
  it("1280: in the nav rail under the Map button, not on the map", async () => {
    mount(1280); await settle();
    await waitFor(() => expect(screen.queryByTestId("regs-paint")).not.toBeNull());
    const rail = screen.getByTestId("nav-rail");
    const sw = screen.getByTestId("regs-paint");
    expect(rail.contains(sw)).toBe(true);
    expect(screen.getByTestId("map-stub").contains(sw)).toBe(false);
    // directly under the Map button: the next tab after Map's is Conditions, the switch sits between them
    const map = screen.getByLabelText("Map"), cond = screen.getByLabelText("Conditions");
    expect(map.compareDocumentPosition(sw) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(sw.compareDocumentPosition(cond) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(screen.getAllByTestId("regs-paint")).toHaveLength(1);
  });

  it.each([390, 1279])("%i: on the map's rail as before, not in the nav", async (w) => {
    mount(w); await settle();
    await waitFor(() => expect(screen.queryByTestId("regs-paint")).not.toBeNull());
    expect(screen.queryByTestId("nav-rail")).toBeNull();
    expect(screen.getByTestId("map-stub").contains(screen.getByTestId("regs-paint"))).toBe(true);
    expect(screen.getAllByTestId("regs-paint")).toHaveLength(1);
  });

  it("1280: flipping the switch in the rail changes the key", async () => {
    mount(1280); await settle();
    await waitFor(() => expect(screen.queryByTestId("regs-paint-salmon")).not.toBeNull());
    expect(screen.getByTestId("regs-paint-standard").getAttribute("aria-checked")).toBe("true");
    const before = key();
    expect(before).toContain("Closed");   // Trout's key is up before the flip
    act(() => { fireEvent.click(screen.getByTestId("regs-paint-salmon")); });
    await settle();
    expect(screen.getByTestId("regs-paint-salmon").getAttribute("aria-checked")).toBe("true");
    expect(key()).not.toBe(before);
    act(() => { fireEvent.click(screen.getByTestId("regs-paint-standard")); });
    await settle();
    expect(key()).toBe(before);
  });
});
