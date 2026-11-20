/**
 * (T856) THE MAP'S TROUT / SALMON SWITCH AND THE FLOATING RAIL: on a desktop-width window the switch is in the map's top
 * row directly after the day strip — not in the nav rail, not on the map's right rail; on a phone or narrow window it is
 * on the map's right rail as before. Flipping it still changes the key. At desktop width the see-through nav rail floats
 * over the map (the web map takes its width as a left inset), the pick banner starts right of it, and while a water's
 * pane is held (T498) the screen keeps that layout — the held map neither shrinks nor loses its inset. Through the real
 * Shell on the fixture; the map renderer is a stub that draws the rail's extra (`railExtra`) and the inset it was given,
 * and a lift button (`onPressStart`, the T716 tap) that opens a water held until its click.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { Text, View } from "react-native";
import type { ReactNode } from "react";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import type { SalmonFacts, StatusIndex } from "@app/core";
import { parseManifest, type ItemId } from "@app/data";
import { HOME, LayoutWidthContext, type PairingInput, type StatusSource } from "@app/ui";
import { LIGHT } from "./theme";
import { NAV_RAIL } from "./NavRail";

const TAP = { sid: 0 };
vi.mock("./MapScreen", async () => {
  // (T566) a map says the look it was handed is drawn: the key under it follows that word
  const { useEffect } = await import("react");
  const { Pressable } = await import("react-native");
  const Drawn = ({ k, on }: { k?: string; on?: (k: string) => void }) => { useEffect(() => { if (k !== undefined) on?.(k); }, [k, on]); return null; };
  return {
    MapScreen: (p: { railExtra?: ReactNode; corner?: ReactNode; insetLeft?: number; lookKey?: string; onLookDrawn?: (k: string) => void;
                     onPressStart?: (l: string, id: number) => void }) =>
      <View testID="map-stub"><Drawn k={p.lookKey} on={p.onLookDrawn} /><Text>map</Text><Text testID="stub-inset">{String(p.insetLeft)}</Text><View testID="stub-corner">{p.corner}</View><View testID="stub-rail">{p.railExtra}</View>
        <Pressable accessibilityRole="button" accessibilityLabel="lift" onPress={() => { p.onPressStart?.("stream", TAP.sid); }}><Text>lift</Text></Pressable></View>,
  };
});
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
// the water's own small map (its Conditions view): no WebGL here
vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: () => null }));
const { Shell } = await import("./Shell");
const { openRegsFixture, openRegsFixtureWithLines } = await import("@app/data/release/node");
const { openRegsStore } = await import("@app/data/regs");
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
  it("1280: in the map's top row directly after the day strip; not in the nav, not on the right rail", async () => {
    mount(1280); await settle();
    await waitFor(() => expect(screen.queryByTestId("regs-paint")).not.toBeNull());
    const sw = screen.getByTestId("regs-paint");
    const row = screen.getByTestId("map-corner-row"), day = screen.getByTestId("map-day");
    expect(row.contains(sw) && row.contains(day)).toBe(true);
    expect(day.compareDocumentPosition(sw) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(screen.getByTestId("nav-rail").contains(sw)).toBe(false);
    expect(screen.getByTestId("stub-rail").contains(sw)).toBe(false);
    expect(screen.getAllByTestId("regs-paint")).toHaveLength(1);
  });

  it.each([390, 1279])("%i: on the map's right rail as before, not in the top row", async (w) => {
    mount(w); await settle();
    await waitFor(() => expect(screen.queryByTestId("regs-paint")).not.toBeNull());
    expect(screen.queryByTestId("nav-rail")).toBeNull();
    const sw = screen.getByTestId("regs-paint");
    expect(screen.getByTestId("stub-rail").contains(sw)).toBe(true);
    expect(screen.getByTestId("map-corner-row").contains(sw)).toBe(false);
    expect(screen.getAllByTestId("regs-paint")).toHaveLength(1);
  });

  it("the map's left inset (the floating rail's width): 92 at 1280, 0 at 1279 and 390", async () => {
    mount(1280); await settle();
    await waitFor(() => expect(screen.queryByTestId("stub-inset")).not.toBeNull());
    expect(screen.getByTestId("stub-inset").textContent).toBe(String(NAV_RAIL.width));
    cleanup();
    for (const w of [1279, 390]) {
      mount(w); await settle();
      await waitFor(() => expect(screen.queryByTestId("stub-inset")).not.toBeNull());
      expect(screen.getByTestId("stub-inset").textContent).toBe("0");
      cleanup();
    }
  });

  it("1280: flipping the switch in the top row changes the key", async () => {
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

/** The rail's wrapper (the nav rail's parent), and the screen's layout around it, as drawn. */
const railLayout = () => {
  const wrap = screen.getByTestId("nav-rail").parentElement!;
  return { rail: getComputedStyle(wrap).position, outer: getComputedStyle(wrap.parentElement!).flexDirection,
           foot: getComputedStyle(screen.getByTestId("map-foot")).left, inset: screen.getByTestId("stub-inset").textContent };
};
const FLOATING = { rail: "absolute", outer: "column", foot: `${NAV_RAIL.width}px`, inset: String(NAV_RAIL.width) };

describe("(T856) the floating rail", () => {
  it("1280: the pick banner starts right of the rail (the search field's place)", async () => {
    mount(1280); await settle();
    const tab = (name: string) => act(() => { fireEvent.click(screen.getByRole("tab", { name })); });
    const press = (name: string) => act(() => { fireEvent.click(screen.getByLabelText(name)); });
    tab("Spots");
    if (!screen.queryByLabelText("Pick on the map")) press("Add a spot");
    press("Pick on the map");
    await settle();
    expect(screen.getByTestId("spot-pick-banner")).toBeTruthy();
    expect(getComputedStyle(screen.getByTestId("spot-pick-layer")).left).toBe(`${NAV_RAIL.width}px`);
  });

  it("1280: while a water's pane is held the rail still floats over the map, which keeps its box and its inset (T498)", async () => {
    // the Chilliwack's first section in the regs fixture (TapCue.shell.test.tsx)
    const fx = SOURCE();
    const st = await openRegsStore(openRegsFixture());
    TAP.sid = (await st.navRows((await st.itemOrd("gnis:8634"))!)).sections[0]!.sid;
    const source = { ...fx, itemForSection: async (s: number) => (s === TAP.sid ? "gnis:8634" as ItemId : fx.itemForSection(s as never)) };
    render(
      <LayoutWidthContext.Provider value={1280}><GestureHandlerRootView>
        <Shell source={source} loader={openRegsFixture()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
               tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={HOME} systemBack={() => ({ sync: () => {}, dispose: () => {} })}
               status={STATUS} pairing={PAIRING} spots={[]} onSaveSpot={async () => {}} />
      </GestureHandlerRootView></LayoutWidthContext.Provider>);
    await settle();
    expect(railLayout()).toEqual(FLOATING);
    // the lift opens the water, its pane built hidden and held until the click (T716) — which never comes here; the
    // clock is frozen so the cue's own timeout cannot take the open back while the pane builds
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      act(() => { fireEvent.click(screen.getByRole("button", { name: "lift" })); });
      const held = () => { const h = screen.queryByTestId("regs-header"); return !!h && !!h.closest('[aria-hidden="true"]'); };
      for (let i = 0; i < 400 && !held(); i++)
        await act(async () => { await vi.advanceTimersByTimeAsync(1); await new Promise((r) => setImmediate(r)); });
      expect(held()).toBe(true);
      expect(railLayout()).toEqual(FLOATING);
    } finally { vi.useRealTimers(); }
  }, 40_000);
});
