/**
 * (T1010) A START ON A BOX, through the real Shell: `?fit=w,s,e,n` (a region page's "Open the map", `@app/ui` `parseStartLink`)
 * opens the browse map framed on that box — the map's `fit` is the box, nothing lit, nothing opened — and does not open at
 * the reader's location. Without it, the map opens unframed. The map renderer is a stub that prints what it was asked to frame.
 */
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { Text } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import { parseStartLink, type Locate } from "@app/ui";
import { vi } from "vitest";
import { LIGHT } from "./theme";

vi.mock("./MapScreen", () => ({
  MapScreen: (p: { fit?: { key: string; bbox: readonly number[] } | null }) =>
    <Text testID="map">{p.fit ? `fit ${p.fit.bbox.join(",")}` : "fit -"}</Text>,
}));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");

afterEach(() => cleanup());
beforeAll(() => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
});

/** a location already granted: a plain open centres on it (T384), so `here` is asked once */
const granted = () => ({ permission: vi.fn(async () => "granted" as const), here: vi.fn(async () => ({ lat: 49.1, lon: -122.3 })) }) satisfies Locate;
const mount = async (search: string, locate: Locate | null = null) => {
  const link = parseStartLink(search);
  render(
    <GestureHandlerRootView>
      <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={link?.route} fitAt={link?.fit ?? null} locate={locate} />
    </GestureHandlerRootView>);
  await act(async () => {});
};

describe("(T1010) the map opened on a box", () => {
  it("?fit=w,s,e,n: the browse map is framed on exactly that box", async () => {
    await mount("?fit=-118.5,49,-114.05,52.2");
    expect(screen.getByTestId("map").textContent).toBe("fit -118.5,49,-114.05,52.2");
    expect(screen.queryByTestId("water-back")).toBeNull();          // nothing opened: the map
  });
  it("no fit: the map is not framed", async () => {
    await mount("");
    expect(screen.getByTestId("map").textContent).toBe("fit -");
  });
  it("a box start does not open at the reader's location (it went somewhere); a plain open does", async () => {
    const plain = granted();
    await mount("", plain);
    await waitFor(() => expect(plain.here).toHaveBeenCalledTimes(1));     // the control: this Shell does open at me
    cleanup();
    const boxed = granted();
    await mount("?fit=-118.5,49,-114.05,52.2", boxed);
    await act(async () => { await new Promise((r) => setTimeout(r, 50)); });   // longer than the plain open took to ask
    expect([boxed.permission.mock.calls.length, boxed.here.mock.calls.length]).toEqual([0, 0]);
    expect(screen.getByTestId("map").textContent).toBe("fit -118.5,49,-114.05,52.2");
  });
});
