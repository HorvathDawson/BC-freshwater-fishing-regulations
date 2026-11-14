/**
 * (T731, owner 2026-11-14: "When you are in the navigate section view and you click upstream or downstream it is like
 * menus briefly close at bottom and cause a flash of a resize then rerender") A STEP ON A WATER'S MAP CHANGES THE BAR IN
 * PLACE, through the real Shell on the regs fixture release. ▼ from the Chilliwack's mouth part goes on into the Vedder
 * (`onCross`: a water whose group is not read yet) and ▲ comes back; a step inside one water (the Coquitlam) is the
 * plain case. Across every step: no element of the bar (the steps, the name, "Show regulations", the bar itself) is ever
 * removed — the same DOM nodes throughout, so the same block at the same height — the loading line is never drawn, the
 * map is never remounted and is never handed an empty part or another inset, and the water's page (its back row, the
 * Map view) stays mounted. Before the fix the cross drew the bar's loading state for the read: every control removed,
 * a spinner in their place, then all of it back.
 *
 * The map renderer is a stub recording what it is handed (no WebGL in jsdom); a MutationObserver sees every node the
 * DOM loses, whatever React batched.
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { useEffect } from "react";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { Text } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId } from "@app/data";
import { HOME, openWater, reduce, type Route } from "@app/ui";
import { LIGHT } from "./theme";

const map = { mounts: 0, unmounts: 0, handed: [] as { fit: string | null; lit: number; insetTop: unknown; insetBottom: unknown }[] };
vi.mock("./MapScreen", () => ({
  MapScreen: (p: { fit?: { key: string } | null; highlight?: readonly unknown[]; insetTop?: number; insetBottom?: number }) => {
    useEffect(() => { map.mounts++; return () => { map.unmounts++; }; }, []);
    map.handed.push({ fit: p.fit?.key ?? null, lit: p.highlight?.length ?? 0, insetTop: p.insetTop, insetBottom: p.insetBottom });
    return <Text testID="map">map</Text>;
  },
}));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");
const { openRegsFixtureWithLines } = await import("@app/data/release/node");

afterEach(() => { cleanup(); map.mounts = 0; map.unmounts = 0; map.handed.length = 0; });
beforeAll(() => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
});

/** The bar and the page around it: none of these may ever leave the DOM during a step. */
const KEPT = ["water-map-bar", "water-map-where", "water-map-name", "water-map-step-down", "water-map-step-up",
              "water-map-regs", "water-map", "water-back"] as const;
const settle = () => act(() => new Promise((r) => setTimeout(r, 300)));

async function stepsDraw(init: Route, steps: readonly ("up" | "down")[]) {
  render(
    <GestureHandlerRootView>
      <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             loader={openRegsFixtureWithLines()} tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={init} />
    </GestureHandlerRootView>);
  await waitFor(() => expect(screen.getByTestId("water-map-where")).toBeTruthy(), { timeout: 20_000 });
  await settle();
  const nodes = KEPT.map((id) => screen.getByTestId(id));
  const mounted = map.mounts;
  map.handed.length = 0;
  const lost: string[] = [];
  let loadingDrawn = false;
  const ids = (n: Node) => (n instanceof Element
    ? [n.getAttribute("data-testid"), ...[...n.querySelectorAll("[data-testid]")].map((e) => e.getAttribute("data-testid"))] : []);
  const seen = new MutationObserver((recs) => {
    for (const r of recs) {
      for (const n of r.removedNodes) lost.push(...ids(n).filter((i): i is string => !!i && (KEPT as readonly string[]).includes(i)));
      for (const n of r.addedNodes) if (ids(n).includes("water-map-status")) loadingDrawn = true;
    }
  });
  seen.observe(document.body, { childList: true, subtree: true });
  const wheres: string[] = [];
  for (const dir of steps) {
    await act(async () => { fireEvent.click(screen.getByTestId(`water-map-step-${dir}`)); });
    await settle();
    wheres.push(screen.getByTestId("water-map-where").textContent ?? "");
  }
  seen.disconnect();
  return {
    wheres, lost, loadingDrawn,
    sameNodes: KEPT.filter((id, i) => screen.getByTestId(id) !== nodes[i]),
    remounted: { mounts: map.mounts - mounted, unmounts: map.unmounts },
    // every render of the map during the steps: always a part framed and lit, the insets never moved
    unlit: map.handed.filter((h) => h.fit === null || h.lit === 0).length,
    insets: [...new Set(map.handed.map((h) => `${h.insetTop}/${h.insetBottom}`))],
  };
}

describe("(T731) a step on a water's map changes the bar in place", () => {
  it("▼ from the Chilliwack into the Vedder, then ▲ back: the bar, the map and the page stay; the loading line is never drawn", async () => {
    const out = await stepsDraw(reduce(HOME, openWater({ item: "gnis:8634" as ItemId, part: 0, view: "map" })), ["down", "up"]);
    expect(out).toEqual({
      wheres: ["Part 2 of 2 · Vedder River", "Part 1 of 5 · Chilliwack River"],
      lost: [], loadingDrawn: false, sameNodes: [], remounted: { mounts: 0, unmounts: 0 }, unlit: 0,
      insets: [out.insets[0]],
    });
  });

  it("▲ then ▼ inside one water (the Coquitlam): the same", async () => {
    const out = await stepsDraw(reduce(HOME, openWater({ item: "gnis:19986" as ItemId, part: 1, view: "map" })), ["up", "down"]);
    expect(out).toEqual({
      wheres: ["Part 2 of 3 · Coquitlam River", "Part 1 of 3 · Coquitlam River"],
      lost: [], loadingDrawn: false, sameNodes: [], remounted: { mounts: 0, unmounts: 0 }, unlit: 0,
      insets: [out.insets[0]],
    });
  });
});
