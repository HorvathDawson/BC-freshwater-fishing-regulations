/**
 * T904 (owner 2026-11-19: "map in background and remove big white bars on desktop") — the Conditions key and the
 * watershed day strip are a CARD floating over the map at the desktop class (window ≥ 1280 px), not a full-width bar
 * under it; at 1279 px and below (tablet, phone) the bar is as it was.
 */
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: () => null }));
import { cleanup, render, screen } from "@testing-library/react";
import { LayoutWidthContext } from "@app/ui";
import { MapLegend, mapFootInset, RunsLegend } from "./shell/MapLegend";
import { tabsAsRail } from "@app/ui";
import { LIGHT } from "./theme";

afterEach(cleanup);

const col = {
  flowParam: "discharge", horizon: 0, mm: { onConditions: true }, cond: { marks: [] }, gearMarked: false, regsLayer: "standard",
  salmonOffered: false, statusOk: true, salmonFacts: null, isolated: null, isolate: () => {}, basinDays: null, pickDay: () => {},
  layers: { water: { drawn: "regulations" }, effective: { groups: { obstacles: false } } },
} as never;
const at = (w: number) => render(
  <LayoutWidthContext.Provider value={w}>
    <MapLegend col={col} conditions palette={LIGHT} theme="light" mapDay={null} />
  </LayoutWidthContext.Provider>);

describe("(T904) the Conditions key at the desktop breakpoint", () => {
  it("1280 px: a card with its own border and margin, no bar (no top-border-only strip)", () => {
    at(1280);
    const card = screen.getByTestId("legend-card");
    const s = getComputedStyle(card);
    expect(s.borderTopWidth).toBe("1px");
    expect(s.borderBottomWidth).toBe("1px");
    expect(s.marginBottom).toBe("12px");
    expect(s.borderTopLeftRadius === "" || s.borderTopLeftRadius === "0px").toBe(true);
  });

  it("1279 px: the bar, as before (no card)", () => {
    at(1279);
    expect(screen.queryByTestId("legend-card")).toBeNull();
  });

  it("the scale bar, attribution and gauge chip drop to the map's bottom at 1280 px; on all three map tabs; lifted over the bar at 1279", () => {
    const inset = (w: number, tab: string) => mapFootInset(tabsAsRail(w >= 1280 ? "desktop" : "tablet"), tab, 140);
    expect(inset(1280, "conditions")).toBe(0);
    expect(inset(1279, "conditions")).toBe(140);
    expect(inset(1280, "map")).toBe(0);
    expect(inset(1280, "runs")).toBe(0);
    expect(inset(1279, "map")).toBe(140);
    expect(inset(1279, "runs")).toBe(140);
    expect(inset(1280, "spots")).toBe(140);
  });

  it("Map tab: the regs key is a card at 1280, (T1211) the floating Map key at 1279", () => {
    const map = { ...(col as object), mm: { onConditions: false }, layers: { water: { drawn: "regulations" }, effective: { groups: { obstacles: false } } } } as never;
    const key = (w: number) => render(<LayoutWidthContext.Provider value={w}><MapLegend col={map} conditions={false} palette={LIGHT} theme="light" mapDay={null} /></LayoutWidthContext.Provider>);
    key(1280);
    expect(screen.getByTestId("legend-card").style.marginBottom).toBe("12px");
    expect(screen.queryByTestId("legend-strip")).toBeNull();
    cleanup();
    key(1279);
    expect(screen.queryByTestId("legend-card")).toBeNull();
    expect(screen.queryByTestId("legend-strip")).toBeNull();
    expect(screen.getByTestId("map-key-chip")).toBeTruthy();
  });

  it("Runs tab: the legend is a card at 1280, the bar at 1279", () => {
    const runs = { runs: { facts: null, chosen: [], year: 2026, species: [], pentad: 0, today: 0, playing: false, loading: false, problem: null,
                           toggle: () => {}, only: () => {}, all: () => {}, setPentad: () => {}, setPlaying: () => {} } } as never;
    const at2 = (w: number) => render(<LayoutWidthContext.Provider value={w}><RunsLegend col={runs} palette={LIGHT} theme="light" /></LayoutWidthContext.Provider>);
    at2(1280);
    expect(screen.getByTestId("runs-legend-card")).toBeTruthy();
    cleanup();
    at2(1279);
    expect(screen.getByTestId("runs-legend")).toBeTruthy();
  });
});
