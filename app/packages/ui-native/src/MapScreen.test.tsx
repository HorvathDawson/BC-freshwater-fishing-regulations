/**
 * The chrome over the map: the forecast HORIZONS in the top-left corner, on the Conditions tab.
 *
 * OWNER 2026-10-20: "the condition 1 3 5 day forecast is gone in condition map?" + "also add
 * 10 day to it". P16 had removed the corner control as a second copy of the panel header's.
 * It is back, and the owner's rule is pinned instead of "exactly one": the map has its
 * control on the Conditions map, the panel has its own in its header, and BOTH draw
 * `horizonChips` from @app/ui (core's HORIZONS and words) and set ONE horizon state — so
 * they can never disagree. The shared state is pinned in HorizonShared.test.tsx.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { horizonChips, type Horizon } from "@app/ui";
import { LIGHT } from "./theme";

/*
 * THE RENDERER IS NOT UNDER TEST HERE. MapLibre needs a WebGL context, which jsdom does not
 * have, and this file is about the control in the map's top-left corner: which one is
 * offered, which is chosen, and what it reports. Stubbing the map is the difference between
 * testing that control and testing nothing.
 */
const mapProps: { last: Record<string, unknown> | null } = { last: null };
vi.mock("@app/map", async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  Map: (p: Record<string, unknown>) => { mapProps.last = p; return null; },
}));

const { MapScreen } = await import("./MapScreen");
const { RAIL_BELOW } = await import("./MapRail");

afterEach(cleanup);

const BASE = {
  at: { atlas: "a", basemap: "b" } as never,
  palette: LIGHT, theme: "light", view: "plain",
  camera: { lon: -122, lat: 49, zoom: 8 } as never,
};

const horizons = (on: Horizon, onPick: (d: Horizon) => void = () => {}) =>
  ({ chips: horizonChips(on), onPick });

describe("<MapScreen> corner", () => {
  it("shows nothing in the corner when there are no horizons", () => {
    render(<MapScreen {...BASE} />);
    expect(screen.queryByText("Now")).toBeNull();
    // No date control: it existed for seasonal regulations, which are not integrated.
    expect(screen.queryByLabelText("Change the date")).toBeNull();
  });

  it("offers now and every forecast day, in core's words, ten days included — and Change last (T52)", () => {
    render(<MapScreen {...BASE} horizons={horizons(0)} />);
    for (const t of ["Now", "+1d", "+3d", "+5d", "+10d", "Change"]) expect(screen.getByText(t)).toBeTruthy();
    // In order, left to right: the same order as the panel's header.
    expect([...document.querySelectorAll("[aria-label]")].map((e) => e.getAttribute("aria-label")))
      .toEqual(["Conditions now, showing", "Forecast 1 day ahead", "Forecast 3 days ahead",
                "Forecast 5 days ahead", "Forecast 10 days ahead", "Expected change over the next 10 days"]);
  });

  it("reports which horizon is showing to a screen reader, not only in colour", () => {
    render(<MapScreen {...BASE} horizons={horizons(10)} />);
    expect(screen.getByLabelText("Forecast 10 days ahead, showing")).toBeTruthy();
    expect(screen.getByLabelText("Conditions now")).toBeTruthy();
  });

  it("fills the chosen chip rather than only tinting its text", () => {
    // These sit over a moving map at 13px, and this control repaints the whole map — which
    // one is on has to be readable from the corner of an eye.
    render(<MapScreen {...BASE} horizons={horizons(1)} />);
    const chosen = screen.getByLabelText("Forecast 1 day ahead, showing");
    const other = screen.getByLabelText("Conditions now");
    const bg = (el: HTMLElement) =>
      getComputedStyle(el.firstElementChild as Element).backgroundColor;
    expect(bg(chosen)).not.toBe(bg(other));
  });

  it("(refinement pass) is ONE joined strip — one card, hairline dividers — each segment a 44 × 44 press", () => {
    render(<MapScreen {...BASE} horizons={horizons(3)} />);
    const strip = screen.getByTestId("map-horizons");
    const segs = [...strip.children] as HTMLElement[];
    expect(segs.map((s) => [s.getAttribute("aria-label"), s.getAttribute("data-hitslop"),
                            (s.firstElementChild as HTMLElement).style.borderLeftWidth || "0px"]))
      .toEqual([["Conditions now", "46 33 6 0 5 0", "0px"], ["Forecast 1 day ahead", "46 33 6 0 5 0", "1px"],
                ["Forecast 3 days ahead, showing", "46 33 6 0 5 0", "1px"],
                ["Forecast 5 days ahead", "46 33 6 0 5 0", "1px"], ["Forecast 10 days ahead", "46 33 6 0 5 0", "1px"],
                ["Expected change over the next 10 days", "46 33 6 0 5 0", "1px"]]);
  });

  it("hands back the day that was tapped, ten included", () => {
    const onPick = vi.fn();
    render(<MapScreen {...BASE} horizons={horizons(0, onPick)} />);
    fireEvent.click(screen.getByLabelText("Forecast 10 days ahead"));
    expect(onPick).toHaveBeenCalledWith(10);
    fireEvent.click(screen.getByLabelText("Forecast 5 days ahead"));
    expect(onPick).toHaveBeenLastCalledWith(5);
  });

  it("(T48) the corner sits under the app's bar: its top is the bar's bottom edge plus 8; no menu, no way-back pill", () => {
    const { rerender } = render(<MapScreen {...BASE} horizons={horizons(0)} />);
    const bare = screen.getByTestId("map-corner").style.top;
    rerender(<MapScreen {...BASE} horizons={horizons(0)} insetTop={76} />);
    expect([bare, screen.getByTestId("map-corner").style.top, mapProps.last!.insetTop,
            screen.queryByTestId("menu-button"), screen.queryByRole("button", { name: "Back to search" })])
      .toEqual(["8px", "84px", 76, null, null]);
  });

  it("offers no layers button when the caller gives no handler", () => {
    // The Conditions tab overrides every choice that sheet would offer, so the button
    // would open a sheet whose answers the tab ignores.
    render(<MapScreen {...BASE} horizons={horizons(0)} />);
    expect(screen.queryByText("Layers")).toBeNull();
    expect(screen.queryByRole("button", { name: "Choose layers" })).toBeNull();
  });
});

describe("<MapScreen> rail (owner 2026-10-21, Fish Washington style; T48: Layers ALONE — Spots is a tab again)", () => {
  const rail = () => {
    const bar = screen.queryByRole("toolbar", { name: "Map tools" });
    return bar ? [...bar.querySelectorAll("[role=button]")].map((b) => b.getAttribute("aria-label")).join(" · ")
      : "no rail";
  };
  it("Layers, said aloud; no layers handler: no rail; it starts RAIL_BELOW under the bar", () => {
    const { rerender } = render(<MapScreen {...BASE} onLayers={() => {}} insetTop={76} />);
    const layers = rail();
    const top = (screen.getByRole("toolbar", { name: "Map tools" }) as HTMLElement).style.top;
    rerender(<MapScreen {...BASE} />);
    expect([layers, top, rail()]).toEqual(["Choose layers", `${76 + RAIL_BELOW}px`, "no rail"]);
  });

  it("the button opens the Layers sheet", () => {
    const layers = vi.fn();
    render(<MapScreen {...BASE} onLayers={layers} />);
    fireEvent.click(screen.getByRole("button", { name: "Choose layers" }));
    expect(layers.mock.calls.length).toBe(1);
  });

  it("the long press reaches the map as given, and only when given", () => {
    const held = vi.fn();
    const { rerender } = render(<MapScreen {...BASE} onLongPress={held} />);
    const given = mapProps.last!.onLongPress;
    rerender(<MapScreen {...BASE} />);
    expect([given === held, mapProps.last!.onLongPress]).toEqual([true, undefined]);
  });
});
