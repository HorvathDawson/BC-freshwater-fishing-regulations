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
import { horizonChips, type Horizon, type MenuControls } from "@app/ui";
import { MenuContext } from "./menu/MenuButton";
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

  it("offers now and every forecast day, in core's words, ten days included", () => {
    render(<MapScreen {...BASE} horizons={horizons(0)} />);
    for (const t of ["Now", "+1d", "+3d", "+5d", "+10d"]) expect(screen.getByText(t)).toBeTruthy();
    // In order, left to right: the same order as the panel's header.
    expect([...document.querySelectorAll("[aria-label]")].map((e) => e.getAttribute("aria-label")))
      .toEqual(["Conditions now, showing", "Forecast 1 day ahead", "Forecast 3 days ahead",
                "Forecast 5 days ahead", "Forecast 10 days ahead"]);
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

  it("hands back the day that was tapped, ten included", () => {
    const onPick = vi.fn();
    render(<MapScreen {...BASE} horizons={horizons(0, onPick)} />);
    fireEvent.click(screen.getByLabelText("Forecast 10 days ahead"));
    expect(onPick).toHaveBeenCalledWith(10);
    fireEvent.click(screen.getByLabelText("Forecast 5 days ahead"));
    expect(onPick).toHaveBeenLastCalledWith(5);
  });

  it("the corner holds the way back to search only while the map shows what search's eye sent it to", () => {
    const back = vi.fn();
    const { rerender } = render(<MapScreen {...BASE} />);
    expect(screen.queryByRole("button", { name: "Back to search" })).toBeNull();
    rerender(<MapScreen {...BASE} onBack={back} />);
    const pill = screen.getByRole("button", { name: "Back to search" });
    expect(pill.textContent).toBe("‹ Search");
    pill.click();
    expect(back).toHaveBeenCalledTimes(1);
  });

  it("gives the horizons their own row under the menu, so the hamburger does not push +10d under the zoom", () => {
    // REVIEW 2026-10-21: in one row the hamburger moved the chips ~45px right; at 390 wide +10d ran under MapLibre's
    // zoom control (top-right) and past the edge at 360. Under the menu, the chips start at the corner's left edge as
    // they did before the menu came, and the row they used to fit in is theirs alone.
    const menu = { open: () => {} } as unknown as MenuControls;
    render(<MenuContext.Provider value={menu}><MapScreen {...BASE} horizons={horizons(0)} /></MenuContext.Provider>);
    const row = screen.getByTestId("map-corner-row");
    const chips = screen.getByTestId("map-horizons");
    expect(row.contains(screen.getByTestId("menu-button"))).toBe(true);
    expect(row.contains(chips)).toBe(false);
    expect(getComputedStyle(row.parentElement as Element).flexDirection).toBe("column");
    expect(row.parentElement?.contains(chips)).toBe(true);
  });

  it("offers no layers button when the caller gives no handler", () => {
    // The Conditions tab overrides every choice that sheet would offer, so the button
    // would open a sheet whose answers the tab ignores.
    render(<MapScreen {...BASE} horizons={horizons(0)} />);
    expect(screen.queryByText("Layers")).toBeNull();
    expect(screen.queryByRole("button", { name: "Choose layers" })).toBeNull();
  });
});

describe("<MapScreen> rail (owner 2026-10-21: Layers and Spots stacked on the side, Fish Washington style)", () => {
  const rail = () => {
    const bar = screen.queryByRole("toolbar", { name: "Map tools" });
    return bar ? [...bar.querySelectorAll("[role=button]")].map((b) => b.getAttribute("aria-label")).join(" · ")
      : "no rail";
  };
  it("Layers then Spots, each said aloud; the Conditions map (no layers handler) keeps Spots; neither: no rail", () => {
    const { rerender } = render(<MapScreen {...BASE} onLayers={() => {}} onSpots={() => {}} />);
    const both = rail();
    rerender(<MapScreen {...BASE} onSpots={() => {}} />);
    const spots = rail();
    rerender(<MapScreen {...BASE} />);
    expect([both, spots, rail()]).toEqual(["Choose layers · Your spots", "Your spots", "no rail"]);
  });

  it("each button opens its own sheet", () => {
    const layers = vi.fn(), spots = vi.fn();
    render(<MapScreen {...BASE} onLayers={layers} onSpots={spots} />);
    fireEvent.click(screen.getByRole("button", { name: "Your spots" }));
    fireEvent.click(screen.getByRole("button", { name: "Choose layers" }));
    fireEvent.click(screen.getByRole("button", { name: "Your spots" }));
    expect([layers.mock.calls.length, spots.mock.calls.length]).toEqual([1, 2]);
  });

  it("the long press reaches the map as given, and only when given", () => {
    const held = vi.fn();
    const { rerender } = render(<MapScreen {...BASE} onLongPress={held} />);
    const given = mapProps.last!.onLongPress;
    rerender(<MapScreen {...BASE} />);
    expect([given === held, mapProps.last!.onLongPress]).toEqual([true, undefined]);
  });
});
