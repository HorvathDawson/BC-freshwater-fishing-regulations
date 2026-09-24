/**
 * The control in the map's top-left corner: the forecast HORIZONS, on the Conditions tab.
 *
 * Conditions are now — there is no reading for last Tuesday — so the corner asks where the
 * water is heading. It used to hold a date pill on the Map tab, which existed for seasonal
 * regulations; regulations are not integrated, and the pill went with them.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { LIGHT } from "./theme";

/*
 * THE RENDERER IS NOT UNDER TEST HERE. MapLibre needs a WebGL context, which jsdom does not
 * have, and this file is about the control in the map's top-left corner: which one is
 * offered, which is chosen, and what it reports. Stubbing the map is the difference between
 * testing that control and testing nothing.
 */
vi.mock("@app/map", async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  Map: () => null,
}));

const { MapScreen } = await import("./MapScreen");

afterEach(cleanup);

const BASE = {
  at: { atlas: "a", basemap: "b" } as never,
  palette: LIGHT, theme: "light", view: "plain",
  camera: { lon: -122, lat: 49, zoom: 8 } as never,
};

describe("<MapScreen> corner control", () => {
  it("shows nothing in the corner when there are no horizons", () => {
    render(<MapScreen {...BASE} />);
    expect(screen.queryByText("Now")).toBeNull();
    // No date control: it existed for seasonal regulations, which are not integrated.
    expect(screen.queryByLabelText("Change the date")).toBeNull();
  });

  it("shows every horizon it is given", () => {
    render(<MapScreen {...BASE}
                      horizons={{ days: [0, 1, 3, 5], value: 0, onPick: () => {} }} />);
    for (const t of ["Now", "+1d", "+3d", "+5d"]) expect(screen.getByText(t)).toBeTruthy();
  });

  it("reports which horizon is showing to a screen reader, not only in colour", () => {
    render(<MapScreen {...BASE}
                      horizons={{ days: [0, 1, 3, 5], value: 3, onPick: () => {} }} />);
    expect(screen.getByLabelText("Forecast 3 days ahead, showing")).toBeTruthy();
    expect(screen.getByLabelText("Conditions now")).toBeTruthy();
  });

  it("fills the chosen chip rather than only tinting its text", () => {
    // These sit over a moving map at 13px, and this control repaints the whole map — which
    // one is on has to be readable from the corner of an eye.
    render(<MapScreen {...BASE}
                      horizons={{ days: [0, 1, 3, 5], value: 1, onPick: () => {} }} />);
    const chosen = screen.getByLabelText("Forecast 1 day ahead, showing");
    const other = screen.getByLabelText("Conditions now");
    const bg = (el: HTMLElement) =>
      getComputedStyle(el.firstElementChild as Element).backgroundColor;
    expect(bg(chosen)).not.toBe(bg(other));
  });

  it("hands back the day that was tapped", () => {
    const onPick = vi.fn();
    render(<MapScreen {...BASE} horizons={{ days: [0, 1, 3, 5], value: 0, onPick }} />);
    fireEvent.click(screen.getByLabelText("Forecast 5 days ahead"));
    expect(onPick).toHaveBeenCalledWith(5);
  });

  it("offers no layers button when the caller gives no handler", () => {
    // The Conditions tab overrides every choice that sheet would offer, so the button
    // would open a sheet whose answers the tab ignores.
    render(<MapScreen {...BASE}
                      horizons={{ days: [0, 1, 3, 5], value: 0, onPick: () => {} }} />);
    expect(screen.queryByText("Layers")).toBeNull();
  });
});
