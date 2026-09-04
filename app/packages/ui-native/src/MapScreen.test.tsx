/**
 * The control in the map's top-left corner: a DATE, or a set of forecast HORIZONS.
 *
 * Never both, because they are not both questions the screen can answer. A regulation
 * applies on a date and the map asks which. Conditions are now — there is no reading for
 * last Tuesday — so on that tab the same corner asks the other direction instead.
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
  on: { year: 2026, month: 9, day: 4 } as never,
  camera: { lon: -122, lat: 49, zoom: 8 } as never,
};

describe("<MapScreen> corner control", () => {
  it("shows the date when there are no horizons", () => {
    render(<MapScreen {...BASE} onDate={() => {}} />);
    // One Text node — "4 SEP" — so match the line rather than its halves.
    expect(screen.getByText(/^4\s+SEP$/)).toBeTruthy();
    expect(screen.queryByText("Now")).toBeNull();
  });

  it("shows the horizons instead of the date, never as well", () => {
    render(<MapScreen {...BASE} onDate={() => {}}
                      horizons={{ days: [0, 1, 3, 5], value: 0, onPick: () => {} }} />);
    for (const t of ["Now", "+1d", "+3d", "+5d"]) expect(screen.getByText(t)).toBeTruthy();
    // The date is GONE, not merely covered: a date picker on the Conditions tab offers a
    // question with no answer behind it.
    expect(screen.queryByText(/SEP/)).toBeNull();
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
