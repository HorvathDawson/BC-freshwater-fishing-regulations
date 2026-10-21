/**
 * The chrome over the map — and, since P16, what is NOT there.
 *
 * The map's top-left corner held the forecast HORIZONS on the Conditions tab, a second copy
 * of the control the Conditions panel puts in its header (with its own labels typed in a
 * second time). P16 item 2: one horizon control, in the panel header. The corner is empty.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
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

describe("<MapScreen> corner", () => {
  it("shows nothing in the corner", () => {
    render(<MapScreen {...BASE} />);
    expect(screen.queryByText("Now")).toBeNull();
    // No date control: it existed for seasonal regulations, which are not integrated.
    expect(screen.queryByLabelText("Change the date")).toBeNull();
  });

  it("offers no horizon control: the Conditions panel's header holds the only one (P16)", () => {
    // NOT A PROP ANY MORE, so a second control cannot be wired back in by a caller: passing
    // one is a type error (this line fails `tsc` if `horizons` returns) …
    const horizons = { days: [0, 1, 3, 5], value: 0, onPick: () => {} };
    // @ts-expect-error -- the map takes no horizons (P16 item 2)
    render(<MapScreen {...BASE} horizons={horizons} />);
    // … and nothing is drawn for one at run time either.
    for (const t of ["Now", "+1d", "+3d", "+5d"]) expect(screen.queryByText(t)).toBeNull();
    expect(screen.queryByLabelText("Conditions now")).toBeNull();
    expect(screen.queryByLabelText("Forecast 1 day ahead")).toBeNull();
  });

  it("offers no layers button when the caller gives no handler", () => {
    // The Conditions tab overrides every choice that sheet would offer, so the button
    // would open a sheet whose answers the tab ignores.
    render(<MapScreen {...BASE} />);
    expect(screen.queryByText("Layers")).toBeNull();
  });
});
