/**
 * T131 — A SPOT'S FACE ON THE MAP (`SpotPin`): the Spots tab's own mark (the icon table's `spots`) on the spot ground,
 * framed in the spot mark; the selected one larger and inverted; a group its count over a second face; the name above
 * (never pressable); ONE button, 44 px, saying what a tap does, and the map's `press` the only thing it calls.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { SPOT_PIN, spotMarkers, type SpotPoint } from "@app/core";
import { SpotPin } from "./SpotPin";
import { DARK, LIGHT, type Palette } from "./theme";

afterEach(cleanup);

const A: SpotPoint = { id: "s1", lat: 49.0974, lon: -121.9675, title: "Tamihi run" };
const B: SpotPoint = { id: "s2", lat: 49.1, lon: -122.0, title: "Vedder River" };

/** What one face is, read back from the DOM: its button, its size and colours, its glyph or count, its name. */
function read(palette: Palette) {
  const b = screen.getByRole("button");
  const face = screen.getByTestId("spot-face");
  const st = getComputedStyle(face);
  return {
    label: b.getAttribute("aria-label"), box: [b.style.width, b.style.height],
    face: [st.width, st.backgroundColor === hexToRgb(palette.spot.mark) ? "mark" : "ground"],
    icon: screen.queryByTestId("icon-spots") !== null, count: screen.queryByTestId("spot-count")?.textContent ?? null,
    stack: screen.queryByTestId("spot-stack") !== null, name: screen.queryByTestId("spot-name")?.textContent ?? null,
  };
}
function hexToRgb(hex: string) {
  const h = hex.replace("#", "");
  return `rgb(${parseInt(h.slice(0, 2), 16)}, ${parseInt(h.slice(2, 4), 16)}, ${parseInt(h.slice(4, 6), 16)})`;
}

describe("SpotPin", () => {
  it.each([["light", LIGHT], ["dark", DARK]] as const)("%s: a spot close in — its mark, its name, one 44 px button", (_n, p) => {
    const presses: string[] = [];
    render(<SpotPin m={spotMarkers([A], 12, null)[0]!} palette={p} press={() => presses.push("press")} />);
    const seen = read(p);
    fireEvent.click(screen.getByRole("button"));
    expect([seen, presses]).toEqual([
      { label: "Open spot Tamihi run", box: ["44px", "44px"], face: [`${SPOT_PIN.face}px`, "ground"], icon: true,
        count: null, stack: false, name: "Tamihi run" }, ["press"]]);
  });

  it("the selected spot: larger, inverted (the mark's fill under a ground-coloured icon)", () => {
    render(<SpotPin m={spotMarkers([A], 12, "s1")[0]!} palette={LIGHT} press={() => {}} />);
    expect(read(LIGHT)).toEqual({ label: "Open spot Tamihi run", box: ["44px", "44px"],
                                  face: [`${SPOT_PIN.selectedFace}px`, "mark"], icon: true, count: null, stack: false,
                                  name: "Tamihi run" });
  });

  it("further out: the mark alone, no name; a group: its count over a second face, saying it zooms in", () => {
    render(<SpotPin m={spotMarkers([A], 9, null)[0]!} palette={LIGHT} press={() => {}} />);
    const far = read(LIGHT);
    cleanup();
    render(<SpotPin m={spotMarkers([A, B], 9, null)[0]!} palette={LIGHT} press={() => {}} />);
    expect([far, read(LIGHT)]).toEqual([
      { label: "Open spot Tamihi run", box: ["44px", "44px"], face: [`${SPOT_PIN.face}px`, "ground"], icon: true,
        count: null, stack: false, name: null },
      { label: "2 spots here. Zoom in to see them", box: ["44px", "44px"], face: [`${SPOT_PIN.face}px`, "ground"],
        icon: false, count: "2", stack: true, name: null }]);
  });
});
