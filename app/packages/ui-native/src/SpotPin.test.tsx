/**
 * T131/T145 — A SPOT'S FACE ON THE MAP (`SpotPin`): a small square of the spot mark framed in the spot ground; the
 * selected one ringed in the mark; a group a larger square with its count; the name above, one line (never pressable);
 * ONE invisible 44 px button saying what a tap does, and the map's `press` the only thing it calls.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { SPOT_PIN, spotFace, spotMarkers, type SpotPoint } from "@app/core";
import { SpotPin } from "./SpotPin";
import { DARK, LIGHT, type Palette } from "./theme";

afterEach(cleanup);

const A: SpotPoint = { id: "s1", lat: 49.0974, lon: -121.9675, title: "Tamihi run" };
const B: SpotPoint = { id: "s2", lat: 49.1, lon: -122.0, title: "Vedder River" };

/** What one face is, read back from the DOM: its button, the face's size, fill and frame, the ring, count and name. */
function read(palette: Palette) {
  const b = screen.getByRole("button");
  const face = getComputedStyle(screen.getByTestId("spot-face"));
  const ring = getComputedStyle(screen.getByTestId("spot-ring"));
  const tone = (c: string) =>
    c === hexToRgb(palette.spot.mark) ? "mark" : c === hexToRgb(palette.spot.ground) ? "ground" : c;
  return {
    label: b.getAttribute("aria-label"), box: [b.style.width, b.style.height],
    face: [face.width, tone(face.backgroundColor), face.borderTopWidth, tone(face.borderTopColor)],
    ring: [ring.width, ring.borderTopWidth], count: screen.queryByTestId("spot-count")?.textContent ?? null,
    name: screen.queryByTestId("spot-name")?.textContent ?? null,
  };
}
function hexToRgb(hex: string) {
  const h = hex.replace("#", "");
  return `rgb(${parseInt(h.slice(0, 2), 16)}, ${parseInt(h.slice(2, 4), 16)}, ${parseInt(h.slice(4, 6), 16)})`;
}

describe("SpotPin", () => {
  it.each([["light", LIGHT], ["dark", DARK]] as const)("%s: a spot close in — a 12 px square of the mark framed in " +
    "the ground, its name, one 44 px button", (_n, p) => {
    const presses: string[] = [];
    render(<SpotPin m={spotMarkers([A], 12, null)[0]!} palette={p} press={() => presses.push("press")} />);
    const seen = read(p);
    fireEvent.click(screen.getByRole("button"));
    expect([seen, presses]).toEqual([
      { label: "Open spot Tamihi run", box: ["44px", "44px"], face: ["12px", "mark", "2px", "ground"],
        ring: ["12px", "0px"], count: null, name: "Tamihi run" }, ["press"]]);
  });

  it.each([["light", LIGHT], ["dark", DARK]] as const)("%s: the selected spot — the same face, ringed in the mark " +
    "(20 px in all)", (_n, p) => {
    render(<SpotPin m={spotMarkers([A], 12, "s1")[0]!} palette={p} press={() => {}} />);
    expect(read(p)).toEqual({ label: "Open spot Tamihi run", box: ["44px", "44px"],
                              face: ["12px", "mark", "2px", "ground"], ring: ["20px", "2px"], count: null,
                              name: "Tamihi run" });
  });

  it("further out: the face alone, no name; a group: an 18 px square with its count, saying it zooms in", () => {
    render(<SpotPin m={spotMarkers([A], 9, null)[0]!} palette={LIGHT} press={() => {}} />);
    const far = read(LIGHT);
    cleanup();
    render(<SpotPin m={spotMarkers([A, B], 9, null)[0]!} palette={LIGHT} press={() => {}} />);
    expect([far, read(LIGHT)]).toEqual([
      { label: "Open spot Tamihi run", box: ["44px", "44px"], face: ["12px", "mark", "2px", "ground"],
        ring: ["12px", "0px"], count: null, name: null },
      { label: "2 spots here. Zoom in to see them", box: ["44px", "44px"], face: ["18px", "mark", "2px", "ground"],
        ring: ["18px", "0px"], count: "2", name: null }]);
  });

  it("every size it draws is spotFace's, and none passes SPOT_PIN.max", () => {
    const sizes: string[] = [];
    const cases: [SpotPoint[], string | null][] = [[[A], null], [[A], "s1"], [[A, B], null], [[A, B], "s2"]];
    for (const [pts, sel] of cases) {
      const m = spotMarkers(pts, 9, sel)[0]!;
      render(<SpotPin m={m} palette={LIGHT} press={() => {}} />);
      const f = spotFace(m);
      sizes.push(`${getComputedStyle(screen.getByTestId("spot-face")).width}=${f.face}`,
                 `${getComputedStyle(screen.getByTestId("spot-ring")).width}=${f.extent}<=${SPOT_PIN.max}`);
      cleanup();
    }
    expect(sizes).toEqual(["12px=12", "12px=12<=26", "12px=12", "20px=20<=26", "18px=18", "18px=18<=26", "18px=18",
                           "26px=26<=26"]);
  });
});
