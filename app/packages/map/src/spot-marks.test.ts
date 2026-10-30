/**
 * (T316) Your spots drawn by the map engine, the look `SpotPin` had: a small square of the spot mark framed in the spot
 * ground; selected, ringed in the mark after a 2 px gap; a group larger with its count; parking a P; the name above on a
 * ground strip, cut to the strip's widest. Every size is `spotFace` / `SPOT_PIN`.
 */
import { describe, expect, it } from "vitest";
import { SPOT_PIN, spotFace, spotMarkers, type SpotPoint } from "@app/core";
import { SCALE } from "./raster";
import { SPOT_FACE, SPOT_LABEL, SPOT_SOURCE, SPOT_TEXT, spotFaceImage, spotImageName, spotImages, spotLabelText,
         spotsData, spotsLayers } from "./spot-marks";

const C = { mark: "#c2410c", ground: "#fff7ed" };
const A: SpotPoint = { id: "s1", lat: 49.0974, lon: -121.9675, title: "Tamihi run" };
const B: SpotPoint = { id: "s2", lat: 49.1, lon: -122.0, title: "Vedder River" };
const P: SpotPoint = { ...A, lat: 49.2, parking: true };

/** The colour at (x, y) px (1×) of an image: "mark", "ground" or "clear". */
function at(img: ReturnType<typeof spotFaceImage>, x: number, y: number): string {
  const i = ((y * SCALE) * img.width + x * SCALE) * 4;
  if (img.data[i + 3] === 0) return "clear";
  const hex = `#${[0, 1, 2].map((k) => img.data[i + k]!.toString(16).padStart(2, "0")).join("")}`;
  return hex === C.mark ? "mark" : hex === C.ground ? "ground" : hex;
}

describe("(T316) spot marks as map layers", () => {
  it("a face: 12 px, a 2 px ground frame round the mark; selected: 20 px, a 2 px mark ring, a 2 px gap", () => {
    const f = spotFaceImage(SPOT_PIN.face, false, C);
    expect([f.width / SCALE, at(f, 0, 0), at(f, 1, 6), at(f, 2, 6), at(f, 6, 6)]).toEqual([12, "ground", "ground", "mark", "mark"]);
    const s = spotFaceImage(SPOT_PIN.face, true, C);
    expect([s.width / SCALE, at(s, 0, 10), at(s, 1, 10), at(s, 2, 10), at(s, 3, 10), at(s, 4, 10), at(s, 6, 10), at(s, 10, 10)])
      .toEqual([20, "mark", "mark", "clear", "clear", "ground", "mark", "mark"]);
    for (const img of spotImages(C)) expect(img.width / SCALE).toBeLessThanOrEqual(SPOT_PIN.max);
  });

  it("the source: each mark its face image, its count or P, its name; the selected sorted over the rest", () => {
    const named = spotMarkers([A, B, P], SPOT_PIN.labelZoom + 2, "s1");
    const d = spotsData(named, C).features as { properties: Record<string, unknown> }[];
    const s1 = d.find((f) => f.properties.key === "spot:s1")!.properties;
    const park = d.find((f) => f.properties.key === "parking:s1")!.properties;
    expect([s1.img, s1.sel, s1.text, s1.label, s1.ext]).toEqual(
      [spotImageName(SPOT_PIN.face, true, C), 1, "", "Tamihi run", spotFace({ selected: true, count: 1 }).extent]);
    expect([park.text, park.img, park.label]).toEqual(["P", spotImageName(SPOT_PIN.groupFace, false, C), ""]);
    const group = spotsData(spotMarkers([A, B], 6, null), C).features[0] as { properties: Record<string, unknown> };
    expect([group.properties.text, group.properties.img]).toEqual(["2", spotImageName(SPOT_PIN.groupFace, false, C)]);
  });

  it("names cut to the strip's widest with an ellipsis; layers: face, text in the ground, name in the mark, all on one source", () => {
    expect(spotLabelText("Tamihi run")).toBe("Tamihi run");
    const long = spotLabelText("The long pool below the old railway bridge");
    expect([long.endsWith("…"), Math.ceil(long.length * 6.5) + 8 <= SPOT_PIN.labelMaxWidth]).toEqual([true, true]);
    const ls = spotsLayers(C) as unknown as { id: string; source: string; paint?: Record<string, unknown> }[];
    expect(ls.map((l) => l.id)).toEqual([SPOT_FACE, SPOT_TEXT, SPOT_LABEL]);
    expect(ls.every((l) => l.source === SPOT_SOURCE)).toBe(true);
    expect([ls[1]!.paint!["text-color"], ls[2]!.paint!["text-color"]]).toEqual([C.ground, C.mark]);
  });
});
