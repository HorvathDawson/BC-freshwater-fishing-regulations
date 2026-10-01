import { describe, expect, it } from "vitest";
import { hatchImage } from "./hatch";
import { colour } from "./chrome";

/** The wetland fill the hatch is really woven from, and its bytes. */
const WETLAND = colour("light", "color.wetland.fill");
const bytes = (hex: string) => [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16));

/** RGBA at (x, y), un-premultiplied so the tests can talk about the colour asked for. */
const at = (img: ReturnType<typeof hatchImage>, x: number, y: number) => {
  const i = (y * img.width + x) * 4;
  const a = img.data[i + 3]! / 255;
  return { r: img.data[i]!, g: img.data[i + 1]!, b: img.data[i + 2]!, a,
           straight: a === 0 ? [0, 0, 0]
             : [img.data[i]! / a, img.data[i + 1]! / a, img.data[i + 2]! / a] };
};

describe("the wetland hatch", () => {
  const img = hatchImage(WETLAND);

  it("is a square tile of RGBA at 2x", () => {
    expect(img.width).toBe(img.height);
    expect(img.pixelRatio).toBe(2);
    expect(img.data.length).toBe(img.width * img.height * 4);
  });

  it("tiles seamlessly, which is the only thing that makes it a pattern", () => {
    // Stripes run at 45 degrees, so a row leaving the right edge must re-enter at the left
    // one row down. If it does not the seam shows as a visible grid over every marsh —
    // and it shows only once the image is REPEATED, which no single-tile eyeball catches.
    for (let y = 0; y < img.height - 1; y++) {
      const right = at(img, img.width - 1, y);
      const left = at(img, 0, y + 1);
      expect(Math.abs(right.a - left.a), `row ${y} seam`).toBeLessThan(0.02);
    }
  });

  it("is translucent everywhere, so the basemap still reads through it", () => {
    // A wetland is context, never an answer. An opaque patch of it hides the river inside.
    let min = 1, max = 0;
    for (let i = 3; i < img.data.length; i += 4) {
      const a = img.data[i]! / 255;
      min = Math.min(min, a); max = Math.max(max, a);
    }
    expect(min).toBeGreaterThan(0.3);
    expect(max).toBeLessThan(0.85);
  });

  it("has stripes: some pixels are darker than the ground and some are the ground", () => {
    const alphas = new Set<number>();
    for (let i = 3; i < img.data.length; i += 4) alphas.add(img.data[i]!);
    expect(alphas.size).toBeGreaterThan(2);      // ground, stripe, and the blend between
  });

  it("is woven from the colour it is given, not a fixed green", () => {
    // The token names a colour and the theme owns it; a hard-coded green would be right in
    // one theme and wrong in the other two.
    const blue = hatchImage(colour("light", "color.status.base"));
    const groundish = at(blue, 2, 0).straight;
    expect(groundish[2]).toBeGreaterThan(groundish[0]!);
    expect(groundish[2]).toBeGreaterThan(groundish[1]!);
  });

  it("survives a colour it cannot parse rather than emitting garbage", () => {
    const grey = hatchImage("not a colour");
    expect(grey.data.length).toBe(grey.width * grey.height * 4);
    expect(at(grey, 0, 0).a).toBeGreaterThan(0);
  });

  it("carries full-strength colour beside its alpha, NOT premultiplied", () => {
    /*
     * `addImage` takes an ImageData-shaped buffer and ImageData is non-premultiplied —
     * `pill.ts` has always written it that way and renders correctly. This test asserted
     * the opposite and so certified the bug: every channel was scaled by the alpha here and
     * then again by the renderer, and the crimson closure hatch drew as a muddy grey.
     *
     * The check: on the stripe, the colour must be the colour asked for, whatever its alpha.
     */
    const green = hatchImage(WETLAND, 0, 0.55, 0);
    const [r, g, b] = bytes(WETLAND);
    let strongest = -1, at = 0;
    for (let i = 0; i < green.data.length; i += 4)
      if (green.data[i + 3]! > strongest) { strongest = green.data[i + 3]!; at = i; }
    expect(strongest).toBeGreaterThan(0);
    expect(green.data[at]).toBe(r);
    expect(green.data[at + 1]).toBe(g);
    expect(green.data[at + 2]).toBe(b);
  });
});
