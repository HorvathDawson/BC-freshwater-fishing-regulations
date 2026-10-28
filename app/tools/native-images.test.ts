import { inflateSync } from "node:zlib";
import { describe, expect, it } from "vitest";
import { generatedImages, nativeImageName, nativeImages, themedImageRefs } from "../packages/map/src/images";
import { pngBytes, pngDataUri } from "../packages/map/src/png";

/** The RGBA back out of a PNG `pngBytes` wrote: IHDR's size, IDAT inflated, the filter bytes dropped. */
function decode(png: Uint8Array): { w: number; h: number; rgba: Uint8Array } {
  const dv = new DataView(png.buffer, png.byteOffset);
  const w = dv.getUint32(16), h = dv.getUint32(20);
  let p = 8, idat: number[] = [];
  while (p < png.length) {
    const len = dv.getUint32(p), type = String.fromCharCode(...png.subarray(p + 4, p + 8));
    if (type === "IDAT") idat = idat.concat([...png.subarray(p + 8, p + 8 + len)]);
    p += 12 + len;
  }
  const raw = inflateSync(Uint8Array.from(idat));
  const rgba = new Uint8Array(w * h * 4);
  for (let y = 0; y < h; y++) {
    expect(raw[y * (w * 4 + 1)]).toBe(0);
    rgba.set(raw.subarray(y * (w * 4 + 1) + 1, (y + 1) * (w * 4 + 1)), y * w * 4);
  }
  return { w, h, rgba };
}

describe("png (T74: the generated images reach MapLibre Native as PNG data URIs)", () => {
  it("round-trips every generated image's pixels exactly", () => {
    for (const theme of ["light", "dark"])
      for (const g of generatedImages(theme)) {
        const d = decode(pngBytes(g.width, g.height, g.data));
        expect([g.id, d.w, d.h]).toEqual([g.id, g.width, g.height]);
        expect(Buffer.from(d.rgba).equals(Buffer.from(g.data))).toBe(true);
      }
  });
  it("spans more than one stored block", () => {
    const w = 200, h = 100, px = new Uint8Array(w * h * 4).map((_, i) => (i * 7) & 255);
    expect(Buffer.from(decode(pngBytes(w, h, px)).rgba).equals(Buffer.from(px))).toBe(true);
  });
  it("is a base64 data URI", () => {
    expect(pngDataUri(1, 1, new Uint8Array([1, 2, 3, 4]))).toMatch(/^data:image\/png;base64,[A-Za-z0-9+/]+=*$/);
  });
});

describe("nativeImages", () => {
  it("names every generated image per theme, with its scale and the pill's stretch zones", () => {
    const imgs = nativeImages("dark");
    const ids = generatedImages("dark").map((g) => g.id);
    expect(Object.keys(imgs).sort()).toEqual(ids.map((id) => nativeImageName(id, "dark")).sort());
    // the five hatch layers, the pill and the route's pin
    expect(ids.filter((id) => id.endsWith("-hatch")).sort())
      .toEqual(["eco_reserve-hatch", "no_access-hatch", "no_access_permit-hatch", "park_closed-hatch", "wetland-hatch"]);
    const pill = imgs[nativeImageName("gauge-pill", "dark")]!;
    expect(pill.source.scale).toBe(2);
    expect(pill.stretchX?.length).toBe(1);
    expect(pill.content?.length).toBe(4);
  });
  it("renames image references in a paint or layout value, leaving every other string", () => {
    expect(themedImageRefs("wetland-hatch", "light")).toBe("wetland-hatch.light");
    expect(themedImageRefs(["case", ["get", "x"], "gauge-pill", "other"], "cvd"))
      .toEqual(["case", ["get", "x"], "gauge-pill.cvd", "other"]);
  });
});
