import { describe, expect, it } from "vitest";
import { hangBelow, labelAnchor, MARK_LABEL_FONT } from "./marks";

describe("(T929) a boundary mark's label stays inside the map by how it hangs, never by moving its point", () => {
  it("centred where there is room; from its corner, hanging inward, where a half-width would hang over an edge", () => {
    expect([200, 100, 290, 40, 0, 370, 390].map((x) => labelAnchor(x, 390)))
      .toEqual(["bottom", "bottom", "bottom", "bottom-left", "bottom-left", "bottom-right", "bottom-right"]);
  });
  it("a ring off the map hangs its label outward, off the map with it — never back onto the map with no ring", () => {
    expect([-10, -600, 391, 450].map((x) => labelAnchor(x, 390)))
      .toEqual(["bottom-right", "bottom-right", "bottom-left", "bottom-left"]);
  });
});

describe("(T997) the lowest labelled ring hangs its label below it", () => {
  it("of two or more labels the lowest on the screen (a tie: the later) hangs below, the rest above; one label hangs above", () => {
    expect([hangBelow([10, 50]), hangBelow([50, 10]), hangBelow([20, 20]), hangBelow([null, 20, 30, null]), hangBelow([40]),
            hangBelow([40, null])])
      .toEqual([[false, true], [true, false], [false, true], [false, false, true, false], [false], [false, false]]);
  });
  it("below, the same corner rules with a `top` anchor", () => {
    expect([200, 40, 370, -10, 450].map((x) => labelAnchor(x, 390, undefined, true)))
      .toEqual(["top", "top-left", "top-right", "top-right", "top-left"]);
  });
});

describe("(T151 review) a mark's label is normal weight on both maps", () => {
  it("the one value, whole (that both maps read it is a source scan: tools/one-parts-list.test.ts)", () => {
    expect(MARK_LABEL_FONT).toEqual({ web: "400 12px/1.25 Archivo_400Regular,sans-serif", native: "Archivo_400Regular" });
  });
});
