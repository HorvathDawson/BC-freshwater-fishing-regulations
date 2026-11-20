import { describe, expect, it } from "vitest";
import { labelAnchor, MARK_LABEL_FONT } from "./marks";

describe("(T929) a boundary mark's label stays inside the map by how it hangs, never by moving its point", () => {
  it("centred where there is room; from its corner, hanging inward, where a half-width would hang over an edge", () => {
    expect([200, 100, 290, 40, 370, -10, 450].map((x) => labelAnchor(x, 390)))
      .toEqual(["bottom", "bottom", "bottom", "bottom-left", "bottom-right", "bottom-left", "bottom-right"]);
  });
});

describe("(T151 review) a mark's label is normal weight on both maps", () => {
  it("the one value, whole (that both maps read it is a source scan: tools/one-parts-list.test.ts)", () => {
    expect(MARK_LABEL_FONT).toEqual({ web: "400 12px/1.25 Archivo_400Regular,sans-serif", native: "Archivo_400Regular" });
  });
});
