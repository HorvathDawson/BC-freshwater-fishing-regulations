import { describe, expect, it } from "vitest";
import { labelShift, MARK_LABEL_FONT } from "./marks";

describe("a boundary mark's label stays inside the map (the prototype's --dx)", () => {
  it("centred where there is room; pushed in from either edge by what would hang over", () => {
    expect([200, 100, 290, 40, 370, -10].map((x) => labelShift(x, 390))).toEqual([0, 0, 0, 60, -80, 110]);
  });
});

describe("(T151 review) a mark's label is normal weight on both maps", () => {
  it("the one value, whole (that both maps read it is a source scan: tools/one-parts-list.test.ts)", () => {
    expect(MARK_LABEL_FONT).toEqual({ web: "400 12px/1.25 Archivo_400Regular,sans-serif", native: "Archivo_400Regular" });
  });
});
