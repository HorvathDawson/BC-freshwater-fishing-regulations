import { describe, expect, it } from "vitest";
import { labelShift } from "./marks";

describe("a boundary mark's label stays inside the map (the prototype's --dx)", () => {
  it("centred where there is room; pushed in from either edge by what would hang over", () => {
    expect([200, 100, 290, 40, 370, -10].map((x) => labelShift(x, 390))).toEqual([0, 0, 0, 60, -80, 110]);
  });
});
