import { describe, expect, it } from "vitest";
import { highlightGeometry } from "./highlight";

describe("highlightGeometry (T215)", () => {
  const size = [612, 792] as const;
  it("flips a top-left box to PDF space; quad points in Acrobat order (TL TR BL BR)", () => {
    expect(highlightGeometry({ pdf: 23, box: [50, 100, 300, 160], size })).toEqual({
      rect: [50, 632, 300, 692], quadPoints: [50, 692, 300, 692, 50, 632, 300, 632] });
  });
  it("clamps to the page; an empty or off-page box is null (the page opens unhighlighted)", () => {
    expect(highlightGeometry({ pdf: 1, box: [-5, -5, 700, 900], size })!.rect).toEqual([0, 0, 612, 792]);
    expect(highlightGeometry({ pdf: 1, box: [10, 10, 10, 50], size })).toBeNull();
    expect(highlightGeometry({ pdf: 1, box: [700, 10, 800, 50], size })).toBeNull();
  });
});
