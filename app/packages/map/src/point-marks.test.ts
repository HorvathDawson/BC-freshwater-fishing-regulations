import { describe, expect, it } from "vitest";
import { markBoxImage, markHit, marksData, pinsData, shiftedLabels } from "./point-marks";

describe("T317 point marks", () => {
  it("the label box is a square card with an ink edge, only its middle stretching", () => {
    const g = markBoxImage("#ffffff", "#000000");
    const px = (x: number, y: number) => Array.from(g.data.slice((y * g.width + x) * 4, (y * g.width + x) * 4 + 4));
    expect([px(0, 0), px(12, 12)]).toEqual([[0, 0, 0, 255], [255, 255, 255, 255]]);
    expect([g.stretchX, g.content]).toEqual([[[3, 21]], [3, 3, 21, 21]]);
  });
  it("a tap's mark is the first feature's `i` (a phone may send it as a string); none is null", () => {
    expect([markHit([{ properties: { i: 2 } }]), markHit([{ properties: { i: "1" } }]), markHit([{}])]).toEqual([2, 1, null]);
  });
  it("a label is moved only where it would hang over an edge; an unlabelled mark has no label point", async () => {
    const marks = [{ lon: 0, lat: 0, label: "Mid", a11y: "", on: false }, { lon: 1, lat: 0, label: "Edge", a11y: "", on: false },
                   { lon: 2, lat: 0, label: null, a11y: "", on: true }];
    const at = await shiftedLabels(marks, 400, (lon) => [lon === 0 ? 200 : 380, 50], (x, y) => [x, y]);
    expect(at).toEqual([[0, 0], [300, 50], null]);
    expect(marksData(marks, at).features.length).toBe(5);
  });
  it("pins carry their tone (the accent by default) and the marker is last, over them", () => {
    const fc = pinsData({ lat: 1, lon: 2 }, [{ lat: 3, lon: 4 }], "#5F26E0");
    expect(fc.features.map((f) => (f as { properties: { kind: string; tone: string } }).properties))
      .toEqual([expect.objectContaining({ kind: "pin", tone: "#5F26E0" }), expect.objectContaining({ kind: "marker" })]);
  });
});
