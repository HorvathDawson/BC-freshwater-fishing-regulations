import { describe, expect, it } from "vitest";
import { spriteGridStaircase, spriteStaircase } from "./sprite";

describe("spriteStaircase", () => {
  it("holds each frame rather than sliding between them", () => {
    const { inputRange, outputRange } = spriteStaircase(4, 100);
    // the two points that bracket frame 2 must map to the same offset
    expect(outputRange[4]).toBe(outputRange[5]);
    expect(outputRange[4]).toBe(-200);
    expect(inputRange[4]!).toBeLessThan(inputRange[5]!);
  });

  it("keeps the input range strictly increasing, which interpolate requires", () => {
    const { inputRange } = spriteStaircase(15, 96);
    for (let i = 1; i < inputRange.length; i++)
      expect(inputRange[i]!).toBeGreaterThan(inputRange[i - 1]!);
  });

  it("shows every frame exactly once across one pass", () => {
    const { outputRange } = spriteStaircase(15, 96);
    const offsets = new Set(outputRange);
    expect(offsets.size).toBe(15);          // frame 0's offset is reused by the wrap point
    for (let i = 0; i < 15; i++) expect(offsets.has(-i * 96)).toBe(true);
  });

  it("returns to the first frame at the end, so the loop has no seam", () => {
    const { inputRange, outputRange } = spriteStaircase(15, 96);
    expect(inputRange.at(-1)).toBe(1);
    expect(outputRange.at(-1)).toBe(0);
    expect(outputRange[0]).toBe(0);
  });

  it("refuses an empty strip rather than producing a degenerate range", () => {
    expect(() => spriteStaircase(0, 96)).toThrow(/at least one frame/);
  });
});

describe("spriteGridStaircase (T814)", () => {
  it("visits every frame of the sheet once, row-major, from one value", () => {
    const { x, y } = spriteGridStaircase(80, 8, 192, 80);
    const seen = x.outputRange.slice(0, -1).filter((_, i) => i % 2 === 0)
      .map((ox, i) => [-ox / 192 + 0, -y.outputRange[i * 2]! / 80 + 0]);
    expect(seen.length).toBe(80);
    expect(seen.slice(0, 10)).toEqual([[0, 0], [1, 0], [2, 0], [3, 0], [4, 0], [5, 0], [6, 0], [7, 0], [0, 1], [1, 1]]);
    expect(seen.at(-1)).toEqual([7, 9]);
    expect(new Set(seen.map((p) => p.join())).size).toBe(80);
  });

  it("holds each frame, keeps the input strictly increasing, and wraps to frame 0 with no seam", () => {
    const { x, y } = spriteGridStaircase(80, 8, 192, 80);
    expect(x.inputRange).toEqual(y.inputRange);
    for (let i = 1; i < x.inputRange.length; i++) expect(x.inputRange[i]!).toBeGreaterThan(x.inputRange[i - 1]!);
    expect([x.outputRange[18], y.outputRange[18]]).toEqual([x.outputRange[19], y.outputRange[19]]);
    expect([x.inputRange.at(-1), x.outputRange.at(-1), y.outputRange.at(-1), x.outputRange[0], y.outputRange[0]])
      .toEqual([1, 0, 0, 0, 0]);
  });

  it("refuses an empty sheet or no columns", () => {
    expect(() => spriteGridStaircase(0, 8, 1, 1)).toThrow(/at least one frame/);
    expect(() => spriteGridStaircase(8, 0, 1, 1)).toThrow(/at least one column/);
  });
});
