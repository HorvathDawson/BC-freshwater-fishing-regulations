import { describe, expect, it } from "vitest";
import { spriteStaircase } from "./sprite";

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
