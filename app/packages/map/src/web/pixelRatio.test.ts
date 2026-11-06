import { describe, expect, it } from "vitest";
import { WEB_MAX_PIXEL_RATIO, webPixelRatio } from "./pixelRatio";

describe("the web map's pixel ratio (T420)", () => {
  it("is the device's up to 2, never more", () => {
    expect(WEB_MAX_PIXEL_RATIO).toBe(2);
    expect(webPixelRatio(1)).toBe(1);
    expect(webPixelRatio(1.5)).toBe(1.5);
    expect(webPixelRatio(2)).toBe(2);
    expect(webPixelRatio(2.625)).toBe(2);
    expect(webPixelRatio(3)).toBe(2);
  });
  it("reads a missing or broken device ratio as 1", () => {
    expect(webPixelRatio(0)).toBe(1);
    expect(webPixelRatio(Number.NaN)).toBe(1);
  });
});
