import { describe, expect, it } from "vitest";
import { bandAt, gaugeTrust, standing, standingWord, type Band } from "./flow";

describe("standing", () => {
  it("names the bands the map colours by", () => {
    expect(standing(0.038)).toBe("much-below");   // 08MH001, 30 Aug 2026
    expect(standing(0.004)).toBe("much-below");   // 08MH016, near record low
    expect(standing(0.5)).toBe("normal");
    expect(standing(0.95)).toBe("much-above");
  });
  it("no record is not normal", () => {
    expect(standing(null)).toBe("no-record");
    expect(standingWord(standing(null))).toBe("NO RECORD");
  });
});

describe("pentad bands", () => {
  const a: Band = [10, 20, 30, 40, 50];
  const b: Band = [20, 30, 40, 50, 60];
  const pentads = [a, b, null];
  it("returns the stored band on a pentad boundary", () => {
    expect(bandAt(pentads, 1)).toEqual(a);
  });
  it("interpolates between them", () => {
    expect(bandAt(pentads, 4)?.[0]).toBeCloseTo(16, 6);
  });
  it("falls back to the neighbour when one end has no record", () => {
    expect(bandAt(pentads, 9)).toEqual(b);
  });
});

describe("a gauge has a limit", () => {
  // Real magnitudes from the build.
  const FRASER_AT_HOPE = 273576;
  const CHILLIWACK_AT_VEDDER = 2182;

  it("the Fraser at Hope says nothing about a small creek", () => {
    expect(gaugeTrust(12, FRASER_AT_HOPE)).toBe("none");
  });
  it("but it does describe the Fraser", () => {
    expect(gaugeTrust(273576, FRASER_AT_HOPE)).toBe("good");
  });
  it("a major branch is fair, not good", () => {
    expect(gaugeTrust(60, CHILLIWACK_AT_VEDDER)).toBe("fair");
  });
  it("a headwater trickle in the same valley is weak, not none", () => {
    expect(gaugeTrust(5, CHILLIWACK_AT_VEDDER)).toBe("weak");
  });
  it("a gauge with no magnitude speaks for nothing", () => {
    expect(gaugeTrust(100, 0)).toBe("none");
  });
});
