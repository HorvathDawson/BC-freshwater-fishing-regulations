import { describe, expect, it } from "vitest";
import { bandAt, standing, standingWord, TRUST_FLOOR, trustWord,
         type Band } from "./flow";

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

describe("the trust bands are the pipeline's, not ours", () => {
  // THERE IS NO `gaugeTrust()` HERE ANY MORE, and its absence is the test. The band is
  // decided once, in pipeline/gauges/consume/shed.py, against the full graph and the FWA watershed
  // codes — neither of which ships to a client. What used to live here banded `reach/gauge`
  // asymmetrically with no drainage gate, which is precisely the rule that let SLESSE CREEK
  // NEAR VEDDER CROSSING speak for the Chilliwack.
  //
  // What the app keeps is the vocabulary, generated from that same Python constant by
  // `pipeline.tools.emit_gauge_policy`. `--check` in CI fails if the two drift.
  it("carries the floors the pipeline banded on", () => {
    expect(TRUST_FLOOR).toEqual({ good: 0.10, fair: 0.01, weak: 0.001 });
  });

  it("has a sentence for every band the bundle can store", () => {
    for (const band of ["good", "fair", "weak"] as const) {
      expect(trustWord(band)).toBeTruthy();
    }
  });

  it("has no band meaning `no gauge` — that is an absent row, not a value", () => {
    expect(Object.keys(TRUST_FLOOR)).not.toContain("none");
  });
});
