import { describe, expect, it } from "vitest";
import { bandAt, PENTADS, standing, standingWord, TRUST_FLOOR, trustWord,
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

describe("pentad bands (T107: the feed's `band_for`, pentad CENTRES, wrapping)", () => {
  const a: Band = [10, 20, 30, 40, 50];
  const b: Band = [20, 30, 40, 50, 60];
  const pentads: (Band | null)[] = Array.from({ length: PENTADS }, () => null);
  pentads[0] = a;
  pentads[1] = b;
  const close = (got: Band | null, want: number[]) => {
    expect(got).not.toBeNull();
    got!.forEach((x, i) => expect(x).toBeCloseTo(want[i]!, 9));
  };
  it("reads between pentad CENTRES: day 4 is a tenth of the way from pentad 0 to pentad 1", () => {
    // Mutation pin: dropping the `- 0.5` centre offset reads day 4 as 3/5 of the way (16, 26, …) and fails here.
    close(bandAt(pentads, 4), [11, 21, 31, 41, 51]);
    close(bandAt(pentads, 8), [19, 29, 39, 49, 59]);
  });
  it("falls back to the neighbour when one end has no record", () => {
    expect(bandAt(pentads, 1)).toEqual(a);   // pentad 72 is empty: pentad 0 answers
    expect(bandAt(pentads, 9)).toEqual(b);   // pentad 2 is empty: pentad 1 answers
  });
  it("is null with no envelope at all", () => {
    expect(bandAt(Array.from({ length: PENTADS }, () => null), 100)).toBeNull();
  });
  it("wraps at New Year: Dec 31 and Jan 1 lie between pentad 72 and pentad 0, a fifth of the gap apart", () => {
    // Mutation pin: clamping instead of wrapping (the old reader) leaves day 1 on pentad 0 alone (0) and day 365 on
    // pentad 72 alone (100), and fails here.
    const ring: (Band | null)[] = Array.from({ length: PENTADS }, () => [7, 7, 7, 7, 7] as Band);
    ring[72] = [100, 100, 100, 100, 100];
    ring[0] = [0, 0, 0, 0, 0];
    close(bandAt(ring, 365), [70, 70, 70, 70, 70]);   // 31 Dec, a common year
    close(bandAt(ring, 366), [50, 50, 50, 50, 50]);   // 31 Dec, a leap year
    close(bandAt(ring, 1), [50, 50, 50, 50, 50]);     // 1 Jan
    close(bandAt(ring, 2), [30, 30, 30, 30, 30]);
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
  // `pipeline.tools.emit.gauge_policy`. `--check` in CI fails if the two drift.
  it("carries the floors the pipeline banded on", () => {
    expect(TRUST_FLOOR).toEqual({ good: 0.10, fair: 0.01, weak: 0.001 });
  });

  it("has words for every band the bundle can store", () => {
    expect(["good", "fair", "weak"].map((b) => trustWord(b as "good")))
      .toEqual(["this water", "a major branch of it", "the trend, not the level"]);
  });

  it("has no band meaning `no gauge` — that is an absent row, not a value", () => {
    expect(Object.keys(TRUST_FLOOR)).not.toContain("none");
  });
});
