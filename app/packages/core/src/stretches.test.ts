/**
 * A river shown as the stretches that differ.
 *
 * The Fraser rendered 201 identical rows, because the atlas cuts a river at confluences,
 * lake outlets, gauge splits and a 25 km cap — none of which is a reason a REGULATION
 * changes. These tests are about which of those cuts a reader should ever see.
 */
import { describe, expect, it } from "vitest";
import { regimesOf, runsOfSameRules, stretchLabel } from "./stretches";

const s = (section: string, setId: number | null) => ({ section, setId });

describe("stretches", () => {
  it("collapses a river covered end to end by one rule set into one stretch", () => {
    const got = runsOfSameRules([s("a", 7), s("b", 7), s("c", 7)]);
    expect(got).toHaveLength(1);
    expect(got[0]!.sections.map((x) => x.section)).toEqual(["a", "b", "c"]);
  });

  it("does NOT merge two runs of the same rules that are apart on the river", () => {
    /*
     * The case a group-by gets wrong. Closed, open, closed again is three stretches in
     * three places; merging the closed pieces because they share a rule set would draw a
     * stretch that does not exist on the ground.
     */
    const got = runsOfSameRules([s("a", 1), s("b", 2), s("c", 1)]);
    expect(got.map((x) => x.setId)).toEqual([1, 2, 1]);
  });

  it("treats unregulated water as one stretch, not a hundred", () => {
    // "The rest of the river, under the general rules" is one answer.
    const got = runsOfSameRules([s("a", null), s("b", null), s("c", 3)]);
    expect(got).toHaveLength(2);
    expect(got[0]!.sections).toHaveLength(2);
  });

  it("never merges regulated water into unregulated", () => {
    // `null` equals only `null` — otherwise a closure would absorb open water beside it.
    const got = runsOfSameRules([s("a", null), s("b", 3)]);
    expect(got.map((x) => x.setId)).toEqual([null, 3]);
  });

  it("keeps every section, in order, across the stretches", () => {
    const input = [s("a", 1), s("b", 1), s("c", 2), s("d", null), s("e", 2)];
    const got = runsOfSameRules(input);
    expect(got.flatMap((x) => x.sections.map((y) => y.section)))
      .toEqual(["a", "b", "c", "d", "e"]);
  });

  it("numbers stretches from the mouth", () => {
    const got = runsOfSameRules([s("a", 1), s("b", 2)]);
    expect(got.map((x) => x.index)).toEqual([0, 1]);
  });

  it("answers for an empty river without inventing a stretch", () => {
    expect(runsOfSameRules([])).toEqual([]);
  });

  describe("the label", () => {
    it("uses the ends where a rule named them", () => {
      expect(stretchLabel({ lower: "Eve River", upper: "Hwy 19" }, 0, 3))
        .toBe("Eve River → Hwy 19");
    });

    it("does not call a whole river 'Stretch 1 of 1'", () => {
      expect(stretchLabel({}, 0, 1)).toBe("the mouth → the source");
    });

    it("numbers the unnamed rather than repeating one string", () => {
      expect(stretchLabel({}, 1, 4)).toBe("Stretch 2 of 4");
    });
  });
});


describe("regimes", () => {
  it("reports a water as its distinct rule sets, not its contiguous runs", () => {
    /*
     * The Fraser: 248 sections, 102 contiguous runs (it spans 151 blue lines), 16 rule
     * sets. A list of 102 is barely better than a list of 248.
     */
    const got = regimesOf([s("a", 1), s("b", 2), s("c", 1), s("d", 2), s("e", 1)]);
    expect(got.map((x) => x.setId)).toEqual([1, 2]);
  });

  it("keeps the separate pieces of a regime apart", () => {
    // So a row can say "in 3 places" instead of implying one continuous stretch.
    const got = regimesOf([s("a", 1), s("b", 2), s("c", 1)]);
    expect(got[0]!.runs).toHaveLength(2);
    expect(got[1]!.runs).toHaveLength(1);
  });

  it("carries every section of a regime, across its pieces", () => {
    const got = regimesOf([s("a", 1), s("b", 2), s("c", 1)]);
    expect(got[0]!.sections.map((x) => x.section)).toEqual(["a", "c"]);
  });

  it("keeps unregulated water as its own regime", () => {
    const got = regimesOf([s("a", null), s("b", 1), s("c", null)]);
    expect(got.map((x) => x.setId)).toEqual([null, 1]);
    expect(got[0]!.runs).toHaveLength(2);
  });

  it("orders regimes by where they first appear from the mouth", () => {
    const got = regimesOf([s("a", 9), s("b", 3)]);
    expect(got.map((x) => x.setId)).toEqual([9, 3]);
    expect(got.map((x) => x.index)).toEqual([0, 1]);
  });
});
