import { describe, expect, it } from "vitest";
import { accessNotice, accessOf, MOSTLY_INSIDE, noPublicAccess, ACCESS_WORDS, TOUCH_ONLY, type AccessArea,
         type SectionAccess }
  from "./access";

const COQ: AccessArea = { tier: "no_access", name: "Coquitlam Watershed" };
const SEY: AccessArea = { tier: "no_access", name: "Seymour Watershed" };
const UBC: AccessArea = { tier: "permit", name: "UBC Malcolm Knapp Research Forest" };
const CAP5: AccessArea = { tier: "reserve", name: "Capilano 5" };
const RES: AccessArea = { tier: "reserve", name: "" };
const PRIV: AccessArea = { tier: "private", name: "" };

/** A section access table built from rows (sid, area, permille), the shape paint carries. */
function table(rows: [number, AccessArea, number][]): SectionAccess {
  const areas = [...new Set(rows.map((r) => r[1]))];
  return accessOf(areas, rows.map(([s]) => s), rows.map(([, a]) => areas.indexOf(a)), rows.map(([, , p]) => p));
}

describe("section access: the fact (T54)", () => {
  const t = table([[1, COQ, 1000], [2, COQ, 400], [2, SEY, 300], [3, COQ, 500], [4, PRIV, 1000], [4, CAP5, 200],
                   [5, UBC, 1000], [6, COQ, 501]]);

  it("reads a section's rows back, by handle, as the map reports it", () => {
    expect(t.of(2)).toEqual([{ area: COQ, share: 0.4 }, { area: SEY, share: 0.3 }]);
    expect(t.of("4")).toEqual([{ area: PRIV, share: 1 }, { area: CAP5, share: 0.2 }]);
    expect(t.of(9)).toEqual([]);
    expect(t.of("x")).toEqual([]);
  });

  it("colours a section only when MOST of it is inside no-access land, its no-access areas summed", () => {
    expect(MOSTLY_INSIDE).toBe(0.5);
    expect([1, 2, 3, 4, 5, 6, 9].map((s) => noPublicAccess(t, s))).toEqual([true, true, false, false, false, true, false]);
    expect(noPublicAccess(null, 1)).toBe(false);
  });

  it("MUTATION PIN: private land, a reserve or permit-only land never turn a stream to the fourth colour", () => {
    // every Tier B area, wholly over a section: none of them is no access
    for (const a of [PRIV, CAP5, RES, UBC]) expect([a.tier, noPublicAccess(table([[7, a, 1000]]), 7)]).toEqual([a.tier, false]);
  });

  it("refuses columns that do not line up, or a sid out of order", () => {
    expect(() => accessOf([COQ], [1, 2], [0], [10])).toThrow("section access: 2 sids, 1 areas, 1 shares");
    expect(() => accessOf([COQ], [2, 1], [0, 0], [10, 10])).toThrow("section access: sid 1 after 2");
    expect(() => accessOf([COQ], [1], [3], [10])).toThrow("section access: area 3 of 1");
  });
});

describe("section access: the page's words (T54, core owns them)", () => {
  it("a section mostly inside: the banner, whole", () => {
    expect(accessNotice([{ area: COQ, share: 1 }])).toEqual({
      tier: "no_access",
      text: "No public access: this section is inside Coquitlam Watershed. The regulations below are the Province's, "
          + "but you may not enter this area to fish.",
      link: "Show area on map",
    });
  });

  it("a section partly inside: the banner says partly", () => {
    expect(accessNotice([{ area: SEY, share: 0.3 }])?.text).toBe(
      "No public access: this section is partly inside Seymour Watershed. The regulations below are the Province's, "
      + "but you may not enter that area to fish.");
  });

  it("two no-access areas are both named, and their shares add up to mostly", () => {
    expect(accessNotice([{ area: COQ, share: 0.4 }, { area: SEY, share: 0.3 }])?.text).toBe(
      "No public access: this section is inside Coquitlam Watershed and Seymour Watershed. The regulations below are "
      + "the Province's, but you may not enter this area to fish.");
  });

  it("no access wins over a gentler note on the same section", () => {
    expect(accessNotice([{ area: PRIV, share: 1 }, { area: COQ, share: 0.9 }])?.tier).toBe("no_access");
  });

  it("private land: the gentle note, whole", () => {
    expect(accessNotice([{ area: PRIV, share: 0.2 }])).toEqual({
      tier: "restricted",
      text: "Access may be restricted here: this section crosses private land. See the map.",
      link: "See the map",
    });
  });

  it("a reserve by name, a nameless reserve, permit-only land with its permit — in the tiers' order", () => {
    expect(accessNotice([{ area: CAP5, share: 1 }])?.text).toBe(
      "Access may be restricted here: this section crosses an Indian reserve (Capilano 5). See the map.");
    expect(accessNotice([{ area: RES, share: 1 }])?.text).toBe(
      "Access may be restricted here: this section crosses an Indian reserve. See the map.");
    expect(accessNotice([{ area: UBC, share: 1 }])?.text).toBe(
      "Access may be restricted here: this section crosses permit-only land (UBC Malcolm Knapp Research Forest), "
      + "where a permit is needed. See the map.");
    expect(accessNotice([{ area: PRIV, share: 0.1 }, { area: CAP5, share: 0.5 }, { area: UBC, share: 0.2 }])?.text).toBe(
      "Access may be restricted here: this section crosses permit-only land (UBC Malcolm Knapp Research Forest), "
      + "where a permit is needed, an Indian reserve (Capilano 5) and private land. See the map.");
  });

  it("nothing to say: no notice — and a touch under 1% is nothing to say", () => {
    expect(TOUCH_ONLY).toBe(0.01);
    expect(accessNotice([])).toBeNull();
    expect(accessNotice([{ area: COQ, share: 0.009 }, { area: PRIV, share: 0.001 }])).toBeNull();
    expect(accessNotice([{ area: COQ, share: 0.009 }, { area: PRIV, share: 0.01 }])?.tier).toBe("restricted");
  });

  it("the legend's word is the status's", () => {
    expect(ACCESS_WORDS.legend).toBe("No public access");
  });
});
