import { describe, expect, it } from "vitest";
import { accessNeedsWeights, accessOf, pageAccessNotices, partAccess, MOSTLY_INSIDE, noPublicAccess, ACCESS_WORDS, TOUCH_ONLY,
         type AccessArea, type AccessRow, type SectionAccess } from "./access";

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

  it("colours a section only when MOST of it is in no-access land, its no-access areas summed", () => {
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

const row = (area: AccessArea, share: number): AccessRow => ({ area, share });
const TAIL = ", closed to the public. The regulations below don't give you access.";

describe("(T387) the part's areas: one answer for every tap on the part", () => {
  it("each area's share of the WHOLE part, every section weighed by its length — the sections touching nothing included", () => {
    // Nicomen Slough's shape: a reserve and private land on some sections, none on the rest
    expect(partAccess([[row(CAP5, 1), row(PRIV, 0.5)], [row(PRIV, 1)], []], [200, 100, 700]))
      .toEqual([row(CAP5, 0.2), row(PRIV, 0.2)]);
  });

  it("the same area on two rows of a section counts once (summed, capped at the whole); a graze under TOUCH_ONLY never counts", () => {
    expect(partAccess([[row(COQ, 0.6), row(COQ, 0.6), row(SEY, 0.009)]], null)).toEqual([row(COQ, 1)]);
    expect(TOUCH_ONLY).toBe(0.01);
  });

  it("tier order, then the larger share, then the name", () => {
    expect(partAccess([[row(PRIV, 1), row(CAP5, 0.5), row(SEY, 0.25), row(COQ, 0.5), row(UBC, 0.5)], []], [1, 1]))
      .toEqual([row(COQ, 0.25), row(SEY, 0.125), row(UBC, 0.25), row(CAP5, 0.25), row(PRIV, 0.5)]);
  });

  it("sections alike need no length; sections that differ with no lengths (or one not measured) give no share — never a guess", () => {
    expect(accessNeedsWeights([[], [], []])).toBe(false);
    expect(accessNeedsWeights([[row(COQ, 1)], [row(COQ, 1), row(PRIV, 0.001)]])).toBe(false);
    expect(accessNeedsWeights([[row(COQ, 1)], []])).toBe(true);
    expect(accessNeedsWeights([[row(COQ, 1)], [row(COQ, 0.9)]])).toBe(true);
    expect(partAccess([[row(COQ, 0.7)], [row(COQ, 0.7)]], null)).toEqual([row(COQ, 0.7)]);
    const unknown = [{ area: COQ, share: null }];
    expect(partAccess([[row(COQ, 1)], []], null)).toEqual(unknown);
    expect(partAccess([[row(COQ, 1)], []], [10, null])).toEqual(unknown);
    expect(partAccess([[row(COQ, 1)], []], [0, 0])).toEqual(unknown);
  });
});

describe("(T387) the page's words: core owns them, pinned whole", () => {
  it("all of a one-part water inside: the banner says all of it", () => {
    expect(pageAccessNotices([row(COQ, 1)], "water")).toEqual([{
      tier: "no_access", title: ACCESS_WORDS.legend, brief: "Coquitlam Watershed",
      text: `All of this water is in Coquitlam Watershed${TAIL}` }]);
    expect(ACCESS_WORDS.legend).toBe("No public access");
  });

  it("some of a part: the share, rounded; two areas each with theirs; a nameless area; no share known: \"Some of\"", () => {
    expect(pageAccessNotices([row(COQ, 0.4)], "part")[0]!.text).toBe(`About 40% of this part of the water is in Coquitlam Watershed${TAIL}`);
    expect(pageAccessNotices([row(COQ, 0.3), row(SEY, 0.104)], "part")[0]).toEqual({
      tier: "no_access", title: "No public access", brief: "Coquitlam Watershed and Seymour Watershed",
      text: `About 40% of this part of the water is in Coquitlam Watershed (about 30%) and Seymour Watershed (about 10%)${TAIL}` });
    expect(pageAccessNotices([row({ tier: "no_access", name: "" }, 0.004)], "section")[0]!.text)
      .toBe(`Under 1% of this section is in land with no public access${TAIL}`);
    expect(pageAccessNotices([{ area: COQ, share: null }, { area: SEY, share: null }], "part")[0]!.text)
      .toBe(`Some of this part of the water is in Coquitlam Watershed and Seymour Watershed${TAIL}`);
  });

  it("the gentle note names each cause with its share, permit-only land first, then reserves, then private land", () => {
    expect(pageAccessNotices([row(UBC, 0.2), row(CAP5, 1), row(RES, 0.5), row(PRIV, 0.05)], "part")).toEqual([{
      tier: "restricted", title: ACCESS_WORDS.restricted,
      brief: "permit-only land (UBC Malcolm Knapp Research Forest), Indian reserve (Capilano 5), Indian reserve and private land",
      text: "This part of the water crosses permit-only land (UBC Malcolm Knapp Research Forest, about 20% of it), where a "
        + "permit is needed, an Indian reserve (Capilano 5, all of it), an Indian reserve (about 50% of it) and private "
        + "land (about 5% of it)." }]);
    expect(pageAccessNotices([{ area: CAP5, share: null }, { area: PRIV, share: null }], "water")[0]!.text)
      .toBe("This water crosses an Indian reserve (Capilano 5) and private land.");
    expect(ACCESS_WORDS.restricted).toBe("Access may be restricted");
  });

  it("(T367) both: the banner first, then the note; nothing: nothing", () => {
    expect(pageAccessNotices([row(COQ, 0.9), row(CAP5, 0.4)], "water").map((n) => n.text)).toEqual([
      `About 90% of this water is in Coquitlam Watershed${TAIL}`, "This water crosses an Indian reserve (Capilano 5, about 40% of it)."]);
    expect(pageAccessNotices([], "part")).toEqual([]);
  });

  it("MUTATION PIN (Nicomen Slough): a tap on a section touching nothing says what a tap on the reserve says", () => {
    const sections = [[row(CAP5, 1), row(PRIV, 0.3)], [], [row(PRIV, 1)], []];
    const page = pageAccessNotices(partAccess(sections, [400, 300, 200, 100]), "water");
    expect(page.map((n) => n.text)).toEqual(["This water crosses an Indian reserve (Capilano 5, about 40% of it) and "
      + "private land (about 32% of it)."]);
    // the T54 read (the tapped section's rows alone) gives each tap its own page — what this ticket removed
    const perTap = sections.map((rows) => JSON.stringify(pageAccessNotices(partAccess([rows], null), "water")));
    expect(new Set(perTap).size).toBe(3);
  });
});
