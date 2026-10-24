/**
 * T06: a rule as a line of words (page_v36 bands, bandTxt, closedGate, covers, describe, cutWords), over the real rule
 * rows in testing/ruleRows.ts. The expected sentences are the ticket's table; describe.parity.test.ts checks every one
 * of them (and the other 709) against page v36's own output in app/fixtures/golden/hydrate.json.
 */
import { describe as group, expect, it } from "vitest";
import { bands, bandTxt, closedGate, covers, cutWords, describe } from "./describe";
import { noRule, ruleView } from "./rules";
import { BOOK } from "./testing/speciesBook";
import { RULE_ROWS } from "./testing/ruleRows";
import type { ExportRule, RuleFacts } from "./inputs";

const view = (rix: number) => ruleView(rix, RULE_ROWS[rix]![0], RULE_ROWS[rix]![1], BOOK, "reach");
/** A real row with some fields or facts changed — for the branches no rule in the fixture takes. */
const edit = (rix: number, fields: Record<string, unknown>, facts: Partial<RuleFacts> = {}) => {
  const [e, f] = RULE_ROWS[rix]!;
  const exp: ExportRule = { ...e, fields: { ...e.fields, ...fields } };
  return ruleView(rix, exp, { ...f, ...facts }, BOOK, "reach");
};

group("describe: page v36's sentence per kind (T06 table)", () => {
  const TABLE: readonly (readonly [number, string])[] = [
    [529, "No fishing for any game fish, Jun 1–30"],
    [522, "No fishing for any game fish"],
    [1367, "Release every kokanee (Mon, Tue, Wed, Thu, Fri)"],
    [576, "No fishing for any game fish (60 min after sunset–60 min before sunrise)"],
    [1539, "No fishing for any game fish, Jul 15–Aug 31 (12:00–00:00)"],
    [3083, "Keep 20 bass a day, shared"],
    [1366, "Keep 5 kokanee a day (Sat, Sun)"],
    [3095, "Inside the day’s total: at most 2 hatchery trout and char from streams"],
    [3092, "Inside the day’s total: at most 1 over 50 cm (trout and char)"],
    [3097, "Dolly Varden/bull trout and lake trout: release any under 60 cm"],
    [3101, "Hatchery trout and char from streams: release any under 30 cm"],
    [3380, "10 hatchery steelhead a year"],
    [3079, "Possession: 2 days’ quota. Fish at home don’t count"],
    [2281, "Have no more than 2 lake trout in possession. Fish at home don’t count"],
    [3359, "Any fish snagged — even by accident — must be released"],
    [78, "Speed restriction (8 km/h)"],
    [3351, "Within 23 m downstream of the lower entrance to any fishway, canal, obstacle or leap."],
    [3067, "Keep 4 trout a day, shared"],
  ];
  for (const [rix, want] of TABLE) it(`rix ${rix} (${RULE_ROWS[rix]![1].kind})`, () => expect(describe(view(rix), BOOK)).toBe(want));
});

group("describe: the branches the table does not reach", () => {
  it("a gate that is not a closure on one fish releases it; 'all game fish' is replaced only as the WHOLE name", () => {
    expect(describe(edit(529, { species: ["KO"] }, { closure: null }), BOOK)).toBe("Release every kokanee, Jun 1–30");
    expect(describe(edit(529, { species: ["ALL_GAME_FISH", "KO"] }), BOOK)).toBe("No fishing for all game fish and kokanee, Jun 1–30");
    expect(describe(edit(529, { origin: "wild", water: "lake" }), BOOK)).toBe("No fishing for wild any game fish from lakes, Jun 1–30");
  });

  it("a pool with no limit, a pool of one fish, and a pool with a size band kept to none", () => {
    expect(describe(edit(3083, { unlimited: true }), BOOK)).toBe("No limit on bass");
    expect(describe(edit(1366, {}), BOOK)).toBe("Keep 5 kokanee a day (Sat, Sun)");
    expect(describe(edit(3083, { water: "stream" }, { bands: [[0, 30, 0], [30, null, null]] }), BOOK))
      .toBe("Keep 20 bass from streams a day, shared, none under 30 cm");
  });

  it("a sizecap with a band kept to none, and a subcap of several fish joined with 'or'", () => {
    expect(describe(edit(3092, {}, { bands: [[0, 30, 0], [30, 50, null], [50, null, 1]] }), BOOK))
      .toBe("Inside the day’s total: none under 30 cm, at most 1 over 50 cm (trout and char)");
    expect(describe(edit(3095, { species: ["DV", "LT", "EB"], origin: null }), BOOK))
      .toBe("Inside the day’s total: at most 2 Dolly Varden/bull trout, lake trout or brook trout from streams");
  });

  it("a size rule across a middle band", () => {
    expect(describe(edit(3097, {}, { bands: [[0, 30, 0], [30, 50, null], [50, 60, 0], [60, null, null]] }), BOOK))
      .toBe("Dolly Varden/bull trout and lake trout: release any under 30 cm, release any 50–60 cm");
  });

  it("an annual quota by the month, and one that counts only fish over a size", () => {
    expect(describe(edit(3380, { period: "monthly" }), BOOK)).toBe("10 hatchery steelhead a month");
    expect(describe(edit(3380, {}, { bands: [[0, 50, null], [50, null, 10]] }), BOOK))
      .toBe("10 hatchery steelhead over 50 cm a year. Smaller fish don’t count");
  });

  it("a possession rule with no plain sentence says the answers have no words for it", () => {
    expect(describe(edit(3079, {}, { plain: null }), BOOK))
      .toBe("(no words in the answers for z2:possession_quota::possession_quota.r1)");
  });

  it("a missing rule says it is missing (T06 test 6)", () => {
    expect(describe(noRule(7), BOOK)).toBe("(rule 7: missing from the page’s data)");
  });
});

group("bands, bandTxt (T06 test 3), closedGate, covers", () => {
  it("reads the answers' bands with an open top as Infinity", () => {
    expect(bands(view(3092))).toEqual([{ a: 0, b: 50, take: null }, { a: 50, b: Infinity, take: 1 }]);
    expect(bands(view(529))).toEqual([]);
  });

  it("says a band", () => {
    expect(bandTxt(0, 50)).toBe("under 50 cm");
    expect(bandTxt(50, Infinity)).toBe("over 50 cm");
    expect(bandTxt(30, 50)).toBe("30–50 cm");
  });

  it("a closed gate is a gate whose facts say closure", () => {
    expect(closedGate(view(529))).toBe(true);
    expect(closedGate(view(1367))).toBe(false);
    expect(closedGate(view(3083))).toBe(false);
    expect(closedGate(noRule(7))).toBe(false);
  });

  it("covers a fish in the rule's species and not in its exceptions", () => {
    expect(covers(view(3092), "RB")).toBe(true);
    expect(covers(view(3092), "ST")).toBe(false);
    expect(covers(view(3092), "KO")).toBe(false);
  });
});

group("cutWords", () => {
  it("keeps a short text, and cuts a long one at a word with an ellipsis", () => {
    expect(cutWords("No fishing", 20)).toBe("No fishing");
    expect(cutWords("No fishing for any game fish, Jun 1–30", 20)).toBe("No fishing for any…");
    expect(cutWords("Within 23 m downstream of the lower entrance", 30)).toBe("Within 23 m downstream of the…");
    expect(cutWords("Release every kokanee; Mon–Fri only", 26)).toBe("Release every kokanee…");
  });
});
