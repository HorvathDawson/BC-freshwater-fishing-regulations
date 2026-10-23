/**
 * (T128) the one golden rewrite for the owner's sub-cap wording (subcapTotal.ts): each old form, and the checks that keep
 * the lead honest — a lead naming another number, other water words, or an own rule's "Of the N" where the golden's total
 * is not an own rule, throws instead of rewriting.
 */
import { describe, expect, it } from "vitest";
import { parityGate } from "./golden";
import { SUBCAP_REWRITES, subcapSentence, type GoldenTotal } from "./subcapTotal";

const STREAMS: GoldenTotal = { n: 2, where: " in streams", own: false };
const lead = (s: string) => `${s}, up to 9 may be x`;   // the store sentence: only its lead is read

describe("subcapSentence", () => {
  it("'Up to K may be X' → the lead, 'up to K' at the total", () => {
    expect(subcapSentence("Up to 2 may be hatchery brook, brown or cutthroat trout", lead("Of your 2 trout and char a day in streams"), STREAMS))
      .toBe("Of your 2 trout and char a day in streams, up to 2 may be hatchery brook, brown or cutthroat trout");
  });
  it("'Up to K' under a larger total reads 'only K' (tree.py _kind_says); the wild clause and How to tell follow the form", () => {
    const five: GoldenTotal = { n: 5, where: "", own: false };
    expect(subcapSentence("Up to 1 may be hatchery steelhead only — wild steelhead go back How to tell ›", lead("Of your 5 trout and char a day"), five))
      .toBe("Of your 5 trout and char a day, only 1 may be hatchery steelhead — wild steelhead go back How to tell ›");
    expect(subcapSentence("Up to 2 may be hatchery steelhead only — wild steelhead go back How to tell ›", lead("Of your 2 trout a day in streams"), STREAMS))
      .toBe("Of your 2 trout a day in streams, up to 2 may be hatchery steelhead only — wild steelhead go back How to tell ›");
    expect(subcapSentence("Only 1 of the 2 may be hatchery steelhead — wild steelhead go back", lead("Of your 2 trout and char a day in streams"), STREAMS))
      .toBe("Of your 2 trout and char a day in streams, only 1 may be hatchery steelhead — wild steelhead go back How to tell ›");
  });
  it("'Only K of the T[ from streams]' and '(part of the T from streams)' carry their own T, which must be the lead's", () => {
    expect(subcapSentence("Only 1 of the 2 from streams may be Dolly Varden/bull trout", lead("Of your 2 trout and char a day in streams"), STREAMS))
      .toBe("Of your 2 trout and char a day in streams, only 1 may be Dolly Varden/bull trout");
    expect(subcapSentence("Up to 2 hatchery rainbow trout (part of the 2 from streams)", lead("Of your 2 trout a day in streams"), STREAMS))
      .toBe("Of your 2 trout a day in streams, up to 2 may be hatchery rainbow trout");
    expect(() => subcapSentence("Only 1 of the 4 may be lake trout", lead("Of your 2 trout and char a day in streams"), STREAMS)).toThrow(/names 2/);
  });
  it("an own rule: 'under <the water>’s rule', and 'Of the N' only there", () => {
    const own: GoldenTotal = { n: 1, where: " under this lake’s rule", own: true };
    expect(subcapSentence("Up to 1 may be brook trout", lead("Of the 1 a day under this lake’s rule"), own))
      .toBe("Of the 1 a day under this lake’s rule, up to 1 may be brook trout");
    expect(() => subcapSentence("Up to 1 may be brook trout", lead("Of the 1 a day"), { n: 1, where: "", own: false })).toThrow(/own rule/);
  });
  it("mutations throw: another number, other water words, an unknown old form, a store sentence naming no total", () => {
    expect(() => subcapSentence("Up to 2 may be rainbow trout", lead("Of your 3 trout and char a day in streams"), STREAMS)).toThrow(/names 3/);
    expect(() => subcapSentence("Up to 2 may be rainbow trout", lead("Of your 2 trout and char a day"), STREAMS)).toThrow(/says ""/);
    expect(() => subcapSentence("Keep 2 rainbow trout", lead("Of your 2 trout and char a day in streams"), STREAMS)).toThrow(/no known form/);
    expect(() => subcapSentence("Up to 2 may be rainbow trout", "Up to 2 may be rainbow trout", STREAMS)).toThrow(/names no total/);
  });
});

const gate = await parityGate();
describe.skipIf(gate.skip)("subcapRuled over the paired store", () => {
  it("rewrote kind sentences in the golden (the release names totals), at least one in every state it touched", () => {
    expect(Object.keys(SUBCAP_REWRITES).length).toBeGreaterThan(0);
    expect(Object.values(SUBCAP_REWRITES).every((n) => n > 0)).toBe(true);
  });
});
