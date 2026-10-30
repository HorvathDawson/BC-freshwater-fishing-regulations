/**
 * T294: the rating scale and the prediction from rated visits — weights, ties, one visit, no rating, and the words,
 * on fixed inputs (AGENTS 33). Timestamps from LOCAL parts so the days read the same in any zone.
 */
import { describe, expect, it } from "vitest";
import { likeDay, type LikeConditions } from "./likeVisit";
import { isRating, PREDICT_POWER, PREDICT_WORDS, predictDay, predictLabel, predictReasons, predictText, RATING,
         ratingWord, VISIT_WORDS, visitDateText, type RatedLike } from "./spotVisits";

const NOW = new Date(2026, 9, 22, 10).getTime();
const THEN: LikeConditions = { flow: 0.42, tempC: 14, rainMm: 2, windKph: 8, cloudPct: 40, pressureHpa: 1012 };
const DAY = "2026-10-22";
/** A rated visit whose conditions differ from the day by `over`, scored with the real `likeDay`. */
const rated = (id: string, rating: number, at: number, over: Partial<LikeConditions> = {}): RatedLike =>
  ({ id, rating, visitedAt: at, day: likeDay({ ...THEN, ...over }, { date: DAY, ...THEN }) });
const OCT12 = new Date(2026, 9, 12, 8).getTime(), SEP3 = new Date(2026, 8, 3, 8).getTime(),
  AUG20 = new Date(2025, 7, 20, 8).getTime();

describe("the rating scale (owner: default 1–5 stars)", () => {
  it("is 1–5 with a word each; only whole numbers in range are ratings", () => {
    expect(RATING).toEqual({ max: 5, words: ["Poor", "Fair", "OK", "Good", "Great"] });
    expect([1, 5].map(isRating)).toEqual([true, true]);
    expect([0, 6, 2.5, null, "4", undefined].map(isRating)).toEqual([false, false, false, false, false, false]);
    expect([1, 2.4, 2.5, 4.6, 5].map(ratingWord)).toEqual(["Poor", "Fair", "OK", "Great", "Great"]);
  });

  it("names every star to a screen reader", () => {
    expect(VISIT_WORDS.rateLabel(4)).toBe("Rate this visit 4 of 5: Good");
    expect(VISIT_WORDS.clearLabel(4)).toBe("Rated 4 of 5. Press to clear the rating");
    expect(VISIT_WORDS.chipLabel("12 Oct", null)).toBe("Visit on 12 Oct, not rated");
    expect(VISIT_WORDS.chipLabel("12 Oct", 5)).toBe("Visit on 12 Oct, rated 5 of 5");
  });
});

describe("predictDay — similarity-weighted ratings", () => {
  it("pins the power: a visit twice as alike counts 2³ times as much", () => {
    expect(PREDICT_POWER).toBe(3);
  });

  it("one rated visit predicts its own rating, resting on 1", () => {
    const p = predictDay(DAY, [rated("a", 4, OCT12, { flow: 0.67 })]);
    expect([p.value, p.rests, p.top?.id, p.far]).toEqual([4, 1, "a", false]);
    expect(predictText(p)).toBe("4.0★");
  });

  it("two equally alike visits predict their mean", () => {
    const p = predictDay(DAY, [rated("a", 5, OCT12), rated("b", 2, SEP3)]);
    expect(p.value).toBe(3.5);
  });

  it("weighs by likeness cubed: 100% and 50% alike → (1·5 + 0.125·1) / 1.125 = 4.6", () => {
    // flow, rain, cloud, pressure all a full span apart, temp + wind the same: 20% alike — too low; build 50% instead:
    // flow + rain a full span apart (0.5 of the weight lost) → 50 %
    const half = rated("b", 1, SEP3, { flow: 0.92, rainMm: 17 });
    expect(half.day.score).toBe(50);
    const p = predictDay(DAY, [rated("a", 5, OCT12), half]);
    expect(p.value).toBe(Math.round(((1 * 5 + 0.125 * 1) / 1.125) * 10) / 10);
    expect(p.value).toBe(4.6);
    expect(p.top?.id).toBe("a");
  });

  it("a tie on likeness names the more recent visit", () => {
    const p = predictDay(DAY, [rated("old", 2, AUG20), rated("new", 4, OCT12), rated("mid", 5, SEP3)]);
    expect(p.top?.id).toBe("new");
    expect(p.value).toBe(3.7);
  });

  it("an unrated or uncomparable visit is left out; none left → no prediction", () => {
    const nothing: RatedLike = { id: "x", rating: 5, visitedAt: OCT12,
      day: likeDay({ flow: null, tempC: null, rainMm: null, windKph: null, cloudPct: null }, { date: DAY, ...THEN }) };
    const unrated = { ...rated("u", 3, OCT12), rating: null as unknown as number };
    expect(predictDay(DAY, [nothing, unrated])).toEqual({ date: DAY, value: null, rests: 0, top: null, far: false });
    expect(predictDay(DAY, [nothing, unrated, rated("a", 2, SEP3)]).rests).toBe(1);
    expect(predictText(predictDay(DAY, []))).toBe("—");
  });

  it("every visit 0% alike: no number, and said as not much like any", () => {
    const base = rated("z", 5, OCT12);
    const zero: RatedLike = { ...base, day: { ...base.day, score: 0 } };
    const p = predictDay(DAY, [zero]);
    expect([p.value, p.rests, p.far]).toEqual([null, 1, true]);
  });

  it("far when the most alike visit is below 'similar' (65%)", () => {
    const p = predictDay(DAY, [rated("a", 3, OCT12, { flow: 0.92, rainMm: 17, cloudPct: 100 })]);
    expect(p.top!.day.score! < 65).toBe(true);
    expect(p.far).toBe(true);
  });
});

describe("the words", () => {
  it("says the visit the day is most like, then why, never fish", () => {
    const p = predictDay(DAY, [rated("a", 5, OCT12, { tempC: 18 }), rated("b", 2, AUG20, { flow: 0.8 })]);
    expect(predictReasons(p, NOW)).toEqual([
      "Most like your 5★ visit on 12 Oct (97% alike)",
      "Flow: the same", "Rain in the 3 days before: the same", "Cloud: the same", "Pressure: the same",
      "Air temperature: 4° cooler", "Wind: the same"]);
    expect(predictLabel(p, NOW)).toBe("Today, 22 Oct: about 3.9 of 5, good, from 2 rated visits.");
    expect(predictLabel(predictDay(DAY, []), NOW)).toBe("Today, 22 Oct: None of your rated visits can be compared with this day.");
    expect(visitDateText(AUG20, NOW)).toBe("20 Aug 2025");
    expect(PREDICT_WORDS.restsOn(1)).toBe("From 1 rated visit");
    expect(PREDICT_WORDS.unratedLeftOut(2)).toBe("2 visits are not rated and are left out.");
    // it is how your ratings compare — the words never promise or count fish
    const all = JSON.stringify([PREDICT_WORDS.intro, PREDICT_WORDS.top, PREDICT_WORDS.best, PREDICT_WORDS.far]);
    expect(all).toContain("not a promise of fish");
    expect(/\b(catch|bite|will be good)\b/i.test(all)).toBe(false);
  });
});
