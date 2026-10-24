/**
 * T51: the "Like your visit?" score on fixed inputs — whole values and whole strings (AGENTS 33).
 * Timestamps from LOCAL parts so the days read the same in any zone the suite runs in.
 */
import { describe, expect, it } from "vitest";
import { LIKE_WEIGHTS, likeAgeLine, likeDay, likeDayHead, likeDayLabel, likeDays, likeness, likeNothingThen, likeRead,
         likeVisitHead, type LikeConditions } from "./likeVisit";

const THEN: LikeConditions = { flow: 0.42, tempC: 14, rainMm: 2, windKph: 8, cloudPct: 40 };
const same = (over: Partial<LikeConditions> = {}) => ({ date: "2026-10-24", ...THEN, ...over });
const NOW = new Date(2026, 9, 22, 10).getTime();

describe("likeDay — the score", () => {
  it("an identical day is 100 %, very similar, every factor 100 % alike with its weight as its share", () => {
    expect(likeDay(THEN, same(), { hour: "09:00" })).toEqual({
      date: "2026-10-24", score: 100, read: "very similar", basis: "all", note: null, caveat: null,
      factors: [
        { key: "flow", likeness: 1, share: 0.45,
          line: "Flow: 42nd percentile then, 42nd percentile forecast. The same: 100% alike, 45% of the score." },
        { key: "temp", likeness: 1, share: 0.2,
          line: "Air temperature: 14°C then, 14°C forecast at 09:00. The same: 100% alike, 20% of the score." },
        { key: "rain", likeness: 1, share: 0.2,
          line: "Rain in the 3 days before: 2 mm then, 2 mm forecast. The same: 100% alike, 20% of the score." },
        { key: "wind", likeness: 1, share: 0.1,
          line: "Wind: 8 km/h then, 8 km/h forecast at 09:00. The same: 100% alike, 10% of the score." },
        { key: "cloud", likeness: 1, share: 0.05,
          line: "Cloud: 40% then, 40% forecast at 09:00. The same: 100% alike, 5% of the score." },
      ],
    });
  });

  // Each factor moved ALONE by half its span: the score drops by exactly half that factor's weight.
  it.each([
    ["flow", { flow: 0.67 }, 78, "similar", "Flow: 42nd percentile then, 67th percentile now. 25 points higher: 50% alike, 45% of the score."],
    ["temp", { tempC: 8 }, 90, "very similar", "Air temperature: 14°C then, 8°C forecast. 6° cooler: 50% alike, 20% of the score."],
    ["rain", { rainMm: 9.5 }, 90, "very similar", "Rain in the 3 days before: 2 mm then, 9.5 mm forecast. 7.5 mm wetter: 50% alike, 20% of the score."],
    ["wind", { windKph: 20.5 }, 95, "very similar", "Wind: 8 km/h then, 21 km/h forecast. 13 km/h windier: 50% alike, 10% of the score."],
    ["cloud", { cloudPct: 90 }, 98, "very similar", "Cloud: 40% then, 90% forecast. 50 points cloudier: 50% alike, 5% of the score."],
  ] as const)("%s moving alone lowers the score by half its weight", (key, over, score, read, line) => {
    const d = likeDay(THEN, same(over), { today: true });
    expect([d.score, d.read, d.factors.find((f) => f.key === key)!.line]).toEqual([score, read, line]);
    expect(d.factors.filter((f) => f.key !== key).map((f) => f.likeness)).toEqual([1, 1, 1, 1]);
  });

  it("a factor a whole span or more apart is 0 % alike, never negative", () => {
    expect([likeness("flow", 0.1, 0.9), likeness("temp", 0, 12), likeness("rain", 0, 40)]).toEqual([0, 0, 0]);
    const d = likeDay(THEN, same({ flow: 0.95, tempC: 30, rainMm: 40, windKph: 60, cloudPct: 0 }));
    // every factor past its span but cloud, 40 of 100 points apart: 0.6 × 5 % = 3 %
    expect([d.score, d.read, d.factors.map((f) => f.likeness)]).toEqual([3, "different", [0, 0, 0, 0, 0.6]]);
  });

  it("the reads switch at 85 and 65 (mutation pin on the thresholds)", () => {
    expect([likeRead(0.85), likeRead(0.849), likeRead(0.65), likeRead(0.649)])
      .toEqual(["very similar", "similar", "similar", "different"]);
  });

  it("flow weighs most: one flow move outweighs the same move in any weather factor", () => {
    const moved = (over: Partial<LikeConditions>) => likeDay(THEN, same(over)).score!;
    const flow = moved({ flow: 0.42 + 0.25 });
    expect(Math.max(...(["temp", "rain", "wind", "cloud"] as const).map((k) => LIKE_WEIGHTS[k]))).toBeLessThan(LIKE_WEIGHTS.flow);
    expect([flow, moved({ tempC: 8 }), moved({ rainMm: 9.5 })]).toEqual([78, 90, 90]);
  });
});

describe("likeDay — missing data, said", () => {
  it("no flow at the visit: scored on the weather only, and says so", () => {
    const d = likeDay({ ...THEN, flow: null }, same({ tempC: 8 }));
    expect([d.score, d.read, d.basis, d.note, d.factors[0]!.line, d.factors[1]!.share]).toEqual([
      // temp 50 % alike at a re-normalised 0.2/0.55 share: 1 − 0.5 × 0.3636 = 0.818
      82, "similar", "weather", "No flow then to compare: scored on the weather only.",
      "Flow: no flow then to compare. Not scored.", 0.2 / 0.55]);
  });

  it("no flow forecast for the day: the weather only, said differently", () => {
    const d = likeDay(THEN, same({ flow: null }));
    expect([d.score, d.basis, d.note, d.factors[0]!.line]).toEqual([
      100, "weather", "No flow forecast for this day: scored on the weather only.",
      "Flow: no flow forecast for this day. Not scored."]);
  });

  it("T171: a day ELF forecast is scored, names its model, and carries the low-water caveat", () => {
    const d = likeDay(THEN, same({ flow: 0.17, flowModel: "ELF", flowLowWater: true }));
    expect([d.score, d.basis, d.note, d.caveat, d.factors[0]!.line]).toEqual([
      78, "all", null,
      "ELF is a low-water model: it carries the river's recent fall forward and has no rain or snowmelt in it, so it "
        + "cannot see a rise coming.",
      "Flow: 42nd percentile then, 17th percentile forecast by ELF. 25 points lower: 50% alike, 45% of the score."]);
    // a flood model's day says "forecast" and carries no caveat; nor does today's reading, whoever forecast it
    const c = likeDay(THEN, same({ flow: 0.17, flowModel: "CLEVER", flowLowWater: false }));
    expect([c.caveat, c.factors[0]!.line]).toEqual([null,
      "Flow: 42nd percentile then, 17th percentile forecast. 25 points lower: 50% alike, 45% of the score."]);
    expect(likeDay(THEN, same({ flow: 0.17, flowModel: "ELF", flowLowWater: true }), { today: true }).caveat).toBeNull();
    // no flow that day: no caveat to carry, whatever model was named
    expect(likeDay(THEN, same({ flow: null, flowModel: "ELF", flowLowWater: true })).caveat).toBeNull();
  });

  it("a weather value missing on one side is left out and named, never guessed", () => {
    const d = likeDay({ ...THEN, cloudPct: null }, same({ windKph: null }));
    expect(d.factors.slice(3).map((f) => [f.likeness, f.share, f.line])).toEqual([
      [null, 0, "Wind: no forecast for this day. Not scored."],
      [null, 0, "Cloud: not recorded at the visit. Not scored."],
    ]);
    expect([d.score, d.basis]).toEqual([100, "all"]);
  });

  it("nothing on both sides: no score, and says there is nothing to compare", () => {
    const none = { flow: null, tempC: null, rainMm: null, windKph: null, cloudPct: null };
    const d = likeDay(THEN, { date: "2026-10-24", ...none });
    expect([d.score, d.read, d.basis, d.note]).toEqual([null, null, null, "Nothing to compare for this day."]);
    expect([likeNothingThen(none), likeNothingThen(THEN)]).toEqual([true, false]);
  });
});

describe("the words around it", () => {
  it("five days from today, each with its head", () => {
    expect(likeDays(NOW)).toEqual(["2026-10-22", "2026-10-23", "2026-10-24", "2026-10-25", "2026-10-26"]);
    expect(likeDays(NOW).map((d) => likeDayHead(d, NOW))).toEqual([
      { top: "Today", sub: "22 Oct" }, { top: "Tomorrow", sub: "23 Oct" }, { top: "Sat", sub: "24 Oct" },
      { top: "Sun", sub: "25 Oct" }, { top: "Mon", sub: "26 Oct" }]);
  });

  it("the visit's column names its year only when it is another year", () => {
    expect([likeVisitHead(new Date(2026, 7, 30, 9).getTime(), NOW), likeVisitHead(new Date(2025, 7, 30).getTime(), NOW)])
      .toEqual([{ top: "Your visit", sub: "30 Aug" }, { top: "Your visit", sub: "30 Aug 2025" }]);
  });

  it("a day said whole", () => {
    expect([
      likeDayLabel(likeDay(THEN, same()), NOW),
      likeDayLabel(likeDay(THEN, same({ flow: null, tempC: 2, rainMm: 30 })), NOW),
      likeDayLabel(likeDay(THEN, { date: "2026-10-22", flow: null, tempC: null, rainMm: null, windKph: null, cloudPct: null }), NOW),
    ]).toEqual([
      "Sat, 24 Oct: 100%, very similar to your visit.",
      "Sat, 24 Oct: 27%, different from your visit, on the weather only.",
      "Today, 22 Oct: Nothing to compare for this day.",
    ]);
  });

  it("the forecast's age, live, offline, and never fetched", () => {
    const t = NOW - 2 * 3_600_000;
    expect([likeAgeLine(t, NOW, false), likeAgeLine(t - 3 * 3_600_000, NOW, true), likeAgeLine(null, NOW, true)])
      .toEqual(["Forecast checked 2h ago.", "Offline: showing the forecast checked 5h ago.",
                "The forecast has not been fetched yet. It is fetched when you are online."]);
  });
});
