/**
 * T51: the "Like your visit?" score on fixed inputs — whole values and whole strings (AGENTS 33).
 * Timestamps from LOCAL parts so the days read the same in any zone the suite runs in.
 */
import { describe, expect, it } from "vitest";
import { LIKE_CARRIED_WEIGHT, LIKE_FACTORS, LIKE_WEIGHTS, likeAgeLine, likeDay, likeDayHead, likeDayLabel, likeDays, likeness, likeNothingThen, likeRead,
         likeVisitHead, type LikeConditions } from "./likeVisit";

const THEN: LikeConditions = { flow: 0.42, tempC: 14, rainMm: 2, windKph: 8, cloudPct: 40, pressureHpa: 1012 };
const same = (over: Partial<LikeConditions> = {}) => ({ date: "2026-10-24", ...THEN, ...over });
const NOW = new Date(2026, 9, 22, 10).getTime();
const OTHERS = ["flow", "rain", "cloud", "pressure", "temp", "wind"] as const;

describe("likeDay — the tiers (T284, owner 2026-10-29)", () => {
  it("pins the order and the weights: flow = rain 0.25, cloud = pressure 0.15, temp = wind 0.10; they sum to 1", () => {
    expect(LIKE_FACTORS).toEqual(OTHERS);
    expect(LIKE_WEIGHTS).toEqual({ flow: 0.25, rain: 0.25, cloud: 0.15, pressure: 0.15, temp: 0.1, wind: 0.1 });
    expect(Object.values(LIKE_WEIGHTS).reduce((a, b) => a + b, 0)).toBeCloseTo(1, 10);
    expect(LIKE_CARRIED_WEIGHT).toBe(0.5);
  });

  it("an identical day is 100 %, every factor 100 % alike, listed in tier order with its share", () => {
    const d = likeDay(THEN, same(), { hour: "09:00" });
    expect([d.score, d.read, d.basis, d.note, d.caveat]).toEqual([100, "very similar", "all", null, null]);
    expect(d.factors.map((f) => [f.key, f.likeness, f.share, f.plain])).toEqual([
      ["flow", 1, 0.25, "Flow: the same"], ["rain", 1, 0.25, "Rain in the 3 days before: the same"],
      ["cloud", 1, 0.15, "Cloud: the same"], ["pressure", 1, 0.15, "Pressure: the same"],
      ["temp", 1, 0.1, "Air temperature: the same"], ["wind", 1, 0.1, "Wind: the same"]]);
    expect(d.factors[3]!.line).toBe("Pressure: 1012 hPa then, 1012 hPa forecast at 09:00. The same: 100% alike, 15% of the score.");
  });

  // Each factor moved ALONE by half its span: the score drops by exactly half that factor's weight.
  it.each([
    ["flow", { flow: 0.67 }, 88, "very similar", "Flow: 6 in 10 days higher then, 7 in 10 days lower now. 25 points higher: 50% alike, 25% of the score."],
    ["rain", { rainMm: 9.5 }, 88, "very similar", "Rain in the 3 days before: 2 mm then, 9.5 mm forecast. 7.5 mm wetter: 50% alike, 25% of the score."],
    ["cloud", { cloudPct: 90 }, 93, "very similar", "Cloud: 40% then, 90% forecast. 50 points cloudier: 50% alike, 15% of the score."],
    ["pressure", { pressureHpa: 1002 }, 93, "very similar", "Pressure: 1012 hPa then, 1002 hPa forecast. 10 hPa lower: 50% alike, 15% of the score."],
    ["temp", { tempC: 8 }, 95, "very similar", "Air temperature: 14°C then, 8°C forecast. 6° cooler: 50% alike, 10% of the score."],
    ["wind", { windKph: 20.5 }, 95, "very similar", "Wind: 8 km/h then, 21 km/h forecast. 13 km/h windier: 50% alike, 10% of the score."],
  ] as const)("%s moving alone lowers the score by half its weight", (key, over, score, read, line) => {
    const d = likeDay(THEN, same(over), { today: true });
    expect([d.score, d.read, d.factors.find((f) => f.key === key)!.line]).toEqual([score, read, line]);
    expect(d.factors.filter((f) => f.key !== key).map((f) => f.likeness)).toEqual([1, 1, 1, 1, 1]);
  });

  it("tier 1 outweighs tier 2 outweighs tier 3: the same move costs more in a higher tier (mutation pin)", () => {
    const moved = (over: Partial<LikeConditions>) => likeDay(THEN, same(over)).score!;
    const flow = moved({ flow: 0.67 }), rain = moved({ rainMm: 9.5 });
    const cloud = moved({ cloudPct: 90 }), pressure = moved({ pressureHpa: 1002 });
    const temp = moved({ tempC: 8 }), wind = moved({ windKph: 20.5 });
    expect(flow).toBe(rain);
    expect(cloud).toBe(pressure);
    expect(temp).toBe(wind);
    expect(flow).toBeLessThan(cloud);
    expect(cloud).toBeLessThan(temp);
  });

  it("a factor a whole span or more apart is 0 % alike, never negative", () => {
    expect([likeness("flow", 0.1, 0.9), likeness("temp", 0, 12), likeness("rain", 0, 40), likeness("pressure", 1000, 1030)])
      .toEqual([0, 0, 0, 0]);
    const d = likeDay(THEN, same({ flow: 0.95, tempC: 30, rainMm: 40, windKph: 60, cloudPct: 0, pressureHpa: 1040 }));
    // every factor past its span but cloud, 40 of 100 points apart: 0.6 × 15 % = 9 %
    expect([d.score, d.read, d.factors.map((f) => f.likeness)]).toEqual([9, "different", [0, 0, 0.6, 0, 0, 0]]);
  });

  it("the reads switch at 85 and 65 (mutation pin on the thresholds)", () => {
    expect([likeRead(0.85), likeRead(0.849), likeRead(0.65), likeRead(0.649)])
      .toEqual(["very similar", "similar", "similar", "different"]);
  });
});

describe("likeDay — flow with and without, said", () => {
  it("no flow at the visit: scored on the weather only, and says so", () => {
    const d = likeDay({ ...THEN, flow: null }, same({ tempC: 8 }));
    expect([d.score, d.read, d.basis, d.note, d.factors[0]!.line, d.factors[1]!.share]).toEqual([
      // temp 50 % alike at a re-normalised 0.1/0.75 share: 1 − 0.5 × 0.1333 = 0.933
      93, "very similar", "weather", "No flow then to compare: scored on the weather only.",
      "Flow: no flow then to compare. Not scored.", 0.25 / 0.75]);
  });

  it("no flow at all for the day (the station has no reading): the weather only, said differently", () => {
    const d = likeDay(THEN, same({ flow: null }));
    expect([d.score, d.basis, d.note, d.factors[0]!.line]).toEqual([
      100, "weather", "No flow forecast for this day: scored on the weather only.",
      "Flow: no flow forecast for this day. Not scored."]);
  });

  it("a flow carried forward from today's reading is scored at HALF weight, says so, and carries the caveat", () => {
    const d = likeDay(THEN, same({ flow: 0.17, flowCarried: true }));
    const f = d.factors[0]!;
    // weights re-normalised: 0.125 / (0.125 + 0.75) of the score, 25 points apart → 50 % alike
    expect([f.share, f.likeness, d.score, d.basis, d.note]).toEqual([0.125 / 0.875, 0.5, 93, "all", null]);
    expect(f.line).toBe("Flow: 6 in 10 days higher then, 8 in 10 days higher from today's reading. 25 points lower: 50% alike, 14% of the score.");
    expect(d.caveat).toBe("No river forecast reaches this day, so its flow is today's reading carried forward: less certain.");
    // against a forecast day with the same flow: the carried one is pulled less by the same difference
    expect(d.score!).toBeGreaterThan(likeDay(THEN, same({ flow: 0.17, flowModel: "CLEVER" })).score!);
  });

  it("T171: a day ELF forecast is scored, names its model, and carries the low-water caveat", () => {
    const d = likeDay(THEN, same({ flow: 0.17, flowModel: "ELF", flowLowWater: true }));
    expect([d.score, d.basis, d.note, d.caveat, d.factors[0]!.line]).toEqual([
      88, "all", null,
      "ELF is a low-water model: it carries the river's recent fall forward and has no rain or snowmelt in it, so it "
        + "cannot see a rise coming.",
      "Flow: 6 in 10 days higher then, 8 in 10 days higher forecast by ELF. 25 points lower: 50% alike, 25% of the score."]);
    // a flood model's day says "forecast" and carries no caveat; nor does today's reading, whoever forecast it
    const c = likeDay(THEN, same({ flow: 0.17, flowModel: "CLEVER", flowLowWater: false }));
    expect([c.caveat, c.factors[0]!.line]).toEqual([null,
      "Flow: 6 in 10 days higher then, 8 in 10 days higher forecast. 25 points lower: 50% alike, 25% of the score."]);
    expect(likeDay(THEN, same({ flow: 0.17, flowModel: "ELF", flowLowWater: true }), { today: true }).caveat).toBeNull();
    // no flow that day: no caveat to carry, whatever model was named
    expect(likeDay(THEN, same({ flow: null, flowModel: "ELF", flowLowWater: true })).caveat).toBeNull();
  });

  it("a value missing on one side is left out and named, never guessed", () => {
    const d = likeDay({ ...THEN, cloudPct: null }, same({ windKph: null }));
    expect(d.factors.filter((f) => f.key === "cloud" || f.key === "wind").map((f) => [f.likeness, f.share, f.line])).toEqual([
      [null, 0, "Cloud: not recorded at the visit. Not scored."],
      [null, 0, "Wind: no forecast for this day. Not scored."],
    ]);
    expect([d.score, d.basis]).toEqual([100, "all"]);
  });

  it("nothing on both sides: no score, and says there is nothing to compare", () => {
    const none = { flow: null, tempC: null, rainMm: null, windKph: null, cloudPct: null, pressureHpa: null };
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
      "Sat, 24 Oct: 53%, different from your visit, on the weather only.",
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

describe("(T284) the plain reasons", () => {
  it("each scored factor says how far apart it is, in a few words; a left-out one says nothing", () => {
    const d = likeDay({ flow: null, tempC: 14, rainMm: 5, windKph: 9, cloudPct: 40 },
                      { date: "2026-10-30", flow: null, tempC: 10, rainMm: 5, windKph: 6, cloudPct: null });
    // in tier order: flow, rain, cloud, pressure, temp, wind
    expect(d.factors.map((f) => f.plain)).toEqual([null, "Rain in the 3 days before: the same", null, null,
      "Air temperature: 4° cooler", "Wind: 3 km/h calmer"]);
  });
});
