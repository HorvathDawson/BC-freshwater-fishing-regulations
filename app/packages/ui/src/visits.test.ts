/**
 * T294: the visits block (which visit is shown, its stars) and "The next few days" predicted from rated visits — the
 * whole view on fixed inputs: three rated visits, one unrated left out, nothing rated falls back to one visit.
 */
import { describe, expect, it } from "vitest";
import { addVisit, rateVisit, type LikeFetched, type Spot, type Visit } from "@app/data/spots";
import { likeVisitView } from "./likeVisit";
import { nextRating, visitsView } from "./visits";

const NOW = new Date(2026, 9, 22, 10).getTime();
const at = (m: number, d: number) => new Date(2026, m, d, 9, 20).getTime();
const weather = (tempC: number) => ({ tempC, windKph: 8, windDir: 210, rain3h: 0, pressureHpa: 1014, code: 3, at: "x",
  window: { before: null, after: null, at: { at: "x", tempC, cloudPct: 40, pressureHpa: 1014, humidityPct: 70 } } });
const leadUp = (rain: number) => ({ station: "08MH016", gauge: null,
  weather: [1, 2, 3, 4, 5, 6, 7].map((d) => ({ date: `2026-01-0${d}`, maxC: 20, minC: 9, precipMm: rain / 3, windMaxKph: 10, code: 1 })) });
const reading = (p: number, t: number) => ({ station: "08MH016", discharge: 41.2, level: null, percentile: p, at: new Date(t).toISOString() });

/** The first visit, 30 Aug: flow 42nd, 14°, 3 mm before — the coming days below are the same. Rated 5. */
const FIRST: Spot = {
  id: "s1", createdAt: at(7, 30), updatedAt: at(7, 30), visitedAt: at(7, 30), lat: 49.1, lon: -121.96, item: null,
  waterName: "Chilliwack River", title: "Tamihi", notes: "", photos: [], trace: null, panel: null,
  reading: reading(0.42, at(7, 30)), weather: weather(14), leadUp: leadUp(3), rating: 5,
};
const visit = (id: string, t: number, p: number, temp: number, rating: number | null): Visit => ({
  id, createdAt: t, visitedAt: t, rating, notes: "", photos: [], trace: null, panel: null,
  reading: reading(p, t), weather: weather(temp), leadUp: leadUp(3) });
/** 12 Sep: flow 80th (38 points higher than the coming days), rated 2. 3 Oct: 18°, rated 4. 10 Oct: not rated. */
const SPOT = [visit("v1", at(8, 12), 0.8, 14, 2), visit("v2", at(9, 3), 0.42, 18, 4), visit("v3", at(9, 10), 0.42, 14, null)]
  .reduce((s, v) => addVisit(s, v, NOW), FIRST);

const days = ["2026-10-19", "2026-10-20", "2026-10-21", "2026-10-22", "2026-10-23", "2026-10-24", "2026-10-25", "2026-10-26"];
const FETCHED: LikeFetched = {
  weatherAt: NOW - 3_600_000, flowAt: NOW - 3_600_000,
  weather: { days: days.map((date) => ({ date, precipMm: 1 })),
             hours: days.map((date) => ({ at: `${date}T09:00`, tempC: 14, windKph: 8, cloudPct: 40, pressureHpa: 1014 })) },
  flow: { "2026-10-22": 0.42, "2026-10-23": 0.42, "2026-10-24": 0.42, "2026-10-25": 0.42, "2026-10-26": 0.42 },
  flowNow: "2026-10-22",
};

describe("visitsView", () => {
  it("lists every visit oldest first with its stars, the shown one on, and the shown visit's stars", () => {
    const v = visitsView(SPOT, "v2", NOW);
    expect(v.heading).toBe("Your visits");
    expect(v.chips.map((c) => [c.id, c.date, c.rated, c.on])).toEqual([
      ["s1", "30 Aug", "5★", false], ["v1", "12 Sep", "2★", false], ["v2", "3 Oct", "4★", true], ["v3", "10 Oct", "Not rated", false]]);
    expect(v.chips[3]!.label).toBe("Visit on 10 Oct, not rated");
    expect([v.rate.value, v.rate.word, v.canRemove]).toEqual([4, "Good", true]);
    expect(v.rate.stars.map((s) => [s.n, s.on])).toEqual([[1, true], [2, true], [3, true], [4, true], [5, false]]);
    expect(v.rate.stars[3]!.label).toBe("Rated 4 of 5. Press to clear the rating");
    expect(v.rate.stars[4]!.label).toBe("Rate this visit 5 of 5: Great");
    // no id: the latest; a spot of one visit cannot remove it
    expect(visitsView(SPOT, null, NOW).chips.find((c) => c.on)!.id).toBe("v3");
    expect(visitsView(FIRST, null, NOW).canRemove).toBe(false);
    expect(visitsView(SPOT, "v3", NOW).rate.word).toBe("Not rated");
  });

  it("a star pressed rates; the lit rating pressed again clears it", () => {
    expect([nextRating(null, 3), nextRating(3, 4), nextRating(4, 4)]).toEqual([3, 4, null]);
  });
});

describe("likeVisitView — predicted from 3 rated visits", () => {
  it("reads every coming day against every rated visit, weighted by likeness; says which visit, and never fish", () => {
    const v = likeVisitView(SPOT, FETCHED, false, NOW);
    expect([v.mode, v.best, v.hint, v.note]).toEqual(["predict", "Best going by your ratings",
      "1 visit is not rated and is left out.", null]);
    expect(v.intro).toContain("not a promise of fish");
    expect(v.visit).toEqual({ top: "From your ratings", sub: "5★ 30 Aug · 2★ 12 Sep · 4★ 3 Oct",
                              label: "From your ratings: 5★ 30 Aug · 2★ 12 Sep · 4★ 3 Oct." });
    // 30 Aug 100 % alike (w 1), 3 Oct temp 4° apart (1 − 0.1·4/12 = 96.7 → 97 %, w .912), 12 Sep flow 38 points apart
    // (1 − 0.25·0.76 = 81 %, w .531): (5 + .912·4 + .531·2) / 2.443 = 3.99 → 4.0
    const d = v.days![0]!;
    expect([d.date, d.top, d.score, d.read, d.basis, d.best]).toEqual(
      ["2026-10-22", "Today", "4.0★", "Good", "From 3 rated visits", true]);
    expect(d.reasons).toEqual(["Most like your 5★ visit on 30 Aug (100% alike)", "Flow: the same",
      "Rain in the 3 days before: the same", "Cloud: the same", "Pressure: the same", "Air temperature: the same",
      "Wind: the same"]);
    expect(d.label).toBe("Today, 22 Oct: about 4.0 of 5, good, from 3 rated visits.");
    expect(v.days!.filter((x) => x.best)).toHaveLength(1);
  });

  it("one rated visit: its rating, resting on 1; nothing rated: compared with the shown visit, and how to rate said", () => {
    const one = likeVisitView(FIRST, FETCHED, false, NOW);
    expect([one.mode, one.days![0]!.score, one.days![0]!.basis]).toEqual(["predict", "5.0★", "From 1 rated visit"]);
    const none = rateVisit(rateVisit(rateVisit(SPOT, "s1", null), "v1", null), "v2", null);
    const like = likeVisitView(none, FETCHED, false, NOW, "v1");
    expect([like.mode, like.best, like.hint, like.visit.sub]).toEqual(["like", "Closest to your visit",
      "Rate your visits to see how good each coming day may be by your own ratings.", "12 Sep"]);
    expect(like.days![0]!.score).toBe("81%");
    expect(likeVisitView(none, FETCHED, false, NOW).visit.sub).toBe("10 Oct");   // no visit shown: the latest
  });
});
