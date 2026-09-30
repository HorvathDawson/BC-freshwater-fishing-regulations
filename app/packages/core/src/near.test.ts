import { describe, expect, it } from "vitest";
import {
  NEAR_RANK, importance, nearScore, normSignal, rankNear, sizeSignal, type WaterSignals,
} from "./near";

const none: WaterSignals = { mag: null, areaHa: null, pieces: 1, towns: 3, gauged: false,
                             stocked: false, listed: false };
const w = (name: string, kind: string, km: number, s: Partial<WaterSignals> = {}) =>
  ({ name, kind, km, signals: { ...none, ...s } });

// Hand-made, but with the bundle's own figures where a real water is named.
const FRASER = { mag: 296_885, pieces: 251, towns: 845, gauged: true, listed: true };
const MID_RIVER = { mag: 2_200, pieces: 20, towns: 70, gauged: true, listed: true }; // Chilliwack R.
const DECENT_CREEK = { mag: 300, pieces: 6, towns: 10 };                           // Paul Creek-ish
const TINY_CREEK = { mag: null, pieces: 1, towns: 5 };                             // Dahlie Creek

describe("which water near a town is listed first", () => {
  it("the Fraser 20 km away beats a tiny creek 2 km away", () => {
    const order = rankNear([w("Dahlie Creek", "stream", 2, TINY_CREEK),
                            w("Fraser River", "stream", 20, FRASER)]);
    expect(order.map((x) => x.name)).toEqual(["Fraser River", "Dahlie Creek"]);
  });

  it("a decent creek 1 km away beats a mid-size river 24 km away", () => {
    const order = rankNear([w("Chilliwack River", "stream", 24, MID_RIVER),
                            w("Paul Creek", "stream", 1, DECENT_CREEK)]);
    expect(order.map((x) => x.name)).toEqual(["Paul Creek", "Chilliwack River"]);
  });

  it("the same water nearer is always ranked higher", () => {
    for (const s of [FRASER, MID_RIVER, DECENT_CREEK, TINY_CREEK])
      expect(nearScore(w("a", "stream", 3, s))).toBeGreaterThan(nearScore(w("a", "stream", 9, s)));
  });

  it("at the same distance, more water is ranked higher", () => {
    const at = (s: Partial<WaterSignals>) => nearScore(w("a", "stream", 8, s));
    expect(at(FRASER)).toBeGreaterThan(at(MID_RIVER));
    expect(at(MID_RIVER)).toBeGreaterThan(at(DECENT_CREEK));
    expect(at(DECENT_CREEK)).toBeGreaterThan(at(TINY_CREEK));
  });

  it("the river a town stands on leads, and its big neighbours follow the creeks at the door", () => {
    // Smithers, from the bundle: the Bulkley at 1.5 km; the Zymoetz and Telkwa at 12.
    const order = rankNear([
      w("Chicken Lake Creek", "stream", 1.12, { pieces: 1, towns: 7 }),
      w("Dahlie Creek", "stream", 1.35, { pieces: 1, towns: 8 }),
      w("Bulkley River", "stream", 1.48, { mag: 16_673, pieces: 76, towns: 42, gauged: true,
                                            listed: true }),
      w("Zymoetz River", "stream", 11.57, { mag: 6_739, pieces: 98, towns: 35, gauged: true,
                                             listed: true }),
      w("Tenas Creek", "stream", 13.2, { mag: 259, pieces: 1, towns: 8 }),
    ]).map((x) => x.name);
    expect(order[0]).toBe("Bulkley River");
    expect(order.indexOf("Zymoetz River")).toBeLessThan(order.indexOf("Chicken Lake Creek"));
    expect(order.at(-1)).toBe("Tenas Creek");
  });

  it("a lake is sized by its area, on the same log scale as a stream's magnitude", () => {
    // One hectare weighs as one headwater: a 180 ha lake is a magnitude-180 creek.
    expect(sizeSignal("lake", null, 180)).toBeCloseTo(sizeSignal("stream", 180, null), 9);
    const lake = importance("lake", { ...none, areaHa: 4975, listed: true });   // Kamloops Lake
    const creek = importance("stream", { ...none, mag: 300, listed: true });
    expect(lake).toBeGreaterThan(creek);
    expect(lake - importance("lake", { ...none, listed: true }))
      .toBeCloseTo(NEAR_RANK.W.size * normSignal(4975, NEAR_RANK.REF.area), 6);
    // Williston is not a pond: it reaches the top of the scale, as the Fraser does.
    expect(sizeSignal("lake", null, 172_669)).toBe(1);
    // …and a stream's area (none) or a lake's missing area is no figure, never a guess.
    expect(sizeSignal("stream", null, 500)).toBe(0);
    expect(sizeSignal("lake", null, null)).toBe(0);
  });

  it("a big lake near town outranks a pond at the door; a pond does not beat a river", () => {
    const order = rankNear([
      w("Pond", "lake", 1, { areaHa: 2 }),
      w("Big Lake", "lake", 8, { areaHa: 3000, listed: true }),
      w("River", "stream", 3, { mag: 16_673, pieces: 76, towns: 42, listed: true }),
    ]).map((x) => x.name);
    expect(order).toEqual(["River", "Big Lake", "Pond"]);
  });

  it("a gauge, stocking or a synopsis entry lifts a water, never lowers it", () => {
    const base = importance("stream", none);
    expect(importance("stream", { ...none, gauged: true })).toBeGreaterThan(base);
    expect(importance("stream", { ...none, stocked: true })).toBeGreaterThan(base);
    expect(importance("stream", { ...none, listed: true })).toBeGreaterThan(base);
  });

  it("normalises each size signal to 0..1 on a log scale, so none can swamp the rest", () => {
    expect(normSignal(null, 100)).toBe(0);
    expect(normSignal(0, 100)).toBe(0);
    expect(normSignal(100, 100)).toBeCloseTo(1, 9);
    expect(normSignal(1e9, 100)).toBe(1);
    // Log, not linear: a tenth of the reference is well over a tenth of the way.
    expect(normSignal(10, 100)).toBeGreaterThan(0.5);
    // The Fraser is bounded by the sum of the weights — not 300,000 times a creek.
    expect(importance("stream", { ...none, ...FRASER }))
      .toBeLessThanOrEqual(1 + Object.values(NEAR_RANK.W).reduce((a, b) => a + b, 0));
  });

  it("ties fall to the nearer water, then the name — never to the input order", () => {
    const a = w("B Creek", "stream", 2), b = w("A Creek", "stream", 2), c = w("C Creek", "stream", 1);
    expect(rankNear([a, b, c]).map((x) => x.name)).toEqual(["C Creek", "A Creek", "B Creek"]);
    expect(rankNear([c, b, a]).map((x) => x.name)).toEqual(["C Creek", "A Creek", "B Creek"]);
  });
});
