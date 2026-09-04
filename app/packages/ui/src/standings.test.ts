/**
 * The map and the sheet must answer with the same number.
 *
 * The failure this exists for: the Harrison drawn as unmeasured grey for its whole length,
 * while a tap on that grey opened a sheet reading "very low for the time of year, fairly
 * confident, from 2 gauges". The map joined through `section_gauge` — the ONE station
 * matched to a reach — and that station was quiet.
 */
import { describe, expect, it } from "vitest";
import type { Panel } from "@app/data";
import { answerFrom } from "./panel";

const HARRISON: Panel = {
  areaKm2: 4584,
  members: [
    { station: "08MG013" as never, role: "up", areaKm2: 4313, years: 60, regulated: false, sameRiver: true },
    { station: "08MG001" as never, role: "up", areaKm2: 795, years: 70, regulated: false, sameRiver: true },
    { station: "08MG022" as never, role: "down", areaKm2: 6200, years: 30, regulated: false, sameRiver: true },
    { station: "08MG012" as never, role: "up", areaKm2: 220, years: 25, regulated: false, sameRiver: true },
  ],
};

const reporting = (...live: string[]) => ({
  stations: Object.fromEntries(live.map((s) =>
    [s, { percentile: 0.05, parameter: "discharge" as const }])),
});

describe("colouring a reach from its panel", () => {
  it("still answers when the nearest gauge has gone quiet", () => {
    // The whole point. 08MG022 and 08MG012 are silent; the panel answers from the rest.
    const { answer } = answerFrom(HARRISON, reporting("08MG013", "08MG001"));
    expect(answer.ok).toBe(true);
    if (answer.ok) expect(answer.value.percentile).toBeLessThan(0.15);
  });

  it("gives the map the same number it gives the sheet", () => {
    // Not "similar" — the same call, so they cannot drift.
    const idx = reporting("08MG013", "08MG001");
    const a = answerFrom(HARRISON, idx);
    const b = answerFrom(HARRISON, idx);
    expect(a.answer).toEqual(b.answer);
  });

  it("refuses rather than colouring when every donor is quiet", () => {
    // A grey reach is a real answer. A reach coloured from nothing is not.
    expect(answerFrom(HARRISON, reporting()).answer.ok).toBe(false);
  });

  it("does not read a quiet donor as a river at zero", () => {
    // Zero is the bottom of the scale. A silent gauge is not a dry river.
    const { rows } = answerFrom(HARRISON, reporting("08MG013"));
    const quiet = rows.filter((r) => r.percentile === null);
    expect(quiet.length).toBe(3);
    for (const r of quiet) expect(r.weight).toBe(0);
  });

  it("weights the nearby gauges above the small tributary one", () => {
    // 08MG013 drains 4,313 km² against this reach's 4,584, and 08MG001 drains 795 — both
    // within a factor of ten, which is the finest the calibration resolves, so the two are
    // genuinely indistinguishable to this arithmetic and weigh the same. 08MG012 drains
    // 220 — twenty times off — and must weigh less.
    const { rows } = answerFrom(HARRISON, reporting("08MG013", "08MG001", "08MG012"));
    const w = Object.fromEntries(rows.map((r) => [r.station, r.weight]));
    expect(w["08MG013"]).toBeCloseTo(w["08MG001"]!, 6);
    expect(w["08MG012"]).toBeLessThan(w["08MG013"]!);
  });

  it("does not invent an ordering finer than the measurement", () => {
    // Below ten times, the 9,495-pair calibration has one number: 11.7 points. Ranking two
    // donors inside that band would be reading a precision out of it that is not there.
    const near: Panel = { areaKm2: 1000, members: [
      { station: "A" as never, role: "up", areaKm2: 1010, years: 40, regulated: false, sameRiver: true },
      { station: "B" as never, role: "up", areaKm2: 4000, years: 40, regulated: false, sameRiver: true },
    ] };
    const { rows } = answerFrom(near, reporting("A", "B"));
    expect(rows[0]!.weight).toBeCloseTo(rows[1]!.weight, 6);
  });
});
