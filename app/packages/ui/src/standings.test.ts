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
    { station: "08MG013" as never, role: "up", areaKm2: 4313, years: 60 },
    { station: "08MG001" as never, role: "up", areaKm2: 795, years: 70 },
    { station: "08MG022" as never, role: "down", areaKm2: 6200, years: 30 },
    { station: "08MG012" as never, role: "up", areaKm2: 220, years: 25 },
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

  it("weights the same-size gauge above the small tributary one", () => {
    // 08MG013 drains 4,313 km² against this reach's 4,584 — all but the same water.
    // 08MG012 drains 220. A single-station join could pick either.
    const { rows } = answerFrom(HARRISON, reporting("08MG013", "08MG001", "08MG012"));
    expect(rows[0]!.station).toBe("08MG013");
    expect(rows[0]!.weight).toBeGreaterThan(0.8);
  });
});
