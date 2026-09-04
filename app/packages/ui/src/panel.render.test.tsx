/**
 * Panel + today's readings -> an answer, on rows shaped exactly as the bundle stores them.
 *
 * The numbers here are a real row from the built bundle: section `354087621:0`, catchment
 * 1.338 km², with donors at 3.408, 32.216 and 207.206 km². It is the case that caught a
 * bug — the nearest donor is 2.5x away and carries nearly all the weight, the farthest is
 * 155x away and carries 0.006 of it, and an earlier rule labelled the whole panel by the
 * farthest.
 */
import { describe, expect, it } from "vitest";
import type { Panel } from "@app/data";
import { answerFrom } from "./panel";

const REAL: Panel = {
  areaKm2: 1.338,
  members: [
    { station: "08HA003" as never, role: "up", areaKm2: 207.206, years: 76 },
    { station: "08HA013" as never, role: "up", areaKm2: 32.216, years: 16 },
    { station: "08HA020" as never, role: "up", areaKm2: 3.408, years: 10 },
  ],
};

const idx = (over: Record<string, number | null> = {}) => ({
  stations: Object.fromEntries(REAL.members.map((m) => [
    m.station,
    // `?? 0.12` would coalesce an explicit null back to the default — the point of `over`
    // is to be able to silence one station, so presence of the key is what counts.
    { percentile: m.station in over ? over[m.station] ?? null : 0.12,
      parameter: "discharge" as const },
  ])),
});

describe("turning a panel into an answer", () => {
  it("weights the nearest donor far above the distant ones", () => {
    const { rows } = answerFrom(REAL, idx());
    // Sorted by weight, and the 3.4 km² gauge — 2.5x away — must lead.
    expect(rows[0]!.station).toBe("08HA020");
    // 0.83 / 0.14 / 0.03. The nearest donor does not take everything, because it has only
    // ten years of record and the record gate halves its weight — a 76-year gauge 155x away
    // still gets a small say, which is the intended shape.
    expect(rows[0]!.weight).toBeCloseTo(0.83, 2);
    expect(rows[rows.length - 1]!.weight).toBeLessThan(0.05);
  });

  it("does not let a near-weightless donor set the label", () => {
    const { answer } = answerFrom(REAL, idx());
    expect(answer.ok).toBe(true);
    if (!answer.ok) return;
    // The 207 km2 donor is 155x away — "distant" on its own, and what the worst-member rule
    // used to report for this whole panel off a weight of 0.03.
    expect(answer.value.trust).toBe("near");
    // And dropping that donor entirely barely moves the label or the bars, which is the
    // proof that it was never carrying them.
    const without = answerFrom({ ...REAL, members: REAL.members.slice(1) }, idx());
    expect(without.answer.ok).toBe(true);
    if (!without.answer.ok) return;
    expect(without.answer.value.trust).toBe("near");
    expect(without.answer.value.plusMinus)
      .toBeCloseTo(answer.value.plusMinus, 0);
  });

  it("keeps a quiet station in the table, with no vote", () => {
    // A gauge that stopped reporting still belongs in the table — its absence is the
    // explanation for a wider interval, so hiding the row hides the reason.
    const { rows } = answerFrom(REAL, idx({ "08HA013": null }));
    expect(rows.length).toBe(3);
    expect(rows.find((r) => r.station === "08HA013")!.percentile).toBeNull();
  });

  it("still answers when a minor donor goes quiet", () => {
    const { answer } = answerFrom(REAL, idx({ "08HA013": null }));
    expect(answer.ok).toBe(true);                     // 0.83 of the weight is untouched
  });

  it("refuses when the only representative donor goes quiet", () => {
    // Silencing the 3.4 km2 gauge leaves 24x and 155x, together worth 0.04 of a weight —
    // under MIN_TOTAL_WEIGHT. Two distant gauges reporting is not the same as coverage,
    // and this is the case where saying nothing is the honest answer.
    const { answer, rows } = answerFrom(REAL, idx({ "08HA020": null }));
    expect(answer).toMatchObject({ ok: false, why: "too-uncertain" });
    expect(rows.length).toBe(3);                      // and the reader still sees why
  });

  it("says it is offline rather than guessing when the feed did not arrive", () => {
    expect(answerFrom(REAL, null)).toMatchObject({ answer: { ok: false, why: "offline" } });
  });

  it("says no-station when the section has no panel at all", () => {
    expect(answerFrom(undefined, idx()))
      .toMatchObject({ answer: { ok: false, why: "no-station" } });
  });

  it("refuses when nothing in the panel is reporting", () => {
    const dark = { stations: {} };
    expect(answerFrom(REAL, dark).answer).toMatchObject({ ok: false });
  });

  it("reads a level percentile only from a level station", () => {
    // A level percentile shown as a flow is arithmetic across two units.
    const mixed = { stations: {
      "08HA020": { percentile: 0.4, parameter: "level" as const },
    } };
    const asFlow = answerFrom(REAL, mixed, "discharge");
    expect(asFlow.rows.find((r) => r.station === "08HA020")!.percentile).toBeNull();
    const asLevel = answerFrom(REAL, mixed, "level");
    expect(asLevel.rows.find((r) => r.station === "08HA020")!.percentile).toBe(0.4);
  });

  it("puts the rows in the order the arithmetic used", () => {
    // The table beneath the number and the number itself come from one calculation, so a
    // reader can check the claim rather than take it.
    const { rows } = answerFrom(REAL, idx());
    expect(rows.map((r) => r.weight)).toEqual([...rows.map((r) => r.weight)]
      .sort((a, b) => b - a));
  });
});
