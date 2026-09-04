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
    { station: "08HA003" as never, role: "up", areaKm2: 207.206, years: 76, regulated: false, sameRiver: true },
    { station: "08HA013" as never, role: "up", areaKm2: 32.216, years: 16, regulated: false, sameRiver: true },
    { station: "08HA020" as never, role: "up", areaKm2: 3.408, years: 10, regulated: false, sameRiver: true },
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
  it("keeps the three donors within a factor of two of each other", () => {
    /*
     * THE POINT OF INVERSE-VARIANCE WEIGHTING, on the row that motivated it.
     *
     * These are 2.5x, 24x and 155x from a 1.34 km² reach, with 10, 16 and 76 years of
     * record. The old `share` weighting made that 0.83 / 0.14 / 0.03 — it threw the
     * 76-year gauge away on the strength of catchment size alone. The calibration says
     * those distances cost 11.7, 13.4 and 16.1 percentile points, so they are all worth
     * hearing and none of them is worth eight times another.
     *
     * DISTANCE IS NOT THE ONLY VARIANCE. The 155x gauge slightly outweighs the 2.5x one
     * here, because it has 76 years against 10 and record length is variance too. That is
     * the formula working, not a slip: both terms are precision, and both are priced.
     */
    const { rows } = answerFrom(REAL, idx());
    const ws = rows.map((r) => r.weight);
    expect(Math.max(...ws) / Math.min(...ws)).toBeLessThan(2);
    // A FLAT spread, and that is the measurement rather than a slackening. The old `share`
    // weighting made this 0.83 / 0.14 / 0.03 by assuming a 155x donor is 1/155th as good;
    // the calibration says it is out by 18.5 points against 11.7, which is about half as
    // good. Nothing is thrown away any more.
    expect(rows[rows.length - 1]!.weight).toBeGreaterThan(0.2);
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
    // It tightens by about a point and a half — the distant donor was widening the honest
    // interval, which is exactly what it should do now that it is actually counted.
    expect(Math.abs(without.answer.value.plusMinus - answer.value.plusMinus))
      .toBeLessThan(2);
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

  it("still answers from the distant pair when the nearest donor goes quiet", () => {
    /*
     * THIS USED TO REFUSE, and refusing was the bug. Under the old `share` weighting the
     * 24x and 155x gauges were worth 0.04 together — below MIN_TOTAL_WEIGHT — so the
     * answer vanished the moment the nearest gauge fell silent. Measured province-wide,
     * that arithmetic left 244,719 of 249,237 sections holding a panel and saying nothing.
     *
     * They are out by 15.5 and 18.5 points against a best case of 11.7. That is a wide
     * answer, not an absent one, and the interval says so.
     */
    const { answer, rows } = answerFrom(REAL, idx({ "08HA020": null }));
    expect(answer.ok).toBe(true);
    // Wider than the best case of 11.7 by a clear margin, because the two that remain are
    // 24x and 155x away — a wide answer, not an absent one.
    if (answer.ok) expect(answer.value.plusMinus).toBeGreaterThan(13.5);
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
