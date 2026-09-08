/**
 * Looking ahead is the same arithmetic, over a different day's numbers.
 *
 * The property worth guarding: a horizon the model does not reach must be ABSENT, never
 * today's value. A map that silently shows today when asked for Friday is worse than one
 * that shows nothing — it is confidently wrong, and nothing on screen says so.
 */
import { describe, expect, it } from "vitest";
import type { Panel } from "@app/data";
import { answerFrom, HORIZONS } from "./panel";

const PANEL: Panel = {
  areaKm2: 500,
  members: [{ station: "08A" as never, role: "up", areaKm2: 520, years: 40, regulated: false, sameRiver: true }],
};

type Ahead = Record<string, { discharge?: number; level?: number; model?: string }>;

const idx = (now: number | null, ahead: Ahead = {}) => ({
  stations: { "08A": { percentile: now, parameter: "discharge" as const, ahead } },
});

describe("forecast horizons", () => {
  it("offers now and the days the publisher ranks", () => {
    // Mirrors HORIZONS in pipeline/gauges/feed/publish.py.
    expect(HORIZONS).toEqual([0, 1, 3, 5]);
  });

  it("reads today at horizon 0", () => {
    const { answer } = answerFrom(PANEL, idx(0.2, { "3": { discharge: 0.8 } }), "discharge", 0);
    expect(answer.ok && answer.value.percentile).toBeCloseTo(0.2, 6);
  });

  it("reads the forecast at a horizon, not today", () => {
    const { answer } = answerFrom(PANEL, idx(0.2, { "3": { discharge: 0.8 } }), "discharge", 3);
    expect(answer.ok && answer.value.percentile).toBeCloseTo(0.8, 6);
  });

  it("says nothing rather than today when the run does not reach", () => {
    // THE ONE THAT MATTERS. Falling back to today would paint Friday's map with Monday's
    // water and give no sign of it.
    const { answer } = answerFrom(PANEL, idx(0.2, { "1": { discharge: 0.8 } }), "discharge", 5);
    expect(answer.ok).toBe(false);
  });

  it("does not read a forecast level as a forecast flow", () => {
    const both = idx(0.2, { "1": { level: 0.9 } });
    expect(answerFrom(PANEL, both, "discharge", 1).answer.ok).toBe(false);
    expect(answerFrom(PANEL, both, "level", 1).answer.ok).toBe(true);
  });

  it("weights a forecast exactly as it weights a reading", () => {
    // The donors, the shares and the combine are identical; only the numbers differ. So a
    // forecast for a spot carries the same interval and the same trust as a reading does.
    const now = answerFrom(PANEL, idx(0.4), "discharge", 0);
    const later = answerFrom(PANEL, idx(null, { "1": { discharge: 0.4 } }), "discharge", 1);
    expect(later.answer).toEqual(now.answer);
    expect(later.rows[0]!.weight).toBeCloseTo(now.rows[0]!.weight, 12);
  });

  it("still refuses when a station has no ahead block at all", () => {
    expect(answerFrom(PANEL, idx(0.2), "discharge", 1).answer.ok).toBe(false);
  });
});

describe("one quantity for the whole panel", () => {
  /*
   * A stage and a discharge are not the same claim: a stage is about one cross-section and
   * moves when the channel does, a discharge is about the whole river. Averaging them
   * produces a number in no units at all.
   *
   * Measured on the Harrison: a lake gauge's LEVEL at the 40th percentile averaged with the
   * river's DISCHARGE at the 6th, disagreeing past MAX_USEFUL_SPREAD, so the panel refused
   * and the map drew "no baseline" — while the same reach coloured fine at +1 day, because
   * the forecast block carries discharge only and the level could not intrude.
   */
  const MIXED: Panel = {
    areaKm2: 500,
    members: [
      { station: "FLOW" as never, role: "up", areaKm2: 520, years: 40, regulated: false, sameRiver: true },
      { station: "STAGE" as never, role: "up", areaKm2: 505, years: 90, regulated: false, sameRiver: true },
    ],
  };
  const mixedIdx = {
    stations: {
      FLOW: { percentile: 0.06, parameter: "discharge" as const, discharge: 0.06 },
      STAGE: { percentile: 0.40, parameter: "level" as const, level: 0.40 },
    },
  };

  it("does not average a level percentile with a discharge one", () => {
    const { rows } = answerFrom(MIXED, mixedIdx, "discharge", 0);
    const by = Object.fromEntries(rows.map((r) => [r.station, r.percentile]));
    // Discharge is chosen, so the stage-only station contributes nothing and says so.
    expect(by["FLOW"]).toBeCloseTo(0.06, 6);
    expect(by["STAGE"]).toBeNull();
  });

  it("answers from the flow gauge alone rather than refusing", () => {
    const { answer } = answerFrom(MIXED, mixedIdx, "discharge", 0);
    expect(answer.ok).toBe(true);
    if (answer.ok) expect(answer.value.percentile).toBeCloseTo(0.06, 6);
  });

  it("does NOT substitute a level when asked for flow", () => {
    /*
     * THIS USED TO FALL BACK, and the fallback is what was wrong.
     *
     * A panel whose donors only report stage cannot answer a question about flow, and it
     * used to answer anyway — quietly returning the level percentile. Measured across the
     * 358 stations publishing both, the two disagree by a median of 11.6 points and by more
     * than 25 points at 84 of them, so the substitution was not a near-enough answer; it
     * was a different one, presented identically.
     *
     * Refusing is the point. The reach draws as unmeasured, which is true.
     */
    const stageOnly = { stations: {
      FLOW: { percentile: null, parameter: "discharge" as const },
      STAGE: { percentile: 0.4, parameter: "level" as const, level: 0.4 },
    } };
    const { answer } = answerFrom(MIXED, stageOnly, "discharge", 0);
    expect(answer.ok, "a flow question must not be answered with a stage").toBe(false);

    // …and the same panel DOES answer when the level is what was asked for.
    const asLevel = answerFrom(MIXED, stageOnly, "level", 0).answer;
    expect(asLevel.ok).toBe(true);
    if (asLevel.ok) expect(asLevel.value.percentile).toBeCloseTo(0.4, 6);
  });

  it("gives the same answer at a horizon as it does now, for the same numbers", () => {
    // The bug showed as "now" and "+1 day" disagreeing about the same reach. They may
    // differ because the water differs — never because the arithmetic does.
    const ahead = { stations: {
      FLOW: { percentile: 0.9, parameter: "discharge" as const,
              ahead: { "1": { discharge: 0.06 } } },
      STAGE: { percentile: 0.9, parameter: "level" as const,
               ahead: { "1": { level: 0.40 } } },
    } };
    const now = answerFrom(MIXED, mixedIdx, "discharge", 0);
    const later = answerFrom(MIXED, ahead, "discharge", 1);
    expect(later.answer.ok).toBe(now.answer.ok);
    if (later.answer.ok && now.answer.ok)
      expect(later.answer.value.percentile).toBeCloseTo(now.answer.value.percentile, 6);
  });
});
