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
  members: [{ station: "08A" as never, role: "up", areaKm2: 520, years: 40 }],
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
