/**
 * OWNER 2026-10-22 — a water's own gauge comes first: "own gauge should be main (unless far from click) low magnitude
 * compared to click spot", "it can still use auxiliary gauges", and never across quantities.
 *
 * WHO may lead is the pipeline's (`may_lead` in pipeline/gauges/consume/panel.py — far and much-smaller are tested
 * there, pipeline/tests/test_own_gauge.py) and arrives as `Contribution.lead`. These pin what leading MEANS in the
 * arithmetic every screen and the map share; `tools/one-formula.test.ts` runs the same cases through the Python.
 */
import { describe, expect, it } from "vitest";
import { OWN_LEAD_SHARE, estimate, leadShares, leaderOf, probit, type Contribution } from "./trust";

const donor = (percentile: number, over: Partial<Contribution> = {}): Contribution =>
  ({ percentile, role: "up", areaKm2: 100, years: 40, ...over });

const close = (xs: readonly number[]) => xs.map((x) => expect.closeTo(x, 9));

describe("what leading means", () => {
  it("is 70% — the number the report proposes and the owner decides", () => {
    expect(OWN_LEAD_SHARE).toBe(0.7);
  });

  it("gives the own gauge the main weight and the auxiliaries the rest, in their own proportion", () => {
    expect(leadShares([1, 1, 1], 0)).toEqual(close([0.7, 0.15, 0.15]));
    expect(leadShares([0.2, 0.75, 0.25], 0)).toEqual(close([0.7, 0.225, 0.075]));
  });

  it("is a floor: an own gauge already carrying more keeps it", () => {
    expect(leadShares([9, 1], 0)).toEqual(close([0.9, 0.1]));
  });

  it("without a leader, is the usual weighting", () => {
    expect(leadShares([2, 1, 1], null)).toEqual(close([0.5, 0.25, 0.25]));
    expect(leadShares([0, 0], null)).toEqual([0, 0]);
  });

  it("leads with the best-ranked own gauge that is reporting, and only one", () => {
    expect(leaderOf([0, 2, 1], [1, 1, 1])).toBe(2);
    expect(leaderOf([0, 2, 1], [1, 1, 0])).toBe(1);          // rank 1 is quiet: rank 2 leads
    expect(leaderOf([0, 0, 0], [1, 1, 1])).toBeNull();
    expect(leaderOf([0, 2, 1], [1, 0, 0])).toBeNull();
  });
});

describe("the estimate, led", () => {
  it("sits where the own gauge's 70% puts it, with the auxiliaries still counting", () => {
    // Three donors of identical usual weight: a third each before; led, 0.7 / 0.15 / 0.15 in probit space.
    const led = estimate(100, [donor(0.1, { lead: 1 }), donor(0.6), donor(0.6)]);
    const usual = estimate(100, [donor(0.1), donor(0.6), donor(0.6)]);
    if (!led.ok || !usual.ok) throw new Error("expected two estimates");
    expect(probit(led.value.percentile)).toBeCloseTo(0.7 * probit(0.1) + 0.3 * probit(0.6), 5);
    expect(probit(usual.value.percentile)).toBeCloseTo((probit(0.1) + 2 * probit(0.6)) / 3, 5);
    expect(led.value.donors).toBe(3);
  });

  it("is the usual estimate exactly where nothing may lead — far, much smaller, or another water's gauge", () => {
    const panel = [donor(0.1, { areaKm2: 60 }), donor(0.6, { sameRiver: false }), donor(0.8, { role: "down" })];
    expect(estimate(100, panel.map((c) => ({ ...c, lead: 0 })))).toEqual(estimate(100, panel));
  });

  it("does not let a quiet own gauge lead: the auxiliaries answer as usual", () => {
    // A member with no reading in the asked quantity is not a contribution at all — the Pitt's level-only 08MH035
    // under Flow. What is left is the usual answer from the others.
    expect(estimate(1181.2, [donor(0.166, { areaKm2: 26.4, years: 69, sameRiver: false })]))
      .toEqual(estimate(1181.2, [donor(0.166, { areaKm2: 26.4, years: 69, sameRiver: false, lead: 0 })]));
  });

  it("counts a leading own gauge as a measurement of this water, so a disagreeing tributary cannot silence it", () => {
    // Every donor on another blue line, disagreeing by 90 points: refused. One of them this water's own gauge: answered.
    const others = [donor(0.03, { sameRiver: false }), donor(0.97, { sameRiver: false })];
    expect(estimate(100, others)).toEqual({ ok: false, why: "too-uncertain" });
    expect(estimate(100, [{ ...others[0]!, lead: 1 }, others[1]!]).ok).toBe(true);
  });
});
