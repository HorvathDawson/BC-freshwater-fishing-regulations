/**
 * The trust ladder is written twice — once for the pipeline and once for the app — and
 * this is what keeps the two copies equal.
 *
 * The same guard the magnitude zoom ladder has, and for the same reason: a screen that
 * promises "± 12 points" while the pipeline gated on "± 18" is a lie no type can catch,
 * because both numbers are perfectly valid floats.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { ERROR_BY_RATIO, MAX_USEFUL_SPREAD, estimate, interval, trustFor, weightFor }
  from "@app/core";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const PY = readFileSync(join(ROOT, "pipeline/atlas/gauges/panel.py"), "utf8");

/** `(10.0, "close", 11.7),` — the tuples out of the Python table. */
function pythonLadder(): [number, string, number][] {
  const block = PY.split("ERROR_BY_RATIO")[1] ?? "";
  const body = block.slice(0, block.indexOf(")\n"));
  const out: [number, string, number][] = [];
  for (const m of body.matchAll(
      /\(\s*([\d_.]+|float\("inf"\))\s*,\s*"(\w+)"\s*,\s*([\d.]+)\s*\)/g)) {
    const raw = m[1]!;
    out.push([raw.startsWith("float") ? Infinity : Number(raw.replace(/_/g, "")),
              m[2]!, Number(m[3])]);
  }
  return out;
}

describe("the trust ladder", () => {
  it("is the same in the app as in the pipeline", () => {
    const py = pythonLadder();
    expect(py.length, "could not parse ERROR_BY_RATIO out of panel.py").toBeGreaterThan(1);
    expect(py).toEqual(ERROR_BY_RATIO.map((r) => [...r]));
  });

  it("gets worse with distance and never runs out of rungs", () => {
    const errs = [2, 40, 400, 4_000, 90_000].map((r) => trustFor(r).plusMinus);
    expect(errs).toEqual([...errs].sort((a, b) => a - b));
    expect(trustFor(Number.MAX_VALUE).klass).toBe("remote");
  });

  it("never claims a precise answer, however close the donor", () => {
    // The finding the whole design rests on: even a donor of nearly identical size is out
    // by more than ten percentile points, so nothing here may be drawn as a bare number.
    expect(trustFor(1).plusMinus).toBeGreaterThan(10);
  });

  it("treats a nonsense ratio as the most distant rather than the closest", () => {
    // A ratio is max/min and so is >= 1. Below one means the caller inverted it, and the
    // safe reading of a bug is the least confident one — not the most.
    expect(trustFor(0.5).klass).toBe("remote");
    expect(trustFor(Number.NaN).klass).toBe("remote");
  });

  it("draws an interval in POINTS around a percentile in 0-1", () => {
    // The unit split that this module exists to make unambiguous.
    const [lo, hi] = interval({ percentile: 0.20, plusMinus: 12, trust: "close",
                                spread: 0, donors: 1 });
    expect(lo).toBeCloseTo(8, 5);
    expect(hi).toBeCloseTo(32, 5);
  });

  it("widens to the donors' own disagreement when they disagree more than the transfer", () => {
    const [lo, hi] = interval({ percentile: 0.50, plusMinus: 12, trust: "close",
                                spread: 50, donors: 3 });
    expect(hi - lo).toBeCloseTo(50, 5);
  });

  it("clamps to the scale rather than reporting a percentile below zero", () => {
    const [lo, hi] = interval({ percentile: 0.02, plusMinus: 22, trust: "remote",
                                spread: 0, donors: 1 });
    expect(lo).toBe(0);
    expect(hi).toBeLessThanOrEqual(100);
  });

  it("keeps a refusal threshold both sides agree on", () => {
    expect(MAX_USEFUL_SPREAD).toBe(
      Number(/MAX_USEFUL_SPREAD = ([\d.]+)/.exec(PY)![1]));
  });
});

describe("combining a panel", () => {
  const near = (percentile: number, over = {}) =>
    ({ percentile, role: "up" as const, areaKm2: 100, years: 40, ...over });

  it("says which absence it is, never just nothing", () => {
    // A reader is owed the reason. Each of these is a different sentence on screen.
    expect(estimate(100, [])).toEqual({ ok: false, why: "no-station" });
    expect(estimate(null, [near(0.2)])).toEqual({ ok: false, why: "no-station" });
    expect(estimate(100, [near(0.2, { areaKm2: 0 })])).toEqual({ ok: false, why: "no-record" });
  });

  it("does not drag the answer toward normal as donors are added", () => {
    // Percentiles are uniform, and averaging uniforms concentrates on 0.5 — so this would
    // play down extremes exactly where the app knows the most.
    const one = estimate(100, [near(0.10)]);
    const three = estimate(100, [near(0.10), near(0.10), near(0.10)]);
    expect(one.ok && three.ok).toBe(true);
    if (one.ok && three.ok)
      expect(three.value.percentile).toBeCloseTo(one.value.percentile, 3);
  });

  it("refuses when the interval cannot separate a low river from a high one", () => {
    const got = estimate(100, [near(0.03), near(0.97)]);
    expect(got).toEqual({ ok: false, why: "too-uncertain" });
  });

  it("weights the trust as it weights the answer", () => {
    /*
     * A distant donor barely votes, so it must not set the label on its own. Taken from
     * the built bundle: a 1.34 km2 section with donors at 3.4, 32 and 207 km2 — the
     * nearest 2.5x away carrying nearly all the weight, the farthest 155x away carrying
     * 0.006 of it. A worst-member rule called that panel "distant".
     */
    const alone = estimate(1.34, [near(0.3, { areaKm2: 3.4 })]);
    const withFar = estimate(1.34, [near(0.3, { areaKm2: 3.4 }),
                                    near(0.3, { areaKm2: 207 })]);
    expect(alone.ok && withFar.ok).toBe(true);
    if (alone.ok && withFar.ok) {
      // it may loosen a little — it must not fall off a cliff
      expect(withFar.value.plusMinus - alone.value.plusMinus).toBeLessThan(1);
    }
  });

  it("does loosen when a distant donor actually carries weight", () => {
    // The other side of it: two donors of equal size, one near and one far, genuinely do
    // make the answer less certain, and the label has to say so.
    const near2 = estimate(100, [near(0.3), near(0.3, { areaKm2: 100 })]);
    const far2 = estimate(100, [near(0.3), near(0.3, { areaKm2: 8_000 })]);
    expect(near2.ok && far2.ok).toBe(true);
    if (near2.ok && far2.ok)
      expect(far2.value.plusMinus).toBeGreaterThan(near2.value.plusMinus);
  });

  it("never reports a confident answer from agreement alone", () => {
    // Three gauges on one river agree because they are the same river. The interval must
    // still carry the transfer error, or agreement would read as certainty.
    const got = estimate(100, [near(0.2), near(0.2), near(0.2)]);
    expect(got.ok).toBe(true);
    if (got.ok) expect(got.value.plusMinus).toBeGreaterThan(10);
  });

  it("counts a downstream donor for less than an upstream one", () => {
    expect(weightFor(0.8, "down", 40)).toBeLessThan(weightFor(0.8, "up", 40));
  });

  it("matches the pipeline's own weighting constants", () => {
    // The other half of the mirror: the ladder is not the only thing written twice.
    const pyPenalty = Number(/DOWNSTREAM_PENALTY = ([\d.]+)/.exec(PY)![1]);
    const pyYears = Number(/RECORD_FULL_YEARS = (\d+)/.exec(PY)![1]);
    expect(weightFor(1, "down", 999)).toBeCloseTo(pyPenalty, 9);
    expect(weightFor(1, "up", pyYears)).toBeCloseTo(1, 9);
    expect(weightFor(1, "up", pyYears / 2)).toBeCloseTo(0.5, 9);
  });
});
