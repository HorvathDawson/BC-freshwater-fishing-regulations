/**
 * THE PIPELINE AND THE APP MUST BE ONE FORMULA. This test runs both.
 *
 * The weighting exists twice — `pipeline/atlas/gauges/panel.py` decides which donors go in
 * the bundle, `packages/core/src/trust.ts` decides what they are worth on screen — and it
 * has to, because one runs in Python at build time and the other in the browser on a tap.
 * Two implementations of one formula drift; that is not a risk, it is a certainty given
 * enough edits.
 *
 * `trust-ladder.test.ts` beside this one parses the CONSTANTS out of the Python and
 * compares them. That catches a changed number and misses a changed shape — and a changed
 * shape is exactly what happened: the weight went from `share` to inverse variance in one
 * file, and the same edit had to be made by hand in the other. Nothing would have failed if
 * it had not been.
 *
 * So this executes the real Python against the real TypeScript over the same inputs and
 * demands the same answers. It is the only check that cannot be satisfied by a copy that
 * merely looks similar.
 */
import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { errorFor, weightFactors } from "@app/core";

const ROOT = new URL("../../", import.meta.url).pathname;
const PY = `${ROOT}.venv/bin/python`;

/** Ratios and records spanning every row of the ladder, plus its edges. */
const RATIOS = [1, 1.5, 2.5, 9.9, 10, 10.1, 24, 99, 100, 155, 316, 999, 1000, 5000, 10000,
                50000];
const YEARS = [3, 6, 10, 16, 20, 40, 76];
const ROLES = ["up", "down"] as const;
const RIVERS = [true, false] as const;   // same blue line, or a tributary

/** The same cases, evaluated by the pipeline's own code. */
function fromPython(): { errors: number[]; weights: number[] } {
  const script = `
import json, sys
sys.path.insert(0, ${JSON.stringify(ROOT)})
from pipeline.atlas.gauges.panel import error_for, weight_of
ratios = ${JSON.stringify(RATIOS)}
years = ${JSON.stringify(YEARS)}
roles = ${JSON.stringify(ROLES)}
rivers = [bool(x) for x in ${JSON.stringify(RIVERS.map((r) => (r ? 1 : 0)))}]
print(json.dumps({
    "errors": [error_for(r) for r in ratios],
    "weights": [weight_of(1.0 / r, role, y, same)
                for r in ratios for role in roles for y in years for same in rivers],
}))
`;
  return JSON.parse(execFileSync(PY, ["-c", script], { encoding: "utf8" }));
}

// The pipeline's virtualenv is not present on every machine that runs the app's tests.
// Skipped rather than failed there: a check that cannot run is not a check that failed, and
// making it a failure teaches people to ignore it.
const has = existsSync(PY);

describe.skipIf(!has)("one formula, two languages", () => {
  const py = has ? fromPython() : { errors: [], weights: [] };

  it("computes the same donor error, ratio for ratio", () => {
    // Including the exact band edges, where an off-by-one in the interpolation hides.
    expect(RATIOS.map((r) => errorFor(r))).toEqual(
      py.errors.map((e) => expect.closeTo(e, 9)));
  });

  it("computes the same weight, over every ratio, role, record and river", () => {
    const ours: number[] = [];
    for (const r of RATIOS)
      for (const role of ROLES)
        for (const y of YEARS)
          for (const same of RIVERS) {
            const f = weightFactors(1 / r, role, y, same);
            ours.push(f.share * f.role * f.record);
          }
    expect(ours.length).toBe(py.weights.length);
    for (let i = 0; i < ours.length; i++) expect(ours[i]!).toBeCloseTo(py.weights[i]!, 9);
  });

  it("is actually comparing something", () => {
    // A guard on the guard: if the Python ever returns an empty list, every comparison
    // above passes vacuously and the drift it exists to catch sails through.
    expect(py.errors.length).toBe(RATIOS.length);
    expect(py.weights.length)
      .toBe(RATIOS.length * ROLES.length * YEARS.length * RIVERS.length);
    expect(new Set(py.weights).size).toBeGreaterThan(5);
  });
});
