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
import { ERROR_BY_RATIO, MAX_USEFUL_SPREAD, interval, trustFor } from "@app/core";

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
