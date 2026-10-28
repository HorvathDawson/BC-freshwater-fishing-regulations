// T225: the TypeScript DFO feed (src/dfo) against the Python reference (pipeline/regs/dfo_salmon/live.py), byte for
// byte, over recorded scenarios (test/fixtures/dfo/scenario-*.json). Needs the repo's .venv (the Python side runs).
import { existsSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { REPO, runPy, runTs } from "./dfo.scenario";

const FIX = join(REPO, "app/workers/feeds/test/fixtures/dfo");
const scenarios = readdirSync(FIX).filter((f) => /^scenario-.*\.json$/.test(f)).sort();
const hasPython = existsSync(join(REPO, ".venv/bin/python"));

function firstDiff(a: string, b: string): string {
  let i = 0;
  while (i < a.length && a[i] === b[i]) i++;
  return `at ${i}: py …${JSON.stringify(a.slice(Math.max(0, i - 120), i + 160))}\n     ts …${JSON.stringify(b.slice(Math.max(0, i - 120), i + 160))}`;
}

describe.skipIf(!hasPython)("DFO feed parity (TS port vs Python live.run)", () => {
  for (const sc of scenarios)
    it(sc, async () => {
      const path = join(FIX, sc);
      const py = runPy(path);
      const ts = await runTs(path);
      expect(ts.length).toBe(py.length);
      for (let n = 0; n < py.length; n++) {
        const [p, t] = [py[n]!, ts[n]!];
        if (p.index !== t.index) throw new Error(`${sc} step ${n} (${p.name}) index.json differs ${firstDiff(p.index, t.index)}`);
        if (p.unplaced !== t.unplaced) throw new Error(`${sc} step ${n} (${p.name}) unplaced.json differs ${firstDiff(p.unplaced, t.unplaced)}`);
        expect(t.notices, `${sc} step ${n} notices`).toEqual(p.notices);
      }
    });
});
