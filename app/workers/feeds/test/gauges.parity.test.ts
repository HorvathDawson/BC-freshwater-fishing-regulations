// T225 gauge parity: the PYTHON publisher (pipeline/gauges/feed/publish.py, the spec) and the TS port (runGauges) on
// the committed recording (test/fixtures/gauges: 22 stations — level-only, temperature, CLEVER/COFFEE/ELF, a first run,
// an archive refill, a 404 archive, a silent listed station, a hydat + roster notice sent through notify), same seed,
// same frozen clock. Every output file must parse to the same JSON; the bytes differ only in number spelling (Python
// writes 5.0, JS 5) and notified.json's separators. (T556) FAILS where the repo venv is absent: never skipped.
// INTENDED DIFFERENCES (T225 follow-up, owner 2026-10-28) are folded back and checked by `normalizeIntended`
// (parity/gauges/parity.ts): temp_history.json private, the append-only runs archive (forecastIssuedAt is the same since T557).
import { join } from "node:path";
import { beforeAll, describe, expect, it } from "vitest";
import { needPython } from "../parity/gauges/node";
import { parity } from "../parity/gauges/parity";

const FIX = join(__dirname, "fixtures/gauges");

describe("gauge feed parity (python vs ts)", () => {
  beforeAll(() => void needPython());
  it("every output file is the same JSON", async () => {
    const r = await parity(FIX, "https://ntfy.sh/parity-test");
    console.log("intended differences:", r.intended);
    expect(r.intended).toContain("temp_history.json: private");
    expect(r.intended.some((n) => /^runs: \d+ append-only/.test(n))).toBe(true);
    expect(r.misses.py).toEqual([]);
    expect(r.misses.ts).toEqual([]);
    expect(r.posts.ts).toEqual(r.posts.py);
    const bad = r.diffs.filter((d) => !["identical", "bytes-only", "ulp"].includes(d.kind));
    expect(bad).toEqual([]);
    expect(r.diffs.length).toBeGreaterThan(20);
    expect(r.summary.answered).toBe(21);
    expect(r.summary.notify.sent.length).toBe(2);
    // only these byte classes remain
    for (const d of r.diffs.filter((d) => d.kind === "bytes-only"))
      expect(d.detail).toMatch(/^(integral float \(5\.0 vs 5\)|default separators \(', ' ': '\))$/);
  });
});
