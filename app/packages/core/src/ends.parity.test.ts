/**
 * (T517, owner 2026-11-09) ONE NAME, TWO READERS: the app's `spanName` and the pipeline's name check (`name_check.words`,
 * pipeline/tests/test_name_check.py) read the SAME fixture, `ends.parity.json` — its `display.ends` and every case's
 * water, ends (and, for a joined span, its last piece's upper end) — and must say every case's `name`. Imported, never
 * read from disk (core has no platform: AGENTS rule 22).
 */
import { describe, expect, it } from "vitest";
import parity from "./ends.parity.json" with { type: "json" };
import { spanName, type EndsTable } from "./ends";

interface Case { readonly water: string; readonly lo: string | null; readonly hi: string | null;
                 readonly last_hi?: string | null; readonly name: string }
const P = parity as unknown as { readonly ends: EndsTable; readonly cases: readonly Case[] };

describe("ends parity: the app's spanName says what the pipeline's name check says", () => {
  it("holds cases", () => expect(P.cases.length).toBeGreaterThan(0));
  for (const c of P.cases)
    it(`${c.water}: ${c.lo ?? "∅"} → ${c.last_hi !== undefined ? `${c.hi ?? "∅"} … ${c.last_hi ?? "∅"}` : c.hi ?? "∅"}`, () => {
      const name = c.last_hi !== undefined
        ? spanName(P.ends, c.water, { lo: c.lo, hi: c.hi }, { lo: null, hi: c.last_hi })
        : spanName(P.ends, c.water, { lo: c.lo, hi: c.hi });
      expect(name).toBe(c.name);
    });
});
