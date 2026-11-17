// T770 eulachon (owner 2026-11-16 19:09: "As a closure line (Recommended)"), the worker half of
// pipeline/tests/test_t770_gear_lines_stack.py: DFO's "All | No fishing for eulachon" row (Region 6 B(ii)) is the place's
// closure line `o` in the Python's words, never a gear line. The saved page and the shared hand rows are pinned against
// the Python by dfo.vectors.test.ts; these assert the finished strings (rule 33).
import { describe, expect, it } from "vitest";
import { Interner, locationCalendar } from "../src/dfo/salmon";
import { typedRules } from "../src/dfo/typed";
import { REG } from "./wording.fixture";

const year = (rows: [string, string, string][]) => {
  const rules = rows.flatMap(([species, dates, limits_gear], i) => typedRules({ species, dates, limits_gear }, "rows", i, REG));
  const gears = new Interner<unknown>();
  const cal = locationCalendar(rules as never[], new Interner<unknown>(), gears);
  const lines = (runs: number[] | undefined) => (runs ?? []).map((x, k) => (k % 2 ? (x >= 0 ? gears.items[x] : null) : x));
  return { g: lines(cal.g), o: cal.o === undefined ? undefined : lines(cal.o) };
};

describe("(T770) another fish's closure is the place's closure line `o`, not a gear line", () => {
  it("all year: 'Eulachon closed all year: No fishing for eulachon', every day; no gear line", () => {
    expect(year([["All", "Apr 1 to Mar 31", "No fishing for eulachon"]]))
      .toEqual({ g: [0, null], o: [0, ["Eulachon closed all year: No fishing for eulachon"]] });
  });
  it("dated: its dates replace 'all year'; beside a dated bait row each keeps its own field", () => {
    const y = year([["All", "Sep 1 to Dec 31", "Bait ban"], ["All", "Nov 1 until further notice", "No fishing for eulachon"]]);
    expect(y.o).toEqual([0, ["Eulachon closed from Nov 1 until further notice: No fishing for eulachon"]]);
    expect(y.g.filter((_, k) => k % 2).flat().some((l) => /eulachon/i.test(String(l)))).toBe(false);
  });
  it("a place with no other fish's closure carries no `o`", () => {
    expect(year([["All", "Sep 1 to Dec 31", "Bait ban"]]).o).toBeUndefined();
  });
});
