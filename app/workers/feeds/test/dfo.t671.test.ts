// T671 D2 (the Worker half of pipeline/tests/test_t671_salmon_rows.py): a DFO row nobody could type keeps the species
// its cell names. "Chum | To be determined" reads `check` for chum only; "Sockeye | Apr 1 to Mar 31 | To be determined"
// reads `check` for sockeye on its days (it answered for no fish); a cell naming no salmon the table knows reads for
// every salmon, never none. Both modes: the Python's byte-for-byte one and robust.
//
// (T556) THE ROWS AND THE ANSWERS ARE ONE SHARED FILE, `fixtures/dfo/dfo-rows.json`, which the Python test reads too: each
// side held its own copy of "the same rows", which is how two copies of one rule drift. A missing file fails (a read).
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { DAYS, Interner, locationCalendar, type Verdict } from "../src/dfo/salmon";
import { GEAR_PERMISSION, TBD, toRules, type TypedRule } from "../src/dfo/typed";
import { REG } from "./wording.fixture";

const SPECIES = ["CH", "CO", "SK", "PK", "CM"];
type Row = [species: string, dates: string, limits: string];
interface Case {
  name: string;
  rows: Row[];
  /** per row, what is asserted of its first rule (a key left out is not asserted; species null = the rule carries none) */
  typed?: { type?: string; species?: string[] | null; reason?: "tbd" | "gear_permission" }[];
  /** [species, MMDD, the place's own status that day — null where the place says nothing] */
  own?: [string, number, string | null][];
}
const CASES: Case[] = JSON.parse(readFileSync(join(__dirname, "fixtures/dfo/dfo-rows.json"), "utf-8")).cases;
const REASONS = { tbd: TBD, gear_permission: GEAR_PERMISSION };

const rulesOf = (rows: Row[], robust: boolean): TypedRule[] =>
  rows.flatMap(([species, dates, limits_gear], i) => toRules({ species, dates, limits_gear }, "t671", i, robust, REG));

/** the place's own answer for a species on MMDD: its status, or null where the place says nothing that day */
function status(rows: Row[], robust: boolean, sp: string, md: number): string | null {
  const verdicts = new Interner<unknown>();
  const cal = locationCalendar(rulesOf(rows, robust) as never[], verdicts, new Interner<unknown>(), robust);
  const runs = cal.sp[SPECIES.indexOf(sp)]!;
  const day = DAYS.indexOf(md);
  let v = -1;
  for (let k = 0; k < runs.length; k += 2) if (runs[k]! <= day) v = runs[k + 1]!;
  return v >= 0 ? (verdicts.items[v] as Verdict).s : null;
}

it("the shared file was read", () => {
  expect(CASES.length).toBeGreaterThanOrEqual(13);
  expect(CASES.reduce((n, c) => n + (c.own?.length ?? 0), 0)).toBeGreaterThanOrEqual(40);
  expect(CASES.reduce((n, c) => n + (c.typed?.length ?? 0), 0)).toBeGreaterThanOrEqual(6);
});

for (const robust of [false, true])
  describe(`(T671 D2) a row nobody could type keeps its species — ${robust ? "robust" : "pythonParity"}`, () => {
    it.each(CASES.map((c) => [c.name, c] as const))("%s: typed and answered as the shared file says", (_name, c) => {
      (c.typed ?? []).forEach((want, i) => {
        const [species, dates, limits_gear] = c.rows[i]!;
        const rule = toRules({ species, dates, limits_gear }, "t671", i, robust, REG)[0]!;
        if ("type" in want) expect(rule.type).toBe(want.type);
        if ("species" in want) expect(rule.species ?? null).toEqual(want.species);
        if (want.reason) expect(rule.review_reason!.startsWith(REASONS[want.reason])).toBe(true);
      });
      expect((c.own ?? []).map(([sp, md]) => [sp, md, status(c.rows, robust, sp, md)])).toEqual(c.own ?? []);
    });
  });
