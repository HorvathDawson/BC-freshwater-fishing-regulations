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
import { build } from "../src/dfo/feed";
import { ROW_HELD_VERDICT } from "../src/dfo/live";
import { markShared, poolOf, words, type QuotaTier } from "../src/dfo/quota";
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
  /** (T671 5B) the curated place's words and `within` areas (quota.poolOf) */
  place?: { specific_area: string; within: string[] };
  /** (T671 5B) [species, MMDD, the day's quota line — null where nothing opens] */
  quota?: [string, number, string | null][];
  /** (T679 D6) rows that do not type: the error's type name and what the place reads in robust mode */
  robust?: { error: string; own: [string, number, string | null][] };
}
const CASES: Case[] = JSON.parse(readFileSync(join(__dirname, "fixtures/dfo/dfo-rows.json"), "utf-8")).cases;
const REASONS = { tbd: TBD, gear_permission: GEAR_PERMISSION };

const rulesOf = (rows: Row[]): TypedRule[] =>
  rows.flatMap(([species, dates, limits_gear], i) => toRules({ species, dates, limits_gear }, "t671", i, REG));

/** the place's own verdict for a species on MMDD, or null where the place says nothing that day */
function verdict(rules: TypedRule[], sp: string, md: number): (Verdict & { tree?: QuotaTier }) | null {
  const verdicts = new Interner<unknown>();
  const cal = locationCalendar(rules as never[], verdicts, new Interner<unknown>());
  const runs = cal.sp[SPECIES.indexOf(sp)]!;
  const day = DAYS.indexOf(md);
  let v = -1;
  for (let k = 0; k < runs.length; k += 2) if (runs[k]! <= day) v = runs[k + 1]!;
  return v >= 0 ? (verdicts.items[v] as Verdict & { tree?: QuotaTier }) : null;
}
const status = (rows: Row[], sp: string, md: number): string | null => verdict(rulesOf(rows), sp, md)?.s ?? null;

it("the shared file was read", () => {
  expect(CASES.length).toBeGreaterThanOrEqual(13);
  expect(CASES.reduce((n, c) => n + (c.own?.length ?? 0), 0)).toBeGreaterThanOrEqual(40);
  expect(CASES.reduce((n, c) => n + (c.typed?.length ?? 0), 0)).toBeGreaterThanOrEqual(6);
});

describe("(T671 D2, D1; T676) the shared hand rows: typed and answered as the file says", () => {
    it.each(CASES.map((c) => [c.name, c] as const))("%s: typed and answered as the shared file says", (_name, c) => {
      if (c.robust) {
        // (T679 D6) the robust reader: the locator carries `error` and no rules (never a refused page), and live.ts
        // holds the place — with no last answers it reads ROW_HELD_VERDICT, "check", every species, every day
        const recs = c.rows.map(([species, dates, limits_gear], order) =>
          ({ fingerprint: "f".repeat(16), species, dates, limits_gear, fishery_notices: [], source: "", order }));
        const [loc] = build({ region: "2", date_modified: null, preamble: [], rules: recs }, true, REG).locators;
        expect([loc!.rules, loc!.error?.split(":")[0]]).toEqual([[], c.robust.error]);
        expect(c.robust.own.map(([sp, md]) => [sp, md, ROW_HELD_VERDICT.s])).toEqual(c.robust.own);
        expect(() => build({ region: "2", date_modified: null, preamble: [], rules: recs }, false, REG)).toThrow();
        return;
      }
      (c.typed ?? []).forEach((want, i) => {
        const [species, dates, limits_gear] = c.rows[i]!;
        const rule = toRules({ species, dates, limits_gear }, "t671", i, REG)[0]!;
        if ("type" in want) expect(rule.type).toBe(want.type);
        if ("species" in want) expect(rule.species ?? null).toEqual(want.species);
        if (want.reason) expect(rule.review_reason!.startsWith(REASONS[want.reason])).toBe(true);
      });
      expect((c.own ?? []).map(([sp, md]) => [sp, md, status(c.rows, sp, md)])).toEqual(c.own ?? []);
    });
  });

// (T671 5B, owner 2026-11-12) "in the aggregate": a water of MUs 5-6 to 5-11 not in the table shares the ALL row's limits
// (one total across those waters); a water with its own row has its own quota; Region 6 A's words pool nothing (Q28).
// The live join marks the place's rules (live.ts `join`: markShared(rules, poolOf(place))), as the Python's live.join and
// the release do. The same shared cases as pipeline/tests/test_t671_salmon_rows.py.
const POOLED = CASES.filter((c) => c.quota);
it("the pooled cases were read", () => expect(POOLED.length).toBeGreaterThanOrEqual(3));
describe("(T671 5B) the shared places pool or not, and say their quota as the file says", () => {
  it.each(POOLED.map((c) => [c.name, c] as const))("%s", (name, c) => {
    const place = { location_id: name, source_text: { specific_area: c.place!.specific_area },
                    binding: { extents: c.place!.within.map((a) => ({ op: "within", area_id: a })) } };
    const rules = markShared(rulesOf(c.rows), poolOf(place));
    expect(c.quota!.map(([sp, md]) => {
      const v = verdict(rules, sp, md);
      return [sp, md, v && v.s === "open" ? words(v.tree) : null];
    })).toEqual(c.quota);
  });
  it("a pooled place bound to no `within` area is an error, never words with nothing to bind", () => {
    expect(() => poolOf({ location_id: "x", source_text: { specific_area: "in the aggregate (combined total)" }, binding: { extents: [] } }))
      .toThrow(/bound to no `within` area/);
  });
});
