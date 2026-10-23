/**
 * ONE DONOR STATE (T123; owner 2026-10-22, "unify checks that decide the same thing").
 *
 * "What state is this donor gauge in: reads, not real-time, no forecast, reporting but unranked, quiet?" is core's
 * `donorState` (conditions.ts). Every word about a donor — the row's reading (`donorReads`), its weight line, its
 * screen-reader label, its map dot and key (`donorMapLabel`, routeMap.ts), whether it is silent — is a `switch` over it.
 * Before, conditions.ts (`donorReads`, `donorUnranked`, `donorSilent`, `donorWeightLine`) and routeMap.ts
 * (`donorMapLabel`) each tested `percentile` / `realtime` / `observedAt` themselves, and with a horizon the row said
 * "No forecast for this gauge that far ahead." beside a dot saying "not reporting" (masked: `donorSilent` hides those
 * pins).
 *
 * This pins:
 *   1. for each state, the Conditions line and the map label say the same thing (the acceptance fixture, whole values);
 *   2. `donorState` itself, whole, including its order (a reading wins; not-real-time at every horizon; a horizon's
 *      missing forecast before today's reports);
 *   3. no module but `donorState` tests a donor's reading fields: `.percentile` against null, `.realtime` against a
 *      boolean, or the "within a day" `observedAt` parse. Each pattern is proven able to fail on the spelling it
 *      replaced (`SPELLINGS`), so a pass is not a regex that cannot match.
 *
 * Mutations (each run 2026-10-23, each failed the named test and was reverted):
 *   - `donorMapLabel`'s `case "no-forecast"` returning `ROUTE_MAP_WORDS.quiet` (the old map word) fails test 1 here and
 *     routeMap.test "a day ahead with no forecast says no forecast".
 *   - moving `donorState`'s horizon check before `realtime === false` fails test 2 ("not-realtime at a horizon") and
 *     test 1 (the +3 not-real-time row's label turns "no forecast for this gauge").
 *   - re-adding `if (r.percentile !== null) return donorShare(r);` to `donorMapLabel` fails test 3 (`percentile`).
 *   - writing `quiet: r.percentile === null` back into ui `panel.ts` fails test 3 (`percentile`).
 *   - (adversarial review) `quiet: typeof r.percentile !== "number"` in ui `panel.ts` gauge choices fails test 3
 *     (`percentileType`), which the null-comparison pattern alone let through.
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { donorMapKey, donorMapLabel, donorReads, donorRowLabel, donorSilent, donorState, donorWeightLine,
         type DonorFacts, type Horizon } from "@app/core";

const APP = fileURLToPath(new URL("..", import.meta.url));
const ROOTS = ["apps", "packages"];

function sources(dir: string, out: string[] = []): string[] {
  for (const f of readdirSync(dir)) {
    if (f === "node_modules" || f === "dist" || f.startsWith(".")) continue;
    const p = join(dir, f);
    if (statSync(p).isDirectory()) sources(p, out);
    else if (/\.(ts|tsx|mjs)$/.test(f) && !/\.test\.tsx?$/.test(f) && !/\.d\.ts$/.test(f)) out.push(p);
  }
  return out;
}
const files = () => ROOTS.flatMap((r) => sources(join(APP, r))).map((p) => relative(APP, p)).sort();

/**
 * A module's text; "" for one gone since the walk — check-boundaries.test writes and removes a probe module
 * (`packages/ui/src/_bnd.ts`) while the suite runs in parallel.
 */
function read(p: string): string {
  try { return readFileSync(join(APP, p), "utf8"); } catch (e) {
    if ((e as NodeJS.ErrnoException).code === "ENOENT") return "";
    throw e;
  }
}

const DONOR_STATE_FILE = "packages/core/src/conditions.ts";
/** A module's text, with `donorState`'s own body cut out — the one place the fields may be tested. */
function outsideDonorState(p: string): string {
  const text = read(p);
  if (p !== DONOR_STATE_FILE) return text;
  const start = text.indexOf("export function donorState(");
  const end = text.indexOf("\n}\n", start);
  expect(start).toBeGreaterThan(-1);
  return text.slice(0, start) + text.slice(end + 3);
}

/** The ways a module can decide a donor's state itself. */
const TESTS: Record<string, RegExp> = {
  percentile: /\.percentile\s*(?:!==?|===?)\s*null\b/g,
  // `typeof r.percentile === "number"` is the same test spelled another way (review)
  percentileType: /typeof\s+[\w.?]*\.percentile\b/g,
  realtime: /\.realtime\s*(?:!==?|===?)\s*(?:true|false)\b/g,
  withinADay: /\.observedAt\s*\?\s*Date\.parse/g,
};

/** The spellings T123 replaced, one per pattern. */
const SPELLINGS: Record<string, string> = {
  percentile: `  if (r.percentile !== null) return donorShare(r);`,
  percentileType: `  if (typeof r.percentile === "number") return donorShare(r);`,
  realtime: `  if (r.realtime === false) return ROUTE_MAP_WORDS.notRealtime;`,
  withinADay: `  const t = r.observedAt ? Date.parse(r.observedAt) : NaN;`,
};

/**
 * NOT DONORS, so not this decision: a saved spot's own frozen reading (`SpotReading`), which has no state to be in —
 * it was read, and its percentile is shown when it has one. By file and count, so a new test there still fails.
 */
const NOT_DONORS: Record<string, number> = {
  "packages/data/src/spots/words.ts: percentile": 1,
  "packages/ui-native/src/SpotScreen.tsx: percentile": 1,
};

const REF = Date.parse("2026-10-21T05:00:00Z");
const row = (over: Partial<DonorFacts> = {}): DonorFacts => ({
  station: "08MH024", role: "up", percentile: null, weight: 0, areaRatio: 2.4, areaKm2: 120, years: 40,
  regulated: false, factors: { share: 0.42, role: 1, record: 1 }, ...over,
});

/** One fixture per state: the row, the horizon, and the word both lines say. */
const FIXTURES: { state: string; r: DonorFacts; horizon: Horizon; word: string }[] = [
  { state: "reads", r: row({ percentile: 0.2, weight: 0.62 }), horizon: 0, word: "62%" },
  { state: "not-realtime", r: row({ realtime: false }), horizon: 0, word: "not a real-time gauge" },
  { state: "not-realtime", r: row({ realtime: false }), horizon: 3, word: "not a real-time gauge" },
  { state: "no-forecast", r: row(), horizon: 3, word: "no forecast" },
  { state: "no-forecast", r: row(), horizon: "change", word: "no forecast" },
  { state: "unranked", r: row({ observedAt: "2026-10-20T20:35:00-08:00" }), horizon: 0, word: "reporting" },
  { state: "quiet", r: row(), horizon: 0, word: "not reporting" },
];

describe("one donor state: core's donorState decides, every label switches over it (T123)", () => {
  it("each state: the Conditions line and the map label say the same word", () => {
    const got = FIXTURES.map(({ r, horizon, word }) => {
      const o = { ref: REF, horizon };
      // the Conditions line: the row's reading, or for a counted donor its weight line (the number the dot shows)
      const line = donorState(r, o).kind === "reads" ? donorWeightLine(r) : donorReads(r, o);
      const map = donorMapLabel(r, o);
      return { kind: donorState(r, o).kind, line, map, key: donorMapKey(r, o), silent: donorSilent(r, o),
               label: donorRowLabel(r, false, false, o),
               same: line.toLowerCase().includes(word) && map.toLowerCase().includes(word) };
    });
    expect(got).toEqual([
      { kind: "reads", line: "62% of this estimate", map: "62%", key: "08MH024 · 62%", silent: false,
        label: "08MH024, upstream, 2.4× apart, 62 per cent of the answer", same: true },
      { kind: "not-realtime", line: "Not a real-time gauge.", map: "not a real-time gauge",
        key: "08MH024 · not a real-time gauge", silent: true,
        label: "08MH024, upstream, 2.4× apart, not a real-time gauge, so it counts for nothing", same: true },
      { kind: "not-realtime", line: "Not a real-time gauge.", map: "not a real-time gauge",
        key: "08MH024 · not a real-time gauge", silent: true,
        label: "08MH024, upstream, 2.4× apart, not a real-time gauge, so it counts for nothing", same: true },
      { kind: "no-forecast", line: "No forecast for this gauge that far ahead.", map: "no forecast",
        key: "08MH024 · no forecast", silent: true,
        label: "08MH024, upstream, 2.4× apart, no forecast for this gauge, so it counts for nothing", same: true },
      { kind: "no-forecast", line: "No forecast running for this gauge now.", map: "no forecast",
        key: "08MH024 · no forecast", silent: true,
        label: "08MH024, upstream, 2.4× apart, no forecast for this gauge, so it counts for nothing", same: true },
      { kind: "unranked", line: "Reporting, but its reading cannot be ranked today.", map: "reporting, not ranked",
        key: "08MH024 · reporting, not ranked", silent: false,
        label: "08MH024, upstream, 2.4× apart, reporting but not ranked today, so it counts for nothing", same: true },
      { kind: "quiet", line: "Not reporting today.", map: "not reporting", key: "08MH024 · not reporting",
        silent: true, label: "08MH024, upstream, 2.4× apart, not reporting today, so it counts for nothing",
        same: true },
    ]);
    // every state has a fixture
    expect([...new Set(got.map((g) => g.kind))].sort())
      .toEqual(["no-forecast", "not-realtime", "quiet", "reads", "unranked"]);
  });

  it("the decision itself, whole, and its order", () => {
    const seen = "2026-10-20T20:35:00-08:00";        // within a day of REF
    const old = "2026-10-18T20:35:00-08:00";         // more than a day before it
    expect([
      donorState(row({ percentile: 0.4, realtime: false }), { ref: REF }),      // a reading wins over the list
      donorState(row({ percentile: 0.4 }), { ref: REF, horizon: 5 }),           // a forecast is a reading ahead
      donorState(row({ realtime: false }), { ref: REF, horizon: 5 }),           // not-realtime at a horizon
      donorState(row({ observedAt: seen }), { ref: REF, horizon: 1 }),          // ahead: today's report is no forecast
      donorState(row({ observedAt: seen }), { ref: REF }),
      donorState(row({ observedAt: seen })),                                    // no `ref`: nothing to be within
      donorState(row({ observedAt: old }), { ref: REF }),
      donorState(row({ observedAt: "garbage" }), { ref: REF }),
      donorState(row({ realtime: true }), { ref: REF }),
    ].map((s) => s.kind)).toEqual(["reads", "reads", "not-realtime", "no-forecast", "unranked", "quiet", "quiet",
                                   "quiet", "quiet"]);
    expect(donorState(row({ percentile: 0.4 }))).toEqual({ kind: "reads", percentile: 0.4 });
  });

  it("each pattern catches the spelling it replaced", () => {
    expect(Object.fromEntries(Object.entries(TESTS).map(([k, re]) => [k, new RegExp(re.source).test(SPELLINGS[k]!)])))
      .toEqual({ percentile: true, percentileType: true, realtime: true, withinADay: true });
  });

  it("no module but donorState tests a donor's reading fields", () => {
    const hits: Record<string, number> = {};
    for (const p of files()) {
      const text = outsideDonorState(p);
      for (const [k, re] of Object.entries(TESTS)) {
        const n = text.match(re)?.length ?? 0;
        if (n) hits[`${p}: ${k}`] = n;
      }
    }
    expect(hits).toEqual(NOT_DONORS);
  });

  it("donorState holds the tests it replaced (the cut is not empty)", () => {
    const text = readFileSync(join(APP, DONOR_STATE_FILE), "utf8");
    const body = text.slice(text.indexOf("export function donorState("));
    const own = body.slice(0, body.indexOf("\n}\n"));
    expect(Object.fromEntries(Object.entries(TESTS).map(([k, re]) => [k, own.match(re)?.length ?? 0])))
      .toEqual({ percentile: 1, percentileType: 0, realtime: 1, withinADay: 1 });
  });
});
