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
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { answering, bandAt, errorFor, leadShares, leaderOf, PENTADS, percentileInBand, weightFactors, type Band }
  from "@app/core";

const ROOT = new URL("../../", import.meta.url).pathname;
const PY = `${ROOT}.venv/bin/python`;

/** Ratios and records spanning every row of the ladder, plus its edges. */
const RATIOS = [1, 1.5, 2.5, 9.9, 10, 10.1, 24, 99, 100, 155, 316, 999, 1000, 5000, 10000,
                50000];
const YEARS = [0, 2, 3, 6, 10, 16, 20, 40, 76];   // 0 and 2: under MIN_RECORD_YEARS, a record ranks nothing (T110)
const ROLES = ["up", "down"] as const;
const RIVERS = [true, false] as const;   // same blue line, or a tributary

/**
 * A water's own gauge comes first (owner 2026-10-22): `(lead ranks, usual weights)` per panel. Covers a leader below
 * the floor, one above it, a quiet rank 1 handing over to rank 2, a lone own gauge, no leader, and nothing reporting.
 * And the panel as a RANKED RESERVE chosen live (owner 2026-10-22, "chosen live"): eight members, the first two own
 * gauges and some others silent (weight 0) — rank 3 leads and the reporting rest re-normalise; and a reserve with no
 * own gauge reporting.
 */
const LEADS: [number[], number[]][] = [
  [[1, 0, 0], [1, 1, 1]],
  [[1, 0, 0], [0.2, 0.75, 0.25]],
  [[1, 0], [9, 1]],
  [[0, 2, 1], [1, 1, 0]],
  [[0, 2, 1], [0.3, 0.05, 0.6]],
  [[1], [0.15]],
  [[1, 0], [0.15, 0]],
  [[0, 0, 0], [0.4, 0.3, 0.2]],
  [[1, 0], [0, 0.4]],
  [[0, 0], [0, 0]],
  [[1, 2, 3, 0, 0, 0, 0, 0], [0, 0, 0.5, 0.9, 0, 0.3, 0.2, 0]],
  [[1, 0, 0, 0, 2, 0, 0, 0], [0, 0.1, 0, 0.4, 0, 0, 0.05, 0.3]],
];

/** The same cases, evaluated by the pipeline's own code. */
function fromPython(): { errors: number[]; weights: number[]; leaders: (number | null)[]; shares: number[][] } {
  const script = `
import json, sys
sys.path.insert(0, ${JSON.stringify(ROOT)})
from pipeline.atlas.gauges.panel import error_for, lead_shares, leader_of, weight_of
leads = ${JSON.stringify(LEADS)}
ratios = ${JSON.stringify(RATIOS)}
years = ${JSON.stringify(YEARS)}
roles = ${JSON.stringify(ROLES)}
rivers = [bool(x) for x in ${JSON.stringify(RIVERS.map((r) => (r ? 1 : 0)))}]
print(json.dumps({
    "errors": [error_for(r) for r in ratios],
    "weights": [weight_of(1.0 / r, role, y, same)
                for r in ratios for role in roles for y in years for same in rivers],
    "leaders": [leader_of(r, w) for r, w in leads],
    "shares": [lead_shares(w, leader_of(r, w)) for r, w in leads],
}))
`;
  return JSON.parse(execFileSync(PY, ["-c", script], { encoding: "utf8" }));
}

// The pipeline's virtualenv is not present on every machine that runs the app's tests.
// Skipped rather than failed there: a check that cannot run is not a check that failed, and
// making it a failure teaches people to ignore it.
const has = existsSync(PY);

describe.skipIf(!has)("one formula, two languages", () => {
  const py = has ? fromPython() : { errors: [], weights: [], leaders: [], shares: [] };

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

  it("picks the same leading own gauge and gives every donor the same share", () => {
    expect(LEADS.map(([r, w]) => leaderOf(r, w))).toEqual(py.leaders);
    expect(LEADS.map(([r, w]) => leadShares(w, leaderOf(r, w))))
      .toEqual(py.shares.map((xs) => xs.map((x) => expect.closeTo(x, 12))));
  });

  it("is actually comparing something", () => {
    // A guard on the guard: if the Python ever returns an empty list, every comparison
    // above passes vacuously and the drift it exists to catch sails through.
    expect(py.errors.length).toBe(RATIOS.length);
    expect(py.weights.length)
      .toBe(RATIOS.length * ROLES.length * YEARS.length * RIVERS.length);
    expect(new Set(py.weights).size).toBeGreaterThan(5);
    expect(py.leaders).toEqual([0, 0, 0, 1, 2, 0, 0, null, null, null, 2, null]);
    expect(py.shares.length).toBe(LEADS.length);
  });
});

/*
 * ONE COMBINE (T125 item 7). The handoff evaluation scripts (handoff/PLAN/app-data/<topic>/scripts) each ported `estimate`
 * by hand — one with the row error where the app interpolates, one with no lead share, none with the reserve — so a
 * measurement taken for an owner decision was not the app's answer. They now import `answering` from panel.py, and this
 * runs it beside the app's `answering` over the same panels: every refusal, the lead, the reserve standing back.
 * Mutation pins (run by hand, 2026-10-23): the row error (`trust_of`) for `error_for` in `_combine_votes` fails "the
 * same answer"; dropping `or leader is not None` from `measured` fails it on the lone-tributary-own-gauge panel.
 */
type C = Parameters<typeof answering>[1][number];
const c = (percentile: number, areaKm2: number, over: Partial<C> = {}): C =>
  ({ percentile, role: "up", areaKm2, years: 20, ...over });
const PANELS: [number | null, C[]][] = [
  [100, []],                                                              // no-station
  [null, [c(0.3, 100)]],                                                  // no-station: no target
  [100, [c(0, 100), c(0.5, 0)]],                                          // no-record: nothing ranks
  [100, [c(0.4, 50_000, { years: 3, role: "down", sameRiver: false })]],  // too little weight
  [100, [c(0.03, 100, { sameRiver: false }), c(0.97, 100, { sameRiver: false })]],   // spread refusal
  [100, [c(0.03, 100), c(0.97, 100, { sameRiver: false })]],              // measured: answers
  [100, [c(0.03, 100, { sameRiver: false, lead: 1 }), c(0.97, 100, { sameRiver: false })]],  // a leader measures
  [1.34, [c(0.3, 3.4), c(0.3, 32, { role: "down" }), c(0.3, 207, { years: 6 })]],
  [4584, [c(0.12, 4313, { lead: 1 }), c(0.55, 795), c(0.8, 90_000, { sameRiver: false })]],
  [100, [c(0.2, 100), c(0.25, 110), c(0.3, 120), c(0.35, 130),
         c(0.99, 100, { reserve: true, sameRiver: false }), c(0.01, 100, { reserve: true, sameRiver: false })]],
  [100, [c(0.02, 100, { sameRiver: false }), c(0.98, 100, { reserve: true, sameRiver: false }),
         c(0.97, 400, { reserve: true, sameRiver: false })]],              // the reserve stands back, lowest first
  [100, [c(0.5, 100, { reserve: true, sameRiver: false }), c(0.02, 100, { reserve: true, sameRiver: false })]],
  ...[0.05, 0.3, 0.5, 0.7, 0.95].flatMap((p): [number, C[]][] => [10, 1000, 25_000].map((a) =>
    [a, [c(p, a * 2.5), c(1 - p / 2, a / 7, { role: "down", years: 12 }), c(p / 3, a * 40, { sameRiver: false, lead: 2 })]])),
];

function answersFromPython(): { answer: { ok: boolean; why?: string; value?: Record<string, number | string> };
                                  stoodBack: number[] }[] {
  const script = `
import json, sys
sys.path.insert(0, ${JSON.stringify(ROOT)})
from pipeline.atlas.gauges.panel import Contribution, answering
out = []
for target, cs in json.loads(${JSON.stringify(JSON.stringify(PANELS))}):
    got, stood = answering(target, [Contribution(x["percentile"], x["role"], x["areaKm2"], x["years"],
                                                 x.get("sameRiver", True), x.get("lead", 0), x.get("reserve", False))
                                    for x in cs])
    if got["ok"]:
        v = got["value"]
        got = {"ok": True, "value": {"percentile": v["percentile"], "plusMinus": v["plus_minus"],
                                     "trust": v["trust"], "spread": v["spread"], "donors": v["donors"]}}
    out.append({"answer": got, "stoodBack": sorted(stood)})
print(json.dumps(out))
`;
  return JSON.parse(execFileSync(PY, ["-c", script], { encoding: "utf8" }));
}

describe.skipIf(!has)("one combine, two languages (T125 item 7)", () => {
  const py = has ? answersFromPython() : [];

  it("gives the same answer, refusal or value, over every panel", () => {
    expect(py.length).toBe(PANELS.length);
    PANELS.forEach(([target, cs], i) => {
      const ours = answering(target, cs);
      const theirs = py[i]!;
      expect([...ours.stoodBack].sort(), `panel ${i}`).toEqual(theirs.stoodBack);
      if (!ours.answer.ok) { expect(theirs.answer, `panel ${i}`).toEqual(ours.answer); return; }
      const v = ours.answer.value;
      // probit / Φ are closed forms in the app and erf/bisection in Python: equal to 1e-4 of a percentile
      expect(theirs.answer, `panel ${i}`).toEqual({ ok: true, value: {
        percentile: expect.closeTo(v.percentile, 4), plusMinus: expect.closeTo(v.plusMinus, 4),
        trust: v.trust, spread: expect.closeTo(v.spread, 2), donors: v.donors } });
    });
  });

  it("is actually comparing something", () => {
    const whys = py.map((x) => (x.answer.ok ? "ok" : x.answer.why));
    for (const w of ["ok", "no-station", "no-record", "too-uncertain"]) expect(whys).toContain(w);
    expect(py.some((x) => x.stoodBack.length > 0)).toBe(true);
    expect(new Set(py.filter((x) => x.answer.ok).map((x) => x.answer.value!.trust)).size).toBeGreaterThan(1);
  });
});

/*
 * ONE DAY'S ENVELOPE (T107). "What is today's normal envelope for this station, and where does today's value rank in
 * it?" The feed answers it in `pipeline/gauges/feed/publish.py` (`band_for`, `percentile_of`) and publishes the rank;
 * the app answers it in `@app/core` `flow.ts` (`bandAt`, `percentileInBand`) to draw the band and freeze a lead-up day.
 * They disagreed by half a pentad every day and across New Year (08MF005 day 109: 1,818 vs 2,058), so the same reading
 * sat in one place on the feed and another on the chart. Both are run here, over every day of a leap year.
 *
 * Mutation pins (each run by hand, 2026-10-23): dropping the `- 0.5` centre offset in `bandAt` fails "the same band";
 * clamping instead of wrapping fails it on days 1-3 and 364-366; `<` for `<=` at p10 in `percentileInBand` fails
 * "the same rank".
 */

/** Pentad envelopes for a few fixture stations, shaped like the real ones they stand for. */
const knotsOf = (mid: number): Band => [mid * 0.5, mid * 0.7, mid, mid * 1.4, mid * 2.1];
const STATIONS: Record<string, (Band | null)[]> = {
  // a snowmelt river: one freshet peak in June, low all winter
  snowmelt: Array.from({ length: PENTADS }, (_, i) => knotsOf(12.3 + 240.7 * Math.exp(-(((i - 32) / 7) ** 2)))),
  // a ramp, so late December and early January are far apart and a missing wrap cannot hide
  ramp: Array.from({ length: PENTADS }, (_, i) => knotsOf(5 + i * 3.17)),
  // thin pentads, both sides of New Year among them: one side answers, then neither does
  thin: Array.from({ length: PENTADS }, (_, i) =>
    [0, 1, 36, 37, 72].includes(i) ? null : knotsOf(40 + 20 * Math.cos((i / PENTADS) * 2 * Math.PI))),
  // a creek that goes dry: flat knots at zero, where the rank has to choose an edge
  dry: Array.from({ length: PENTADS }, (_, i) => (i > 40 && i < 55 ? [0, 0, 0, 0.4, 1.7] as Band : knotsOf(3.3))),
};
const DAYS = Array.from({ length: 366 }, (_, i) => i + 1);
/** Values ranked on every day: the edges, knots and the gaps between them for every station above. */
const VALUES = [0, 0.2, 1, 1.7, 2.5, 3.3, 6, 12.3, 30, 40, 55, 80, 120, 250, 400, 600];

function envelopeFromPython(): Record<string, { bands: (number[] | null)[]; ranks: (number | null)[][] }> {
  const script = `
import json, sys
from datetime import datetime, timedelta, timezone
sys.path.insert(0, ${JSON.stringify(ROOT)})
from pipeline.gauges.feed.publish import band_for, percentile_of
stations = json.loads(${JSON.stringify(JSON.stringify(STATIONS))})
values = ${JSON.stringify(VALUES)}
out = {}
for name, pentads in stations.items():
    env = {str(i): b for i, b in enumerate(pentads) if b is not None}
    bands, ranks = [], []
    for yday in ${JSON.stringify(DAYS)}:
        when = datetime(2024, 1, 1, 12, tzinfo=timezone.utc) + timedelta(days=yday - 1)   # 2024: a leap year
        b = band_for(env, when)
        bands.append(b)
        ranks.append([percentile_of(v, b) for v in values])
    out[name] = {"bands": bands, "ranks": ranks}
print(json.dumps(out))
`;
  return JSON.parse(execFileSync(PY, ["-c", script], { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 }));
}

describe.skipIf(!has)("one day's envelope, two languages (T107)", () => {
  const py = has ? envelopeFromPython() : {};

  it("draws the same band, day for day, for every fixture station", () => {
    for (const [name, pentads] of Object.entries(STATIONS)) {
      const ours = DAYS.map((d) => bandAt(pentads, d));
      // exact: the same arithmetic in the same order gives the same doubles
      expect({ name, bands: ours }).toEqual({ name, bands: py[name]!.bands });
    }
  });

  it("gives the same rank, value for value, on every day", () => {
    for (const name of Object.keys(STATIONS)) {
      const ours = DAYS.map((_, i) => VALUES.map((v) => percentileInBand(v, py[name]!.bands[i] as Band | null)));
      expect({ name, ranks: ours }).toEqual({ name, ranks: py[name]!.ranks });
    }
  });

  it("is actually comparing something, across New Year", () => {
    // A guard on the guard: every station on every day, the wrap days present and non-trivial, the edges reached.
    for (const name of Object.keys(STATIONS)) expect(py[name]!.bands.length).toBe(366);
    const ramp = py.ramp!.bands;
    expect(ramp[0]![2]).toBeCloseTo((5 + 72 * 3.17 + 5) / 2, 9);       // 1 Jan: halfway between pentads 72 and 0
    expect(ramp[365]![2]).toBeCloseTo((5 + 72 * 3.17 + 5) / 2, 9);     // 31 Dec of a leap year: the same place
    expect(py.thin!.bands[0]).toBeNull();                               // pentads 72 and 0 both empty: no answer
    const ranks = Object.values(py).flatMap((s) => s.ranks.flat());
    expect(ranks).toContain(0.05);
    expect(ranks).toContain(0.95);
    expect(new Set(ranks).size).toBeGreaterThan(500);
  });
});

/**
 * NO SECOND COPY (T107). The envelope's knots and the pentad-position arithmetic live in `@app/core` `flow.ts` and
 * nowhere else in the app's source; a screen that wants a day's band or a rank calls `bandAt` / `percentileInBand`.
 * A second copy is how the half-pentad shift happened. Tests are exempt: they state the knots to check them.
 * Mutation pin: pasting `[0.10, 0.25, 0.50, 0.75, 0.90]`, `[.1, .25, .5, .75, .9]`, `(dayOfYear - 1) / 5` or `(doy - 1) / 5`
 * into any other source file fails it.
 */
describe("one day's envelope has one home", () => {
  // Any spelling of the knots (`0.1`, `.10`) and the pentad position under ANY day name (`doy`, `d`), not just the one
  // flow.ts happens to use: a second copy is written by someone who did not know about the first (T107 review).
  const SECOND_COPY = /(?<![\d.])0?\.10?, ?0?\.25, ?0?\.50?, ?0?\.75\b|\b\w+ - 1\) \/ 5\b/;
  const walk = (dir: string): string[] => readdirSync(dir, { withFileTypes: true }).flatMap((e) => {
    const p = `${dir}/${e.name}`;
    if (e.isDirectory()) return e.name === "node_modules" || e.name === "dist" ? [] : walk(p);
    return /\.(ts|tsx|mjs)$/.test(e.name) && !/\.test\.tsx?$/.test(e.name) ? [p] : [];
  });
  it("states the knots and the pentad position only in flow.ts", () => {
    const APP = `${ROOT}app`;
    const hits = ["packages", "apps", "tools"].flatMap((d) => (existsSync(`${APP}/${d}`) ? walk(`${APP}/${d}`) : []))
      .filter((f) => SECOND_COPY.test(readFileSync(f, "utf8")))
      .map((f) => f.slice(APP.length + 1));
    // the knots are GENERATED from the climatology's PCTILES (T125 item 6); flow.ts keeps the pentad arithmetic
    expect(hits).toEqual(["packages/core/src/flow.ts", "packages/core/src/gauge-policy.generated.ts"]);
  });
});
