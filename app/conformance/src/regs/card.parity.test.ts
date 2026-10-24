/**
 * T08 parity: core's card model — `buildModel(inp, place, settle(…))` — against page v36's OWN buildModel()
 * (app/fixtures/golden/hydrate.json, port/hydrate.mjs, YEAR 2026) on all 134 golden
 * states, inputs from T09 `inputsOf` over the live release paired with the oracles (skipped with the reason otherwise; a
 * failure under REGS_STORE_REQUIRED=1). Every expected value is the oracle's except the ticket's literals (page v36's
 * output, quoted by the ticket). Infinity ↔ the oracle's "Infinity".
 *
 * Ticket item 6 (the year strip vs the oracle's `dayStatus` runs) reads the STORE's strip (rows 30 `rows[].year`, decided
 * by the pipeline with page v36's rule — `pipeline/deliver/answers/year.py`); TS derives nothing: each drawn row's
 * `HydRow.year` is the stored runs, and they must equal page v36's dayStatus on every golden state.
 */
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { buildModel, makePlace, settle, type CardModel, type HydRes } from "@app/core";
import { inputsOf, isGone, oracleRix, paritySkip, releasePairing } from "./golden";
import { HYDRATE } from "./oracles";

type Res = { status: string; daily: number | string | null; win: number; narrow: number | null } | null;
interface OracleModel {
  spp: string[]; line: string | null; missingRows: boolean; status: string | null; broad: number[];
  rows: { kind: string; pool: number | null; win: number | null; daily: number | string | null; narrow: number | null; members: string[] }[];
  R: Record<string, { H: Res; W: Res }>;
}
const H = JSON.parse(readFileSync(HYDRATE, "utf8")) as {
  meta: { version: string; year: number };
  states: Record<string, { s: number; model: OracleModel; dayStatus: [string, number][][] }>;
};
/** (T115) every hydrated state but the 4 whose part the release merged away (taps.json `gone`, golden.ts isGone) */
const ids = Object.keys(H.states).filter((id) => !isGone(id));
const YEAR = H.meta.year;
const at = await releasePairing();
const pairing = paritySkip(at);
/** page v36's rule numbers (the oracle's, and the ticket's literals quoted from it) → the paired store's, by durable key
 *  (golden.ts `oracleRix`): a rule added or dropped ahead of a rix renumbers it without changing a word. */
const R = (await oracleRix()) ?? ((r: number) => r);
const Rn = (r: number | null) => (r === null ? null : R(r));
const mapRes = (h: Res): Res => h && { ...h, win: R(h.win), narrow: Rn(h.narrow) };
const mapModel = (o: OracleModel): OracleModel => ({
  ...o, broad: o.broad.map(R), rows: o.rows.map((r) => ({ ...r, pool: Rn(r.pool), win: Rn(r.win), narrow: Rn(r.narrow) })),
  R: Object.fromEntries(Object.entries(o.R).map(([k, v]) => [k, { H: mapRes(v.H), W: mapRes(v.W) }])),
});

const inf = (x: number | null) => (x === Infinity ? "Infinity" : x);
const res = (h: HydRes | null): Res => h && { status: h.status, daily: inf(h.daily), win: h.win.rix, narrow: h.narrow?.rix ?? null };
const project = (m: CardModel): OracleModel => ({
  spp: [...m.spp], line: m.line, missingRows: m.missingRows, status: m.status, broad: m.broad.map((r) => r.rix),
  rows: m.rows.map((r) => ({ kind: r.kind, pool: r.pool?.rix ?? null, win: r.win?.rix ?? null, daily: inf(r.daily),
    narrow: r.narrow?.rix ?? null, members: [...r.members] })),
  R: Object.fromEntries(Object.entries(m.R).map(([k, v]) => [k, { H: res(v.H), W: res(v.W) }])),
});
const rowLine = (m: CardModel) => m.rows.map((r) => [r.kind, r.pool?.rix ?? null, r.daily, r.members]);

it("the oracle: page v38's buildModel over the 134 golden states, YEAR 2026", () => {
  expect([H.meta.version, YEAR, Object.keys(H.states).length, ids.length]).toEqual(["v38", 2026, 134, 130]);
});

it(`T08 card parity runs against a paired release${pairing.skip ? ` — SKIPPED: ${"reason" in pairing ? pairing.reason : pairing.fail}` : ""}`, () => {
  if (pairing.fail) expect.fail(pairing.fail);
  if ("reason" in pairing) console.warn(`T08 card parity skipped: ${pairing.reason}`);
});

describe.skipIf(pairing.skip)(`T08 card model on the live release (${at.path ?? at.reason})`, () => {
  const model = async (id: string) => {
    const inp = (await inputsOf(id))!, place = makePlace(inp);
    return { inp, place, m: buildModel(inp, place, settle(inp, place)) };
  };

  it.each(ids)("4. %s: buildModel() projected equals page v36's", async (id) => {
    const { m } = await model(id);
    expect(m.ctx.s).toBe(H.states[id]!.s);
    expect(project(m)).toEqual(mapModel(H.states[id]!.model));
  });

  it.each(ids)("6. %s: every row's STORED year strip (rows[].year) equals page v36's dayStatus runs (365 book days)", async (id) => {
    const { m } = await model(id);
    expect(m.rows.map((r) => r.year)).toEqual(rulingApplied(id, H.states[id]!.dayStatus));
    for (const r of m.rows) expect(r.year.reduce((a, [, n]) => a + n, 0)).toBe(365);
  });

  it("5. the ticket's literals (page v36's own output)", async () => {
    const v515 = (await model("vedder_515")).m;
    expect([v515.ctx.s, v515.line, v515.spp, v515.broad]).toEqual([1, "known_with_rules",
      ["RB", "CT", "LMB", "SMB", "BCB", "CRA", "KO", "LW", "MW", "WSG", "ST", "GB", "DV", "LT", "EB"], []]);
    expect(rowLine(v515)).toEqual([["keep", R(3091), 2, ["EB", "GB", "DV", "LT", "ST"]], ["release", null, null, ["KO"]],
      ["keep", R(3088), 15, ["LW", "MW"]], ["keep", R(3083), 20, ["LMB", "SMB"]], ["keep", R(3084), 20, ["BCB"]], ["release", null, null, ["WSG"]],
      ["keep", R(3085), 25, ["CRA"]], ["closed", null, null, []]]);
    expect([v515.R.RB!.H!.status, v515.R.RB!.H!.win.rix, v515.R.RB!.W!.status, v515.R.RB!.W!.win.rix]).toEqual(["release", R(527), "release", R(3098)]);
    expect([v515.R.LMB!.H!.status, v515.R.LMB!.H!.daily, v515.R.LMB!.H!.win.rix, Object.keys(v515.R).length]).toEqual(["keep", 20, R(3083), 15]);

    const v615 = (await model("vedder_615")).m;
    expect([v615.ctx.s, v615.status, v615.broad.map((r) => r.rix), v615.rows.map((r) => [r.kind, r.members])]).toEqual([2, "closed", [R(529)],
      [["closed", ["BCB", "EB", "GB", "CRA", "CT", "DV", "KO", "LT", "LW", "LMB", "MW", "RB", "SMB", "ST", "WSG"]], ["closed", []]]]);
    expect(Object.values(v615.R).every((x) => x.H!.win.rix === R(529))).toBe(true);

    const v1010 = (await model("vedder_1010")).m;
    expect([v1010.ctx.s, v1010.rows.length, rowLine(v1010)[1]]).toEqual([3, 9, ["keep", R(530), 4, ["RB"]]]);   // "row 1": the second row
    expect([v1010.R.RB!.H!.status, v1010.R.RB!.H!.daily, v1010.R.RB!.H!.win.rix, v1010.R.RB!.W!.status, v1010.R.RB!.W!.win.rix,
      v1010.R.CT!.H!.status, v1010.R.CT!.H!.daily, v1010.R.CT!.H!.win.rix]).toEqual(["keep", 4, R(530), "release", R(3098), "keep", 2, R(3091)]);

    const sat = (await model("klwa_1010")).m, mon = (await model("klwa_1012")).m;
    expect([sat.ctx.s, mon.ctx.s]).toEqual([1, 0]);
    expect(rowLine(sat)).toContainEqual(["keep", R(1366), 5, ["KO"]]);
    expect(mon.rows.filter((r) => r.members.includes("KO")).map((r) => r.kind)).toEqual(["release"]);
    for (const m of [sat, mon]) {
      expect(rowLine(m)).toContainEqual(["nolimit", R(1362), Infinity, ["LMB", "SMB"]]);
      expect(m.rows.filter((r) => r.kind === "nolimit" && r.pool?.rix === R(1363)).map((r) => r.members)).toEqual([["YP"]]);
      expect([m.line, m.status, m.broad, m.ctx.broad.map((r) => r.rix)]).toEqual([null, "own", [], [3127, 3143, 3144, 3145, 3403].map(R)]);
    }

    const x15 = (await model("x15p7_1010")).m;
    expect([x15.ctx.s, x15.rows.map((r) => r.kind), x15.R.ST!.H!.win.rix, x15.broad.map((r) => r.rix)])
      .toEqual([4, ["closed", "closed"], R(575), [R(575), R(578)]]);
  });

  it("5. census: nolimit rows in 24 states; missingRows false in all 134", async () => {
    const withNolimit: string[] = [];
    let missing = 0;
    for (const id of ids) {
      const { m } = await model(id);
      if (m.rows.some((r) => r.kind === "nolimit")) withNolimit.push(id);
      if (m.missingRows) missing++;
    }
    expect([withNolimit.length, missing]).toEqual([24, 0]);
    expect(withNolimit.slice(0, 6)).toEqual(["klwa_1010", "klwa_1012", "cow1_1010", "cow2_1010", "cow3_1010", "klmb_1010"]);
  });
});

/**
 * OWNER RULING 2026-10-21 (a) — pipeline/docs/GOTCHAS.md: on the Kitimat, r5's hatchery-steelhead quota does NOT lift
 * Region 6's Nov 1-June 30 stream release, where page v36 (the oracle) lifted it. The one row per state that the ruling
 * changes (by index) gets the page's runs with every Nov 1-June 30 'keep' day made 'release'; nothing else moves, and
 * every other row and state must still equal the page. Mirrors pipeline/tests/test_answers_year_strip_live.py.
 */
const KITIMAT_RULED: Readonly<Record<string, number>> = { x14p0_1010: 2, x14p1_1010: 2, x14p2_1010: 2 };
const NOV1 = 305, JUN30 = 181;   // book day-of-year (365-day year)

export function novJunReleased(runs: readonly [string, number][]): [string, number][] {
  const days = runs.flatMap(([st, n]) => Array.from({ length: n }, () => st));
  const out: [string, number][] = [];
  days.forEach((st, i) => {
    const d = i + 1, s = st === "keep" && (d >= NOV1 || d <= JUN30) ? "release" : st;
    const last = out[out.length - 1];
    if (last && last[0] === s) last[1]++; else out.push([s, 1]);
  });
  return out;
}

function rulingApplied(id: string, page: [string, number][][]): [string, number][][] {
  const row = KITIMAT_RULED[id];
  return row === undefined ? page : page.map((runs, i) => (i === row ? novJunReleased(runs) : runs));
}

describe("the Kitimat ruling's oracle adjustment", () => {
  it("moves only the Nov 1-June 30 keep days", () => {
    expect(novJunReleased([["keep", 74], ["closed", 92], ["keep", 199]]))
      .toEqual([["release", 74], ["closed", 92], ["release", 15], ["keep", 123], ["release", 61]]);
  });
});
