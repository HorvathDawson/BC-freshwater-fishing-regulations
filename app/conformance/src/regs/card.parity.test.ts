/**
 * T08 parity: core's card model — `buildModel(inp, place, settle(…))` — against page v36's OWN buildModel() run over THE
 * SAME RELEASE (app/fixtures/golden/page-oracle.json, regs-recut/page-oracle.mjs: page_v36.js over page blocks the Python
 * reference decoder cut from the release), on every tapped golden state, inputs from T09 `inputsOf` over the release
 * paired with the oracles (skipped with the reason otherwise; a failure under REGS_STORE_REQUIRED=1). Rules by durable
 * key; Infinity ↔ the oracle's "Infinity".
 *
 * Item 6 (the year strip vs the page's `dayStatus` runs) reads the STORE's strip (rows 30 `rows[].year`, decided by the
 * pipeline with page v36's rule — `pipeline/deliver/answers/year.py`); TS derives nothing: each drawn row's
 * `HydRow.year` is the stored runs, and they must equal the page's dayStatus over the same release's rows frames.
 *
 * (T667 slice 5b) Until 2026-11-13 the oracle was page v36's FROZEN data (hydrate.json) and each approved ruling since was
 * a hand amendment here (v36-rulings.json: Lois Lake's folded row, the Peace kokanee, the Kitimat's Nov 1-June 30
 * release, the rename's order flips). The release carries every ruling: there is nothing to amend.
 */
import { describe, expect, it } from "vitest";
import { buildModel, makePlace, settle, type CardModel, type HydRes } from "@app/core";
import { inputsOf, releasePairing, paritySkip, taps } from "./golden";
import { pageOracle, type PageOracleState } from "./oracles";

const O = pageOracle();
const ids = Object.keys(taps().states);
const at = await releasePairing();
const pairing = paritySkip(at);

const inf = (x: number | null) => (x === Infinity ? "Infinity" : x);
const res = (h: HydRes | null) => h && { status: h.status, daily: inf(h.daily), win: h.win.key, narrow: h.narrow?.key ?? null };
const project = (m: CardModel): PageOracleState["model"] => ({
  spp: [...m.spp], line: m.line, missingRows: m.missingRows, status: m.status, broad: m.broad.map((r) => r.key),
  rows: m.rows.map((r) => ({ kind: r.kind, pool: r.pool?.key ?? null, win: r.win?.key ?? null, daily: inf(r.daily),
    narrow: r.narrow?.key ?? null, members: [...r.members] })),
  R: Object.fromEntries(Object.entries(m.R).map(([k, v]) => [k, { H: res(v.H), W: res(v.W) }])),
});

it("the oracle: page v38's code over the paired release, every tapped golden state, YEAR 2026", () => {
  expect([O.meta.version, O.meta.year]).toEqual(["v38", 2026]);
  expect(Object.keys(O.states)).toEqual(ids);
});

it(`T08 card parity runs against a paired release${pairing.skip ? ` — SKIPPED: ${"reason" in pairing ? pairing.reason : pairing.fail}` : ""}`, () => {
  if (pairing.fail) expect.fail(pairing.fail);
  if ("reason" in pairing) console.warn(`T08 card parity skipped: ${pairing.reason}`);
});

describe.skipIf(pairing.skip)(`T08 card model on the live release (${at.path ?? at.reason})`, () => {
  const model = async (id: string) => {
    const inp = (await inputsOf(id))!, place = makePlace(inp);
    return buildModel(inp, place, settle(inp, place));
  };

  it.each(ids)("4. %s: buildModel() projected to rule keys equals page v36's over the same release", async (id) => {
    const m = await model(id);
    expect(m.ctx.s).toBe(O.states[id]!.s);
    expect(project(m)).toEqual(O.states[id]!.model);
  });

  it.each(ids)("6. %s: every row's STORED year strip (rows[].year) equals page v36's dayStatus runs (365 book days)", async (id) => {
    const m = await model(id);
    expect(m.rows.map((r) => r.year)).toEqual(O.states[id]!.dayStatus);
    for (const r of m.rows) expect(r.year.reduce((a, [, n]) => a + n, 0)).toBe(365);
  });
});
