/**
 * T07 parity: core's `settle(inp, makePlace(inp))` — the stored ladder verdicts and display frames decoded, selected and
 * grouped — against page v36's OWN settle() run over THE SAME RELEASE (app/fixtures/golden/page-oracle.json,
 * regs-recut/page-oracle.mjs: page_v36.js up to `segBar` over page blocks the Python reference decoder cut from the
 * release), on every tapped golden state, with inputs from T09 `inputsOf` over the release paired with the oracles
 * (skipped with the reason otherwise; a failure under REGS_STORE_REQUIRED=1).
 *
 * Two paths, neither the code under test: the page's code over the Python's read of the release, against core's code
 * over the app's read of it. Rules are compared by durable key ("entry::rule"). (T667 slice 5b) Until 2026-11-13 the
 * oracle was page v36's FROZEN data (hydrate.json) and each approved ruling since it was a hand amendment here
 * (v36-rulings.json: renames, retired rules, "on parts", a drawn part, a predated binding, the Peace kokanee); the
 * release carries every ruling, so there is nothing to amend. What an approved ruling changes on a golden state is a
 * page-oracle.json diff the release stops on (pipeline/release_follows.py `page-oracle`).
 */
import { describe as group, expect, it } from "vitest";
import { isClosedOn, makePlace, settle, displayStatusOn, type RuleView, type Settled } from "@app/core";
import { inputsOf, parityStore, paritySkip, releasePairing, taps } from "./golden";
import { pageOracle, type PageOracleState } from "./oracles";

const O = pageOracle();
const T = taps();
const ids = Object.keys(T.states);
const at = await releasePairing();
const pairing = paritySkip(at);
/** the paired store's rix → durable key */
const KEY = new Map<number, string>();
for (const [rix, r] of (await (await parityStore())?.rules()) ?? []) KEY.set(rix, `${r.export.entry_id}::${r.export.rule_id}`);
const key = (rix: number): string => { const k = KEY.get(rix); if (k === undefined) throw new Error(`no rule ${rix} in the paired store`); return k; };

const keys = (l: readonly RuleView[]) => l.map((r) => r.key);
const project = (c: Settled): PageOracleState["settle"] => ({
  status: c.status?.status ?? null, broad: keys(c.broad), speaks: keys(c.speaks), beside: keys(c.beside), shown: keys(c.shown),
  inpart: c.inpart.map((x) => [x.rule.key, x.now] as const), lifted: [...c.lifted].map(([a, b]) => [key(a), b == null ? null : key(b)] as const),
  timed: keys(c.timed), side: keys(c.side), standing: keys(c.standing), anglerclosed: keys(c.anglerclosed),
});

it("the oracle: page v38's code over the paired release, every tapped golden state, YEAR 2026", () => {
  expect([O.meta.generator, O.meta.page, O.meta.version, O.meta.year]).toEqual(["app/tools/regs-recut/page-oracle.mjs", "page_v36.js", "v38", 2026]);
  expect(O.meta.golden, "page-oracle.json and taps.json are from one sync (regs-recut/sync.mjs)").toBe(T.meta.golden);
  expect(Object.keys(O.states)).toEqual(ids);
});

it(`T07 settle parity runs against a paired release${pairing.skip ? ` — SKIPPED: ${"reason" in pairing ? pairing.reason : pairing.fail}` : ""}`, () => {
  if (pairing.fail) expect.fail(pairing.fail);
  if ("reason" in pairing) console.warn(`T07 settle parity skipped: ${pairing.reason}`);
});

group.skipIf(pairing.skip)(`T07 settle on the live release (${at.path ?? at.reason})`, () => {
  const settled = async (id: string) => { const inp = (await inputsOf(id))!; return { inp, c: settle(inp, makePlace(inp)) }; };

  it.each(ids)("4. %s: settle() projected to rule keys equals page v36's over the same release", async (id) => {
    const { c } = await settled(id);
    expect(c.s).toBe(O.states[id]!.s);
    expect(project(c)).toEqual(O.states[id]!.settle);
  });

  it.each(ids)("%s: makePlace — every candidate's via and rank is page v36's", async (id) => {
    const { inp } = await settled(id);
    const p = makePlace(inp), o = O.states[id]!;
    expect(Object.fromEntries(p.cands.filter((r) => r.via === "trib").map((r) => [r.key, r.via]))).toEqual(o.via);
    expect(Object.fromEntries(p.cands.filter((r) => r.rank !== r.baseRank).map((r) => [r.key, r.rank]))).toEqual(o.ranks);
    expect([p.key, p.starts, p.moments, p.tidal]).toEqual([inp.key, inp.year!.segStarts, inp.year!.moments, inp.key!.tidal]);
  });

  it("6. displayStatusOn / isClosedOn on the Vedder: Jun 1–30 (leap days 153–182) closed, the days either side not", async () => {
    const inp = (await inputsOf("vedder_515"))!;
    const days = Array.from({ length: 366 }, (_, i) => i + 1);
    expect(days.filter((d) => isClosedOn(inp, d))).toEqual(days.filter((d) => d >= 153 && d <= 182));
    expect([displayStatusOn(inp, 152), displayStatusOn(inp, 153), displayStatusOn(inp, 182), displayStatusOn(inp, 183), isClosedOn(inp, 167)])
      .toEqual(["own", "closed", "closed", "own", true]);
    expect(inp.year!.cells.map((c) => c.display)[2]).toBe(T.states.vedder_615!.cell.display);   // taps.json: frame ids move per build
  });
});
