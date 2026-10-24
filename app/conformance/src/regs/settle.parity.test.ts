/**
 * T07 parity: core's `settle(inp, makePlace(inp))` — the stored ladder verdicts and display frames decoded, selected and
 * grouped — against page v36's OWN settle() (app/fixtures/golden/hydrate.json, port/hydrate.mjs running
 * page_v36.js lines 1–246 in node, YEAR 2026) on all 134 golden states, with inputs from T09 `inputsOf` over the live
 * release paired with the oracles (skipped with the reason otherwise; a failure under REGS_STORE_REQUIRED=1). Every
 * expected value is read from the oracle except the ticket's literal table (page v36's output, quoted by the ticket).
 */
import { readFileSync } from "node:fs";
import { describe as group, expect, it } from "vitest";
import { describe, isClosedOn, makePlace, settle, displayStatusOn, type RuleView, type Settled } from "@app/core";
import { inputsOf, oracleRix, parityStore, paritySkip, releasePairing, taps } from "./golden";
import { HYDRATE } from "./oracles";

interface OracleSettle {
  status: string | null; broad: number[]; speaks: number[]; beside: number[]; shown: number[]; inpart: [number, boolean][];
  lifted: [number, number][]; timed: number[]; side: number[]; standing: number[]; anglerclosed: number[];
}
const H = JSON.parse(readFileSync(HYDRATE, "utf8")) as {
  meta: { version: string; year: number };
  states: Record<string, { s: number; via: Record<string, "trib">; ranks: Record<string, number>; settle: OracleSettle }>;
};
const T = taps();
/** (T115) every hydrated state but the 4 whose part the release merged away (taps.json `gone`) */
const ids = Object.keys(H.states).filter((id) => !(id in T.gone));
const at = await releasePairing();
const pairing = paritySkip(at);
/** page v36's rule numbers (the oracle's, and the ticket's literals quoted from it) → the paired store's, by durable key
 *  (golden.ts `oracleRix`): a rule added or dropped ahead of a rix renumbers it without changing a word. */
const R = (await oracleRix()) ?? ((r: number) => r);
/**
 * CONTENT the oracle predates (expected changes, each with its commit; the oracle is page v36 over the answers its blocks
 * were cut from, 2026-10-20). Each entry ADDS a rule, by durable key, to the oracle's list for that state, and asserts the
 * oracle lacks it — so the entry fails, and must go, once page_v36.html's blocks and hydrate.json are re-cut
 * (`python -m pipeline.deliver.answers.reference.page_data --html …` then port/hydrate.mjs).
 * - T89 (010b5c28, review 894854c0; batch 3 "Nilkitkwa r4 +3"): nilkitkwa_lake.r4 — Rainbow Alley open all year — binds the
 *   Babine River between Babine and Nilkitkwa Lakes, Babine parts 1 (Nilkitkwa Lake → Fort Babine Bridge, 2 sections) and
 *   2 (Fort Babine Bridge → Babine Lake outlet, 1): an advisory the day shows.
 */
const ORACLE_PREDATES: Readonly<Record<string, { readonly list: "shown"; readonly key: string }>> = {
  x17p1_1010: { list: "shown", key: "r6:nilkitkwa_lake@6-8::nilkitkwa_lake.r4" },
  x17p2_1010: { list: "shown", key: "r6:nilkitkwa_lake@6-8::nilkitkwa_lake.r4" },
};
const NOW = new Map<string, number>();
for (const [rix, r] of (await (await parityStore())?.rules()) ?? []) NOW.set(`${r.export.entry_id}::${r.export.rule_id}`, rix);
function amended(id: string, o: OracleSettle): OracleSettle {
  const a = ORACLE_PREDATES[id];
  if (!a) return o;
  const rix = NOW.get(a.key);
  if (rix === undefined) throw new Error(`ORACLE_PREDATES ${id}: no rule ${a.key} in the store`);
  if (o[a.list].includes(rix)) throw new Error(`ORACLE_PREDATES ${id}: the oracle already has ${a.key} — it was re-cut; drop the entry`);
  // before the first rule numbered after it (the list is not wholly sorted: its standing rules come last)
  const l = o[a.list], at = l.findIndex((x) => x > rix);
  return { ...o, [a.list]: at < 0 ? [...l, rix] : [...l.slice(0, at), rix, ...l.slice(at)] };
}
const mapSettle = (o: OracleSettle): OracleSettle => ({
  status: o.status, broad: o.broad.map(R), speaks: o.speaks.map(R), beside: o.beside.map(R), shown: o.shown.map(R),
  inpart: o.inpart.map(([r, now]) => [R(r), now]), lifted: o.lifted.map(([a, b]) => [R(a), b == null ? b : R(b)]),
  timed: o.timed.map(R), side: o.side.map(R), standing: o.standing.map(R), anglerclosed: o.anglerclosed.map(R),
});
const mapKeys = <V>(o: Readonly<Record<string, V>>): Record<string, V> => Object.fromEntries(Object.entries(o).map(([k, v]) => [String(R(+k)), v]));

const ix = (l: readonly RuleView[]) => l.map((r) => r.rix);
const project = (c: Settled): OracleSettle => ({
  status: c.status?.status ?? null, broad: ix(c.broad), speaks: ix(c.speaks), beside: ix(c.beside), shown: ix(c.shown),
  inpart: c.inpart.map((x) => [x.rule.rix, x.now]), lifted: [...c.lifted], timed: ix(c.timed), side: ix(c.side),
  standing: ix(c.standing), anglerclosed: ix(c.anglerclosed),
});

it("the oracle: page v38's settle over the 134 golden states, YEAR 2026", () => {
  expect([H.meta.version, H.meta.year, Object.keys(H.states).length, ids.length]).toEqual(["v38", 2026, 134, 130]);
  expect(ids).toEqual(Object.keys(T.states));
  expect(Object.keys(H.states).filter((id) => id in T.gone)).toEqual(Object.keys(T.gone));
});

it(`T07 settle parity runs against a paired release${pairing.skip ? ` — SKIPPED: ${"reason" in pairing ? pairing.reason : pairing.fail}` : ""}`, () => {
  if (pairing.fail) expect.fail(pairing.fail);
  if ("reason" in pairing) console.warn(`T07 settle parity skipped: ${pairing.reason}`);
});

group.skipIf(pairing.skip)(`T07 settle on the live release (${at.path ?? at.reason})`, () => {
  const settled = async (id: string) => { const inp = (await inputsOf(id))!; return { inp, c: settle(inp, makePlace(inp)) }; };

  it.each(ids)("4. %s: settle() projected to rix lists equals page v36's", async (id) => {
    const { c } = await settled(id);
    expect(c.s).toBe(H.states[id]!.s);
    expect(project(c)).toEqual(amended(id, mapSettle(H.states[id]!.settle)));
  });

  it.each(ids)("%s: makePlace — every candidate's via and rank is page v36's", async (id) => {
    const { inp } = await settled(id);
    const p = makePlace(inp), o = H.states[id]!;
    expect(Object.fromEntries(p.cands.filter((r) => r.via === "trib").map((r) => [r.rix, r.via]))).toEqual(mapKeys(o.via));
    expect(Object.fromEntries(p.cands.filter((r) => r.rank !== r.baseRank).map((r) => [r.rix, r.rank]))).toEqual(mapKeys(o.ranks));
    expect([p.key, p.starts, p.moments, p.tidal]).toEqual([inp.key, inp.year!.segStarts, inp.year!.moments, inp.key!.tidal]);
  });

  it("5. the ticket's literals (page v36's own output)", async () => {
    const count = (c: Settled) => [c.s, c.status?.status ?? null, c.speaks.length, c.beside.length, c.shown.length];
    const v515 = (await settled("vedder_515")).c;
    expect(count(v515)).toEqual([1, "own", 74, 0, 17]);
    expect([[...v515.lifted], ix(v515.standing), v515.inpart]).toEqual([[[R(3316), R(3378)], [R(3403), R(3404)]], [R(3351), R(3352)], []]);
    const v615 = await settled("vedder_615");
    expect([...count(v615.c), ix(v615.c.broad)]).toEqual([2, "closed", 59, 0, 17, [R(529)]]);
    const b = v615.c.broad[0]!;
    expect([b.key, b.rank, b.via]).toEqual(["r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4::chilliwack_vedder_rivers.r8", 0, "reach"]);
    expect(describe(b, v615.inp.statics.exportSpecies)).toBe("No fishing for any game fish, Jun 1–30");
    const v1010 = (await settled("vedder_1010")).c;
    expect([...count(v1010), [...v1010.lifted]]).toEqual([3, "own", 72, 0, 17, [[R(3316), R(3378)], [R(3095), R(530)], [R(3101), R(530)], [R(3403), R(3404)]]]);
    const sat = (await settled("klwa_1010")).c, mon = (await settled("klwa_1012")).c;
    expect([...count(sat), ix(sat.broad), [...sat.lifted]]).toEqual([1, "own", 64, 0, 22, [3127, 3143, 3144, 3145, 3403].map(R),
      [[R(3316), R(3378)], [R(3127), R(1363)], [R(3135), R(1362)], [R(3137), R(1363)]]]);
    expect([...count(mon), ix(mon.broad)]).toEqual([0, "own", 62, 0, 22, [3127, 3143, 3144, 3145, 3403].map(R)]);
    const cow = (await settled("cow0_1010")).c;
    expect([...count(cow), cow.broad.map((r) => r.key), cow.inpart.map((x) => [x.rule.rix, x.now])]).toEqual([3, "closed", 57, 0, 18,
      ["r1:cowichan_river_see_map_below@1-4::cowichan_river.r6", "zp:white_sturgeon_licence::white_sturgeon_licence.r4"], [[R(78), true], [R(79), true]]]);
    const x14 = (await settled("x14p2_1010")).c;
    expect([...count(x14), x14.broad.length, ix(x14.side)]).toEqual([3, "own", 73, 1, 25, 2, [R(2216)]]);
    const x17 = (await settled("x17p3_1010")).c;
    expect([...count(x17), x17.broad.length, ix(x17.anglerclosed)]).toEqual([7, "own", 72, 0, 25, 2, [R(2105)]]);
    const x15 = (await settled("x15p7_1010")).c;
    expect([...count(x15), ix(x15.broad)]).toEqual([4, "closed", 63, 0, 17, [R(575), R(578)]]);
    const x31 = await settled("x31p_1010");
    expect([x31.c.s, x31.c.status?.status, displayStatusOn(x31.inp, 284)]).toEqual([0, "tidal", "tidal"]);
  });

  // (T115) 130: the 4 states whose part the release merged away (x15p14/15/17/18, taps.json `gone`; all 4 "own") are not tapped
  it("5. census over the 130: own 97, closed 26, base 6, tidal 1; timed is empty on every golden state", async () => {
    const tally: Record<string, number> = {};
    let timed = 0;
    for (const id of ids) {
      const { c } = await settled(id);
      const k = String(c.status?.status ?? null);
      tally[k] = (tally[k] ?? 0) + 1;
      timed += c.timed.length;
    }
    expect([tally, timed]).toEqual([{ own: 97, closed: 26, base: 6, tidal: 1 }, 0]);
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
