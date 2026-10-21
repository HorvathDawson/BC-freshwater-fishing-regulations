/**
 * T12: the first parity test — the harness itself. Every golden state's inputs (T09 over `parityStore()`: the province
 * release, else the committed fixture) tap to taps.json's key, segment, cell and per-day display frames (all store
 * answers; nothing here is computed in TS), the request mapping is the oracle's, the two oracles are from one sync, the
 * inputs cache memoises, and the frozen VM fixtures are fresh (from T21). REGS_PARITY=fixture is the CI path.
 */
import { afterAll, describe, expect, it } from "vitest";
import { buildRegsPage, type DisplayFrame, type RegsInputs } from "@app/core";
import { loadRegsInputs } from "@app/data/regs";
import { syncFixtures } from "./fixtures";
import {
  caseIds, cases, golden, hasProvinceRegs, inputsOf, parityGate, parityMode, parityStore, requestOf, storeLabel, taps,
} from "./golden";

const T = taps();
const gate = await parityGate();
const label = await storeLabel();
const ids = caseIds("keep");

/** buildRegsPage is declared and throws until T21 builds it. */
const T21 = ((): boolean => {
  try { buildRegsPage({} as never, { today: { month: 1, day: 1 }, who: { age: "adult", residency: "resident" } }); return true; }
  catch (e) { return !/buildRegsPage: T21/.test(String(e)); }
})();

it(`the harness runs against a paired regs store${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ` (${label})`}`, () => {
  if (gate.fail) expect.fail(gate.fail);
  if ("reason" in gate) console.warn(`harness parity skipped: ${gate.reason}`);
});

it("7. golden.json and taps.json are from one sync", () => {
  expect(golden().meta.sha, "golden.json and taps.json differ — re-run app/design/regs-v11/port/sync.mjs").toBe(T.meta.golden);
  expect([T.meta.golden, golden().meta.version, ids.length, Object.keys(golden().states).length]).toEqual(["bdda3586", "v38", 134, 134]);
});

it("6. requestOf: the tap's item, part, date and weekday as taps.json gives them; today is Oct 10", () => {
  expect(requestOf("vedder_515")).toEqual({ target: { kind: "part", item: "gnis:3062", partIx: 0 }, day: { month: 5, day: 15 },
                                            weekday: "Friday", today: { month: 10, day: 10 } });
  expect(() => requestOf("nope_101")).toThrow("taps.json has no state nope_101");
});

describe.skipIf(gate.skip)(`T12 harness parity (store: ${label})`, () => {
  let tapEqual = 0;
  afterAll(() => console.info(`harness parity: ${tapEqual}/${ids.length} states tap-equal (store: ${label})`));

  /** a display frame → its id, through the year's per-segment cells (the frames are the store's, by identity) */
  const displayIds = (inp: RegsInputs): Map<DisplayFrame, number> =>
    new Map(inp.year!.displayBySeg.map((f, i) => [f!, inp.year!.cells[i]!.display]));

  it.each(ids)("5. %s: inputs tap to taps.json's akey, segment, cell and every day's display frame", async (id) => {
    const t = T.states[id]!, inp = await inputsOf(id);
    expect(inp).not.toBeNull();
    expect([inp!.key?.akey ?? null, inp!.s, inp!.cell]).toEqual([t.akey, t.s, t.cell]);
    const ix = displayIds(inp!);
    expect(inp!.year!.displayByDay.map((f) => ix.get(f!))).toEqual(t.displayByDay);
    tapEqual++;
  });

  it("5. named states, literally", async () => {
    const pick = async (id: string) => { const i = (await inputsOf(id))!; return [id, i.key!.akey, i.s, i.cell]; };
    expect(await Promise.all(["vedder_515", "klwa_1010", "klwa_1012", "x15p7_1010"].map(pick))).toEqual([
      ["vedder_515", 886, 1, { ladder: 1057, rows: 1723, gear: 320, licence: 1, display: 64 }],
      ["klwa_1010", 1229, 1, T.states.klwa_1010!.cell],
      ["klwa_1012", 1229, 0, T.states.klwa_1012!.cell],
      ["x15p7_1010", 1039, 4, { ...T.states.x15p7_1010!.cell, display: 704 }],
    ]);
  });

  it("cases('keep') yields every state with its golden state, tap and inputs", async () => {
    const seen: string[] = [];
    for await (const c of cases("keep")) {
      seen.push(c.id);
      if (c.id === "vedder_515") {
        expect([c.g.id, c.g.water, c.tap.akey, (await c.inputs()).key!.akey]).toEqual(["vedder_515", "Vedder River", 886, 886]);
        await expect(c.page()).rejects.toThrow(T21 ? /^$/ : "buildRegsPage: T21");
      }
    }
    expect(seen).toEqual(Object.keys(T.states));
  });

  it("8. memo: the same promise per id; all 134 states, warm, re-tapped in ≤ 3 s", async () => {
    expect(inputsOf("vedder_515")).toBe(inputsOf("vedder_515"));
    await Promise.all(ids.map((id) => inputsOf(id)));
    const st = (await parityStore())!;
    const t0 = performance.now();
    for (const id of ids) { const r = requestOf(id); await loadRegsInputs(st, r.target, r.day, r.weekday, T.meta.year); }
    expect(performance.now() - t0).toBeLessThan(3000);
  });

  it("9. storeLabel: REGS_PARITY names it; auto is the province store when it is on this machine, else the fixture", () => {
    const mode = parityMode();
    expect(label).toBe(mode !== "auto" ? mode : hasProvinceRegs() ? "province" : "fixture");
  });

  it.skipIf(!T21)("10. the frozen VM fixtures are fresh (buildRegsPage lands in T21)", async () => {
    await syncFixtures();
  });

  it.todo("keep.strip — the year strip is read from the STORE's per-row runs: PENDING the pipeline (owner rule 2026-10-21), never derived in TS");
});
