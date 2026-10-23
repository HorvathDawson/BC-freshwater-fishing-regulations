/**
 * T12: the first parity test — the harness itself. Every golden state's inputs (T09 over `parityStore()`: the province
 * release, else the committed fixture) tap to taps.json's key, segment, cell and per-day display frames (all store
 * answers; nothing here is computed in TS), the request mapping is the oracle's, the two oracles are from one sync, the
 * inputs cache memoises, a case builds its page (T21), and the frozen VM fixtures are fresh. REGS_PARITY=fixture is the CI path.
 */
import { afterAll, describe, expect, it } from "vitest";
import type { DisplayFrame, RegsInputs } from "@app/core";
import { loadRegsInputs } from "@app/data/regs";
import { syncFixtures } from "./fixtures";
import {
  caseIds, cases, golden, inputsOf, parityGate, parityMode, parityStore, releasePairing, requestOf, storeLabel, taps,
} from "./golden";

const T = taps();
const gate = await parityGate();
const label = await storeLabel();
const ids = caseIds("keep");


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
      // keys and frame ids come from taps.json (the Python reference decoder), never literals: a rebuild that adds an
      // answer key renumbers them (T56, batch 3), and the test is about the two decoders agreeing, not about a vintage
      ["vedder_515", T.states.vedder_515!.akey, 1, { ...T.states.vedder_515!.cell, display: 64 }],
      ["klwa_1010", T.states.klwa_1010!.akey, 1, T.states.klwa_1010!.cell],
      ["klwa_1012", T.states.klwa_1012!.akey, 0, T.states.klwa_1012!.cell],
      ["x15p7_1010", T.states.x15p7_1010!.akey, 4, T.states.x15p7_1010!.cell],
    ]);
  });

  it("cases('keep') yields every state with its golden state, tap and inputs", async () => {
    const seen: string[] = [];
    for await (const c of cases("keep")) {
      seen.push(c.id);
      if (c.id === "vedder_515") {
        expect([c.g.id, c.g.water, c.tap.akey, (await c.inputs()).key!.akey]).toEqual(["vedder_515", "Vedder River", T.states.vedder_515!.akey, T.states.vedder_515!.akey]);
        const p = await c.page();
        expect([p.version, p.target, p.day, p.keep.order[0]]).toEqual(["regs-v38", c.tap && requestOf(c.id).target, { month: 5, day: 15 }, "strip"]);
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

  it("9. storeLabel: REGS_PARITY names it; auto is the live release when it is paired, else the fixture", async () => {
    const mode = parityMode();
    expect(label).toBe(mode !== "auto" ? mode : (await releasePairing()).path !== null ? "province" : "fixture");
  });

  it("10. the frozen VM fixtures are fresh (buildRegsPage, T21)", async () => {
    await syncFixtures();
  });

});
