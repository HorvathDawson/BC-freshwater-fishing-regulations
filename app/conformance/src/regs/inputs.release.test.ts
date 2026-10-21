/**
 * T09 on the LIVE release, paired with the oracles (golden.ts `releasePairing`; skipped with the reason otherwise, a
 * failure under REGS_STORE_REQUIRED=1): `loadRegsInputs` gives the ticket's values (Vedder, Kalamalka, the Similkameen
 * hours window, tidal water, the Chilliwack's closed stretches, an unnamed stream), and on every one of the 134 golden
 * taps the key, segment, cell and the display frame of each of the 366 days equal taps.json — the Python reference
 * decoder's (`encode.segment_index` over the store the goldens were cut from). The store-free cases are
 * packages/data/src/regs/inputs.test.ts.
 */
import { beforeAll, describe, expect, it } from "vitest";
import type { DisplayFrame, RegsInputs } from "@app/core";
import { loadRegsInputs, RegsInputsError, targetFor, type RegsStore } from "@app/data/regs";
import { golden, inputsOf, paritySkip, regsStore, releasePairing, requestOf, taps } from "./golden";

const T = taps();
const ids = Object.keys(T.states);
const at = await releasePairing();
const pairing = paritySkip(at);

it(`T09 inputs run against a paired release${pairing.skip ? ` — SKIPPED: ${"reason" in pairing ? pairing.reason : pairing.fail}` : ""}`, () => {
  if (pairing.fail) expect.fail(pairing.fail);
  if ("reason" in pairing) console.warn(`T09 inputs skipped: ${pairing.reason}`);
});

const load = (st: RegsStore, item: string, partIx: number, month: number, day: number, wd: RegsInputs["weekday"]) =>
  loadRegsInputs(st, { kind: "part", item, partIx }, { month, day }, wd, 2026);
/** The frame id each display frame object was read as (the year's cells say which). */
const displayIds = (inp: RegsInputs): Map<DisplayFrame, number> =>
  new Map(inp.year!.displayBySeg.map((f, i) => [f!, inp.year!.cells[i]!.display]));

describe.skipIf(pairing.skip)(`T09 loadRegsInputs on the live release (${at.path ?? at.reason})`, () => {
  let st: RegsStore;
  beforeAll(async () => { st = (await regsStore())!; });

  it("1. vedder_515: the item, key, segment, cell, the year and the rules through the part's via", async () => {
    const inp = await load(st, "gnis:3062", 0, 5, 15, "Friday");
    const it_ = inp.item!;
    expect([it_.ord, it_.name, it_.kind, it_.sections, it_.id]).toEqual([7670, "Vedder River", "stream", 5, "gnis:3062"]);
    expect([inp.key!.akey, inp.s, inp.cell]).toEqual([886, 1, { ladder: 1057, rows: 1723, gear: 320, licence: 1, display: 64 }]);
    expect([inp.year!.cells.length, inp.year!.segStarts]).toEqual([4, [1, 122, 153, 183]]);
    expect(inp.frames!.display).toEqual({ status: "own", closing: [], strip: 20 });
    expect(inp.year!.displayByDay[166]!.status).toBe("closed");              // day 167, Jun 15
    const r = inp.rules.get(529);
    expect([r.rank, r.via]).toEqual([0, "reach"]);
    expect([inp.mergedParts, inp.place]).toEqual([null, null]);
  });

  it("2. vedder_615 and vedder_1010: the segment and cell of the day", async () => {
    const jun = await load(st, "gnis:3062", 0, 6, 15, "Monday"), oct = await load(st, "gnis:3062", 0, 10, 10, "Saturday");
    expect([jun.s, jun.cell]).toEqual([2, { ladder: 2457, rows: 1724, gear: 146, licence: 1, display: 305 }]);
    expect([oct.s, oct.cell]).toEqual([3, { ladder: 1056, rows: 1722, gear: 146, licence: 1, display: 1 }]);
  });

  it("3. klwa_1010 (Sat) and klwa_1012 (Mon): a weekday split — two moments, two ladder frames", async () => {
    const sat = await load(st, "wbk:-22", 0, 10, 10, "Saturday"), mon = await load(st, "wbk:-22", 0, 10, 12, "Monday");
    expect([sat.s, sat.cell]).toEqual([1, { ladder: 3452, rows: 2381, gear: 433, licence: 2, display: 111 }]);
    expect([mon.s, mon.cell]).toEqual([0, { ladder: 3451, rows: 2380, gear: 433, licence: 2, display: 111 }]);
    expect([sat.year!.moments!.length, sat.year!.ladderBySeg.length, sat.year!.cells.map((c) => c.ladder)]).toEqual([2, 2, [3451, 3452]]);
    expect(sat.year!.ladderBySeg[0]).not.toEqual(sat.year!.ladderBySeg[1]);
  });

  it("4. x15p7_1010 (an hours window: outside it) and x31p_1010 (tidal)", async () => {
    const x15 = await load(st, "gnis:39325", 7, 10, 10, "Saturday");
    expect([x15.s, x15.cell!.display]).toEqual([4, 704]);
    const x31 = await load(st, "wbk:329504244", 0, 10, 10, "Saturday");
    expect([x31.key!.tidal, x31.frames!.display.status]).toEqual([true, "tidal"]);
  });

  it("5. ape_501: a rule the part reaches by the tributary walk ranks 1", async () => {
    const r = requestOf("ape_501");
    const inp = await loadRegsInputs(st, r.target, r.day, r.weekday, 2026);
    const v = inp.rules.get(1644);
    expect([v.via, v.rank]).toEqual(["trib", 1]);
  });

  it.each(ids)("6. %s: key, segment, cell and the display frame of all 366 days equal taps.json", async (id) => {
    const t = T.states[id]!, inp = (await inputsOf(id))!;
    expect([inp.key!.akey, inp.s, inp.cell]).toEqual([t.akey, t.s, t.cell]);
    expect([inp.item!.ord, inp.year!.segStarts, inp.year!.moments]).toEqual([t.item_ord, t.starts, t.moments]);
    const ix = displayIds(inp);
    expect(inp.year!.displayByDay.map((f) => ix.get(f!))).toEqual(t.displayByDay);
    expect(inp.frames!.display).toBe(inp.year!.displayBySeg[inp.s!]);
  });

  it("7. mergedParts: chw0_1010's closed group of two stretches (8 sections), each with its first decided closure", async () => {
    const inp = (await inputsOf("chw0_1010"))!;
    const m = inp.mergedParts!;
    expect(m.length).toBe(2);
    expect(m.reduce((a, p) => a + p.sections, 0)).toBe(8);
    expect(m.map((p) => p.label).every((l) => l.length > 0)).toBe(true);
    expect(m.map((p) => p.broad!.rank).sort()).toEqual([-1, 0]);
  });

  it("7b. mergedParts on every golden state: present exactly where page v36 draws its banner, with its stretches and sections", async () => {
    const G = golden().states;
    const banner = (id: string) => /Closed all year on (\d+) stretches \((\d+) sections\)/.exec(JSON.stringify(G[id]!.keep));
    const got = await Promise.all(ids.map(async (id) => {
      const m = (await inputsOf(id))!.mergedParts;
      return [id, m && [m.length, m.reduce((a, p) => a + p.sections, 0)]] as const;
    }));
    const want = ids.map((id) => { const b = banner(id); return [id, b && [Number(b[1]), Number(b[2])]] as const; });
    expect(got).toEqual(want);
    expect(got.filter(([, m]) => m !== null).length).toBe(7);
  });

  it("8. an unnamed section: no item, the key, and the place's name; a handle outside B.C. has neither", async () => {
    const inp = await loadRegsInputs(st, { kind: "section", handle: 1178240 }, { month: 10, day: 10 }, "Saturday", 2026);
    expect([inp.item, inp.partIx, inp.key!.akey, inp.key!.kind, inp.place, inp.mergedParts])
      .toEqual([null, 0, 2191, "stream", { label: "Region 5", name: "Unnamed stream · Region 5" }, null]);
    expect(inp.frames).not.toBeNull();
    const out = await loadRegsInputs(st, { kind: "section", handle: 2_000_000_000 }, { month: 10, day: 10 }, "Saturday", 2026);
    expect([out.key, out.place, out.item, out.frames]).toEqual([null, null, null, null]);
    for (const u of T.unnamed) {
      const v = await loadRegsInputs(st, { kind: "section", handle: u.handle }, { month: 10, day: 10 }, "Saturday", 2026);
      expect([u.handle, v.key?.akey, v.place?.name]).toEqual([u.handle, u.akey, u.name]);
    }
  });

  it("9. targetFor: no section → the first part with a key; a section of the Cowichan's part 1 → part 1", async () => {
    expect(await targetFor(st, "gnis:3062", null)).toEqual({ kind: "part", item: "gnis:3062", partIx: 0 });
    const cow = T.states.cow1_1010!;
    const sids = await (await st.water()).all("SELECT sid FROM part_section WHERE ord = ? AND part_ix = ? ORDER BY sid", cow.item_ord, 1);
    expect(sids.length).toBeGreaterThan(0);
    for (const r of sids) expect(await targetFor(st, cow.item, Number(r.sid))).toEqual({ kind: "part", item: cow.item, partIx: 1 });
  });

  it("10. refusals: an unknown water, a part the water does not have", async () => {
    await expect(load(st, "nope", 0, 10, 10, "Saturday")).rejects.toThrow(/no water/);
    await expect(load(st, "gnis:3062", 99, 10, 10, "Saturday")).rejects.toBeInstanceOf(RegsInputsError);
  });

  it("11. timing: all 134 golden taps, warm, in ≤ 3 s (node)", async () => {
    await Promise.all(ids.map((id) => inputsOf(id)));
    const t0 = performance.now();
    for (const id of ids) {
      const r = requestOf(id);
      await loadRegsInputs(st, r.target, r.day, r.weekday, 2026);
    }
    expect(performance.now() - t0).toBeLessThan(3000);
  });
});
