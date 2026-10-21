/**
 * T09, store-free (always run): `loadRegsInputs` / `targetFor` / `displayByDay` over a hand-built store — what is read, in
 * which shape, each distinct frame once, and the refusals. The live release (the ticket's Vedder / Kalamalka / Cowichan
 * values and all 134 golden taps) is app/conformance/src/regs/inputs.release.test.ts.
 */
import { describe, expect, it } from "vitest";
import type { CellIds, DisplayFrame, ExportRule, Moment, Picker, RuleFacts } from "@app/core";
import { displayByDay, loadRegsInputs, RegsInputsError, targetFor, type InputsStore } from "./inputs";
import type { PartRow, StoredRule } from "./reads";

const WEEK = m(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]), WEEKEND = m(["Saturday", "Sunday"]);
function m(weekdays: string[]): Moment { return { weekdays, hours: null } as unknown as Moment; }

const OPEN: DisplayFrame = { status: "own", closing: [], strip: 1 };
const SHUT: DisplayFrame = { status: "closed", closing: [[100, ["RB"]], [101, ["RB", "CT"]]], strip: 2 } as unknown as DisplayFrame;
const rule = (rix: number, rank: number): StoredRule => ({ rix,
  export: { entry_id: `e${rix}`, rule_id: `r${rix}`, type: "t", family: "f", label: "L", verbatim: "V", fields: {}, prov: { rank } } as ExportRule,
  facts: { kind: "gate" } as unknown as RuleFacts });
const facts = (label: string) => ({ order: 0, label, short: label, runs: label, place: null, hint: "", km: 1, closed_all_year: false,
  paper_licence: [] });
const cell = (ladder: number, display: number): CellIds => ({ ladder, rows: 1, gear: 9, licence: 4, display });

/** One water (gnis:1, ord 3) of three parts — part 0 on key 2 (no moments, closed Jun 1–30), part 1 on key 5 (a weekday
 *  split), part 2 outside B.C. — a picker grouping parts 0 and 1 as closed, and two sections (4097 named → part 1; 4098
 *  unlisted, on key 5). `asked` logs every read. */
function fake(o: { openGroup?: boolean } = {}) {
  const asked: string[] = [];
  const log = <T>(what: string, v: T): Promise<T> => { asked.push(what); return Promise.resolve(v); };
  const parts: PartRow[] = [
    { itemOrd: 3, partIx: 0, akey: 2, facts: facts("Mouth to bridge"), sections: 4 },
    { itemOrd: 3, partIx: 1, akey: 5, facts: facts("Above the bridge"), sections: 2 },
    { itemOrd: 3, partIx: 2, akey: null, facts: null, sections: 1 }];
  const picker: Picker = { headed: true, choices: [
    { parts: [0, 1], closed: !o.openGroup, sections: 6, heading: "h", text: "t" }, { parts: [2], closed: false, sections: 1, heading: "h", text: "t" }] };
  const keys = {
    2: { akey: 2, ruleset: 7, licensingSet: 1, steelheadWater: false, steelhead: null, steelheadRules: false, kind: "stream", tidal: false, seg: 10, mom: null, nseg: 3 },
    5: { akey: 5, ruleset: 8, licensingSet: 1, steelheadWater: false, steelhead: null, steelheadRules: false, kind: "stream", tidal: false, seg: 11, mom: 3, nseg: 2 },
  } as const;
  const cells: Record<number, CellIds[]> = { 2: [cell(1, 20), cell(2, 21), cell(1, 20)], 5: [cell(3, 21), cell(4, 20)] };
  const display: Record<number, DisplayFrame> = { 20: OPEN, 21: SHUT };
  const st = {
    statics: () => log("statics", { exportSpecies: { fish: {}, groups: {} }, moments: [WEEK, WEEKEND], pdfPages: { 4: 6, 80: 86 } }),
    rules: () => log("rules", new Map([[100, rule(100, 0)], [101, rule(101, 0)]])),
    lics: () => log("lics", new Map([[0, { words: "lic" }]])),
    itemOrd: (id: string) => log(`itemOrd ${id}`, id === "gnis:1" ? 3 : id === "gnis:2" ? 4 : null),
    item: (ord: number) => log(`item ${ord}`, { ord, id: ord === 3 ? "gnis:1" : "gnis:2", name: "Test River", kind: "stream", shown: true,
      picker: ord === 3 ? 0 : null, sections: 7, outsideBc: 1, steelhead: 2, entries: [], x: null }),
    parts: (ord: number) => log(`parts ${ord}`, ord === 3 ? parts : [parts[2]!, { ...parts[1]!, partIx: 1 }]),
    picker: (id: number) => log(`picker ${id}`, picker),
    akey: (a: 2 | 5) => log(`akey ${a}`, keys[a]),
    segStarts: (seg: number) => log(`segStarts ${seg}`, seg === 10 ? [1, 153, 183] : [1, 1]),
    segMoments: (mom: number) => log(`segMoments ${mom}`, [WEEK, WEEKEND]),
    cells: (a: number) => log(`cells ${a}`, cells[a]!),
    cell: (a: number, s: number) => log(`cell ${a} ${s}`, cells[a]![s]!),
    ruleset: (id: number) => log(`ruleset ${id}`, id === 7 ? { sections: 4, reach: [100], trib: [101] } : { sections: 2, reach: [100, 101], trib: [] }),
    ladder: (a: number, id: number) => log(`ladder ${a} ${id}`, { common: [[], [], [], [], [], []], rows: [], id }),
    rows: (a: number, id: number) => log(`rows ${a} ${id}`, { spp: [], fish: {}, rows: [], steelhead_line: null, id }),
    gear: (a: number, id: number) => log(`gear ${a} ${id}`, { id }),
    licence: (a: number, id: number) => log(`licence ${a} ${id}`, { id }),
    display: (a: number, id: number) => log(`display ${a} ${id}`, display[id]!),
    sectionAkey: (h: number) => log(`sectionAkey ${h}`, h === 4097 || h === 4099 ? 2 : h === 4098 ? 5 : 0),
    sectionPlace: (h: number) => log(`sectionPlace ${h}`, h === 4099 ? null : 0),
    sectionPart: (ord: number, sid: number) => log(`sectionPart ${ord} ${sid}`, ord === 3 && sid === 4097 ? 1 : null),
    place: (ix: number) => log(`place ${ix}`, { place: ix, regions: ["5"], label: "Region 5",
      names: { stream: "Unnamed stream · Region 5", lake: "Unnamed lake · Region 5", wetland: "Unnamed wetland · Region 5" } }),
  };
  return { store: st as unknown as InputsStore, asked };
}

const JUN15 = { month: 6, day: 15 };   // leap day 167, a Monday in 2026

describe("loadRegsInputs: a part", () => {
  it("reads the item, its parts and picker, the key, the day's segment and cell, and the five frames", async () => {
    const { store } = fake();
    const inp = await loadRegsInputs(store, { kind: "part", item: "gnis:1", partIx: 0 }, JUN15, "Monday", 2026);
    expect(inp.item).toEqual({ id: "gnis:1", ord: 3, name: "Test River", kind: "stream", sections: 7,
      parts: [facts("Mouth to bridge"), facts("Above the bridge"), null], partSections: [4, 2, 1],
      picker: (await store.picker(0)), steelhead: 2 });
    expect([inp.partIx, inp.place, inp.key?.akey, inp.s, inp.cell]).toEqual([0, null, 2, 1, cell(2, 21)]);
    expect(inp.frames).toEqual({ ladder: { common: [[], [], [], [], [], []], rows: [], id: 2 },
      rows: { spp: [], fish: {}, rows: [], steelhead_line: null, id: 1 }, gear: { id: 9 }, licence: { id: 4 }, display: SHUT });
    expect(inp.ruleset).toEqual({ reach: [100], trib: [101] });
    expect([inp.target, inp.day, inp.weekday, inp.calendarYear]).toEqual([{ kind: "part", item: "gnis:1", partIx: 0 }, JUN15, "Monday", 2026]);
    expect(inp.lics.get(0)).toEqual({ words: "lic" });
    expect(inp.statics.pdfPages).toEqual({ 4: 6, 80: 86 });   // the store's page map, passed through (never computed)
  });

  it("the year: every segment's frames by s, each distinct frame read once; displayByDay shares the frame objects", async () => {
    const { store, asked } = fake({ openGroup: true });   // an open group: no mergedParts reads in the log
    const inp = await loadRegsInputs(store, { kind: "part", item: "gnis:1", partIx: 0 }, JUN15, "Monday", 2026);
    const y = inp.year!;
    expect([y.segStarts, y.moments, y.cells]).toEqual([[1, 153, 183], null, [cell(1, 20), cell(2, 21), cell(1, 20)]]);
    expect(y.ladderBySeg.map((l) => (l as unknown as { id: number }).id)).toEqual([1, 2, 1]);
    expect(y.rowsBySeg.map((r) => (r as unknown as { id: number }).id)).toEqual([1, 1, 1]);
    expect(y.displayBySeg).toEqual([OPEN, SHUT, OPEN]);
    expect(y.ladderBySeg[0]).toBe(y.ladderBySeg[2]);
    expect(asked.filter((a) => /^(ladder|rows|display|gear|licence) 2 /.test(a)).sort())
      .toEqual(["display 2 20", "display 2 21", "gear 2 9", "ladder 2 1", "ladder 2 2", "licence 2 4", "rows 2 1"]);
    expect(y.displayByDay.length).toBe(366);
    const ix = (d: number) => y.displayByDay[d - 1];
    expect([ix(1), ix(152), ix(153), ix(182), ix(183), ix(366)]).toEqual([OPEN, OPEN, SHUT, SHUT, OPEN, OPEN]);
    expect(ix(1)).toBe(ix(366));
    expect(new Set(y.displayByDay).size).toBe(2);
  });

  it("rules: every store rule, viewed through this part's via (101 is a tributary rule here: rank 1)", async () => {
    const { store } = fake();
    const inp = await loadRegsInputs(store, { kind: "part", item: "gnis:1", partIx: 0 }, JUN15, "Monday", 2026);
    expect([inp.rules.get(100).via, inp.rules.get(100).rank, inp.rules.get(101).via, inp.rules.get(101).rank]).toEqual(["reach", 0, "trib", 1]);
    expect(inp.rules.get(999).missing).toBe(true);
    expect(inp.rules.size).toBe(2);
  });

  it("a weekday split key: the moments, and the day's segment on the weekday given", async () => {
    const { store } = fake();
    const mon = await loadRegsInputs(store, { kind: "part", item: "gnis:1", partIx: 1 }, JUN15, "Monday", 2026);
    const sat = await loadRegsInputs(store, { kind: "part", item: "gnis:1", partIx: 1 }, { month: 6, day: 13 }, "Saturday", 2026);
    expect([mon.s, mon.year!.moments, mon.frames!.display, sat.s, sat.frames!.display]).toEqual([0, [WEEK, WEEKEND], SHUT, 1, OPEN]);
    // each day on its own 2026 weekday: Jun 13 2026 is a Saturday (segment 1), Jun 15 a Monday (segment 0)
    expect([mon.year!.displayByDay[165 - 1], mon.year!.displayByDay[167 - 1]]).toEqual([OPEN, SHUT]);
  });

  it("mergedParts: a closed picker group of ≥ 2 parts → each part's label, sections and its first decided closure", async () => {
    const { store } = fake();
    const inp = await loadRegsInputs(store, { kind: "part", item: "gnis:1", partIx: 0 }, JUN15, "Monday", 2026);
    expect(inp.mergedParts!.map((p) => [p.partIx, p.label, p.sections, p.broad?.rix, p.broad?.via]))
      .toEqual([[0, "Mouth to bridge", 4, 101, "trib"], [1, "Above the bridge", 2, 101, "reach"]]);
  });

  it("mergedParts is null for an open group, and for a part whose group is one part", async () => {
    expect((await loadRegsInputs(fake({ openGroup: true }).store, { kind: "part", item: "gnis:1", partIx: 0 }, JUN15, "Monday", 2026))
      .mergedParts).toBeNull();
    expect((await loadRegsInputs(fake().store, { kind: "part", item: "gnis:1", partIx: 2 }, JUN15, "Monday", 2026)).mergedParts).toBeNull();
  });

  it("a part outside B.C. (no akey): no key, segment, cell, frames, year or ruleset — the item and rules still there", async () => {
    const { store, asked } = fake();
    const inp = await loadRegsInputs(store, { kind: "part", item: "gnis:1", partIx: 2 }, JUN15, "Monday", 2026);
    expect([inp.key, inp.s, inp.cell, inp.frames, inp.year, inp.ruleset, inp.place, inp.mergedParts])
      .toEqual([null, null, null, null, null, null, null, null]);
    expect([inp.item?.id, inp.partIx, inp.rules.get(101).via]).toEqual(["gnis:1", 2, "reach"]);
    expect(asked.some((a) => a.startsWith("akey"))).toBe(false);
  });

  it("refuses a water the store does not hold, and a part the water does not have", async () => {
    const { store } = fake();
    await expect(loadRegsInputs(store, { kind: "part", item: "nope", partIx: 0 }, JUN15, "Monday", 2026))
      .rejects.toEqual(new RegsInputsError("regs: no water nope in the regs store"));
    await expect(loadRegsInputs(store, { kind: "part", item: "gnis:1", partIx: 99 }, JUN15, "Monday", 2026))
      .rejects.toEqual(new RegsInputsError("regs: gnis:1 has 3 part(s), no part 99"));
    await expect(loadRegsInputs(store, { kind: "part", item: "gnis:1", partIx: 99 }, JUN15, "Monday", 2026))
      .rejects.toBeInstanceOf(RegsInputsError);
  });
});

describe("loadRegsInputs: a section (unnamed water)", () => {
  it("an unnamed section: no item, part 0, the place's label and its name for the key's kind", async () => {
    const { store } = fake();
    const inp = await loadRegsInputs(store, { kind: "section", handle: 4097 }, JUN15, "Monday", 2026);
    expect([inp.item, inp.partIx, inp.place, inp.key?.akey, inp.s, inp.mergedParts])
      .toEqual([null, 0, { label: "Region 5", name: "Unnamed stream · Region 5" }, 2, 1, null]);
    expect(inp.rules.get(101).via).toBe("trib");
  });

  it("a section outside B.C. (section_akey 0): no key, no place", async () => {
    const inp = await loadRegsInputs(fake().store, { kind: "section", handle: 1 }, JUN15, "Monday", 2026);
    expect([inp.item, inp.place, inp.key, inp.s, inp.frames, inp.year]).toEqual([null, null, null, null, null, null]);
  });

  it("a section with a key and no place is the store's fault, said", async () => {
    await expect(loadRegsInputs(fake().store, { kind: "section", handle: 4099 }, JUN15, "Monday", 2026))
      .rejects.toEqual(new RegsInputsError("regs: section 4099 has a key (2) but no place"));
  });
});

describe("targetFor: the part a tap opens", () => {
  it("a section the water lists (part_section) opens its part", async () => {
    expect(await targetFor(fake().store, "gnis:1", 4097)).toEqual({ kind: "part", item: "gnis:1", partIx: 1 });
  });
  it("a section it does not list: the first part on the section's key", async () => {
    expect(await targetFor(fake().store, "gnis:1", 4098)).toEqual({ kind: "part", item: "gnis:1", partIx: 1 });
  });
  it("no section (or one outside B.C.): the first part with a key", async () => {
    expect(await targetFor(fake().store, "gnis:1", null)).toEqual({ kind: "part", item: "gnis:1", partIx: 0 });
    expect(await targetFor(fake().store, "gnis:2", 1)).toEqual({ kind: "part", item: "gnis:2", partIx: 1 });
  });
  it("an unknown water", async () => {
    await expect(targetFor(fake().store, "nope", null)).rejects.toEqual(new RegsInputsError("regs: no water nope in the regs store"));
  });
});

describe("displayByDay", () => {
  it("366 days, each the frame of its segment; Feb 29 (day 60) at its first moment, other days on their own weekday", () => {
    const y = { segStarts: [1, 1], moments: [WEEK, WEEKEND], cells: [], ladderBySeg: [], rowsBySeg: [], displayBySeg: [SHUT, OPEN] };
    const d = displayByDay(y, 2026);
    expect(d.length).toBe(366);
    // 2026: Jan 1 Thursday, Jan 3 Saturday, Feb 28 Saturday, Mar 1 Sunday, Mar 2 Monday
    expect([d[0], d[2], d[58], d[59], d[60], d[61]]).toEqual([SHUT, OPEN, OPEN, SHUT, OPEN, SHUT]);
  });
});
