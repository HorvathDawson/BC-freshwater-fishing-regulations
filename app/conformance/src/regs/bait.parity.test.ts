/**
 * T31 parity: the Gear tab's Bait item — core `buildRegsPage(…).gear.bait` (bait.ts `buildBait`) over each golden state's
 * inputs (T12 `cases("gear")`, the live release paired with the oracles) — against golden.json (regs v38 @ 7fb6809f)
 * `gear.bait` on all 134 states: label, the ban note with its ⓘ, the dated ban lines, the ✓ / ✕ list, the source line.
 * With T30 that is the whole Gear tab, so this suite also holds every drawn line of it to golden `gear.lines_`.
 *
 * Every expected value is the golden's; the ticket's literals were re-read from the v38 golden.
 */
import { afterAll, describe, expect, it } from "vitest";
import { buildRegsPage, type BaitVM, type YesNoVM } from "@app/core";
import { eqPlain } from "./eq";
import { ADULT_RESIDENT, caseIds, golden, inputsOf, parityGate, requestOf, summary } from "./golden";
import { excluded } from "./exclusions";
import { gearProj } from "./gearPage";

interface GDated { now: boolean; chip: string; text: string; when: string; inForce: boolean }
interface GBait { label: string; notes: string[]; dated: GDated[]; list: { col: string; items: { name: string; note: string | null }[] }[]; src: unknown }
const G = golden();
const gbait = (id: string) => (G.states[id]!.gear as unknown as { bait: GBait | null }).bait;
const gate = await parityGate();

const ids = caseIds("gear");
const gearOf = async (id: string) => buildRegsPage((await inputsOf(id))!, { today: requestOf(id).today, who: ADULT_RESIDENT, pid: id }).gear;
const baitOf = async (id: string) => (await gearOf(id))!.bait;
const proj = async (id: string) => gearProj((await gearOf(id))!, id).bait;

it(`T31 bait parity runs against a paired regs store${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ""}`, () => {
  if (gate.fail) expect.fail(gate.fail);
});

describe.skipIf(gate.skip)("T31 bait vs golden gear.bait (and the whole Gear tab's lines)", () => {
  let passed = 0;
  afterAll(() => console.info(summary("gear", passed, ids.length).replace(/^gear/, "gear.bait + gear.lines_ (T31)")));

  /** The golden's bait and Gear lines, with the keyed Bella Coola who-word (exclusions.ts SNAPSHOT_SUB_IDS) asserted and set
   *  to the app's per-part word — every other field is compared as the golden has it. */
  const want = (id: string, got: ReturnType<typeof gearProj>) => {
    const g = structuredClone(G.states[id]!.gear as unknown as { bait: (GBait & { src: { who: string } }) | null; lines_: string[] });
    if (excluded("gear", id, "bait.src.who")) {
      expect([got.bait?.src?.who, g.bait?.src.who]).toEqual(["This water’s ›", "Bella Coola River ›"]);
      g.lines_ = g.lines_.map((l) => l.replace(/^Bella Coola River › /, "This water’s › "));
      g.bait!.src.who = "This water’s ›";
    }
    return g;
  };

  it.each(ids)("1. %s: gear.bait and every drawn line of the Gear tab equal golden", async (id) => {
    const got = gearProj((await gearOf(id))!, id), g = want(id, got);
    expect(got.bait).toEqual(g.bait);
    const l = eqPlain(got.lines_, g.lines_);
    expect(l.ok, l.ok ? "" : l.diff).toBe(true);
    passed++;
  });

  // (T115) the 4 states whose part the release merged away (x15p14/15/17/18, taps.json `gone`) are not cases: each had bait, the ban note and a dated Now line
  it("1. tallies: 103 with bait, 63 with the ban note, 64 with a dated line (63 Now), 20 All year", async () => {
    const t = { bait: 0, notes: 0, dated: 0, now: 0, allYear: 0 };
    for (const id of ids) {
      const b = (await gearOf(id))!.bait;
      if (!b) continue;
      t.bait++; if (b.notes.length) t.notes++; if (b.dated.length) t.dated++;
      if (b.dated.some((d) => d.chip === "Now")) t.now++; if (b.dated.some((d) => d.when === "All year")) t.allYear++;
    }
    expect(t).toEqual({ bait: 103, notes: 63, dated: 64, now: 63, allYear: 20 });
  });

  it("mutation pins: a swapped column, a lost note, a wrong chip or a reordered item fails the comparison", async () => {
    const v = (await gearOf("vedder_1010"))!, g = want("vedder_1010", gearProj(v, "vedder_1010"));
    const eq = (b: BaitVM) => { const p = gearProj({ ...v, bait: b }, "vedder_1010");
      return JSON.stringify(p.bait) === JSON.stringify(g.bait) && eqPlain(p.lines_, g.lines_).ok; };
    const b = v.bait!;
    expect(eq(b)).toBe(true);
    const [ok, no] = b.list as [YesNoVM[number], YesNoVM[number]];
    const mut: Record<string, BaitVM> = {
      swapped: { ...b, list: [no, ok] },
      noNote: { ...b, list: [{ ...ok, items: ok.items.map((i) => ({ ...i, note: null })) }, no] },
      reordered: { ...b, list: [{ ...ok, items: [...ok.items].reverse() }, no] },
      chip: { ...b, dated: b.dated.map((d) => ({ ...d, chip: "Now" as const, inForce: true })) },
      banNote: { ...b, notes: [[{ t: "text", s: "No natural bait of any kind while the ban runs. " }]] },
      src: { ...b, src: null },
    };
    expect(Object.entries(mut).filter(([, m]) => eq(m)).map(([k]) => k)).toEqual([]);
  });

  it("2. vedder_1010: the May ban is Next; roe names the ban it is excepted from; insects on streams", async () => {
    const b = (await baitOf("vedder_1010"))!;
    expect(b.notes).toEqual([]);
    expect(b.dated).toEqual([{ chip: "Next", when: "May 1–31", says: "bait ban", inForce: false }]);
    expect(b.list).toEqual([
      { col: "ok", head: "Allowed", items: [{ mark: "✓", name: "Worms", note: null, terms: [] }, { mark: "✓", name: "Roe (fish eggs)", note: "the fish or fish parts ban excepts it", terms: [] },
        { mark: "✓", name: "Water insects and crayfish", note: "on streams", terms: [] }] },
      { col: "no", head: "Not allowed", items: [{ mark: "✕", name: "Fish or fish parts", note: null, terms: [] }] },
    ]);
    expect((await proj("vedder_1010"))!.src).toEqual({ who: "Province ›", open: "srcs:bait|vedder_1010", pages: [{ text: "Rule book p.8 ↗", pdf: 10 }] });
    expect(b.src?.open).toBe("srcs:bait");
  });

  it("3. vedder_515: the ban note with its ⓘ, the ban in force, only a ✕ column", async () => {
    const p = (await proj("vedder_515"))!;
    expect([p.notes, p.dated.map((d) => [d.chip, d.text]), p.list.map((c) => c.col), p.list[0]!.items.map((i) => i.name), (p.src as { who: string }).who])
      .toEqual([["No natural bait of any kind while the ban runs. ⓘ"], [["Now", "May 1–31: bait ban · in force today"]], ["no"],
        ["Worms", "Roe (fish eggs)", "Water insects and crayfish", "Fish or fish parts"], "This water’s ›"]);
  });

  it("4. glenn_501: a ban every day is 'All year'; its source names the downstream water", async () => {
    const p = (await proj("glenn_501"))!;
    expect([p.dated, p.src]).toEqual([[{ now: true, chip: "Now", text: "All year: bait ban · in force today", when: "All year:", inForce: true }],
      { who: "Duncan River ›", open: "srcs:bait|glenn_501", pages: [{ text: "Rule book p.36 ↗", pdf: 38 }] }]);
  });

  it("5. x15p0_1010: one ✓ column, roe's carry limit, no dated line", async () => {
    const p = (await proj("x15p0_1010"))!;
    expect([p.list.map((c) => c.col), p.list[0]!.items.find((i) => i.name === "Roe (fish eggs)")!.note, p.dated, (p.src as { who: string }).who])
      .toEqual([["ok"], "carry up to 1 kg", [], "This water’s ›"]);
  });

  it("6. a lake: water insects not on lakes (klwa_1010, the golden's first)", async () => {
    const first = Object.keys(G.states).find((id) => gbait(id)?.list.some((c) => c.col === "no" && c.items.some((i) => i.note === "not on lakes")));
    expect(first).toBe("klwa_1010");
    const no = (await baitOf("klwa_1010"))!.list.find((c) => c.col === "no")!;
    expect(no.items.find((i) => i.name === "Water insects and crayfish")!.note).toBe("not on lakes");
  });

  it("a closed day draws no Bait", async () => {
    expect(await baitOf("vedder_615")).toBeNull();
  });
});
