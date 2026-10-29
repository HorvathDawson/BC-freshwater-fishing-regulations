/**
 * T30 parity: the Gear tab's top block — core `buildRegsPage(…).gear` without Bait (gear.ts `buildGearTop`) over each
 * golden state's inputs (T12 `cases("gear")`, the live release paired with the oracles) — against golden.json (regs v38 @
 * 7fb6809f) `gear` on all 134 states: closed, day, mode, every line (key, label, values, ⓘ, changes, source line).
 *
 * Every expected value is the golden's; the ticket's literals were re-read from the v38 golden. Nothing here is computed
 * from the implementation: the level words are held to the golden's own rule sheets (case 7).
 */
import { afterAll, describe, expect, it } from "vitest";
import { bookBits, buildRegsPage, gearCtxOf, levelWord, stripRuns, type GearLineVM, type GearVM, type RuleView } from "@app/core";
import { ADULT_RESIDENT, caseIds, cases, golden, inputsOf, parityGate, requestOf, summary } from "./golden";
import { gearProj } from "./gearPage";

interface GGear { closed: string | null; day: string | null; mode: unknown; lines: unknown[]; bait: unknown; lines_: string[] }
const G = golden();
const gg = (id: string) => G.states[id]!.gear as unknown as GGear;
const gate = await parityGate();
const ids = caseIds("gear");

/** The page's Gear tab for a golden state (bait left to T31). */
async function gearOf(id: string) {
  const page = buildRegsPage((await inputsOf(id))!, { today: requestOf(id).today, who: ADULT_RESIDENT, pid: id });
  return { page, gear: page.gear! };
}
const gtop = (id: string) => { const g = gg(id); return { closed: g.closed, day: g.day, mode: g.mode, lines: g.lines }; };

it(`T30 gear parity runs against a paired regs store${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ""}`, () => {
  if (gate.fail) expect.fail(gate.fail);
});

describe.skipIf(gate.skip)("T30 gear lines vs golden gear", () => {
  let passed = 0;
  afterAll(() => console.info(summary("gear", passed, ids.length).replace(/^gear/, "gear (T30 top)")));

  it.each(ids)("1. %s: closed, day, mode and every line equal golden gear", async (id) => {
    const got = gearProj((await gearOf(id)).gear, id);
    expect({ closed: got.closed, day: got.day, mode: got.mode, lines: got.lines }).toEqual(gtop(id));
    passed++;
  });

  // (T115) the 4 states whose part the release merged away (x15p14/15/17/18, taps.json `gone`) are not cases: all 4 open, with 3 lines;
  // (T183) and the West Road's x27p1 (its Region 6 stretch shares part 0's rules now, taps.json `gone`) neither: open, 3 lines, a mode (artificial fly only)
  it("1. tallies: 23 closed, 101 open with 3 lines, 10 modes, the 8 Weight lines with no source, the dated changes", async () => {
    const t = { closed: 0, threeLines: 0, mode: 0, nullSrc: [] as string[], lineChanges: [] as string[], modeChanges: [] as string[] };
    for (const id of ids) {
      const v = (await gearOf(id)).gear;
      if (v.closed !== null) t.closed++;
      if (v.lines.length === 3) t.threeLines++;
      if (v.mode) t.mode++;
      t.nullSrc.push(...v.lines.filter((l) => l.src === null).map((l) => `${id} ${l.k}`));
      t.lineChanges.push(...v.lines.filter((l) => l.changes.length).map((l) => `${id} ${l.k}`));
      if (v.mode?.changes.length) t.modeChanges.push(id);
    }
    expect(t).toEqual({ closed: 23, threeLines: 100, mode: 10,   // (T228) x37p0 gone: the Coquitlam's no-access split cut its golden part in two
     
      nullSrc: ["vedder_515", "cow1_1010", "cow2_1010", "cow3_1010", "x13p0_1010", "x16p8_1010", "x17p7_1010", "x17p1_1010"].map((x) => `${x} weight`),
      lineChanges: ["vedder_1010 hook", "x16p3_1010 hook", "x17p4_1010 hook"], modeChanges: ["vedder_515", "cow1_1010", "cow3_1010"] });
  });

  it("mutation pins: a reworded value, a wrong chip, a moved change, a lost page, who or ⓘ fails the comparison", async () => {
    const v = (await gearOf("vedder_1010")).gear, want = JSON.stringify(gtop("vedder_1010"));
    const eq = (x: GearVM) => { const p = gearProj(x, "vedder_1010"); return JSON.stringify({ closed: p.closed, day: p.day, mode: p.mode, lines: p.lines }) === want; };
    expect(eq(v)).toBe(true);
    const [lines, hook, weight] = v.lines as [GearLineVM, GearLineVM, GearLineVM];
    const mut: Record<string, GearVM> = {
      value: { ...v, lines: [lines, { ...hook, values: ["One hook, lure or fly", "Single barbless hook"] }, weight] },
      chip: { ...v, lines: [lines, { ...hook, changes: hook.changes.map((c) => ({ ...c, chip: "Now" as const, inForce: true })) }, weight] },
      moved: { ...v, lines: [lines, { ...hook, changes: [] }, { ...weight, changes: hook.changes }] },
      page: { ...v, lines: [lines, { ...hook, src: { ...hook.src!, pages: hook.src!.pages.slice(1) } }, weight] },
      who: { ...v, lines: [{ ...lines, src: { ...lines.src!, who: "B.C. ›" } }, hook, weight] },
      restTerm: { ...v, lines: [lines, hook, { ...weight, valueTerms: [[]] }] },
      day: { ...v, day: "Sat, Oct 10" },
    };
    expect(Object.entries(mut).filter(([, m]) => eq(m)).map(([k]) => k)).toEqual([]);
  });

  it("2. vedder_1010: Hooks — two values, the barbless ⓘ, the May fly-only change, three levels; Weight takes the rest term", async () => {
    const v = (await gearOf("vedder_1010")).gear;
    expect(v.day).toBe("Today, Oct 10");
    const hook = v.lines.find((l) => l.k === "hook")!;
    expect(hook.values).toEqual(["One hook, lure or fly", "Single barbless hook, no trebles"]);
    expect(hook.terms).toEqual(["single_barbless_hook"]);
    expect(hook.changes).toEqual([{ chip: "Next", when: "May 1–31", says: "fly fishing only", inForce: false }]);
    expect(hook.src).toMatchObject({ who: "Province · Region 2 · This water’s ›", open: "srcs:gl|hook",
      pages: [{ printed: 8, pdf: 10 }, { printed: 21, pdf: 23 }, { printed: 22, pdf: 24 }] });
    const lines = v.lines.find((l) => l.k === "lines")!, weight = v.lines.find((l) => l.k === "weight")!;
    expect([lines.values, lines.src?.who, weight.values, weight.terms]).toEqual([["1 line"], "Province ›", ["Up to 1 kg (2.2 lb)"], ["fly_fishing"]]);
  });

  it("3. vedder_515: a Friday; the fly-only mode carries the change back; Weight has no source line", async () => {
    const v = (await gearOf("vedder_515")).gear;
    expect(v.day).toBe("Fri, May 15");
    expect(v.mode).toMatchObject({ value: "Fly fishing only", terms: ["fly_fishing"], src: { who: "This water’s · Province ›" } });
    expect(v.mode!.changes.map((c) => `${c.when}: ${c.says}`))
      .toEqual(["Jul 1–Apr 30: one hook, lure or fly and weight on the line up to 1 kg (2.2 lb)"]);
    const hook = v.lines.find((l) => l.k === "hook")!, weight = v.lines.find((l) => l.k === "weight")!;
    expect([hook.values, hook.src?.who]).toEqual([["One fly", "Single barbless hook, no trebles"], "Province · Region 2 ›"]);
    expect([weight.values, weight.src]).toEqual([["No float or strike indicator, no sinker or split shot, no lure, spinner or other attracting device"], null]);
  });

  it("4. closed days: when it opens, or never; the tidal water says the banner's words (D20 b)", async () => {
    expect((await gearOf("vedder_615")).gear).toEqual({ closed: "No fishing here until Jul 1.", closedMore: [], day: null, mode: null, lines: [], bait: null });
    // (T220) x9p2 and x13p2 are PART_GONE states now (taps.json `gone`): no tap to read
    for (const id of ["chw0_1010", "x5p0_1010"]) expect((await gearOf(id)).gear.closed, id).toBe("No fishing here on any day of the year.");
    const tidal = (await gearOf("x31p_1010")).gear;
    expect([tidal.closed, tidal.closedMore.length, tidal.lines]).toEqual(["Tidal water", 1, []]);
    expect(tidal.closedMore[0]).toMatch(/^Tidal water: a different regulation system\. /);
  });

  it("5. klmb_1010 / klwa_1010: Lines in the data's words", async () => {
    const a = (await gearOf("klmb_1010")).gear.lines[0]!, b = (await gearOf("klwa_1010")).gear.lines[0]!;
    expect([a.values, a.src?.who, a.src?.pages.map((p) => p.printed)]).toEqual([["1 line from shore; any number from a boat"], "Province · This water’s ›", [8, 37]]);
    expect([b.values, b.src?.who]).toEqual([["1 line (2 while alone in a boat)"], "Province ›"]);
  });

  it("6. x16p3_1010: the Hooks change and its three levels", async () => {
    const h = (await gearOf("x16p3_1010")).gear.lines.find((l) => l.k === "hook")!;
    expect([h.changes.map((c) => `${c.when}: ${c.says}`), h.src?.who]).toEqual([["Dec 1–May 31: artificial fly only"], "Province · Region 1 · This water’s ›"]);
  });

  it("7. levelWord equals the golden rule sheet's level, on every rule sheet of every state", async () => {
    let n = 0;
    const levels = new Set<string>(), bad: string[] = [];
    for await (const c of cases("gear")) {
      const inp = await c.inputs(), byKey = new Map<string, RuleView>();
      for (let ix = 0; ix < inp.rules.size; ix++) if (inp.rules.has(ix)) byKey.set(inp.rules.get(ix).key, inp.rules.get(ix));
      for (const [k, sh] of Object.entries(c.g.sheets as Record<string, unknown>)) {
        if (!k.startsWith("rule:")) continue;
        const sheet = (sh === "@" ? G.sheets[k] : sh) as { sub?: string } | undefined;
        const sub = sheet?.sub ?? "";
        if (!sub.includes(" rule · ")) continue;
        const r = byKey.get(k.slice(5));
        if (!r) { bad.push(`${c.id} ${k}: no such rule in the store`); continue; }
        n++; levels.add(sub.split(" rule · ")[0]!);
        if (levelWord(r) !== sub.split(" rule · ")[0]) bad.push(`${c.id} ${k}: ${levelWord(r)} ≠ ${sub}`);
      }
    }
    expect(bad.slice(0, 20), bad.slice(0, 20).join("\n")).toEqual([]);
    expect(n).toBeGreaterThan(100);
    expect([...levels].sort()).toEqual(["B.C.", "Downstream", "Federal or park", "Named area", "Region 1", "Region 3", "This water’s"]);
  });

  it("8. chip learning: every learned pair is B.C.→Province or This water’s→This water; vedder_1010 learns only the first", async () => {
    const pairs = new Set<string>();
    for await (const c of cases("gear")) {
      const inp = await c.inputs(), page = await c.page();
      const ctx = gearCtxOf(inp, { today: { month: 10, day: 10 }, banner: page.keep.banner });
      for (const [k, v] of ctx.chips) pairs.add(`${k}→${v}`);
      if (c.id === "vedder_1010") expect([...ctx.chips]).toEqual([["B.C.", "Province"]]);
    }
    expect([...pairs].sort()).toEqual(["B.C.→Province", "This water’s→This water"]);
  });

  it("9. bookBits(vedder_1010): 365 days whose runs are the golden strip's bar", async () => {
    const inp = (await inputsOf("vedder_1010"))!, bits = bookBits(inp);
    const runs: number[] = [];
    bits.forEach((b, i) => { if (i && b === bits[i - 1]) runs[runs.length - 1]!++; else runs.push(1); });
    const bar = (G.states.vedder_1010!.keep as { strip: { bar: { flex: number }[] } }).strip.bar.map((b) => b.flex);
    expect([bits.length, runs]).toEqual([365, bar]);
    expect(stripRuns(inp).map((r) => r.days)).toEqual(bar);
  });

  it("the mode: never more than one mode part on a day's sentence", async () => {
    for await (const c of cases("gear")) {
      const g = (await c.inputs()).frames?.gear;
      if (g && "summary" in g) expect(g.summary.parts.filter((p) => p.part === "mode").length, c.id).toBeLessThanOrEqual(1);
    }
  });
});
