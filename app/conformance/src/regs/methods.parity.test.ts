/**
 * T32 parity: the Methods tab — core `buildRegsPage(…).methods` (methods.ts `buildMethods`) over each golden state's inputs
 * (T12 `cases("methods")`, the live release paired with the oracles) — against golden.json (regs v38 @ 7fb6809f) `methods`
 * on all 134 states: closed, Ways to fish, Boats, the Reference fold (closed), each source line, and every drawn line of
 * the tab (`methods.lines`).
 *
 * Every expected value is the golden's; the ticket's literals were re-read from the v38 golden.
 */
import { afterAll, describe, expect, it } from "vitest";
import { buildRegsPage, type MethodsVM, type YesNoVM } from "@app/core";
import { eqPlain } from "./eq";
import { ADULT_RESIDENT, caseIds, golden, inputsOf, parityGate, requestOf, summary } from "./golden";
import { excluded } from "./exclusions";
import { methodsProj } from "./gearPage";

interface GMethods { closed: string | null; ways: unknown; boats: unknown; reference: { open: boolean } | null; lines: string[] }
const G = golden();
const gate = await parityGate();
const ids = caseIds("methods");
const methodsOf = async (id: string) => buildRegsPage((await inputsOf(id))!, { today: requestOf(id).today, who: ADULT_RESIDENT, pid: id }).methods!;

it(`T32 methods parity runs against a paired regs store${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ""}`, () => {
  if (gate.fail) expect.fail(gate.fail);
});

describe.skipIf(gate.skip)("T32 ways, boats, reference vs golden methods", () => {
  let passed = 0;
  afterAll(() => console.info(summary("methods", passed, ids.length)));

  /** The golden's methods block, with the keyed Set lines note (exclusions.ts SET_LINE_NOTE_IDS) asserted — golden null, the
   *  app's the pipeline's conditions — and set to the app's; every other field as the golden has it. */
  const want = (id: string, got: ReturnType<typeof methodsProj>) => {
    const g = structuredClone(G.states[id]!.methods as unknown as GMethods & { ways: { list: { col: string; items: { name: string; note: string | null }[] }[] } | null });
    const ok = g.ways?.list[0], at = ok?.col === "ok" ? ok.items.findIndex((i) => i.name === "Set lines") : -1;
    if (at >= 0 && excluded("methods", id, `ways.list.0.items.${at}.note`)) {
      const mine = got.ways!.list[0]!.items[at]!;
      expect([mine.name, ok!.items[at]!.note, typeof mine.note === "string" && mine.note.length > 0]).toEqual(["Set lines", null, true]);
      ok!.items[at]!.note = mine.note;
      g.lines = g.lines.map((l) => (l === "✓ Set lines" ? `✓ Set lines ${mine.note}` : l));
    }
    return g;
  };

  it.each(ids)("1. %s: the tab equals golden, every drawn line too", async (id) => {
    const got = methodsProj(await methodsOf(id), id), g = want(id, got);
    expect({ ...got, lines: undefined }).toEqual({ ...g, lines: undefined });
    const l = eqPlain(got.lines, g.lines);
    expect(l.ok, l.ok ? "" : l.diff).toBe(true);
    if (g.reference) expect(g.reference.open).toBe(false);
    passed++;
  });

  it("1. tallies: 27 closed, 107 ways, 14 boats, 107 identical reference folds", async () => {
    const t = { closed: 0, ways: 0, boats: [] as string[], reference: 0, refShape: new Set<string>() };
    for (const id of ids) {
      const v = await methodsOf(id);
      if (v.closed !== null) t.closed++;
      if (v.ways) t.ways++;
      if (v.boats) t.boats.push(id);
      if (v.reference) { t.reference++; t.refShape.add(JSON.stringify([v.reference.label, v.reference.title, v.reference.sub, v.reference.groups.map((x) => x.head)])); }
    }
    expect([t.closed, t.ways, t.reference]).toEqual([27, 107, 107]);
    expect(t.boats.sort()).toEqual(["bc1_501", "bc1_1010", "x14p0_1010", "x14p1_1010", "x14p2_1010", "x15p2_1010", "x16p1_1010", "x18p0_1010",
      "x21p0_1010", "x21p1_1010", "x21p2_1010", "x27p0_1010", "x27p1_1010", "x27p2_1010"].sort());
    expect([...t.refShape]).toEqual([JSON.stringify(["Reference", "Handling fish", "21 rules that apply everywhere",
      ["When you catch a fish", "When you release a fish", "When you keep a fish", "Carrying fish", "Never"]])]);
  });

  it("mutation pins: a way moved across columns, a lost note or suffix, a reordered group or a lost boat fails the comparison", async () => {
    const v = await methodsOf("bc1_501"), g = want("bc1_501", methodsProj(v, "bc1_501"));
    const eq = (m: MethodsVM) => { const p = methodsProj(m, "bc1_501");
      return JSON.stringify({ ...p, lines: undefined }) === JSON.stringify({ ...g, lines: undefined }) && eqPlain(p.lines, g.lines).ok; };
    expect(eq(v)).toBe(true);
    const ways = v.ways!, [ok, no] = ways.list as [YesNoVM[number], YesNoVM[number]], ref = v.reference!;
    const mut: Record<string, MethodsVM> = {
      moved: { ...v, ways: { ...ways, list: [{ ...ok, items: ok.items.slice(1) }, { ...no, items: [...no.items, { mark: "✕", name: ok.items[0]!.name, note: null }] }] } },
      note: { ...v, ways: { ...ways, list: [{ ...ok, items: ok.items.map((i, k) => (k === 0 ? { ...i, note: null } : i)) }, no] } },
      suffix: { ...v, ways: { ...ways, list: [{ ...ok, items: ok.items.map((i) => ({ ...i, name: i.name.replace(" — not for trout or other game fish", "") })) }, no] } },
      groups: { ...v, reference: { ...ref, groups: [...ref.groups].reverse() } },
      sub: { ...v, reference: { ...ref, sub: "20 rules that apply everywhere" } },
      boats: { ...v, boats: null },
      src: { ...v, ways: { ...ways, src: { ...ways.src!, who: "This water’s ›" } } },
    };
    expect(Object.entries(mut).filter(([, m]) => eq(m)).map(([k]) => k)).toEqual([]);
  });

  it("2. bc1_501: three ways allowed with their conditions, four not; a powered-boat rule", async () => {
    const v = await methodsOf("bc1_501");
    expect(v.ways!.list).toEqual([
      { col: "ok", head: "Allowed", items: [
        { mark: "✓", name: "Angling (rod and line)", note: "with a downrigger: attached to your line by a quick-release · with a light: submerged, attached to the line, within 1 m of the hook" },
        { mark: "✓", name: "Spear or bow — not for trout or other game fish", note: "not for game fish, salmon or protected species · burbot allowed" },
        { mark: "✓", name: "Crayfish traps", note: "release anything in the trap that isn’t a crayfish" }] },
      { col: "no", head: "Not allowed", items: ["Set lines", "Nets", "Snagging", "Chumming"].map((name) => ({ mark: "✕", name, note: null })) },
    ]);
    expect([v.ways!.src?.who, v.ways!.src?.open, v.ways!.src?.pages]).toEqual(["Province ›", "srcs:ways", [{ printed: 8, pdf: 10 }]]);
    expect([v.boats!.items, v.boats!.src?.who, v.boats!.src?.pages]).toEqual([
      ["No powered boats — between the Goat Creek confluence and the Talchako River confluence"], "This water’s ›", [{ printed: 43, pdf: 49 }]]);
  });

  it("3. klmb_1010: ice fishing's clauses and conduct; spear or bow not allowed", async () => {
    const v = await methodsOf("klmb_1010");
    const ok = v.ways!.list.find((c) => c.col === "ok")!.items, no = v.ways!.list.find((c) => c.col === "no")!.items;
    expect(ok.find((i) => i.name === "Ice fishing")!.note).toBe("lines: 1 · hook, lure or fly per line: 1 · Remove your ice hut before breakup · Warn others of your ice hole");
    expect(no.map((i) => i.name)).toContain("Spear or bow");
  });

  it("4. vedder_515 (fly fishing only): angling's note says so", async () => {
    const ok = (await methodsOf("vedder_515")).ways!.list.find((c) => c.col === "ok")!.items;
    expect(ok.find((i) => i.name === "Angling (rod and line)")!.note).toBe("fly fishing only: nothing but the fly on the line, no float, sinker or attractor");
  });

  it("5. the Reference fold: the snagged-fish duty first, the release and never phrases, its pages", async () => {
    const r = (await methodsOf("bc1_501")).reference!;
    const at = (h: string) => r.groups.find((g) => g.head === h)!.items;
    expect([at("When you catch a fish"), at("When you release a fish"), at("Never").length, at("Never")[0]]).toEqual([
      ["Any fish snagged — even by accident — must be released."], ["Gently, back in the water", "Without harming it"], 6, "Sell, buy or trade your catch"]);
    expect([r.src?.open, r.src?.pages]).toEqual(["srcs:always", [{ printed: 8, pdf: 10 }, { printed: 10, pdf: 12 }]]);
  });

  it("6. vedder_615 (closed): the Gear tab's sentence and nothing else", async () => {
    expect(await methodsOf("vedder_615")).toEqual({ closed: "No fishing here until Jul 1.", closedMore: [], ways: null, boats: null, reference: null });
  });

  it("the tidal water (x31p_1010): the banner's words (D20 b)", async () => {
    const v = await methodsOf("x31p_1010");
    expect([v.closed, v.closedMore.length, v.ways]).toEqual(["Tidal water", 1, null]);
    expect(G.states.x31p_1010!.methods).toMatchObject({ closed: "Tidal water" });
  });
});
