/**
 * (T1031 items 4-5) THE SPECIES SHEET over the store (the golden states): one sheet per fish line, two tabs —
 * Identification | Sources — each tab its own key, each the other's tab; the line's picture opens Identification, the line
 * Sources; the Sources tab ends with the fish's source breakdown (`breakdown.ts`): the row's deciding rules by authority,
 * each rule's pages carrying its own highlight. Pinned whole for vedder_1010's Kokanee (AGENTS 33).
 */
import { describe, expect, it } from "vitest";
import { ALL_SOURCES, SOURCE_AUTHORITIES, SPECIES_SHEET, WORDS, bookText, buildRegsPage, setFailureSink, type RegsPage, type SheetBlock, type SheetKey, type SheetVM } from "@app/core";
import { ADULT_RESIDENT, inputsOf, parityGate, requestOf, taps } from "./golden";

const gate = await parityGate();


it(`T1031 species sheet parity runs against a paired regs store${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ""}`, () => {
  if (gate.fail) expect.fail(gate.fail);
});

const pageOf = async (id: string): Promise<RegsPage> =>
  buildRegsPage((await inputsOf(id))!, { today: requestOf(id).today, who: ADULT_RESIDENT, pid: id });
const tabsOf = (vm: SheetVM | null) => vm?.tabs?.map((t) => `${t.label}=${t.open}${t.on ? "*" : ""}`) ?? null;
/** the breakdown as drawn: each group's head, then each rule's title, role and pages, one string each */
function breakdownText(vm: SheetVM): string[] {
  const i = vm.blocks.findIndex((b) => b.k === "head" && b.text === SPECIES_SHEET.BREAKDOWN), b = vm.blocks[i + 1];
  if (i < 0 || b?.k !== "sources") return [];
  return b.groups.flatMap((g) => [g.where, ...g.rows.map((r) => [r.title ?? r.quote, r.role, ...r.book.map((x) =>
    (x.t === "book" ? `${bookText(x)}${x.box?.length ? "[hl]" : ""}` : "s" in x ? x.s : ""))].filter(Boolean).join(" · "))]);
}

describe.skipIf(gate.skip)("T1031 the species sheet over the golden states", () => {
  it("every Other fish line: Sources (the row) and Identification (its picture), each the other's tab, one title", async () => {
    let n = 0, multi = 0, bare = 0;
    for (const id of Object.keys(taps().states)) {
      const page = await pageOf(id);
      for (const r of page.keep.otherFish ?? []) {
        const src = page.sheet(r.open)!, at = `${id} ${r.name}`;
        // (review 2) every line ends with its deciding rules, a row with no picture too (its Sources sheet alone, no tabs)
        expect(breakdownText(src).length > 0, at).toBe(true);
        if (!r.id) { expect(src.tabs, at).toBeUndefined(); bare++; continue; }
        n++;
        const idv = page.sheet(r.id)!;
        expect([tabsOf(src), tabsOf(idv)], at).toEqual([[`${SPECIES_SHEET.ID}=${r.id}`, `${SPECIES_SHEET.SRC}=${r.open}*`],
          [`${SPECIES_SHEET.ID}=${r.id}*`, `${SPECIES_SHEET.SRC}=${r.open}`]]);
        expect(idv.title, at).toBe(src.title);
        // the pictures are the Identification tab's: none on Sources; Identification draws the fish's looks
        expect(src.blocks.some((b) => b.k === "pictures"), at).toBe(false);
        expect(idv.blocks.some((b) => b.k === "image" || b.k === "swipe"), at).toBe(true);
        // each member's own picture key opens the same sheet's Identification tab
        expect(tabsOf(page.sheet(`sp:${r.thumb}` as SheetKey))?.map((t) => t.replace(/\*$/, "")), at).toEqual(tabsOf(src)!.map((t) => t.replace(/\*$/, "")));
        if (idv.blocks.some((b) => b.k === "swipe")) multi++;
        // each breakdown row is its rule's own: the pages (and the highlight boxes) its rule sheet carries
        for (const b of src.blocks) if (b.k === "sources" && !b.overruled) for (const g of b.groups) for (const row of g.rows)
          if (row.key) expect(row.book, `${at} ${row.key}`).toEqual((page.sheet(`rule:${row.key}`)!.blocks.find((x) => x.k === "pages") as { line: unknown }).line);
        // the breakdown's groups are authorities, in the one order
        const heads = src.blocks.flatMap((b, i) => (b.k === "sources" && src.blocks[i - 1]?.k === "head" ? b.groups.map((g) => g.where) : []));
        const order: string[] = [SOURCE_AUTHORITIES.synopsis.name, SOURCE_AUTHORITIES.dfo.name, ALL_SOURCES.LOCAL];
        expect(heads.every((h, i) => order.includes(h) && (i === 0 || order.indexOf(heads[i - 1]!) < order.indexOf(h))), `${at}: ${heads}`).toBe(true);
      }
    }
    expect([n > 50, multi > 0, bare > 0], `${n} lines, ${multi} with several fish, ${bare} with no picture`).toEqual([true, true, true]);
  });

  it("vedder_1010's Kokanee: its Sources tab ends with the breakdown, whole, then every regulation line", async () => {
    const page = await pageOf("vedder_1010"), r = page.keep.otherFish!.find((x) => x.name === "Kokanee")!;
    const vm = page.sheet(r.open)!;
    expect(breakdownText(vm)).toEqual(KOKANEE);
    const last = vm.blocks.at(-1) as Extract<SheetBlock, { k: "link" }>;
    expect(last.line.map((x) => ("open" in x ? x.open : null))).toEqual(["src:r" + /\d+/.exec(String((last.line[0] as { open: string }).open))![0]]);
  });

  it("every fish a page pictures: its picture key opens Identification, and its Sources tab opens (a card row's: `sp:<code>|src`)", async () => {
    const seen = { trout: 0, other: 0, card: 0, none: 0 };
    for (const id of Object.keys(taps().states)) {
      const page = await pageOf(id), codes = new Set<string>();
      // the pictures the page and its lines' sheets draw (a group card inside an Other fish sheet pictures its fish)
      const sheets = (page.keep.otherFish ?? []).map((r) => page.sheet(r.open));
      JSON.stringify([page.keep, sheets], (k, v) => { if (k === "open" && typeof v === "string" && /^sp:[A-Za-z0-9_]+$/.test(v)) codes.add(v.slice(3)); return v; });
      for (const c of codes) {
        const vm = page.sheet(`sp:${c}` as SheetKey), at = `${id} ${c}`;
        if (!vm?.tabs) { seen.none++; continue; }
        expect(vm.tabs.map((t) => t.on), at).toEqual([true, false]);
        const src = page.sheet(vm.tabs[1]!.open)!;
        // (review 1) the fish's own Sources key opens its line's Sources tab, whatever the line
        expect(page.sheet(`sp:${c}|src` as SheetKey)?.key, at).toBe(vm.tabs[1]!.open);
        // one sheet: its Sources tab names the same two tabs, Sources drawn
        expect(src.tabs?.map((t) => `${t.open}${t.on ? "*" : ""}`), at).toEqual([vm.tabs[0]!.open, `${vm.tabs[1]!.open}*`]);
        if (vm.tabs[1]!.open.endsWith("|src")) {
          seen.card++;
          expect([src.title, breakdownText(src).length > 0, (src.blocks.at(-1) as { k: string }).k], at).toEqual([vm.title, true, "link"]);
        } else if (vm.tabs[1]!.open.startsWith("tf:")) seen.trout++; else seen.other++;
      }
    }
    // the golden states picture their fish in the Other fish rows' sheets (no Keep card pictures one since the T810 cutover)
    expect([seen.other > 100, seen.none], JSON.stringify(seen)).toEqual([true, 0]);
  });

  it("a fish of a card drawn on the page (no trout table, so no Other fish list): `sp:<code>` | `sp:<code>|src`", async () => {
    const id = "vedder_1010", inp = { ...(await inputsOf(id))!, trout: null };
    const page = buildRegsPage(inp, { today: requestOf(id).today, who: ADULT_RESIDENT, pid: id });
    const vm = page.sheet("sp:KO")!, src = page.sheet("sp:KO|src")!;
    expect([tabsOf(vm), tabsOf(src)]).toEqual([[`${SPECIES_SHEET.ID}=sp:KO*`, `${SPECIES_SHEET.SRC}=sp:KO|src`],
      [`${SPECIES_SHEET.ID}=sp:KO`, `${SPECIES_SHEET.SRC}=sp:KO|src*`]]);
    // its breakdown is the Kokanee's own (its row's deciding rules that hold for KO), then the row's every line
    expect([src.title, breakdownText(src)]).toEqual([vm.title, KOKANEE]);
    const last = src.blocks.at(-1)!;
    expect(last.k === "link" && last.line[0]!.t === "link" ? /^src:r\d+$/.test(String(last.line[0]!.open)) : false).toBe(true);
    // mutation: a fish the page answers on no line has no Sources tab
    expect(page.sheet("sp:ZZ|src")).toBeNull();
  });

  it("(W46) a rule the flow names that the page does not hold: reported, the breakdown says the rest", async () => {
    const id = "vedder_515", inp = (await inputsOf(id))!;
    const base = buildRegsPage(inp, { today: requestOf(id).today, who: ADULT_RESIDENT, pid: id });
    const row = base.keep.otherFish!.find((x) => x.name === "Protected species")!;
    const lost = breakdownText(base.sheet(row.open)!).at(-1)!;   // a flow-only rule of the row (its title first)
    const gone = (base.sheet(row.open)!.blocks.find((b) => b.k === "sources") as Extract<SheetBlock, { k: "sources" }>).groups.flatMap((g) => g.rows).at(-1)!.key!;
    const rules = { ...inp.rules, byKey: (k: string) => (k === gone ? null : inp.rules.byKey!(k)) };
    const reports: unknown[] = [], undo = setFailureSink((e) => reports.push(e));
    try {
      const page = buildRegsPage({ ...inp, rules } as typeof inp, { today: requestOf(id).today, who: ADULT_RESIDENT, pid: id });
      const got = breakdownText(page.sheet(row.open)!);
      expect([got.includes(lost), reports.some((e) => (e as Error).name === "BreakdownRuleMissing")]).toEqual([false, true]);
    } finally { undo(); }
  });

  it("a stale species key opens nothing", async () => {
    const page = await pageOf("vedder_1010");
    for (const k of ["other:o99", "other:o99|id", "sp:ZZ|src", "tf:LT/wild|id"] as SheetKey[]) expect(page.sheet(k), k).toBeNull();
  });
});

/** vedder_1010's Kokanee (Region 2's stream release and the possession rule), as the store gives them */
const KOKANEE: readonly string[] = [
  "B.C. Synopsis",
  // a rule's own page link names the book as every one does (WORDS.BOOK; owner 2026-11-23: "B.C. Synopsis" everywhere)
  `Release kokanee from streams · Release every one · ${WORDS.BOOK} p.21 ${WORDS.LINK_OUT}`,
  `Possession: twice the daily quota · Possession limit · ${WORDS.BOOK} p.21 ${WORDS.LINK_OUT}`,
];
