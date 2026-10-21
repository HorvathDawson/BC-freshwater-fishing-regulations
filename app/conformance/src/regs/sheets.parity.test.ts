/**
 * T36 parity (B): every sheet a golden state can open, built by `page.sheet(appKeyOf(key))` over the state's inputs and
 * projected by `sheetLines`, against app/design/regs-v11/port/golden/golden.json (regs v38 @ 7fb6809f): title, sub, folds,
 * linked pages, body lines and nested keys. A per-state "@" is the global `golden.sheets[key]`.
 *
 * Built: every kind but `parts` (rule:, gl:, pos:, hatch, srcs:, licsrc:, src:, sp:, other:). `parts` is a PIPELINE NEED
 * (each part's status on the day) — `page.sheet("parts")` is null and the 98 golden parts sheets are counted, not compared.
 * The keyed exclusions (exclusions.ts) and the whole-line substitutions (SHEET_SUBST) are counted in the summary line.
 * An Other fish row drawn as a Keep group is worded by the Keep tab's own projection (keepPage.ts `groupLines`).
 */
import { afterAll, describe, expect, it } from "vitest";
import { appKeyOf, goldenKeyOf, sheetLines, type SheetKey } from "@app/core";
import { eqPlain, type GoldenSheet } from "@app/core/regs/testing";
import { caseIds, golden, inputsOf, parityGate, requestOf, ADULT_RESIDENT, summary } from "./golden";
import { buildRegsPage } from "@app/core";
import { droppedLines, excluded, keepLines, LEGACY_POOL_SHEETS, SHEET_SUBST } from "./exclusions";
import { groupLines } from "./keepPage";

const G = golden();
const gate = await parityGate();
const ids = caseIds("sheets");

/** the sheet kinds T36 builds (the key's prefix); `parts` waits for the pipeline */
const BUILT = ["rule", "gl", "pos", "hatch", "srcs", "licsrc", "src", "sp", "other"] as const;
const kindOf = (k: string) => k.split(/[:|]/)[0]!;
const sheetsOf = (id: string): [string, GoldenSheet][] =>
  Object.entries(G.states[id]!.sheets as Record<string, GoldenSheet | "@">).map(([k, v]) => [k, v === "@" ? G.sheets[k] as GoldenSheet : v]);

describe("A. sheet keys: golden ⇄ app", () => {
  it("every per-state key of every state round-trips through appKeyOf / goldenKeyOf", () => {
    const shapes = new Set<string>(); let n = 0;
    for (const id of Object.keys(G.states)) for (const [k] of sheetsOf(id)) {
      const a = appKeyOf(k, id);
      expect(goldenKeyOf(a, id), `${id} ${k}`).toBe(k);
      expect(a.includes(id), `${id} ${k} → ${a}`).toBe(false);
      shapes.add(k.replace(id, "SID").replace(/^(rule|gl|sp):.*/, "$1:*").replace(/\|o\d+$/, "|oN").replace(/\|r\d+$/, "|rN")); n++;
    }
    expect(n).toBe(7184);
    expect([...shapes].sort()).toEqual(["gl:*", "hatch", "licsrc:lic|SID|adult", "licsrc:lic|SID|adult_alien", "licsrc:lic|SID|adult_non",
      "licsrc:lic|SID|u16_alien", "licsrc:lic|SID|u16_non", "licsrc:lic|SID|u16_res", "other:SID|oN", "parts", "pos:SID|pos", "rule:*",
      "sp:*", "src:SID|rN", "srcs:always|SID", "srcs:bait|SID", "srcs:boats|SID", "srcs:gl|SID|hook", "srcs:gl|SID|lines",
      "srcs:gl|SID|mode", "srcs:gl|SID|weight", "srcs:ways|SID"].filter((s) => shapes.has(s)));
  });
  it("what is not built, by kind (golden per-state sheets): only `parts` (a pipeline need); a kind that lands must join BUILT", () => {
    const by: Record<string, number> = {};
    for (const id of Object.keys(G.states)) for (const [k] of sheetsOf(id)) if (!(BUILT as readonly string[]).includes(kindOf(k))) by[kindOf(k)] = (by[kindOf(k)] ?? 0) + 1;
    expect(by).toEqual({ parts: 98 });
  });
  it("the legacy pooled rows (D19) and the substitutions are named golden keys", () => {
    for (const x of LEGACY_POOL_SHEETS) { const [id, k] = x.split(" "); expect(G.states[id!]!.sheets, x).toHaveProperty([k!]); }
    for (const s of SHEET_SUBST) expect(s.ids.some((id) => Object.keys(G.states[id]!.sheets).some((k) => s.key.test(k))), s.from).toBe(true);
  });
});

it(`T36 sheets parity runs against a paired regs store${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ""}`, () => {
  if (gate.fail) expect.fail(gate.fail);
});

/** the rule sheet's "Technical detail" line: `<key> <fields JSON>` — compared by structure (exclusions: JSON spacing/order) */
const techSplit = (line: string, key: string): unknown => JSON.parse(line.slice(key.length + 1));

describe.skipIf(gate.skip)("C. the part 2 sheets over the store, whole strings (T36 tests 11–14)", () => {
  const pageOf = async (id: string) => buildRegsPage((await inputsOf(id))!, { today: requestOf(id).today, who: ADULT_RESIDENT, pid: id });
  it("11. src:r0 (vedder_1010): titled rows, grouped by whose, deciding first; overruled folded; every row title the rule's own title", async () => {
    const vm = (await pageOf("vedder_1010")).sheet("src:r0")!, l = sheetLines(vm);
    expect([l.title, l.sub, l.lines.slice(0, 4), l.folds]).toEqual(["Trout and char",
      "Vedder River · Oct 10. The more specific rule speaks first: this water, then from downstream, a named area, the region, then the province.",
      ["These decide it (12)", "Region 2 · region-wide", "Keep up to 4 trout and char a day Trout/char: 4", "Sets the number Rule book p.21 ↗"],
      [{ summary: "Overruled (4)", open: false }]]);
    const rows = vm.blocks.flatMap((b) => (b.k === "sources" ? b.groups.flatMap((g) => g.rows) : []));
    expect(rows.length).toBe(12);
    expect(rows.filter((r) => r.title?.startsWith("ALL STEELHEAD"))).toEqual([]);
  });
  it("12. other:o0 (vedder_1010): the release row — pictures, value, sentence, How this was decided, the rules that don't apply folded", async () => {
    const l = sheetLines((await pageOf("vedder_1010")).sheet("other:o0")!, { groupLines });
    expect([l.title, l.lines.slice(0, 8), l.folds, l.lines.at(-1)!.endsWith("See every regulation line ›")]).toEqual(["Kokanee",
      ["Kokanee ›", "↩ Release ⓘ", "You can fish for them, but release every one. No keeping at any time of year. The whole water is closed Jun 1–30.",
        "How this was decided", "What you can keep", "✓ Release every one here.", "Region 2",
        "In streams, release every kokanee. Release From the Region 2 table — all of Region 2. Rule book p.21 ↗"],
      [{ summary: "Rules that don’t apply here (1)", open: false }], true]);
  });
  it("13/14. other:o0 (klwa_1010): the weekday story first, in words; the day-parts and the restatement are the deciding step's lines, once", async () => {
    const vm = (await pageOf("klwa_1010")).sheet("other:o0")!, l = sheetLines(vm, { groupLines });
    expect(l.lines.slice(0, 4)).toEqual(["Kokanee ›", "Today · Sat This water", "Weekends: keep 5 · Weekdays: release",
      "Keep up to 5 on Saturdays and Sundays; release Monday to Friday."]);
    const kinds = vm.blocks.map((b) => b.k);
    expect(kinds.indexOf("weekBox")).toBeLessThan(kinds.indexOf("head"));
    const head = l.lines.indexOf("How this was decided");
    for (const x of l.lines.slice(0, head)) {
      expect(x, "week-in-words").not.toMatch(/\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d/);
      expect(x, "week-sheet-one-story").not.toMatch(/Release every one here|\d+ a day here|\buntil\b/);
    }
    expect(l.lines).toContain("Sat–Sun Keep up to 5 Today This water’s own row in the 2025–2027 BC Freshwater Fishing Regulations Synopsis: Kootenay Lake — Lower West Arm. Rule book p.37 ↗");
    expect(l.lines.some((x) => x.startsWith("Mon–Fri Release every one This water’s own row"))).toBe(true);
    expect(l.lines.filter((x) => x.startsWith("Kokanee from the Lower and Upper West Arms together: no more than 5 a day.")).length).toBe(1);
    expect(l.lines.some((x) => /combined daily quota/i.test(x))).toBe(false);
  });
});

describe.skipIf(gate.skip)("B. every built sheet of every state equals the golden", () => {
  let equal = 0, compared = 0, substituted = 0;
  const byKind: Record<string, number> = {}, skipped: Record<string, number> = {};
  afterAll(() => console.info(`${summary("sheets", equal, ids.length)} · ${compared} sheets (${JSON.stringify(byKind)}) · not compared ${JSON.stringify(skipped)} · ${substituted} golden lines substituted (SHEET_SUBST)`));
  it.each(ids)("%s", async (id) => {
    const inp = (await inputsOf(id))!, page = buildRegsPage(inp, { today: requestOf(id).today, who: ADULT_RESIDENT, pid: id });
    for (const [gk, want] of sheetsOf(id)) {
      const kind = kindOf(gk);
      if (!(BUILT as readonly string[]).includes(kind)) {
        expect(page.sheet(appKeyOf(gk, id)), `${id} ${gk}: a pipeline need, not built`).toBeNull();
        skipped[kind] = (skipped[kind] ?? 0) + 1; continue;
      }
      const key = appKeyOf(gk, id), vm = page.sheet(key);
      expect(vm, `${id} ${gk}: no sheet`).not.toBeNull();
      if (LEGACY_POOL_SHEETS.includes(`${id} ${gk}`)) {
        expect(excluded("sheets", id, gk)?.reason, `${id} ${gk}`).toMatch(/^decisions D19: /);
        skipped.legacyPool = (skipped.legacyPool ?? 0) + 1; continue;
      }
      const got = sheetLines(vm!, { groupLines });
      let gotLines = got.lines, wantLines = keepLines(want.lines, droppedLines("sheets", id, gk));
      for (const s of SHEET_SUBST.filter((x) => x.ids.includes(id) && x.key.test(gk))) {
        expect(wantLines.filter((l) => l.startsWith(s.from)).length, `${id} ${gk}: substitution ${s.from}`).toBe(1);
        wantLines = wantLines.map((l) => (l.startsWith(s.from) ? s.to + l.slice(s.from.length) : l)); substituted++;
      }
      if (gk.startsWith("rule:") && excluded("sheets", id, gk)) {
        const rk = gk.slice(5), at = (ls: readonly string[]) => ls.findIndex((l) => l.startsWith(`${rk} {`) || l === `${rk} null`);
        const gi = at(gotLines), wi = at(wantLines);
        expect([gi >= 0, wi >= 0], `${id} ${gk}: technical line`).toEqual([true, true]);
        expect(techSplit(gotLines[gi]!, rk), `${id} ${gk}: fields`).toEqual(techSplit(wantLines[wi]!, rk));
        gotLines = gotLines.filter((_, i) => i !== gi); wantLines = wantLines.filter((_, i) => i !== wi);
      }
      // (T36) the gl: and hatch pages are compared whole — printed number, PDF page, the ↗ — over the release's own
      // printed → PDF map (static export.pdf_pages); no page is drawn unlinked
      expect({ title: got.title, sub: got.sub || null, folds: got.folds, pages: got.pages }, `${id} ${gk}`)
        .toEqual({ title: want.title, sub: want.sub || null, folds: want.folds, pages: want.pages });
      expect(eqPlain(gotLines, wantLines), `${id} ${gk}`).toEqual({ ok: true });
      expect(got.nested.map((k) => goldenKeyOf(k as SheetKey, id)), `${id} ${gk} nested`).toEqual(want.nested);
      compared++; byKind[kind] = (byKind[kind] ?? 0) + 1;
    }
    equal++;
  });
});

describe.skipIf(gate.skip)("C. (T36) the release's printed → PDF page map", () => {
  it("vedder_1010: the glossary pages link through the stored map (PDF − 2 to p.40, PDF − 6 after); an empty map would draw them unlinked (mutation)", async () => {
    const inp = (await inputsOf("vedder_1010"))!, r = requestOf("vedder_1010");
    expect([inp.statics.pdfPages[4], inp.statics.pdfPages[40], inp.statics.pdfPages[41], inp.statics.pdfPages[80]]).toEqual([6, 42, 47, 86]);
    const pagesOf = (i: typeof inp) => sheetLines(buildRegsPage(i, { today: r.today, who: ADULT_RESIDENT, pid: "vedder_1010" }).sheet("gl:catch_and_release")!).pages;
    expect(pagesOf(inp)).toEqual([{ text: "p.4 ↗", pdf: 6 }]);
    expect(pagesOf({ ...inp, statics: { ...inp.statics, pdfPages: {} } })).toEqual([]);   // unlinked: no PDF page to open
  });
});
