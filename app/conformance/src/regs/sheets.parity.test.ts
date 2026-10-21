/**
 * T36 parity (B): every sheet a golden state can open, built by `page.sheet(appKeyOf(key))` over the state's inputs and
 * projected by `sheetLines`, against app/design/regs-v11/port/golden/golden.json (regs v38 @ 7fb6809f): title, sub, folds,
 * linked pages, body lines and nested keys. A per-state "@" is the global `golden.sheets[key]`.
 *
 * Built so far: rule:, gl:, pos:, hatch. The other kinds (src:, other:, srcs:, licsrc:, sp:, parts) are counted and named
 * in BUILT's test so a kind that lands without a parity run fails here, never passes silently.
 */
import { afterAll, describe, expect, it } from "vitest";
import { appKeyOf, goldenKeyOf, sheetLines, type SheetKey } from "@app/core";
import { eqPlain, type GoldenSheet } from "@app/core/regs/testing";
import { caseIds, golden, inputsOf, parityGate, requestOf, ADULT_RESIDENT, summary } from "./golden";
import { buildRegsPage } from "@app/core";
import { droppedLines, excluded, keepLines } from "./exclusions";

const G = golden();
const gate = await parityGate();
const ids = caseIds("sheets");

/** the sheet kinds T36 builds so far (the key's prefix) */
const BUILT = ["rule", "gl", "pos", "hatch"] as const;
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
  it("what is not built yet, by kind (golden per-state sheets): a kind that lands must join BUILT and this count", () => {
    const by: Record<string, number> = {};
    for (const id of Object.keys(G.states)) for (const [k] of sheetsOf(id)) if (!(BUILT as readonly string[]).includes(kindOf(k))) by[kindOf(k)] = (by[kindOf(k)] ?? 0) + 1;
    expect(by).toEqual({ licsrc: 642, other: 788, parts: 98, sp: 1274, src: 129, srcs: 659 });
  });
});

it(`T36 sheets parity runs against a paired regs store${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ""}`, () => {
  if (gate.fail) expect.fail(gate.fail);
});

/** the rule sheet's "Technical detail" line: `<key> <fields JSON>` — compared by structure (exclusions: JSON spacing/order) */
const techSplit = (line: string, key: string): unknown => JSON.parse(line.slice(key.length + 1));

describe.skipIf(gate.skip)("B. every built sheet of every state equals the golden", () => {
  let equal = 0, compared = 0;
  const byKind: Record<string, number> = {};
  afterAll(() => console.info(`${summary("sheets", equal, ids.length)} · ${compared} sheets (${JSON.stringify(byKind)})`));
  it.each(ids)("%s", async (id) => {
    const inp = (await inputsOf(id))!, page = buildRegsPage(inp, { today: requestOf(id).today, who: ADULT_RESIDENT, pid: id });
    for (const [gk, want] of sheetsOf(id)) {
      if (!(BUILT as readonly string[]).includes(kindOf(gk))) continue;
      const key = appKeyOf(gk, id), vm = page.sheet(key);
      expect(vm, `${id} ${gk}: no sheet`).not.toBeNull();
      const got = sheetLines(vm!);
      let gotLines = got.lines, wantLines = keepLines(want.lines, droppedLines("sheets", id, gk));
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
      compared++; byKind[kindOf(gk)] = (byKind[kindOf(gk)] ?? 0) + 1;
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
