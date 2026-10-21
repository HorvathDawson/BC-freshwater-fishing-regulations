/**
 * T36 parity (B): every sheet a golden state can open, built by `page.sheet(appKeyOf(key))` over the state's inputs and
 * projected by `sheetLines`, against app/design/regs-v11/port/golden/golden.json (regs v38 @ 7fb6809f): title, sub, folds,
 * linked pages, body lines and nested keys. A per-state "@" is the global `golden.sheets[key]`.
 *
 * Built so far: rule:, gl:, pos:, hatch. The other kinds (src:, other:, srcs:, licsrc:, sp:, parts) are counted and named
 * in BUILT's test so a kind that lands without a parity run fails here, never passes silently.
 */
import { afterAll, describe, expect, it } from "vitest";
import { appKeyOf, goldenKeyOf, sheetLines, type Line, type SheetBlock, type SheetKey } from "@app/core";
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

/** every book run's printed page in a sheet's blocks, linked or not (the glossary pages while the release has no PDF map) */
function printedOf(bs: readonly SheetBlock[], out: number[] = []): number[] {
  for (const b of bs) {
    const ls: readonly Line[] = b.k === "para" || b.k === "example" ? [b.text] : b.k === "pages" || b.k === "source" || b.k === "link" ? [b.line]
      : b.k === "list" ? b.items : [];
    for (const l of ls) for (const r of l) if (r.t === "book") out.push(r.printed);
    if (b.k === "fold" || b.k === "term") printedOf(b.body, out);
  }
  return out;
}

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
      let gotPages: unknown = got.pages, wantPages: unknown = want.pages;
      if (/^(gl:|hatch$)/.test(gk) && excluded("sheets", id, gk)?.reason.startsWith("PIPELINE NEED: the release's statics")) {
        // no printed → PDF map in the release: the pages are drawn unlinked; compare the PRINTED numbers and the line sans ↗
        expect(Object.keys(inp.statics.pdfPages), "the map shipped: drop the exclusion").toEqual([]);
        gotPages = printedOf(vm!.blocks);
        wantPages = want.pages.map((p) => +/p\.(\d+)/.exec(p.text)![1]!);
        wantLines = wantLines.map((l) => (/^Rule book( p\.\d+ ↗)+$/.test(l) ? l.replace(/ ↗/g, "") : l));
      }
      expect({ title: got.title, sub: got.sub || null, folds: got.folds, pages: gotPages }, `${id} ${gk}`)
        .toEqual({ title: want.title, sub: want.sub || null, folds: want.folds, pages: wantPages });
      expect(eqPlain(gotLines, wantLines), `${id} ${gk}`).toEqual({ ok: true });
      expect(got.nested.map((k) => goldenKeyOf(k as SheetKey, id)), `${id} ${gk} nested`).toEqual(want.nested);
      compared++; byKind[kindOf(gk)] = (byKind[kindOf(gk)] ?? 0) + 1;
    }
    equal++;
  });
});
