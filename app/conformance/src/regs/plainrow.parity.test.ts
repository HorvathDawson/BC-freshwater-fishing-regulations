/**
 * T19b parity: the plain rows — core `plainRowOf` (+ decided.ts's ladder) over each golden state's inputs (T12
 * `cases("keep")`, the live release paired with the oracles) — against golden.json `keep.plainRows` (one string per block
 * element of each `article.row` that is not a g28 card), regs v38 @ 7fb6809f, on every state.
 *
 * Which rows: page v36's main rows (rowsHtml's trout-and-char split, T19 splitRows) that are plain (`isPlainRow`: no pool,
 * no group headline). klmb_1010's one pooled no-headline row (drawn through page v36's legacy card path, D19) is not a
 * plain row of this port and its golden entry is dropped by its reason (decisions D19), never compared.
 *
 * The sibling functions (T13 runOf, T17 rowTitle, T18 pictures, T19 MORE / splitRows) are the merged ones, bound once by
 * T21 (`plainRowSeams`, core keep.ts); the stand-ins this file used before they merged are gone.
 */
import { afterAll, describe, expect, it } from "vitest";
import {
  buildModel, FISH, isPlainRow, makePlace, plain, plainRowOf, plainRowSeams, settle, splitRows, WORDS, type PlainRowVM, type RegsInputs,
} from "@app/core";
import { eqPlain, norm } from "./eq";
import { cases } from "./golden";
import { plainRowLines } from "./keepPage";

/** The golden text form of a plain row: keepPage.ts `plainRowLines` (one string per block element). */
const linesOf = plainRowLines;

const rowsOf = (inp: RegsInputs): PlainRowVM[] => {
  const place = makePlace(inp), settled = settle(inp, place), card = buildModel(inp, place, settled), seams = plainRowSeams(inp, FISH);
  if (settled.status?.status === "closed" || settled.status?.status === "tidal" || !settled.status) return [];
  return splitRows(card).main.filter((i) => isPlainRow(card.rows[i]!)).map((i) => plainRowOf(i, { inp, card, settled, seams }));
};
/** D19: klmb_1010's pooled row is drawn by page v36's legacy card path — not this port's plain row. */
const D19 = (id: string, g: string[][]) => (id === "klmb_1010" ? g.filter((r) => !/^Rainbow troutSeparate from/.test(r[0]!)) : g);

let passed = 0, total = 0, rows = 0;
afterAll(() => { if (total) console.info(`keep T19b (plain rows) parity: ${passed}/${total} states equal, ${rows} non-pool plain rows · klmb_1010's pooled row not compared (D19)`); });

describe("T19b plain rows vs golden keep.plainRows (every state)", () => {
  // (T115) the 4 states whose part the release merged away (x15p14/15/17/18, taps.json `gone`) are not cases: their 4 plain rows go with them;
  // (T183) and the West Road's x27p1 (its Region 6 stretch shares part 0's rules now, taps.json `gone`) neither (no plain row)
  it("126/126 states: every non-pool plain row's lines equal the golden", async () => {
    const bad: string[] = [];
    for await (const c of cases("keep")) {
      total++;
      const want = D19(c.id, (c.g.keep as { plainRows: string[][] }).plainRows ?? []), got = rowsOf(await c.inputs()).map(linesOf);
      rows += got.length;
      const eq = eqPlain(got.map((r) => JSON.stringify(r)), want.map((r) => JSON.stringify(r)));
      const each = got.length === want.length ? got.map((r, i) => eqPlain(r, want[i]!)).filter((e) => !e.ok) : [];
      if (eq.ok) passed++; else bad.push(`${c.id}: ${got.length} rows vs ${want.length}\n${each.map((e) => ("diff" in e ? e.diff : "")).join("\n")}`);
    }
    expect(bad).toEqual([]);
    expect([total, rows]).toEqual([126, 27]);   // (2026-10-27) x18p3 gone: the Columbia's Mud Lake speed limit split its golden part
  });

  const at = async (id: string) => { for await (const c of cases("keep")) if (c.id === id) return rowsOf(await c.inputs()); return null; };

  it("1. cow1_1010: the char row, whole", async () => {
    const r = await at("cow1_1010");
    if (r) expect(r.map((x) => linesOf(x).map(norm))).toEqual([["Char ↩ Release ⓘ",
      "You can fish for them, but release every one. No keeping at any time of year. The whole water is closed Jul 4–Aug 31.",
      "Brook trout · Dolly Varden/bull trout · Lake trout Brook trout › Bull trout › Lake trout ›", "How this was decided", "What you can keep",
      "✓ Release every one here.", "Region 1, excluding Haida Gwaii", "Release every char. Release Rule book p.13 ↗", "Other rules", "Region 1",
      "Possession: 2 days’ quota. Fish at home don’t count. Carry limit Rule book p.13 ↗ See every regulation line ›"]]);
  });

  it("2. iron_720: the closure run, and the one rule that doesn't apply", async () => {
    const r = await at("iron_720");
    if (!r) return;
    expect(r[0]!.sum).toBe("You can fish for them, but release every one. No keeping at any time of year. The whole water is closed Dec 1–Jun 30.");
    expect(r[0]!.ladder.lost.map((l) => [l.text, l.status, plain(l.book)].join(" "))).toEqual(["In streams, no fishing, Jul 15–Aug 31. Replaced by this river’s rule Rule book p.13 ↗"]);
  });

  it("3–4. x15p8_1010: the trout and char row (closed until Nov 15) and the steelhead row (presence line)", async () => {
    const r = await at("x15p8_1010");
    if (!r) return;
    const tc = r.find((x) => /^Brook trout/.test(x.title))!, st = r.find((x) => x.title === "Steelhead")!;
    expect([tc.title + " " + tc.pill!.text, tc.sum, tc.ladder.sections[0]!.steps[0]!.level,
      [plain(tc.ladder.sections[0]!.steps[0]!.lines[0]!.text), tc.ladder.sections[0]!.steps[0]!.lines[0]!.tag].join(" "), WORDS.RULES_NOT_APPLY(tc.ladder.lost.length)]).toEqual([
      "Brook trout, brown trout, cutthroat trout, Dolly Varden/bull trout, lake trout and rainbow trout ⊘ Closed",
      "Don’t fish for them until Nov 15. If one bites, carefully release it. You can keep them from Nov 16.",
      "Fraser River", "No fishing for trout and char, Sep 1–Nov 15. Closed", "Rules that don’t apply here (5)"]);
    expect([plain(st.presence), st.sum]).toEqual(["A rainbow trout over 50 cm (20 in) here counts as a steelhead. ⓘ",
      "Don’t fish for them. If one bites, carefully release it. Closed Aug 15–Jun 30."]);
  });

  it("5. x16p1_1010: release all year, no closure run", async () => {
    const r = await at("x16p1_1010");
    if (r) expect(r.map((x) => x.sum)).toEqual(["You can fish for them, but release every one. No keeping at any time of year."]);
  });
});
