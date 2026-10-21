/**
 * T15b parity: the top caveat blocks — core `caveatsOf` over each golden state's inputs (T12 `cases("keep")`, the live
 * release paired with the oracles) — against golden.json `keep.order` (which blocks, in order) and `keep.lines` (each
 * block's text), regs v38 @ 7fb6809f. The golden has no field per caveat block, so (tests.md / the ticket): the block's
 * selector sequence must equal the golden's caveat selectors, and the blocks' lines must sit in golden `keep.lines` right
 * after the strip (contiguous, in order). The ticket's literals are asserted beside it as whole strings.
 */
import { afterAll, describe, expect, it } from "vitest";
import { caveatsOf, makePlace, plain, settle, settleAt, type CaveatVM, type RegsInputs } from "@app/core";
import { WORDS } from "@app/core";
import { eqPlain } from "./eq";
import { cases } from "./golden";
import { caveatLines } from "./keepPage";

interface GKeep { order: string[]; lines: string[] }

const CAVEAT_SEL = /^(details\.inpart|div\.caveats)/;
const selOf = (c: CaveatVM) => c.kind === "inpart" ? (c.hot ? "details.inpart.hot" : "details.inpart")
  : c.kind === "uncertain" ? "div.caveats.top.check" : "div.caveats.top";
/** The block's golden text form: keepPage.ts `caveatLines`. */
const linesOf = caveatLines;

const blocks = (inp: RegsInputs): readonly CaveatVM[] => {
  const place = makePlace(inp), s = settle(inp, place);
  return caveatsOf(inp, s, (j) => settleAt(inp, place, j, s.day));
};

let passed = 0, total = 0;
afterAll(() => { if (total) console.info(`keep T15b (caveat blocks) parity: ${passed}/${total} states equal`); });

describe("T15b caveat blocks vs golden keep.order / keep.lines (every state)", () => {
  it("134/134 states: the same blocks in the same order, and their lines contiguous in keep.lines", async () => {
    const bad: string[] = [];
    let drawn = 0;
    for await (const c of cases("keep")) {
      total++;
      const g = c.g.keep as GKeep, got = blocks(await c.inputs());
      const sel = eqPlain(got.map(selOf), g.order.filter((o) => CAVEAT_SEL.test(o)));
      const want = got.flatMap(linesOf);
      const at = want.length ? g.lines.indexOf(want[0]!) : -1;
      const text = want.length ? eqPlain(want, at < 0 ? [] : g.lines.slice(at, at + want.length)) : { ok: true as const };
      if (sel.ok && text.ok) passed++; else bad.push(`${c.id}\n${sel.ok ? "" : sel.diff}\n${text.ok ? "" : text.diff}`);
      if (got.length) drawn++;
    }
    expect(bad).toEqual([]);
    expect([total, drawn]).toEqual([134, 9]);
  });

  const at = async (id: string) => { for await (const c of cases("keep")) if (c.id === id) return blocks(await c.inputs()); return null; };

  it("1. x4p_1010: in-part, hot, 2 spots in force today and 3 other dates", async () => {
    const b = await at("x4p_1010");
    if (!b) return;
    expect(b.map(linesOf)).toEqual([[
      "ⓘFishing is different in some spots today", "Everywhere else, follow this page.",
      "Rainbow trout and Char — release all · May 15–Mar 14 Where: Waters lying west of a line between signs at Henstridge Road and Wharf Road to a line between signs on the south and north shores of Little Shuswap Lake (map A in the regulations) Rule book p.32 ↗",
      "No fishing · Sep 1–Dec 31 Where: Salmon Arm Bay, west of line between Engineer's Point and Sunnybrae Point Rule book p.32 ↗",
      "Other dates (3)",
      "No fishing · Mar 15–May 14 Where: Waters lying west of a line between signs at Henstridge Road and Wharf Road to a line between signs on the south and north shores of Little Shuswap Lake (map A in the regulations) Rule book p.32 ↗",
      "No fishing · Mar 15–May 14 Where: Entire area north of Albas (map B in the regulations) Rule book p.32 ↗",
      "No fishing · Mar 15–May 14 Where: East of a line between fishing boundary signs on Murdock and Semaphore points, to Hwy 1 bridge (map C in the regulations) Rule book p.32 ↗",
    ]]);
    expect([b[0]!.kind, b[0]!.hot]).toEqual(["inpart", true]);
  });

  it("2. x14p2_1010: one side of the channel, Rule book p.51", async () => {
    const b = await at("x14p2_1010");
    if (!b) return;
    expect(b.map(linesOf)).toEqual([["⚠ On one side of the channel only",
      "No fishing West half of the channel only — on the east half, this water's other regulations apply · between fishing boundary signs near Kitimat Hatchery outfall Rule book p.51 ↗"]]);
  });

  it("3. x15p2_1010: at certain times — the night closure of the other moment", async () => {
    const b = await at("x15p2_1010");
    if (!b) return;
    expect(b.map(linesOf)).toEqual([["⚠ At certain times", "No fishing for any game fish 60 min after sunset–60 min before sunrise Rule book p.23 ↗"]]);
  });

  it("4–5. the angler closures (x17p3/4/7, x19p6/7/4)", async () => {
    const nilk = "Angling closed to non-guided non-resident aliens, Sep 1-Oct 31, on Saturdays and Sundays — between Nilkitkwa Lake and the Nilkitkwa River confluence Rule book p.50 ↗";
    for (const id of ["x17p3_1010", "x17p4_1010", "x17p7_1010"]) {
      const b = await at(id);
      if (b) expect(b.map(linesOf), id).toEqual([["⚠ Closed to some anglers", nilk]]);
    }
    const b6 = await at("x19p6_1010");
    if (b6) expect(b6.map(linesOf)).toEqual([["⚠ Closed to some anglers",
      "Angling closed to non-guided non-resident aliens, Jul 1-Dec 31, on Saturdays and Sundays — between the Chimdemash Creek confluence and the Zymoetz River confluence (1500 m upstream) Rule book p.53 ↗"]]);
    for (const id of ["x19p7_1010", "x19p4_1010"]) {
      const b = await at(id);
      if (b) expect(b.map(linesOf), id).toEqual([["⚠ Closed to some anglers",
        "Angling closed to non-guided non-resident aliens, Jul 1-Dec 31, on Saturdays and Sundays — between the Shegunia River confluence and the Sedan Creek confluence Rule book p.53 ↗"]]);
    }
  });
});
