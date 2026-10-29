/**
 * T14 parity: the closed / tidal banner — `bannerOf(inp, settle(…), stripOf(…))` — against the golden
 * (app/fixtures/golden/golden.json, page v38 @ 7fb6809f) `keep.banner` on every golden state, inputs from T09
 * `inputsOf` over the harness's paired store (REGS_STORE_REQUIRED=1 turns a skip into a failure).
 *
 * Every expected value is the golden's except the ticket's literals (page v38's output, re-read from the golden at v38;
 * v38 keeps "No fishing for any game fish." whole on a closed-all-year chain — integration 1, the T14 "." bug is fixed).
 * The reopening day is checked against T13 `nextOpen` (the golden's golink day is null: the prototype keeps it as data-md).
 */
import { afterAll, describe, expect, it } from "vitest";
import { bannerOf, buildModel, makePlace, nextOpen, plain, settle, stripOf, type BannerVM } from "@app/core";
import { eqPlain, norm } from "./eq";
import { caseIds, golden, inputsOf, parityGate, requestOf, summary } from "./golden";

interface GBanner { big: string[]; paras: string[]; golink: { text: string; day: null }[]; chain: { text: string; scope: string; src: string }[]; lines: string[] }
const G = golden();
const gbanner = (id: string) => (G.states[id]!.keep as { banner: GBanner | null } | null)?.banner ?? null;
const gate = await parityGate();
const ids = caseIds("keep");

async function bannerFor(id: string): Promise<BannerVM | null> {
  const inp = (await inputsOf(id))!, place = makePlace(inp), s = settle(inp, place);
  return bannerOf(inp, s, stripOf(inp, s, buildModel(inp, place, s), requestOf(id).today));
}
/** The merged list as the golden's lines tail: the label, then each item "<text> <level> <book>". */
const mergedLines = (m: NonNullable<BannerVM["merged"]>) =>
  [m.label, ...m.items.map((x) => [x.text, x.level, plain(x.book)].filter(Boolean).join(" "))];

it(`T14 banner parity runs against a paired regs store${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ""}`, () => {
  if (gate.fail) expect.fail(gate.fail);
  if ("reason" in gate) console.warn(`T14 banner parity skipped: ${gate.reason}`);
});

describe.skipIf(gate.skip)("T14 banner vs golden keep.banner", () => {
  let passed = 0, drawn = 0;
  afterAll(() => console.info(`${summary("keep", passed, ids.length).replace(/^keep/, "keep.banner")} · ${drawn} with a banner`));

  it.each(ids)("1. %s: big, paras, chain (text + level + Rule book), link, merged list", async (id) => {
    const v = await bannerFor(id), g = gbanner(id);
    if (g === null) { expect(v).toBeNull(); passed++; return; }
    drawn++;
    expect(v).not.toBeNull();
    expect({ big: v!.big, paras: v!.paras, chain: v!.chain.map((c) => ({ text: `${c.text} ${c.scope}`, scope: c.scope })),
             golink: v!.reopen ? [v!.reopen.text] : [] })
      .toEqual({ big: g.big, paras: g.paras, chain: g.chain.map((c) => ({ text: c.text, scope: c.scope })), golink: g.golink.map((l) => l.text) });
    // each chain line of the golden, in order after the headline and the paragraphs: "<text> <level> Source <Rule book …>"
    v!.chain.forEach((c, k) => {
      const line = norm(g.lines[g.big.length + g.paras.length + k]!);
      expect(line.startsWith(norm(`${c.text} ${c.scope} Source ${plain(c.book)}`)), `${line} ← ${plain(c.book)}`).toBe(true);
    });
    if (v!.reopen) expect(v!.reopen.day).toEqual(nextOpen((await inputsOf(id))!, requestOf(id).day));
    const tail = v!.merged ? mergedLines(v!.merged) : [];
    const r = eqPlain(tail, g.lines.slice(g.lines.length - tail.length));
    expect(r.ok, r.ok ? "" : r.diff).toBe(true);
    if (!v!.merged) expect(g.lines.some((l) => /^Closed all year on /.test(l))).toBe(false);
    passed++;
  });

  it("1. 23 tapped golden states draw a banner (27 less T67's two and T220's two gone closed states)", () => {
    expect(ids.filter((id) => gbanner(id) !== null).length).toBe(23);   // (T220) x13p2 + x9p2 gone: restored braid pieces renumbered the Dean and grew the Kootenay's closed parts
  });

  it("2. vedder_615: the strip said 'Closed' and 'Opens Jul 1', so the headline goes; the link keeps its own words", async () => {
    const v = (await bannerFor("vedder_615"))!;
    expect([v.kind, v.big, v.paras]).toEqual(["closed", [], ["No fishing for any game fish Jun 1–30."]]);
    expect(v.chain.map((c) => [c.text, c.scope, plain(c.book)])).toEqual([["Jun 1–30.", "This water", "Rule book p.22 ↗"]]);
    expect(v.reopen).toEqual({ text: "See the rules from then ›", day: { month: 7, day: 1 } });
    expect(v.merged).toBeNull();
  });

  it("3. cow0_1010: four closures cover Jul 4–Nov 15; reopens Nov 16", async () => {
    const v = (await bannerFor("cow0_1010"))!;
    expect(v.paras).toEqual(["No fishing for any game fish Jul 4–Nov 15. Together these closures cover it:"]);
    expect(v.chain.map((c) => [c.text, c.scope, plain(c.book)])).toEqual([
      ["Jul 4–14.", "This water", "Rule book p.15 ↗"], ["Jul 15–Aug 31.", "This water", "Rule book p.15 ↗"],
      ["From streams, Jul 15–Aug 31.", "Named area", "Rule book p.13 ↗"], ["Sep 1–Nov 15.", "This water", "Rule book p.15 ↗"]]);
    expect(v.reopen?.day).toEqual({ month: 11, day: 16 });
  });

  it("4. chw0_1010: closed all year — the game-fish sentence kept whole; two stretches, each with its closure", async () => {
    const v = (await bannerFor("chw0_1010"))!;
    expect([v.big, v.paras, v.reopen, v.chain.map((c) => [c.text, c.scope])])
      .toEqual([["Closed all year"], [], null, [["No fishing for any game fish.", "This water"]]]);
    expect(v.merged!.label).toBe("Closed all year on 2 stretches (8 sections)");
    expect(v.merged!.items.map((x) => [x.text.replace(/^.* · /, ""), x.level, plain(x.book)]))
      .toEqual([["7 sections", "This water", "Rule book p.22 ↗"], ["1 section", "Federal / parks", "Rule book p.9 ↗"]]);
  });

  it("5. x31p_1010: the tidal banner, in the data's words", async () => {
    expect(await bannerFor("x31p_1010")).toEqual({ kind: "tidal", big: ["Tidal water"], paras: ["Tidal water: a different regulation system. The B.C. freshwater fishing regulations do not apply here: no provincial rule, quota, closure, gear rule, licence or stamp. See the federal (DFO) tidal waters sport fishing regulations or the Fishing BC app; a federal Tidal Waters Sport Fishing Licence is required."],
      chain: [], reopen: null, merged: null });
  });
});
