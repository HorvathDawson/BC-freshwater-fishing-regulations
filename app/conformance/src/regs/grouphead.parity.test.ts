/**
 * T17 parity: every golden g28 group's head — title, scope kicker, the number line (form a, the owner's r20h pick), the
 * origin block — built by core `headCtx`/`groupHeadOf`/`originOf` over the state's inputs (T12 `inputsOf`, the paired
 * release) and projected to the golden's shape (keepGroups.ts), against app/fixtures/golden/golden.json
 * (regs v38 @ 7fb6809f). Expected values are the golden's; the ticket's literals are restated from it.
 */
import { afterAll, describe, expect, it } from "vitest";
import { groupHeadOf, plain, type TreeTier } from "@app/core";
import { caseIds, golden, inputsOf, parityGate, summary } from "./golden";
import { comparableGroups } from "./exclusions";
import { groupHeadProj, keepOf } from "./keepGroups";

const gate = await parityGate();
const ids = caseIds("keep");
type GGroup = { title: string; scope: string | null; head: unknown; headLines: string[]; origin: unknown };
const goldenGroups = (id: string) => (golden().states[id]!.keep as { groups: GGroup[] }).groups;

it(`T17 group-head parity runs against a paired regs store${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ""}`, () => {
  if (gate.fail) expect.fail(gate.fail);
});

describe.skipIf(gate.skip)("T17 group head", () => {
  let equal = 0, groups = 0;
  afterAll(() => console.info(`${summary("keep", equal, ids.length)} · ${groups} groups (group head)`));

  it.each(ids)("%s: every group's title, scope, head (form a) and origin equal the golden", async (id) => {
    const k = keepOf((await inputsOf(id))!);
    const want = goldenGroups(id).map(({ title, scope, head, headLines, origin }) => ({ title, scope, head, headLines, origin }));
    expect(comparableGroups(id, k.groups).map((g) => groupHeadProj(g))).toEqual(want);   // D19 (a): exclusions.ts
    groups += want.length; equal++;
  });

  const head = async (id: string, i = 0) => groupHeadProj(keepOf((await inputsOf(id))!).groups[i]!);

  it("1. vedder_1010 (form a): the stream's 2 leads, the region's 4 follows; the scope moved; origin on the share", async () => {
    const h = await head("vedder_1010");
    expect([h.head.form, h.head.lead, h.head.leadN, h.head.big, h.head.ofTotal, h.scope, h.origin]).toEqual(["a",
      "2 a day per angler from streams", "2", "4", "of the 4 a day per angler across Region 2, lakes included", null, null]);
    const k = keepOf((await inputsOf("vedder_1010"))!).groups[0]!.ctx;
    expect([k.moveTag, k.tag, k.tagShare?.id, k.tierOrg]).toEqual([true, "All Region 2 streams", "t3", true]);
  });

  it("2. klwa_1010: the lake's own rule, no form; its Rule book page alone under the head", async () => {
    const h = await head("klwa_1010");
    expect([h.head.form, h.scope, h.head.text, h.head.big, h.origin]).toEqual([null, "This water",
      "This lake’s rule: up to 2 a day per angler", "2", { lines: ["Rule book p.37 ↗"], tag: null, howtell: false, pages: [{ text: "Rule book p.37 ↗", pdf: 39 }] }]);
  });

  it("3–4. Bella Coola and Glenn: form a over Region 5 / Region 4", async () => {
    for (const id of ["bc1_501", "bc0_501"]) {
      const h = await head(id);
      expect([h.head.form, h.head.lead, h.head.ofTotal, h.origin?.lines]).toEqual(["a", "2 a day per angler from streams",
        "of the 5 a day per angler across Region 5, lakes included", ["Rule book p.42 ↗"]]);
    }
    const g = await head("glenn_501");
    expect([g.head.form, g.head.big, g.head.ofTotal, g.origin?.lines]).toEqual(["a", "5",
      "of the 5 a day per angler across Region 4, lakes included", ["Rule book p.34 ↗"]]);
  });

  it("5. ape_501: the river's rule reaches the tributary, said under it", async () => {
    const h = await head("ape_501");
    expect([h.head.form, h.scope, h.head.text]).toEqual([null, "This water",
      "The Bella Coola River’s rule: up to 1 a day per angler Its row covers its tributaries, so it applies here"]);
  });

  // (T206, oracles.ts LOIS_FOLD) page v36's second group was "Steelhead"; with lois_lake.r3 retired it is both fish's row
  it("6. x25p_1010: Region 2-wide; the rainbow trout and steelhead row is separate from the 4", async () => {
    const [a, b] = [await head("x25p_1010", 0), await head("x25p_1010", 1)];
    expect([a.scope, a.head.text, b.title, b.scope, b.head.text]).toEqual(["Region 2-wide", "4 a day per angler across Region 2",
      "Rainbow trout and steelhead", "Separate from your 4 trout and char", "This lake’s rule: up to 6 a day per angler"]);
  });

  it("7. cow1_1010: 'Trout' (trout and char except char), the origin's How to tell and its page", async () => {
    const h = await head("cow1_1010");
    expect([h.title, h.head.form, h.head.ofTotal, h.origin]).toEqual(["Trout", "a",
      "of the 4 a day per angler across Region 1, excluding Haida Gwaii, lakes included",
      { lines: ["Keep hatchery fish only — wild trout go back. How to tell › Rule book p.13 ↗"], tag: null, howtell: true,
        pages: [{ text: "Rule book p.13 ↗", pdf: 15 }] }]);
  });

  it("8. property (r20h): form a leads with the headline's number; form b with the total's, the share's number after it", async () => {
    let n = 0;
    for (const id of ids) for (const gr of keepOf((await inputsOf(id))!).groups) {
      const { relTier, hdrRoot, g } = gr.ctx;
      if (!relTier) continue;
      n++;
      const a = groupHeadOf(gr.ctx, { tot: "a" }), b = groupHeadOf(gr.ctx, { tot: "b" });
      const firstNum = (...ls: (readonly { t: string; s?: string }[] | null)[]) => ls.flat().find((r) => r?.t === "num")?.s;
      expect(plain(a.lead), id).toMatch(new RegExp(`\\b${g.headline.n}\\b`));
      expect(firstNum(a.lead, a.words), id).toBe(String(g.headline.n));
      expect(firstNum(b.words, b.shareOf), id).toBe(String(hdrRoot!.n));
      expect(plain(b.shareOf), id).toMatch(new RegExp(`^↳ .*\\b${relTier.n}\\b`));
      expect(b.form, id).toBe("b");
    }
    expect(n).toBe(54);   // (T228) x37p0 gone: the Coquitlam's no-access split cut its golden part in two: its group goes with it;   // (2026-10-27) x18p3 gone: the Columbia's Mud Lake speed limit split its golden part: its group goes with it; (T115) the 4 states whose part the release merged away (x15p14/15/17/18, taps.json `gone`) are not cases: their 2 groups (x15p17, x15p18) go with them; (T183) and the West Road's x27p1 (its Region 6 stretch shares part 0's rules now, taps.json `gone`): its group goes with it
  });

  it("9. property (r20e/r20f): a moved tag leaves the head (and heads the share, if any); no head scope says ' streams' over an ' across ' total", async () => {
    let moved = 0;
    for (const id of ids) for (const gr of keepOf((await inputsOf(id))!).groups) {
      const c = gr.ctx, words = plain(groupHeadOf(c, { tot: "a" }).words);
      // a moved tag heads the root's share; with no share under the root (bc1_501: the river's own tier) it is not drawn
      // again — the own tier's words say whose rule it is (prototype round 20e)
      if (c.moveTag) { moved++; expect([c.scope, c.tagShare?.tier ?? "share"], id).toEqual([null, "share"]); }
      if (/ across /.test(words)) expect(c.scope ?? "", id).not.toMatch(/ streams$/);
      expect((c.g.tree[0] as TreeTier | undefined)?.node ?? "tier", id).toBe("tier");
    }
    expect(moved).toBeGreaterThan(0);
  });
});
