/**
 * tail.ts, store-free, over the hand-built Vedder-like group (testing/keepFixtures.ts; sentences copied from golden
 * vedder_515 / vedder_1010 / x15p0_1010). Golden parity on all 97 groups is app/conformance/src/regs/tail.parity.test.ts.
 */
import { describe, expect, it } from "vitest";
import type { FishManifest } from "./fish.generated";
import { headCtx } from "./grouphead";
import { plain } from "./line";
import { MORE, moreOf, notKeptOf, quietOf, STEEL_LINE, workedOf } from "./tail";
import { treeEnv } from "./tree";
import { cardOf, GROUP, INP, TROUT_ROW } from "./testing/keepFixtures";

const FISH: FishManifest = {
  RB: [{ file: "r.webp", caption: "Rainbow trout", credit: "", type: "", source: "", phase: null, standInFor: null, width: 2, height: 1 }],
};
const ctxOf = (g = GROUP(), line: "known_no_rules" | null = null) => headCtx(0, cardOf([{ ...TROUT_ROW, group: g }], { line }), INP())!;

describe("notKeptOf", () => {
  it("the store's words, whose rule with its Rule book page, the pictures (golden vedder_515)", () => {
    const ctx = ctxOf();
    const [x] = notKeptOf(ctx.g, treeEnv(ctx, FISH, new Set([0])));
    expect([x!.status, x!.says, plain(x!.whose), x!.mark, x!.presence, x!.pictures.map((p) => (p.sil ? `(${p.code})` : p.code))]).toEqual([
      "release", "Rainbow trout and cutthroat trout: release every one", "This river’s rule · Rule book p.22 ↗", null, null, ["RB", "(CT)"]]);
  });
  it("a closure carries ✕ and its own presence line (never presOf); a rule with no book: no page", () => {
    const g = GROUP({ not_kept: [{ species: ["ST"], status: "closed", says: "Steelhead: no fishing for them", by: 99, rules: [99], whose: "the river’s rule",
      presence: { line: "known_with_rules", says: "A rainbow trout over 50 cm (20 in) here counts as a steelhead." } }] });
    const ctx = ctxOf(g);
    const [x] = notKeptOf(g, treeEnv(ctx, FISH, new Set([0])));
    expect([x!.mark, plain(x!.whose), plain(x!.presence)]).toEqual(["✕", "The river’s rule", "A rainbow trout over 50 cm (20 in) here counts as a steelhead. ⓘ"]);
  });
  it("a released fish's stored stamps (rows 31) are its lines, in order; none stored, none drawn (golden anderson_1010)", () => {
    const st = { doc: "steelhead_stamp", act: "targeting" as const, name: "Steelhead Conservation Surcharge Stamp",
      when: "Only if you fish for steelhead, even to release them" };
    const g = GROUP({ not_kept: [{ species: ["ST", "DV"], status: "release", says: "Steelhead and Dolly Varden/bull trout: release every one",
      by: 99, rules: [99], whose: "Region 3’s rule", stamps: [st] }] });
    const ctx = ctxOf(g);
    const [x] = notKeptOf(g, treeEnv(ctx, FISH, new Set([0])));
    expect(x!.stamps.map(plain)).toEqual(["You need Steelhead Conservation Surcharge Stamp › only if you fish for steelhead, even to release them"]);
    expect(notKeptOf(ctxOf().g, treeEnv(ctxOf(), FISH, new Set([0])))[0]!.stamps).toEqual([]);
  });
});

describe("workedOf", () => {
  it("the row's elsewhere sentence; a keep-here question takes its colon, a lake / stream / elsewhere line reads as kept elsewhere", () => {
    expect(workedOf(ctxOf().row, GROUP())).toEqual({ label: "How it adds up",
      says: "Your own limit, counted across every Region 2 stream you fish today.",
      lines: [{ at: "here", q: "If you keep 4 hatchery rainbow trout here:", a: "that’s your trout and char for the day, everywhere." },
        { at: "elsewhere", q: "Kept 1 at a lake?", a: "Keep 3 more here." }] });
  });
  it("neither a sentence nor a line: null", () => {
    const ctx = headCtx(0, cardOf([{ ...TROUT_ROW, elsewhere: null, group: GROUP({ worked: [] }) }]), INP())!;
    expect(workedOf(ctx.row, ctx.g)).toBeNull();
  });
});

describe("quietOf / MORE", () => {
  it("only 'recorded here, but no steelhead rule applies', only over rainbow trout (golden x15p0_1010)", () => {
    const ctx = ctxOf(GROUP(), "known_no_rules");
    expect(plain(quietOf(ctx.row, ctx.card))).toBe(`${STEEL_LINE.known_no_rules} ⓘ`);
    expect(plain(quietOf(ctx.row, ctx.card))).toBe("Steelhead have been recorded here, but no steelhead rule applies: treat any rainbow, however big, as a rainbow trout. ⓘ");
    expect(quietOf(ctxOf().row, ctxOf().card)).toBeNull();
    const ko = headCtx(0, cardOf([{ ...TROUT_ROW, all_members: ["KO"], group: GROUP() }], { line: "known_no_rules" }), INP())!;
    expect(quietOf(ko.row, ko.card)).toBeNull();
  });
  it("every carded row ends on 'See every regulation line ›', opening its row's sheet", () => {
    expect([MORE, moreOf(3)]).toEqual(["See every regulation line ›", { text: "See every regulation line ›", open: "src:r3" }]);
  });
});
