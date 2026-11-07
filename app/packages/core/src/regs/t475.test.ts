/**
 * T475 (owner 2026-11-07) — the quota block over the store's new fields: form a's headline "2 hatchery trout/char a day
 * from streams" with its ⓘ (the row's `share:r<row>` sheet, the stored `headline.info`), the share tier's origin sentence
 * not drawn again under it (its How to tell and Rule book stay), "Up to 2 of the 4 from streams" folded and shortened,
 * a closed fish's ⓘ (`not_kept.term`). Hand-built from keepFixtures with the store's T475 words (pipeline
 * test_answers_t475_quota_wording.py pins them); a store without the fields draws as before (the other tests).
 */
import { describe, expect, it } from "vitest";
import type { GroupTotal, ShareInfo } from "../answers.generated";
import type { FishManifest } from "./fish.generated";
import { groupHeadOf, headCtx, leadSaysOrigin, shortOf } from "./grouphead";
import { plain, type Run } from "./line";
import type { KeepEnv } from "./keep";
import type { SheetCtx, TierVM } from "./model";
import { parseSheetKey } from "./sheetKey";
import { sheetLines, sheetOf } from "./sheets";
import { notKeptOf } from "./tail";
import { treeEnv, treeOf } from "./tree";
import { cardOf, GROUP, INP, kind, OWN, ROOT, SHARE, tier, TROUT_ROW } from "./testing/keepFixtures";

const FISH: FishManifest = {};
const INFO: ShareInfo = {
  title: "Your 2 from streams and the 4 across Region 2",
  says: "The 2 from streams are part of your 4 trout and char a day across Region 2, lakes included. Every one you keep that day, "
    + "at any stream or lake, counts toward the 4; no more than 2 of them may come from streams.",
  by: 4,
  examples: [
    { kept: 2, q: "Kept 2 on another stream?", a: "None more from streams today — up to 2 more from lakes.", species: ["RB"],
      origin: "hatchery", share: null, here: 0, other: 0, away: 2, at: "stream", over_cm: null, most: "exactly", of: null },
    { kept: 1, q: "Kept 1 at a lake?", a: "Up to 2 more here, or up to 3 more at lakes.", species: ["RB"], origin: "hatchery",
      share: null, here: 2, other: null, away: 3, at: "lake", over_cm: null, most: "exactly", of: null }],
};
const SHARE475 = { ...SHARE, says: "Up to 2 of the 4 from streams", origin_kind: "hatchery" as const };
const G475 = (o: Partial<GroupTotal> = {}): GroupTotal => GROUP({
  headline: { n: 2, unit: "hatchery trout/char a day from streams", says: "2 hatchery trout/char a day from streams",
    origin: "hatchery", fish: "trout/char", info: INFO },
  tree: [{ ...ROOT, children: [SHARE475, OWN] }], ...o });
const ctxOf = (g: GroupTotal) => headCtx(0, cardOf([{ ...TROUT_ROW, group: g }]), INP())!;
const shareVM = (g: GroupTotal, tot: "a" | "b" = "a"): TierVM => {
  const ctx = ctxOf(g);
  return treeOf(treeEnv(ctx, FISH, new Set([0]), tot))!.find((n): n is TierVM => n.t === "tier" && n.tier === "share")!;
};

describe("T475 ask 1: the headline and its ⓘ", () => {
  it("form a's lead is the store's one line with its number big and an ⓘ opening the row's share sheet", () => {
    const h = groupHeadOf(ctxOf(G475()), { tot: "a" });
    expect(plain(h.lead)).toBe("2 hatchery trout/char a day from streams ⓘ");
    expect(h.lead!.filter((r) => r.t === "num")).toEqual([{ t: "num", s: "2", size: "big" }]);
    expect(h.lead!.at(-1)).toEqual({ t: "link", s: "ⓘ", open: "share:r0", label: "How this limit is shared" });
    expect(parseSheetKey("share:r0")).toBe("share:r0");
  });
  it("no stored info: no ⓘ (a store before T475)", () => {
    const h = groupHeadOf(ctxOf(GROUP()), { tot: "a" });
    expect(h.lead!.some((r: Run) => r.t === "link")).toBe(false);
  });
  it("the share's origin sentence is the lead's: not drawn again, How to tell and the Rule book stay", () => {
    const ctx = ctxOf(G475());
    expect(leadSaysOrigin(ctx, SHARE475, "a")).toBe(true);
    const s = shareVM(G475());
    expect([s.folded, s.origin?.says, s.origin?.howtell, plain(s.origin?.book)]).toEqual([true, [], true, "Rule book p.21 ↗"]);
  });
  it("form b (the lead is not drawn) and a store before T475 keep the sentence", () => {
    const sentence = "From streams, keep hatchery fish only — wild trout and char go back.";
    expect(plain(shareVM(G475(), "b").origin?.says)).toBe(sentence);
    expect(plain(shareVM(GROUP()).origin?.says)).toBe(sentence);
  });
  it("the sheet: the store's words, its examples, Source › with the share rule's page", () => {
    const ctx = ctxOf(G475());
    const env = { card: ctx.card } as unknown as KeepEnv;
    const sc = { inp: ctx.inp, page: {} as SheetCtx["page"], today: { m: 10, d: 10 } as SheetCtx["today"], env } as SheetCtx;
    const l = sheetLines(sheetOf(sc, "share:r0")!);
    expect([l.title, l.lines]).toEqual([INFO.title, [INFO.says, "For example:",
      "Kept 2 on another stream? None more from streams today — up to 2 more from lakes.",
      "Kept 1 at a lake? Up to 2 more here, or up to 3 more at lakes.", "Source › Rule book p.21 ↗"]]);
    expect(sheetOf(sc, "share:r1")).toBeNull();
  });
});

describe("T475 asks 3-4: the new sub-limit words", () => {
  it("'Up to 2 of the 4 from streams' folds into form a's lead like 'Only 2 of the 4 may be from streams' did", () => {
    expect(shareVM(G475()).folded).toBe(true);
  });
  it("shortOf reads the new share and folded-kind words", () => {
    expect(shortOf(SHARE475)).toBe("2 of them from streams");
    const own = tier({ tier: "own", n: 4, whose: "this river’s rule", relation: "Not part of the 2 from streams; part of the 4",
      children: [kind({ species: ["RB"], n: 4, says: "Up to 4 hatchery rainbow trout", folded: true })] });
    expect(shortOf(own)).toBe("4 hatchery rainbow trout a day under this river’s rule, not part of the 2 from streams");
  });
});

describe("T475 ask 2: the steelhead line's ⓘ", () => {
  it("a closed line carries its stored term; a release line none", () => {
    const g = G475({ not_kept: [{ species: ["ST"], status: "closed", says: "Steelhead: closed, no catch and release", by: 3, rules: [3],
      whose: "this river’s rule", term: "no_fishing_for" }, ...GROUP().not_kept] });
    const ctx = ctxOf(g);
    expect(notKeptOf(g, treeEnv(ctx, FISH, new Set([0]))).map((x) => [x.says, x.term])).toEqual([
      ["Steelhead: closed, no catch and release", "no_fishing_for"], ["Rainbow trout and cutthroat trout: release every one", null]]);
  });
});
