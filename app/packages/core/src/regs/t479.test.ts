/**
 * T479 (owner 2026-11-07, review of T475) — three words over the store's fields: "How to tell hatchery ›" beside the
 * hatchery-only wording; "Exception to regional quota" on a water's own line the store flags `over_region` (the pipeline
 * decides it from the resolved rules, test_answers_t479_over_region.py; the app only words the flag); the worked list
 * titled "Examples of how to interpret regulations" (ui-native draws it as a fold, closed by default).
 */
import { describe, expect, it } from "vitest";
import type { GroupTotal } from "../answers.generated";
import type { FishManifest } from "./fish.generated";
import { groupHeadOf, headCtx, originOf } from "./grouphead";
import type { TierVM } from "./model";
import { workedOf } from "./tail";
import { treeEnv, treeOf } from "./tree";
import { WORDS } from "./words";
import { cardOf, GROUP, INP, OWN, ROOT, SHARE, TROUT_ROW } from "./testing/keepFixtures";

const FISH: FishManifest = {};
const ctxOf = (g: GroupTotal) => headCtx(0, cardOf([{ ...TROUT_ROW, group: g }]), INP())!;
const tiers = (g: GroupTotal): TierVM[] =>
  treeOf(treeEnv(ctxOf(g), FISH, new Set([0]), "a"))!.filter((n): n is TierVM => n.t === "tier");
const LAKE8 = { ...ROOT, n: 8, whose: "This lake’s rule", says: "This lake’s rule: up to 8 a day per angler", children: [] };

describe("T479 the words", () => {
  it("1: the hatch sheet's link names what it tells", () => {
    expect(WORDS.HOW_TELL).toBe("How to tell hatchery ›");
    const o = originOf(ctxOf(GROUP({ origin: "Keep hatchery fish only — wild trout and char go back." })))!;
    expect(o.howtell).toBe(true);
  });
  it("2: a water's own tier the store flags says it is an exception to the regional quota; unflagged says nothing", () => {
    const flagged = tiers(GROUP({ tree: [{ ...ROOT, children: [SHARE, { ...OWN, over_region: true }] }] }));
    expect(flagged.find((t) => t.tier === "own")!.exception).toBe("Exception to regional quota");
    expect(flagged.find((t) => t.tier === "share")!.exception).toBeNull();
    expect(tiers(GROUP()).every((t) => t.exception === null)).toBe(true);
  });
  it("2: a water's own total over the region's says it in the head", () => {
    const g = (over: boolean) => GROUP({ headline: { n: 8, unit: "a day per angler", says: "8 a day per angler" },
      tree: [{ ...LAKE8, ...(over ? { over_region: true as const } : {}) }] });
    expect(groupHeadOf(ctxOf(g(true)), { tot: "a" }).exception).toBe("Exception to regional quota");
    expect(groupHeadOf(ctxOf(g(false)), { tot: "a" }).exception).toBeNull();
    expect(groupHeadOf(ctxOf(GROUP()), { tot: "a" }).exception).toBeNull();
  });
  it("3: the worked list's title", () => {
    expect(workedOf(ctxOf(GROUP()).row, GROUP())!.label).toBe("Examples of how to interpret regulations");
  });
});
