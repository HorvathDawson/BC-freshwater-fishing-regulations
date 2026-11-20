/**
 * T481 (owner 2026-11-07, "here-max + one breadcrumb") — core's half over the store's new fields (`water_max`, a tier's
 * `short`, an own tier's `within` / `fish`; pipeline test_answers_t481_pin.py pins them): "Fishing more than one water
 * today? ›" under the region's total, and its sheet in the water's own numbers; no link under a water's own total.
 * (T810 cutover) The pin bar's group line left with the bar (owner 2026-11-17 23:29).
 */
import { describe, expect, it } from "vitest";
import type { GroupTotal, TreeTier } from "../answers.generated";
import { groupHeadOf, headCtx } from "./grouphead";
import type { KeepEnv } from "./keep";
import type { SheetCtx } from "./model";
import { parseSheetKey } from "./sheetKey";
import { sheetLines, sheetOf } from "./sheets";
import { cardOf, GROUP, INP, OWN, ROOT, SHARE, TROUT_ROW } from "./testing/keepFixtures";

const OWN481 = { ...OWN, within: "total", fish: "hatchery rainbow", short: "up to 4 hatchery rainbow trout" } as TreeTier;
const SHARE481 = { ...SHARE, says: "Up to 2 of the 4 from streams", short: "up to 2 from streams" } as TreeTier;
const VEDDER = (o: Partial<GroupTotal> = {}) => GROUP({ tree: [{ ...ROOT, children: [SHARE481, OWN481] }], water_max: 4, ...o });
const ctxOf = (g: GroupTotal) => headCtx(0, cardOf([{ ...TROUT_ROW, group: g }]), INP())!;
const sheetCtx = (g: GroupTotal): SheetCtx => {
  const ctx = ctxOf(g);
  return { inp: ctx.inp, page: {} as SheetCtx["page"], today: { m: 10, d: 10 } as unknown as SheetCtx["today"],
    env: { card: ctx.card } as unknown as KeepEnv } as SheetCtx;
};

describe("T481 the region's total, said once: \"Fishing more than one water today? ›\"", () => {
  it("under the region's total, opening the row's multi sheet", () => {
    const h = groupHeadOf(ctxOf(VEDDER()), { tot: "a" });
    expect(h.multi).toEqual([{ t: "link", s: "Fishing more than one water today? ›", open: "multi:r0" }]);
    expect(parseSheetKey("multi:r0")).toBe("multi:r0");
  });
  it("no link under a water's own total", () => {
    const own = { ...ROOT, says: "This lake’s rule: up to 8 a day per angler", whose: "This lake’s rule", n: 8, children: [] } as TreeTier;
    expect(groupHeadOf(ctxOf(GROUP({ tree: [own] })), { tot: "a" }).multi).toBeNull();
  });
  it("the sheet: per angler, the region's total over every water, the share inside it, Source ›", () => {
    const l = sheetLines(sheetOf(sheetCtx(VEDDER()), "multi:r0")!);
    expect([l.title, l.lines]).toEqual(["Fishing more than one water today", [
      "Quotas are per angler per day, across every water you fish.",
      "Region 2’s 4 a day caps all the trout and char you keep in Region 2 today, whatever the water.",
      "The 2 from streams and any share from lakes are parts of that 4, not additions to it.",
      "A water’s own rule can sit outside a share, but never outside the region’s total.",
      "Possession limits are separate.",
      "Source › Rule book p.21 ↗"]]);
    expect(sheetOf(sheetCtx(VEDDER()), "multi:r1")).toBeNull();
  });
});
