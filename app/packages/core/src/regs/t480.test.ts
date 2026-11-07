/**
 * T480 (owner 2026-11-07: "Why is size written now in 2 different styles? We need to commit to a single style.") — every
 * kind's size in one style: the size words in a chip, what happens outside it as words beside it. Words only: the store's
 * `size_rule` / `detail` split at their own seams, and put back together they are the store's words exactly.
 */
import { describe, expect, it } from "vitest";
import { sizeOf, sizeVM } from "./tree";
import { WORDS } from "./words";

const sp = (s: string) => s.replace(/ /g, " ");
// every form tree.py `size_rule` writes (bottom, top, range, gaps, slot), as the scratch release holds them
const RULES = [
  "must be 30 cm (12 in) or longer; release any shorter",
  "must be 50 cm (20 in) or less; release any longer",
  "must be 30–50 cm (12–20 in); release any other size",
  "must be 30 cm (12 in) or less, or 50 cm (20 in) or longer; release any other size",
];

describe("T480 one size style", () => {
  it("a size rule: the size in the chip, the release beside it; together exactly the store's words", () => {
    for (const r of RULES) {
      const v = sizeVM(r, "", [], false)!;
      expect([v.lead, v.rule, v.def, v.then?.startsWith("release any ")]).toEqual([null, true, false, true]);
      expect(`must be ${sp(v.chip)}; ${v.then}`).toBe(r);
      expect(sp(v.text)).toBe(r);
    }
    expect(sizeVM(RULES[0], "", [], false)).toMatchObject({ chip: "30 cm (12 in) or longer", then: "release any shorter" });
  });
  it("a slot: \"release any\" leads the chip", () => {
    const v = sizeVM("release any between 60 cm (24 in) and 90 cm (35 in)", "", [], false)!;
    expect([v.lead, sp(v.chip), v.then]).toEqual(["release any", "between 60 cm (24 in) and 90 cm (35 in)", null]);
  });
  it("an unknown rule form: the whole words in the chip (never dropped)", () => {
    expect(sizeVM("keep 30 cm or longer", "", [], false)).toMatchObject({ lead: null, chip: "keep 30 cm or longer", then: null });
  });
  it("a detail size: the chip alone; none: null", () => {
    expect(sizeVM(null, "50 cm (20 in) or less", [], false)).toMatchObject({ lead: null, then: null, rule: false, def: false });
    expect(sizeVM(null, "", ["catch and release"], false)).toBeNull();
  });
  it("the steelhead's definition: its words lead, the rainbow and its size are the chip — not a limit", () => {
    const { size, q } = sizeOf("rainbow trout over 50 cm (20 in)");
    const v = sizeVM(null, size, q, true)!;
    expect([v.def, v.lead, sp(v.chip), v.then]).toEqual([true, WORDS.COUNTS_AS_STEELHEAD, "rainbow trout over 50 cm (20 in)", null]);
    expect(WORDS.COUNTS_AS_STEELHEAD).toBe("Counts as a steelhead:");
    // a steelhead's real size limit is a limit, not a definition
    expect(sizeVM(null, "30 cm (12 in) or longer", [], true)!.def).toBe(false);
    // a rainbow kind's "over" size is not the steelhead's definition
    expect(sizeVM(null, size, q, false)!.def).toBe(false);
  });
});
