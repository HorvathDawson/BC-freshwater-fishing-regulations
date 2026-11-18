/**
 * The tab row's scroll functions (prototype regs v38 `mini` / the tab press), and shortOf (grouphead.ts: form b's lines).
 * (T810 cutover) The pin bar's units (groupLine, breadcrumb, pinChain, pinLineEstimate, boldFirst) left with the bar.
 */
import { describe, expect, it } from "vitest";
import { tabInView, tabScrollY } from "./tabScroll";
import { shortOf, type ShortTier } from "./grouphead";

describe("shortOf (grouphead.ts: the form b lines and the breadcrumb's levels)", () => {
  it.each([
    [{ tier: "total", n: 4, says: "4 a day per angler across Region 2, lakes included" }, "4 a day across Region 2"],
    [{ tier: "total", n: 2, says: "This lake’s rule: up to 2 a day per angler", whose: "This lake’s rule" }, "This lake’s rule: 2 a day"],
    [{ tier: "share", n: 2, says: "Only 2 of the 4 may be from streams, not counting hatchery rainbow trout" }, "2 of them from streams"],
    [{ tier: "share", n: 2, says: "2 a day per angler from streams" }, "2 a day from streams"],
    // (T481) the store's clause where it has one; else the old fields, lower-cased, no "of those, " (the breadcrumb's)
    [{ tier: "size", n: 1, over_cm: 50, says: "x", short: "may be over 50 cm (20 in) — 2 if one is a hatchery steelhead" },
      "may be over 50 cm (20 in) — 2 if one is a hatchery steelhead"],
    [{ tier: "size", n: 1, over_cm: 50, says: "Up to 1 over 50 cm (20 in) combined" }, "may be over 50 cm"],
    [{ tier: "count", n: 1, says: "Up to 1 brown trout (part of the 2)" }, "up to 1 brown trout"],
    [{ tier: "count", n: 1, says: "Up to 1 brown trout", short: "up to 1 brown trout" }, "up to 1 brown trout"],
    [{ tier: "own", n: 4, says: "", whose: "This river’s rule", short: "up to 4 hatchery rainbow trout" }, "up to 4 hatchery rainbow trout"],
    // own with no stored short (an older store): golden vedder_1010 r0 t2
    [{ tier: "own", n: 4, says: "", whose: "This river’s rule", relation: "Not part of the 2 from streams; x",
       children: [{ node: "kind", folded: true, says: "Up to 4 may be hatchery rainbow trout" }] },
      "4 hatchery rainbow trout a day under this river’s rule, not part of the 2 from streams"],
    [{ tier: "total", n: null, says: "No limit" }, "No limit"],
    [{ tier: "total", n: 3, says: "Up to 3 a day" }, "3 a day"],
  ] as const)("%j", (t, want) => { expect(shortOf(t as unknown as ShortTier)).toBe(want); });   // literal tiers carry only the fields shortOf reads
});

describe("tabInView (mini, app coordinates)", () => {
  const tops = { gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700 };
  it.each([
    [0, "keep"], [1487, "keep"], [1488, "gear"], [2087, "gear"], [2095, "methods"], [2690, "licence"],
  ] as const)("y %i → %s", (y, want) => { expect(tabInView(tops, y, 600, 3400, 0)).toBe(want); });
  it("the end of the page reaches the licence (or methods without one)", () => {
    expect(tabInView(tops, 2700, 600, 3300, 0)).toBe("licence");
    expect(tabInView({ ...tops, licence: null }, 2000, 600, 2602, 0)).toBe("methods");
    expect(tabInView({ ...tops, licence: null, methodsHeight: 0 }, 2000, 600, 2602, 0)).toBe("gear");
  });
  it("no gear section: always keep", () => {
    expect(tabInView({ gear: null, methods: null, methodsHeight: 0, licence: null }, 9000, 600, 3300, 0)).toBe("keep");
  });
  it("the header offset (prototype coordinates) moves the threshold", () => {
    expect(tabInView(tops, 1369, 600, 3400, 119)).toBe("gear");
    expect(tabInView(tops, 1368, 600, 3400, 119)).toBe("keep");
  });
});

describe("tabScrollY", () => {
  it("a tab press: keep → 0, a section → its top + 2, a missing section → 0", () => {
    expect([tabScrollY("keep", { gear: 1500, methods: null, licence: null }), tabScrollY("gear", { gear: 1500, methods: null, licence: null }),
      tabScrollY("licence", { gear: 1500, methods: null, licence: null })]).toEqual([0, 1502, 0]);
  });
});
