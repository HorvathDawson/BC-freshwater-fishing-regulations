/**
 * T28 units: the pin bar (prototype regs v38 pinUpdate / mini / tab press, round UR6). The golden replay over every state
 * lives in conformance (pins.parity.test.ts); here every expected value is a literal.
 */
import { describe, expect, it } from "vitest";
import {
  anchoredScrollY, appPinLines, boldFirst, groupLine, PIN_LH, PIN_ROW, pinChain, pinLineEstimate, tabInView, tabScrollY,
  type PinAnchor, type PinLine,
} from "./pins";
import { shortOf, type ShortTier } from "./grouphead";

const total4 = { tier: "total" as const, n: 4, says: "4 a day per angler across Region 2, lakes included" };
const VP = { header: 119, LH: PIN_LH, ROW: PIN_ROW };
const G2: PinLine = { cls: "g two", text: "Trout and char · 2 a day from streams", sub: "(of 4 across Region 2)" };

describe("groupLine (pinUpdate's head line)", () => {
  it("form a: the share leads, the total in brackets under it", () => {
    expect(groupLine("Trout and char", total4, { n: 2, says: "2 a day per angler from streams" }, true)).toEqual(G2);
  });
  it("form b (or no headline): the name and the root's short line", () => {
    expect(groupLine("Trout and char", total4, { n: 2, says: "2 a day per angler from streams" }, false))
      .toEqual({ cls: "g", text: "Trout and char · 4 a day across Region 2", sub: null });
    expect(groupLine("Trout and char", total4, null, true)).toEqual({ cls: "g", text: "Trout and char · 4 a day across Region 2", sub: null });
  });
});

describe("shortOf (grouphead.ts, the strings golden anchors carry)", () => {
  it.each([
    [total4, "4 a day across Region 2"],
    [{ tier: "total", n: 2, says: "This lake’s rule: up to 2 a day per angler", whose: "This lake’s rule" }, "This lake’s rule: 2 a day"],
    [{ tier: "share", n: 2, says: "Only 2 of the 4 may be from streams, not counting hatchery rainbow trout" }, "2 of them from streams"],
    [{ tier: "share", n: 2, says: "2 a day per angler from streams" }, "2 a day from streams"],
    [{ tier: "size", n: 1, over_cm: 50, says: "Only 1 of the fish in this box may be over 50 cm (20 in) — 2 if one is a hatchery steelhead" },
      "of those, 1 over 50 cm (2 if one is a hatchery steelhead)"],
    // own (UR6 relation): golden vedder_1010 r0 t2 "↳ 4 hatchery rainbow trout a day under this river’s rule, not part of the 2 from streams"
    [{ tier: "own", n: 4, says: "", whose: "This river’s rule", relation: "Not part of the 2 from streams; x",
       children: [{ node: "kind", folded: true, says: "Up to 4 may be hatchery rainbow trout" }] },
      "4 hatchery rainbow trout a day under this river’s rule, not part of the 2 from streams"],
    // own, no relation: golden x3p_1010 r0 t2 "↳ 3 of them (lake trout) under this lake’s rule"
    [{ tier: "own", n: 3, says: "", whose: "This lake’s rule", children: [{ node: "kind", folded: true, says: "All 3 may be lake trout only — y" }] },
      "3 of them (lake trout) under this lake’s rule"],
    [{ tier: "own", n: 4, says: "", whose: "This river’s rule", replaces: true,
       children: [{ node: "kind", folded: true, says: "Up to 4 may be hatchery rainbow trout" }] },
      "plus up to 4 hatchery rainbow trout under this river’s rule"],
    [{ tier: "own", n: 2, says: "", whose: "This river’s rule", children: [] }, "2 of them under this river’s rule"],
    [{ tier: "count", n: 1, says: "1 may be a steelhead (part of the 4)" }, "of those, 1 may be a steelhead"],
    [{ tier: "total", n: null, says: "No limit" }, "No limit"],
    [{ tier: "total", n: 3, says: "Up to 3 a day" }, "3 a day"],
  ] as const)("%j", (t, want) => { expect(shortOf(t as unknown as ShortTier)).toBe(want); });   // literal tiers carry only the fields shortOf reads
});

/** One group article at content 200..1700 with a share tier (head 300, rows to 1000) and a size tier inside it (head 380,
 *  rows to 900), then an own tier (head 1100, rows to 1250). */
const art = (over: Partial<PinAnchor> = {}): PinAnchor => ({
  row: "r0", group: true, tree: true, formA: false, headlineN: 2, top: 200, height: 1500,
  groupLine: { cls: "g", text: "Trout and char · 4 a day across Region 2", sub: null }, pin: { top: 200, height: 70 },
  tiers: [
    { tid: "t1", tier: "share", n: 2, head: { top: 300, height: 60 }, end: 1000, short: "2 of them from streams" },
    { tid: "t2", tier: "size", n: 1, head: { top: 380, height: 60 }, end: 900, short: "of those, 1 over 50 cm" },
    { tid: "t3", tier: "own", n: 4, head: { top: 1100, height: 60 }, end: 1250, short: "plus up to 4 under this river’s rule" },
  ],
  ...over,
});
const H26 = (l: PinLine) => pinLineEstimate(l);
const texts = (ls: readonly PinLine[]) => ls.map((l) => l.text);

describe("pinChain (pinUpdate, regs v38 round UR6)", () => {
  it("nothing before the article's head has gone under the bar", () => {
    expect(pinChain([art()], 0, VP, H26)).toEqual([]);
    expect(pinChain([art()], 80, VP, H26)).toEqual([]);   // 200 - 80 = 120: not < 119
  });
  it("the group line once the head is under the bar; a tier's line once its head is under the stack", () => {
    expect(texts(pinChain([art()], 90, VP, H26))).toEqual(["Trout and char · 4 a day across Region 2"]);
    // B = 119 + 26 = 145: the share head (300) is under it from y 156
    expect(texts(pinChain([art()], 155, VP, H26))).toEqual(["Trout and char · 4 a day across Region 2"]);
    expect(texts(pinChain([art()], 156, VP, H26))).toEqual(["Trout and char · 4 a day across Region 2", "↳ 2 of them from streams"]);
    // B = 171: the size head (380) from y 210
    expect(texts(pinChain([art()], 210, VP, H26))).toEqual(["Trout and char · 4 a day across Region 2", "↳ 2 of them from streams", "↳ of those, 1 over 50 cm"]);
  });
  it("a tier's line stays until its section's last row is within one line + PIN_ROW of the stack's foot", () => {
    // size: end 900 - y > B(171) + 26 + 12 = 209 → y < 691
    expect(pinChain([art()], 690, VP, H26)).toHaveLength(3);
    expect(texts(pinChain([art()], 691, VP, H26))).toEqual(["Trout and char · 4 a day across Region 2", "↳ 2 of them from streams"]);
    // share: 1000 - y > 145 + 26 + 12 → y < 817
    expect(pinChain([art()], 816, VP, H26)).toHaveLength(2);
    expect(pinChain([art()], 817, VP, H26)).toHaveLength(1);
  });
  it("the stack's MEASURED height decides: a two-line group line pushes every test down", () => {
    const tall = (l: PinLine) => (l.cls === "c" ? 26 : 60);
    expect(texts(pinChain([art()], 150, VP, H26))).toEqual(["Trout and char · 4 a day across Region 2"]);   // 300-150 = 150 ≥ 145
    expect(texts(pinChain([art()], 150, VP, tall))).toEqual(["Trout and char · 4 a day across Region 2", "↳ 2 of them from streams"]);
  });
  it("form a: a share whose n is the headline's is not repeated (compares n, not words)", () => {
    const a = art({ formA: true, groupLine: G2 });
    expect(texts(pinChain([a], 210, VP, H26))).toEqual([G2.text, "↳ of those, 1 over 50 cm"]);
    expect(texts(pinChain([art({ formA: true, groupLine: G2, headlineN: 3 })], 210, VP, H26))).toHaveLength(3);
  });
  it("a tier with no head or no rows never pins", () => {
    const a = art({ tiers: [{ ...art().tiers[0]!, head: null }, { ...art().tiers[1]!, end: null }] });
    expect(pinChain([a], 300, VP, H26)).toHaveLength(1);
  });
  it("the article is the first whose box spans the bar's line (top < header, bottom > header + LH)", () => {
    const second = art({ row: "r1", top: 1700, height: 400, pin: { top: 1700, height: 70 }, tiers: [],
      groupLine: { cls: "g", text: "Whitefish · 15 a day", sub: null } });
    expect(texts(pinChain([art(), second], 1550, VP, H26))).toEqual(["Trout and char · 4 a day across Region 2"]);   // its bottom 1700-1550 = 150 > 119+26
    expect(texts(pinChain([art(), second], 1600, VP, H26))).toEqual(["Whitefish · 15 a day"]);
  });
  it("a group-less article pins NOTHING (IMPLEMENT-HANDOFF C5: the v35 fix; no stale lines)", () => {
    expect(pinChain([art({ group: false, tree: false, groupLine: null })], 400, VP, H26)).toEqual([]);
    expect(pinChain([art({ tree: false, groupLine: null })], 400, VP, H26)).toEqual([]);
    expect(pinChain([art({ pin: null })], 400, VP, H26)).toEqual([]);
    expect(appPinLines(pinChain([art({ group: false })], 400, VP, H26))).toEqual([]);
  });
});

describe("pinLineEstimate / boldFirst", () => {
  it("an unmeasured line is one LH, two with a sub", () => {
    expect([pinLineEstimate({ cls: "c", text: "x", sub: null }), pinLineEstimate(G2)]).toEqual([26, 52]);
  });
  it("the first whole number, split around", () => {
    expect(boldFirst("Trout and char · 2 a day from streams")).toEqual({ pre: "Trout and char · ", n: "2", post: " a day from streams" });
    expect(boldFirst("↳ of those, 1 over 50 cm")).toEqual({ pre: "↳ of those, ", n: "1", post: " over 50 cm" });
    expect(boldFirst("Kokanee · no limit")).toBeNull();
  });
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

describe("anchoredScrollY / tabScrollY", () => {
  it("an anchored scroll lands under the stack + 8", () => {
    expect([anchoredScrollY(1000, 65), anchoredScrollY(30, 65), anchoredScrollY(1000, 0)]).toEqual([927, 0, 992]);
  });
  it("a tab press: keep → 0, a section → its top + 2, a missing section → 0", () => {
    expect([tabScrollY("keep", { gear: 1500, methods: null, licence: null }), tabScrollY("gear", { gear: 1500, methods: null, licence: null }),
      tabScrollY("licence", { gear: 1500, methods: null, licence: null })]).toEqual([0, 1502, 0]);
  });
});
