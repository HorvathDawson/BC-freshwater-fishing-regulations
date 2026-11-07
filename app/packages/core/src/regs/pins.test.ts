/**
 * T28 units: the pin bar (prototype regs v38 pinUpdate / mini / tab press, round UR6). The golden replay over every state
 * lives in conformance (pins.parity.test.ts (retired, T439)); here every expected value is a literal.
 */
import { describe, expect, it } from "vitest";
import {
  anchoredScrollY, appPinLines, boldFirst, breadcrumb, groupLine, PIN_LH, PIN_ROW, pinChain, pinLineEstimate, tabInView, tabScrollY,
  type PinAnchor, type PinLine,
} from "./pins";
import { shortOf, type ShortTier } from "./grouphead";

const VP = { header: 119, LH: PIN_LH, ROW: PIN_ROW };
const G2: PinLine = { cls: "g two", text: "Trout and char · 2 a day from streams", sub: "(of 4 across Region 2)" };

// (T481) stored trees, only the fields the bar reads
const tier = (o: Record<string, unknown>) => ({ node: "tier", by: 0, children: [], ...o }) as never;
const total = (n: number | null, says: string, children: unknown[] = []) => [tier({ id: "t0", tier: "total", n, says, children })];
const SHARE = tier({ id: "t1", tier: "share", n: 2, says: "Up to 2 of the 4 from streams", short: "up to 2 from streams" });
const OWN = tier({ id: "t2", tier: "own", n: 4, says: "This river’s rule: up to 4 hatchery rainbow trout a day", whose: "This river’s rule",
  within: "total", fish: "hatchery rainbow", short: "up to 4 hatchery rainbow trout" });

describe("groupLine (T481: the water's limit, never the region's total)", () => {
  it("a plain root: the name and its number a day", () => {
    expect(groupLine("Kokanee", { tree: total(10, "10 a day per angler across Region 4"), water_max: 10 }))
      .toEqual({ cls: "g", text: "Kokanee · 10 a day", sub: null, said: [], n: 10 });
  });
  it("a share and nothing beside it: the share, from where the words say", () => {
    expect(groupLine("Trout and char", { tree: total(4, "4 a day per angler across Region 2, lakes included", [SHARE]), water_max: 2 }))
      .toEqual({ cls: "g", text: "Trout and char · 2 a day from streams", sub: null, said: ["t1"], n: 2 });
  });
  it("a share and a water's own tier beside it: the true most leads, the parts inside it (the Vedder)", () => {
    const l = groupLine("Trout and char", { tree: total(4, "4 a day per angler across Region 2, lakes included", [SHARE, OWN]), water_max: 4 });
    expect(l).toEqual({ cls: "g", text: "Trout and char · 4 a day — up to 2 of any kind, the rest hatchery rainbow", sub: null,
      said: ["t1", "t2"], n: 2 });
  });
  it("nothing in the line adds up to more than water_max", () => {
    for (const wm of [2, 3, 4]) {
      const l = groupLine("Trout and char", { tree: total(4, "4 a day per angler across Region 2", [SHARE, OWN]), water_max: wm });
      const nums = [...l.text.matchAll(/\b(\d+)\b/g)].map((m) => +m[1]!);
      expect(Math.max(...nums)).toBeLessThanOrEqual(wm);
    }
  });
  it("no number: the root's words", () => {
    expect(groupLine("Kokanee", { tree: total(null, "No limit") }).text).toBe("Kokanee · No limit");
  });
});

describe("breadcrumb (T481: the path below the root, one line)", () => {
  const size = (n: number, short = "may be over 50 cm (20 in) — 2 if one is a hatchery steelhead") => ({ tier: "size", n, short });
  it("the size level counts from the first line: \"1 of them may be over …\", the steelhead clause where it applies", () => {
    expect(breadcrumb([size(1)], 2)).toBe("1 of them may be over 50 cm (20 in) — 2 if one is a hatchery steelhead");
    expect(breadcrumb([size(1, "may be over 50 cm (20 in)")], 4)).toBe("1 of them may be over 50 cm (20 in)");
    expect(breadcrumb([size(0, "may be over 50 cm (20 in)")], 4)).toBe("none of them may be over 50 cm (20 in)");
  });
  it("under a count level, \"of those, 1 may be over\" (\"of them\" would point at the first line's fish)", () => {
    expect(breadcrumb([{ tier: "count", n: 2, short: "up to 2 brown trout" }, size(1, "may be over 50 cm (20 in)")], 4))
      .toBe("up to 2 brown trout; of those, 1 may be over 50 cm (20 in)");
  });
  it("a count inside a size level is counted from the first line, not from the fish over the size (the order flip)", () => {
    expect(breadcrumb([size(1, "may be over 50 cm (20 in)"), { tier: "count", n: 1, short: "up to 1 brown trout combined" }], 2))
      .toBe("1 of them may be over 50 cm (20 in); up to 1 brown trout combined");
  });
  it("the fold: a level at or over its parent's number caps nothing further", () => {
    expect(breadcrumb([{ tier: "count", n: 2, short: "up to 2 brown trout" }], 2)).toBe("");
    expect(breadcrumb([{ tier: "own", n: 3, short: "up to 3 lake trout" }, { tier: "count", n: 3, short: "up to 3 x" }], 5)).toBe("up to 3 lake trout");
    expect(breadcrumb([size(2)], 2)).toBe("");
  });
  it("no number in a breadcrumb is over the number it counts from", () => {
    const got = breadcrumb([{ tier: "own", n: 3, short: "up to 3 lake trout" }, size(1, "may be over 50 cm (20 in)"),
      { tier: "count", n: 5, short: "up to 5 y" }], 4);
    expect(got).toBe("up to 3 lake trout; of those, 1 may be over 50 cm (20 in)");
    for (const m of got.replace(/\d+ cm \(\d+ in\)/g, "").matchAll(/\b(\d+)\b/g)) expect(+m[1]!).toBeLessThanOrEqual(4);
  });
});

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

/** One group article at content 200..1700 with a share tier (said by the group line), a size tier inside it (head 380,
 *  rows to 900) holding a count tier (head 450, rows to 800), then a water's own tier (head 1100, rows to 1250). */
const LINE1: PinLine = { cls: "g", text: "Trout and char · 2 a day from streams", sub: null, said: ["t1"], n: 2 };
const art = (over: Partial<PinAnchor> = {}): PinAnchor => ({
  row: "r0", group: true, tree: true, formA: false, headlineN: 2, top: 200, height: 1500,
  groupLine: LINE1, pin: { top: 200, height: 70 },
  tiers: [
    { tid: "t1", tier: "share", n: 2, head: { top: 300, height: 60 }, end: 1000, short: "2 of them from streams" },
    { tid: "t2", tier: "size", n: 1, head: { top: 380, height: 60 }, end: 900, short: "may be over 50 cm (20 in)" },
    { tid: "t3", tier: "count", n: 1, head: { top: 450, height: 40 }, end: 800, short: "up to 1 brown trout combined" },
    { tid: "t4", tier: "own", n: 1, head: { top: 1100, height: 60 }, end: 1250, short: "up to 1 lake trout" },
  ],
  ...over,
});
const H26 = (l: PinLine) => pinLineEstimate(l);
const texts = (ls: readonly PinLine[]) => ls.map((l) => l.text);

describe("pinChain (T481: ONE bar, at most two lines)", () => {
  it("nothing before the article's head has gone under the bar", () => {
    expect(pinChain([art()], 0, VP, H26)).toEqual([]);
    expect(pinChain([art()], 80, VP, H26)).toEqual([]);   // 200 - 80 = 120: not < 119
  });
  it("the share is the first line's: it never pins on its own", () => {
    expect(texts(pinChain([art()], 200, VP, H26))).toEqual([LINE1.text]);
  });
  it("line 2 is the breadcrumb of every tier the scroll is inside, in path order", () => {
    // B = 119 + 26 = 145: the size head (380) is under it from y 236, the count head (450) from 306
    expect(texts(pinChain([art()], 235, VP, H26))).toEqual([LINE1.text]);
    const size = pinChain([art()], 236, VP, H26);
    expect(texts(size)).toEqual([LINE1.text, "↳ 1 of them may be over 50 cm (20 in)"]);
    expect(size[1]!.tier).toBe("size");
    expect(texts(pinChain([art()], 306, VP, H26))).toEqual([LINE1.text, "↳ 1 of them may be over 50 cm (20 in); up to 1 brown trout combined"]);
  });
  it("a level leaves the breadcrumb when its section's last row is within one line + PIN_ROW of the bar's foot", () => {
    // count: 800 - y > 145 + 26 + 12 → y < 617; size: 900 - y > 183 → y < 717
    expect(texts(pinChain([art()], 616, VP, H26))).toHaveLength(2);
    expect(texts(pinChain([art()], 617, VP, H26))).toEqual([LINE1.text, "↳ 1 of them may be over 50 cm (20 in)"]);
    expect(texts(pinChain([art()], 717, VP, H26))).toEqual([LINE1.text]);
  });
  it("never more than two lines", () => {
    for (let y = 0; y < 1700; y += 7) expect(pinChain([art()], y, VP, H26).length).toBeLessThanOrEqual(2);
  });
  it("a tier the first line says never pins (a water's own tier beside the share)", () => {
    const a = art({ groupLine: { ...LINE1, said: ["t1", "t4"] } });
    expect(texts(pinChain([a], 1000, VP, H26))).toEqual([LINE1.text]);
    expect(texts(pinChain([art()], 1000, VP, H26))).toEqual([LINE1.text, "↳ up to 1 lake trout"]);
  });
  it("the stack's MEASURED height decides: a taller group line pushes every test down", () => {
    const tall = (l: PinLine) => (l.cls === "c" ? 26 : 60);
    expect(texts(pinChain([art()], 230, VP, H26))).toEqual([LINE1.text]);   // 380-230 = 150 ≥ 145
    expect(texts(pinChain([art()], 230, VP, tall))).toEqual([LINE1.text, "↳ 1 of them may be over 50 cm (20 in)"]);
  });
  it("a tier with no head or no rows never pins", () => {
    const a = art({ tiers: [{ ...art().tiers[1]!, head: null }, { ...art().tiers[2]!, end: null }] });
    expect(pinChain([a], 400, VP, H26)).toHaveLength(1);
  });
  it("the article is the first whose box spans the bar's line (top < header, bottom > header + LH)", () => {
    const second = art({ row: "r1", top: 1700, height: 400, pin: { top: 1700, height: 70 }, tiers: [],
      groupLine: { cls: "g", text: "Whitefish · 15 a day", sub: null } });
    expect(texts(pinChain([art(), second], 1550, VP, H26))).toEqual([LINE1.text]);   // its bottom 1700-1550 = 150 > 119+26
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
