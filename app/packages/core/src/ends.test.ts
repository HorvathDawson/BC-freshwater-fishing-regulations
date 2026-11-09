/**
 * (T517, owner 2026-11-09) THE ONE NAME FUNCTION: every part name is composed from its two end tokens and the release's
 * `display.ends` — a part, ends said at their place, a part with no ends (its water's name), a span the live salmon
 * joins. Whole strings (AGENTS rule 33).
 */
import { describe, expect, it } from "vitest";
import { endWords, spanName, type EndsTable } from "./ends";

const ENDS: EndsTable = {
  mouth: ["Mouth"],
  "split:cable": ["Cable car crossing", "Cable car crossing (200 m downstream of Vedder Crossing bridge)"],
  "split:bridge": ["Vedder Crossing Bridge"],
  // a token whose LOWER-end words read otherwise (pipeline `end_table`: `to:<words>` last)
  "lake:chilliwack": ["Chilliwack Lake outlet", "Chilliwack Lake", "to:Chilliwack Lake inlet"],
  source: ["Source"],
  "area:reserve": ["Chilliwack River Ecological Reserve boundary"],
  "bc_border@km72.1": ["B.C. border (km 72.1)"],
  "bc_border@km80": ["B.C. border (km 80)"],
};
const W = "Chilliwack River";

describe("endWords: one end token in words", () => {
  it("the upper end its first words; the lower end its `to:` words where it has them, else its first", () => {
    expect([endWords(ENDS, "lake:chilliwack", "hi"), endWords(ENDS, "lake:chilliwack", "lo"),
            endWords(ENDS, "split:cable", "lo"), endWords(ENDS, "split:cable", "hi")])
      .toEqual(["Chilliwack Lake outlet", "Chilliwack Lake inlet", "Cable car crossing", "Cable car crossing"]);
  });
  it("a token the release does not hold throws, naming it (never the code said as a name)", () => {
    expect(() => endWords(ENDS, "split:gone", "hi")).toThrow("ends: token split:gone is not in this release's display.ends");
  });
});

describe("spanName: the one name of a part or a joined span", () => {
  it("one part: lower end → upper end; the lower end in its `to:` words", () => {
    expect(spanName(ENDS, W, { lo: "mouth", hi: "split:cable" })).toBe("Mouth → Cable car crossing");
    expect(spanName(ENDS, W, { lo: "lake:chilliwack", hi: "source" })).toBe("Chilliwack Lake inlet → Source");
  });
  it("(owner 2026-11-09: no \"(n)\") ends that would collide are distinct tokens said at their place", () => {
    expect([spanName(ENDS, W, { lo: "bc_border@km72.1", hi: "bc_border@km80" }),
            spanName(ENDS, W, { lo: "bc_border@km80", hi: "source" })])
      .toEqual(["B.C. border (km 72.1) → B.C. border (km 80)", "B.C. border (km 80) → Source"]);
  });
  it("lo = hi is not special (no \"Within\"): an area token both ends says itself both ends", () => {
    expect(spanName(ENDS, W, { lo: "area:reserve", hi: "area:reserve" }))
      .toBe("Chilliwack River Ecological Reserve boundary → Chilliwack River Ecological Reserve boundary");
  });
  it("no ends (a lake, a whole water, an unkeyed part): its WATER's name — no stored name exists", () => {
    expect(spanName(ENDS, W, { lo: null, hi: null })).toBe(W);
  });
  it("one end only, or a joined span without both ends: throws, naming it", () => {
    expect(() => spanName(ENDS, W, { lo: "mouth", hi: null })).toThrow("ends: a part of Chilliwack River with no upper end");
    expect(() => spanName(ENDS, W, { lo: null, hi: null }, { lo: null, hi: null }))
      .toThrow("ends: a joined span of Chilliwack River with no lower end");
  });
  it("a joined span: the first piece's lower end → the last piece's upper end", () => {
    expect(spanName(ENDS, W, { lo: "mouth", hi: "split:cable" }, { lo: "split:cable", hi: "split:bridge" }))
      .toBe("Mouth → Vedder Crossing Bridge");
  });
});
