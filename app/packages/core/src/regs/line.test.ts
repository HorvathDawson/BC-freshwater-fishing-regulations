/**
 * The rich-text runs every presenter emits, and `plain()` — the golden comparison form (contracts.md §1, §5).
 * Expected strings are golden values (port/golden/golden.json @ v38) where the ticket names one; the store-backed
 * comparison of the same forms against every golden page link is conformance/src/regs/line.golden.test.ts.
 */
import { describe, expect, it } from "vitest";
import { boldN, nbsp, plain, plainAll, squash, text, type Line } from "./line";

describe("plain: runs within one Line concatenate as is", () => {
  it("invents no space between two text runs, and puts ONE between separate Lines", () => {
    expect(plain([{ t: "text", s: "Open" }, { t: "text", s: "today" }])).toBe("Opentoday");
    expect(plainAll([[{ t: "text", s: "Open" }], [{ t: "text", s: "today" }]])).toBe("Open today");
  });

  it("reads a num run as its digits (golden vedder_515 keep.groups[0].head)", () => {
    const lead: Line = [{ t: "num", s: "2", size: "big" }, { t: "text", s: " a day per angler from streams" }];
    const words: Line = [{ t: "text", s: "of the " }, { t: "num", s: "4", size: "inline" },
      { t: "text", s: " a day per angler across Region 2, lakes included" }];
    expect(plain(lead)).toBe("2 a day per angler from streams");
    expect(plainAll([lead, words]))
      .toBe("2 a day per angler from streams of the 4 a day per angler across Region 2, lakes included");
  });

  it("reads a term run as ⓘ for one id and as the multi-term words for two", () => {
    const say: Run0 = { t: "text", s: "Only 2 of the 4 may be from streams" };
    expect(plain([say, { t: "term", ids: ["daily_quota"] }])).toBe("Only 2 of the 4 may be from streamsⓘ");
    expect(plain([say, { t: "term", ids: ["daily_quota", "stream"] }]))
      .toBe("Only 2 of the 4 may be from streamsWhat these words mean");
  });

  it("reads a book run as the Rule book link, bare without the label, and unlinked without a pdf page", () => {
    expect(plain([{ t: "book", printed: 21, pdf: 23 }])).toBe("Rule book p.21 ↗");
    expect(plain([{ t: "book", printed: 80, pdf: 86, bare: true }])).toBe("p.80 ↗");
    expect(plain([{ t: "book", printed: 80, pdf: null }])).toBe("p.80");
  });

  it("reads a link run as its words", () => {
    expect(plain([{ t: "text", s: "Kokanee" }, { t: "link", s: " How to tell ›", open: "hatch" }]))
      .toBe("Kokanee How to tell ›");
  });

  it("collapses whitespace and trims; null and undefined read as empty", () => {
    expect(plain(null)).toBe("");
    expect(plain(undefined)).toBe("");
    expect(plain([{ t: "text", s: " a  " }, { t: "text", s: "b" }])).toBe("a b");
    expect(plainAll([null, [{ t: "text", s: "x" }]])).toBe("x");
    expect(plainAll([])).toBe("");
    expect(squash("  a \n\t b  ")).toBe("a b");
  });

  it("builds a text run", () => {
    expect(text("Open")).toEqual({ t: "text", s: "Open" });
  });
});

type Run0 = { t: "text"; s: string };

describe("nbsp: a number keeps its unit on the same line", () => {
  it("joins cm and in to the number before them with U+00A0", () => {
    expect(nbsp("over 50 cm (20 in)")).toBe("over 50 cm (20 in)");
  });
  it("leaves a unit with no space, and a word that only starts like a unit", () => {
    expect(nbsp("50cm")).toBe("50cm");
    expect(nbsp("2 inches")).toBe("2 inches");
  });
});

describe("boldN: the sentence's number set as a num run", () => {
  it("splits the first whole-word n out of the sentence, inline by default", () => {
    expect(boldN("Only 2 of the 4 may be from streams", 2)).toEqual([
      { t: "text", s: "Only " }, { t: "num", s: "2", size: "inline" }, { t: "text", s: " of the 4 may be from streams" }]);
  });
  it("sets it big when asked, with no empty text run at either end", () => {
    expect(boldN("2 a day", 2, "big")).toEqual([{ t: "num", s: "2", size: "big" }, { t: "text", s: " a day" }]);
    expect(boldN("keep 2", 2)).toEqual([{ t: "text", s: "keep " }, { t: "num", s: "2", size: "inline" }]);
  });
  it("is one text run when n is null or absent from the sentence", () => {
    expect(boldN("No limit", null)).toEqual([{ t: "text", s: "No limit" }]);
    expect(boldN("No limit", 3)).toEqual([{ t: "text", s: "No limit" }]);
  });
  it("bolds the whole-word 2, not the 2 inside 12", () => {
    expect(boldN("12 or 2", 2)).toEqual([{ t: "text", s: "12 or " }, { t: "num", s: "2", size: "inline" }]);
  });
  it("keeps the unit with its number (the prototype nbsp's before it bolds)", () => {
    expect(boldN("Only 1 may be over 50 cm (20 in)", 1)).toEqual([
      { t: "text", s: "Only " }, { t: "num", s: "1", size: "inline" }, { t: "text", s: " may be over 50 cm (20 in)" }]);
  });
  it("loses nothing: plain(boldN(x, n)) is x", () => {
    for (const [x, n] of [["Only 2 of the 4 may be from streams", 2], ["12 or 2", 2], ["No limit", null],
      ["Up to 2 may be hatchery brook or brown trout", 2]] as const)
      expect(plain(boldN(x, n))).toBe(x);
  });
});
