/**
 * The word table: frozen, and every fixed word a presenter may say is in ALL_WORDS (the set the no-hand-facts render
 * test subtracts against). The tab labels are also compared with every golden state in
 * conformance/src/regs/line.golden.test.ts (core cannot read files).
 */
import { describe, expect, it } from "vitest";
import { MONTHS, WEEKDAYS } from "../calendar.generated";
import { ALL_WORDS, MONTH1, WD3, WORDS, wordsIn } from "./words";

describe("WORDS", () => {
  it("is frozen, and every value is a non-empty string or a function", () => {
    expect(Object.isFrozen(WORDS)).toBe(true);
    for (const [k, v] of Object.entries(WORDS)) {
      if (typeof v === "string") expect(v.length, k).toBeGreaterThan(0);
      else expect(typeof v, k).toBe("function");
    }
  });

  it("names the four tabs in the page's order", () => {
    expect([WORDS.TAB_KEEP, WORDS.TAB_GEAR, WORDS.TAB_METHODS, WORDS.TAB_LICENCE])
      .toEqual(["What you can keep", "Gear", "Methods", "Licence"]);
  });
});

describe("ALL_WORDS", () => {
  it("holds every string constant, the short weekdays and the month initials", () => {
    expect(ALL_WORDS.has("What you can keep")).toBe(true);
    expect(ALL_WORDS.has("Rule book")).toBe(true);
    expect(ALL_WORDS.has("Fri")).toBe(true);
    expect(ALL_WORDS.has("J")).toBe(true);
    for (const v of Object.values(WORDS)) if (typeof v === "string") expect(ALL_WORDS.has(v), v).toBe(true);
  });

  it("derives the short weekdays and month initials from the generated calendar", () => {
    expect(WD3).toEqual(WEEKDAYS.map((w) => w.slice(0, 3)));
    expect(WD3.join(" ")).toBe("Mon Tue Wed Thu Fri Sat Sun");
    expect(MONTH1).toEqual(MONTHS.map((m) => m[0]));
    expect(MONTH1.join("")).toBe("JFMAMJJASOND");
  });
});

describe("wordsIn: the fixed words of a Line, data left out", () => {
  it("returns text and link words and the fixed words of term and book runs, never a number", () => {
    expect(wordsIn([
      { t: "text", s: "Only " }, { t: "num", s: "2", size: "inline" }, { t: "term", ids: ["daily_quota"] },
      { t: "term", ids: ["a", "b"] }, { t: "book", printed: 21, pdf: 23 }, { t: "book", printed: 80, pdf: 86, bare: true },
      { t: "link", s: "How to tell ›", open: "hatch" },
    ])).toEqual(["Only ", "ⓘ", "What these words mean", "Rule book", "How to tell ›"]);
  });
});
