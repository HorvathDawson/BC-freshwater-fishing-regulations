/**
 * The word table: frozen, and every fixed word a presenter may say is in ALL_WORDS (the set the no-hand-facts render
 * test subtracts against). The tab labels are also compared with every golden state in
 * conformance/src/regs/line.golden.test.ts (core cannot read files).
 */
import { describe, expect, it } from "vitest";
import { MONTHS, WEEKDAYS } from "../calendar.generated";
import { ALL_WORDS, MONTH1, WD3, WORDS, wordsIn } from "./words";

describe("WORDS", () => {
  it("is frozen, and every value is a non-empty string, a function or a frozen list", () => {
    expect(Object.isFrozen(WORDS)).toBe(true);
    for (const [k, v] of Object.entries(WORDS)) {
      if (typeof v === "string") expect(v.length, k).toBeGreaterThan(0);
      else if (Array.isArray(v)) expect(Object.isFrozen(v), k).toBe(true);
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

  it("holds the fixed fragments of every function word, never the data put into it (T06)", () => {
    for (const w of ["under ", " cm", "over ", " + ", " more", "(no words in the answers for ", ")", "(rule ",
      ": missing from the page’s data)", "Missing from the page’s data: rule "]) expect(ALL_WORDS.has(w), w).toBe(true);
    expect(ALL_WORDS.has("")).toBe(false);
    expect([...ALL_WORDS].some((w) => w.includes("\u0000"))).toBe(false);
  });

  it("says describe()'s words (T06)", () => {
    expect(WORDS.UNDER(50)).toBe("under 50 cm");
    expect(WORDS.OVER(50)).toBe("over 50 cm");
    expect(WORDS.MORE(3)).toBe(" + 3 more");
    expect(WORDS.NO_WORDS("z2:a::a.r1")).toBe("(no words in the answers for z2:a::a.r1)");
    expect(WORDS.MISSING_RULE(7)).toBe("(rule 7: missing from the page’s data)");
    expect(WORDS.MISSING_DATA(7)).toBe("Missing from the page’s data: rule 7");
    expect(WORDS.LC_KEEP).toEqual(["Dolly", "Arctic", "Nooksack", "Salish", "Cultus", "Enos", "Coastal", "Westslope", "Misty",
      "Vananda", "Paxton", "Hadley", "Morrison", "Rocky", "Speckled", "Charlotte"]);
    expect(ALL_WORDS.has("Dolly")).toBe(false);   // a proper-noun prefix is a match key, not a word the page says
  });

  it("holds the Gear tab's words and fragments (T30)", () => {
    for (const w of ["Lines", "Hooks", "Weight on the line", "Next", "Now", "· in force today", "Sources", "›", "No fishing here until ",
      ".", "No fishing here on any day of the year.", "B.C. rule", "Region ", " rule", " · "]) expect(ALL_WORDS.has(w), w).toBe(true);
  });

  it("holds the Bait item's words and fragments (T31)", () => {
    for (const w of ["Bait", "No natural bait of any kind while the ban runs. ", "Roe (fish eggs)", "on streams", "allowed · the ",
      " ban excepts it", "carry up to ", " kg", " is still OK", "Allowed", "Not allowed", "✓", "✕"]) expect(ALL_WORDS.has(w), w).toBe(true);
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
