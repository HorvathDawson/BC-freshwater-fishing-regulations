/**
 * T06: species words (page_v36 expand, spName, lc, groupFor, join, lcNames, Names, writtenName, cap, clean) over a
 * literal subset of `export.species`. Every expected value is a whole composed string (AGENTS 33).
 */
import { describe, expect, it } from "vitest";
import { cap, clean, expand, gameFish, groupFor, join, lc, lcNames, Names, spName, writtenName } from "./names";
import { BOOK } from "./testing/speciesBook";
import { WORDS } from "./words";

describe("expand, spName, gameFish", () => {
  it("expands a group to its members, keeps a fish code, drops duplicates, and takes null", () => {
    expect(expand(BOOK, ["CHAR", "EB", "KO"])).toEqual(["DV", "LT", "EB", "KO"]);
    expect(expand(BOOK, null)).toEqual([]);
    expect(expand(BOOK, undefined)).toEqual([]);
  });

  it("names a fish, a group, a protected species, a salmon, and says an unknown code back", () => {
    expect(spName(BOOK, "EB")).toBe("Brook trout");
    expect(spName(BOOK, "TROUT_CHAR")).toBe("Trout and char");
    expect(spName(BOOK, "GREEN_STURGEON")).toBe("Green sturgeon");
    expect(spName(BOOK, "CH")).toBe("Chinook");
    expect(spName(BOOK, "ZZ")).toBe("ZZ");
  });

  it("game fish are ALL_GAME_FISH's members", () => {
    expect(gameFish(BOOK)).toBe(BOOK.groups.ALL_GAME_FISH!.members);
    expect(gameFish(BOOK).length).toBe(22);
  });
});

describe("lc, cap, clean", () => {
  it("lower-cases the first letter unless the name starts with a proper noun (WORDS.LC_KEEP)", () => {
    expect(lc("Brook trout")).toBe("brook trout");
    expect(lc("Dolly Varden/bull trout")).toBe("Dolly Varden/bull trout");
    expect(lc("Cultus Lake sculpin")).toBe("Cultus Lake sculpin");
    expect(lc("Charlotte's char")).toBe("Charlotte's char");
    expect(lc("Char")).toBe("char");
    expect(WORDS.LC_KEEP.length).toBe(16);
  });

  it("cap upper-cases the first letter; clean drops asterisks and folds white space", () => {
    expect(cap("brook trout")).toBe("Brook trout");
    expect(cap("")).toBe("");
    expect(clean(" Kokanee catch\nand release **Monday**  only ")).toBe("Kokanee catch and release Monday only");
    expect(clean(null)).toBe("");
    expect(clean(undefined)).toBe("");
  });
});

describe("groupFor, join, lcNames, Names", () => {
  it("names a whole closed group, never SA, never an open group, never one fish", () => {
    expect(groupFor(BOOK, ["SMB", "LMB"])).toBe("Bass");
    expect(groupFor(BOOK, ["DV", "LT", "EB"])).toBe("Char");
    expect(groupFor(BOOK, ["CH", "CULTUS_LAKE_SCULPIN"])).toBeNull();   // SA is skipped by code
    expect(groupFor(BOOK, ["DV", "LT"])).toBeNull();
    expect(groupFor(BOOK, ["KO"])).toBeNull();
    expect(groupFor(BOOK, [])).toBeNull();
  });

  it("joins with commas and the conjunction", () => {
    expect(join([])).toBe("");
    expect(join(["a"])).toBe("a");
    expect(join(["a", "b"])).toBe("a and b");
    expect(join(["a", "b", "c"], " or ")).toBe("a, b or c");
  });

  it("says species in the page's words (T06 test 1)", () => {
    expect(lcNames(BOOK, ["EB", "GB"])).toBe("brook trout and brown trout");
    expect(lcNames(BOOK, ["DV", "LT"])).toBe("Dolly Varden/bull trout and lake trout");
    expect(lcNames(BOOK, ["RB", "CT", "EB", "GB", "DV"], " or ", 3)).toBe("rainbow trout, cutthroat trout + 3 more");
    expect(lcNames(BOOK, ["RB", "CT", "EB"], " or ", 3)).toBe("rainbow trout, cutthroat trout or brook trout");
    expect(lcNames(BOOK, ["LMB", "SMB"])).toBe("bass");
    expect(Names(BOOK, ["EB", "GB"])).toBe("Brook trout and brown trout");
    expect(Names(BOOK, ["DV", "KO"], " or ")).toBe("Dolly Varden/bull trout or kokanee");
  });
});

describe("writtenName (T06 test 2)", () => {
  it("says 'trout' for trout-and-char except char (rix 3067), 'fish' for no species, and the codes' names otherwise", () => {
    expect(writtenName(BOOK, { species: ["TROUT_CHAR"], species_except: ["CHAR"] })).toBe("trout");
    expect(writtenName(BOOK, { species: [] })).toBe("fish");
    expect(writtenName(BOOK, {})).toBe("fish");
    expect(writtenName(BOOK, { species: ["TROUT_CHAR"], species_except: ["ST"] })).toBe("trout and char");
    expect(writtenName(BOOK, { species: ["DV", "LT"] })).toBe("Dolly Varden/bull trout and lake trout");
    expect(writtenName(BOOK, { species: ["DV", "LT", "KO"] }, " or ")).toBe("Dolly Varden/bull trout, lake trout or kokanee");
  });
});
