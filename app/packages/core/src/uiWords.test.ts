import { describe, expect, it } from "vitest";
import { SPLASH_WORDS, splashLineFor } from "./uiWords";

describe("the splash line (T244, owner 2026-10-29)", () => {
  it("is short, and no line promises fish", () => {
    expect(SPLASH_WORDS.lines.filter((l) => [...l].length > 30)).toEqual([]);
    expect(SPLASH_WORDS.lines.filter((l) => /biting|catch|fish are|bite/i.test(l))).toEqual([]);
  });
  it("one line per seed, wrapping, and a bad seed gives the first", () => {
    const n = SPLASH_WORDS.lines.length;
    expect([splashLineFor(0), splashLineFor(n), splashLineFor(-1), splashLineFor(Number.NaN)])
      .toEqual([SPLASH_WORDS.lines[0], SPLASH_WORDS.lines[0], SPLASH_WORDS.lines[1], SPLASH_WORDS.lines[0]]);
  });
});
