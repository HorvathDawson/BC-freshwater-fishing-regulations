/**
 * T12: the golden comparison rule (contracts.md §1/§5, tests.md §1.2; golden ≥ b999cfd7) — store-free. Every parity
 * suite and every core unit test over a hand fixture compares through these, so they are pinned whole-string here.
 */
import { describe, expect, it } from "vitest";
import { plainAll, text, type Line } from "../line";
import { diffLines, eqPlain, norm } from "./eq";

const OVER = "Only 1 of the fish in this box may be over 50 cm (20 in)";

describe("norm", () => {
  it("collapses every whitespace run to one space and trims", () => {
    expect(norm("  Open\n  today ")).toBe("Open today");
    expect(norm("50 cm\t(20 in)")).toBe("50 cm (20 in)");
    expect(norm("")).toBe("");
  });
});

describe("eqPlain", () => {
  it("equal after norm is ok", () => {
    expect(eqPlain(["Open today"], ["Open  today"])).toEqual({ ok: true });
    expect(eqPlain([], [])).toEqual({ ok: true });
  });

  it("a missing line is a failure whose diff names line 2", () => {
    const r = eqPlain(["a", "b"], ["a"]);
    expect(r).toEqual({ ok: false, diff: [
      "@@ line 2 @@",
      '    "a"',
      '  + "b"   (actual)',
      "first difference at line 2 char 1: expected end-of-lines, got \"b\"",
    ].join("\n") });
  });

  it("an extra expected line and a changed word", () => {
    expect(eqPlain(["x", "open"], ["x", "closed", "y"])).toEqual({ ok: false, diff: [
      "@@ line 2 @@",
      '    "x"',
      '  - "closed"   (expected)',
      '  - "y"   (expected)',
      '  + "open"   (actual)',
      'first difference at line 2 char 1: expected "closed", got "open"',
    ].join("\n") });
  });

  it("whitespace-only differences never fail; a fused word does (no space is invented)", () => {
    expect(eqPlain(["streams  of the 4"], ["streams of the 4"])).toEqual({ ok: true });
    expect(eqPlain(["streamsof the 4"], ["streams of the 4"]).ok).toBe(false);
  });
});

describe("diffLines", () => {
  it("one changed line: expected with -, actual with +, the first differing char (1-based)", () => {
    expect(diffLines([`${OVER} ⓘ`], [OVER])).toBe([
      "@@ line 1 @@",
      `  - "${OVER}"   (expected)`,
      `  + "${OVER} ⓘ"   (actual)`,
      'first difference at line 1 char 57: expected end-of-line, got " ⓘ"',
    ].join("\n"));
  });

  it("context is ctx lines either side; a long tail is cut at 40 lines", () => {
    const e = Array.from({ length: 10 }, (_, i) => `l${i}`);
    const a = [...e]; a[5] = "L5";
    expect(diffLines(a, e, 1)).toBe([
      "@@ line 6 @@",
      '    "l4"',
      '  - "l5"   (expected)',
      '  + "L5"   (actual)',
      '    "l6"',
      'first difference at line 6 char 1: expected "l5", got "L5"',
    ].join("\n"));
    const many = Array.from({ length: 60 }, (_, i) => `m${i}`);
    const out = diffLines(many, [], 2).split("\n");
    expect(out.length).toBe(40);
    expect(out[38]).toBe("  … 23 more lines");
    expect(out[39]).toBe('first difference at line 1 char 1: expected end-of-lines, got "m0"');
  });

  it("equal input says so", () => {
    expect(diffLines(["a"], [" a "])).toBe("no difference");
  });
});

describe("contracts.md §1 head form a: plainAll of the lead and the words equals the golden head text", () => {
  it("golden vedder_515 keep.groups[0].head.text (≥ b999cfd7: one space where the two elements abut)", () => {
    const lead: Line = [{ t: "num", s: "2", size: "big" }, text(" a day per angler from streams")];
    const words: Line = [text("of the "), { t: "num", s: "4", size: "big" }, text(" a day per angler across Region 2, lakes included")];
    expect(eqPlain([plainAll([lead, words])],
                   ["2 a day per angler from streams of the 4 a day per angler across Region 2, lakes included"])).toEqual({ ok: true });
  });
});
