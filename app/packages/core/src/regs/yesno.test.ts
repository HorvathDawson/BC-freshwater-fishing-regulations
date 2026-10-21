/** T31 prototype yesNo: ✓ then ✕, an empty column omitted, an empty note is none. */
import { describe, expect, it } from "vitest";
import { yesNo } from "./yesno";

describe("yesNo", () => {
  it("one ✕ column when nothing is allowed", () => {
    expect(yesNo([], [["Nets", ""]])).toEqual([{ col: "no", head: "Not allowed", items: [{ mark: "✕", name: "Nets", note: null, terms: [] }] }]);
  });
  it("✓ items first, each with its note", () => {
    expect(yesNo([["Worms", ""], ["Roe (fish eggs)", "carry up to 1 kg"]], [["Fish or fish parts", ""]])).toEqual([
      { col: "ok", head: "Allowed", items: [{ mark: "✓", name: "Worms", note: null, terms: [] }, { mark: "✓", name: "Roe (fish eggs)", note: "carry up to 1 kg", terms: [] }] },
      { col: "no", head: "Not allowed", items: [{ mark: "✕", name: "Fish or fish parts", note: null, terms: [] }] },
    ]);
  });
  it("(T35) an item's glossary ⓘ ids ride with it (the set_line ⓘ after an allowed \"Set lines\"); none: []", () => {
    expect(yesNo([["Set lines", "", ["set_line"]], ["Nets", "under ice"]], [])).toEqual([
      { col: "ok", head: "Allowed", items: [{ mark: "✓", name: "Set lines", note: null, terms: ["set_line"] }, { mark: "✓", name: "Nets", note: "under ice", terms: [] }] },
    ]);
  });
  it("nothing: no columns", () => {
    expect(yesNo([], [])).toEqual([]);
  });
});
