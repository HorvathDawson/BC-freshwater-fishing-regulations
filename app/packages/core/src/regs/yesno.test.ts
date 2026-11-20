/** T31 prototype yesNo: ✓ then ✕, an empty column omitted, an empty note is none. */
import { describe, expect, it } from "vitest";
import { sentence, yesNo } from "./yesno";

describe("yesNo", () => {
  it("one ✕ column when nothing is allowed", () => {
    expect(yesNo([], [["Nets", ""]])).toEqual([{ col: "no", head: "Not allowed", inline: false, items: [{ mark: "✕", name: "Nets", notes: [], terms: [] }] }]);
  });
  it("✓ items first, each with its note", () => {
    expect(yesNo([["Worms", ""], ["Roe (fish eggs)", "carry up to 1 kg"]], [["Fish or fish parts", ""]])).toEqual([
      { col: "ok", head: "Allowed", inline: false, items: [{ mark: "✓", name: "Worms", notes: [], terms: [] }, { mark: "✓", name: "Roe (fish eggs)", notes: ["Carry up to 1 kg."], terms: [] }] },
      { col: "no", head: "Not allowed", inline: false, items: [{ mark: "✕", name: "Fish or fish parts", notes: [], terms: [] }] },
    ]);
  });
  it("(T35) an item's glossary ⓘ ids ride with it (the set_line ⓘ after an allowed \"Set lines\"); none: []", () => {
    expect(yesNo([["Set lines", "", ["set_line"]], ["Nets", "under ice"]], [])).toEqual([
      { col: "ok", head: "Allowed", inline: false, items: [{ mark: "✓", name: "Set lines", notes: [], terms: ["set_line"] }, { mark: "✓", name: "Nets", notes: ["Under ice."], terms: [] }] },
    ]);
  });
  it("(T922) several conditions are several items, each a capital first and a full stop last; one already stopped is not stopped twice", () => {
    const v = yesNo([["Ice fishing", ["lines: 1", "hook, lure or fly per line: 1", "Warn others of your ice hole.", ""]]], []);
    expect(v[0]!.items[0]!.notes).toEqual(["Lines: 1.", "Hook, lure or fly per line: 1.", "Warn others of your ice hole."]);
    expect(sentence("with a light: submerged, within 1 m of the hook")).toBe("With a light: submerged, within 1 m of the hook.");
    expect(sentence("")).toBe("");
  });
  it("nothing: no columns", () => {
    expect(yesNo([], [])).toEqual([]);
  });
});
