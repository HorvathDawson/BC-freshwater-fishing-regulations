/** T31 prototype yesNo: ✓ then ✕, an empty column omitted, an empty note is none. */
import { describe, expect, it } from "vitest";
import { yesNo } from "./yesno";

describe("yesNo", () => {
  it("one ✕ column when nothing is allowed", () => {
    expect(yesNo([], [["Nets", ""]])).toEqual([{ col: "no", head: "Not allowed", items: [{ mark: "✕", name: "Nets", note: null }] }]);
  });
  it("✓ items first, each with its note", () => {
    expect(yesNo([["Worms", ""], ["Roe (fish eggs)", "carry up to 1 kg"]], [["Fish or fish parts", ""]])).toEqual([
      { col: "ok", head: "Allowed", items: [{ mark: "✓", name: "Worms", note: null }, { mark: "✓", name: "Roe (fish eggs)", note: "carry up to 1 kg" }] },
      { col: "no", head: "Not allowed", items: [{ mark: "✕", name: "Fish or fish parts", note: null }] },
    ]);
  });
  it("nothing: no columns", () => {
    expect(yesNo([], [])).toEqual([]);
  });
});
