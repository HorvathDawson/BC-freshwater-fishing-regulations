import { describe, expect, it } from "vitest";
import { GoneViews, unresolvedTag } from "./goneView";

const NULL_VIEW = (t: number) => new Error(`\`reactTag\` ${t} resolved to \`view\` null which is null or a wrong type`);

describe("(T445) a call that outlived its view", () => {
  it("reads the tag from the binding's two words for it, and nothing else", () => {
    expect([unresolvedTag(NULL_VIEW(2298)), unresolvedTag(new Error("`reactTag` 12 resolved null")),
            unresolvedTag(new Error("something else 2298")), unresolvedTag(null)]).toEqual([2298, 12, null, null]);
  });

  it("drops only a tag known to be gone: a replaced atlas source, or any once the map unmounted", () => {
    const g = new GoneViews();
    g.held(10);
    expect(g.stale(NULL_VIEW(10))).toBe(false);          // the live source: a defect, reported
    g.held(20);                                          // a style load made the source anew
    expect([g.stale(NULL_VIEW(10)), g.stale(NULL_VIEW(20)), g.stale(NULL_VIEW(99))]).toEqual([true, false, false]);
    expect(g.stale(new Error("boom"))).toBe(false);
    g.unmount();
    expect([g.stale(NULL_VIEW(99)), g.stale(new Error("boom"))]).toEqual([true, false]);
  });
});
