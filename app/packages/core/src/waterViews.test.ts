/**
 * A water's views: the one order the strip draws and the swipe walks, every step from every view, both ends.
 */
import { describe, expect, it } from "vitest";
import { TO_REGULATIONS, WATER_BACK, WATER_VIEWS, WATER_VIEWS_LABEL, WATER_VIEW_LABEL, stepView, waterExit,
         type WaterView } from "./waterViews";

describe("the views and their words", () => {
  it("Regulations · Conditions · Map, left to right (owner 2026-10-21)", () => {
    expect(WATER_VIEWS.map((v) => [v, WATER_VIEW_LABEL[v]]))
      .toEqual([["regs", "Regulations"], ["conditions", "Conditions"], ["map", "Map"]]);
    expect({ WATER_VIEWS_LABEL, TO_REGULATIONS }).toEqual({
      WATER_VIEWS_LABEL: "Views of this water",
      TO_REGULATIONS: { label: "Regulations", hint: "The fishing regulations for this water" },
    });
  });
});

describe("leaving a water (owner 2026-10-22 late): ‹ one layer, × all of them", () => {
  it("the first water's ‹ names the tab it was opened from; a water opened inside another says Back; × always names the tab", () => {
    expect([WATER_BACK, waterExit(1, "Map"), waterExit(2, "Conditions"), waterExit(5, "Map")]).toEqual([
      "Back",
      { back: "Map", close: "Close, back to Map" },
      { back: "Back", close: "Close, back to Conditions" },
      { back: "Back", close: "Close, back to Map" },
    ]);
  });

  it("(owner 2026-10-22 late) a water over an OPEN SEARCH (a map tap beside it) names search, which is where back goes; " +
     "a water opened from a result never is (the search was consumed: it names the map)", () => {
    expect([waterExit(1, "Map", true), waterExit(2, "Conditions", true), waterExit(1, "Map", false)]).toEqual([
      { back: "Search", close: "Close, back to Search" },
      { back: "Back", close: "Close, back to Search" },
      { back: "Map", close: "Close, back to Map" },
    ]);
  });
});

describe("stepView", () => {
  it("next is the view to the right, prev the view to the left; nothing past either end (no wrap)", () => {
    const rows = WATER_VIEWS.flatMap((v) => (["next", "prev"] as const).map((d) => [v, d, stepView(v, d)]));
    expect(rows).toEqual([
      ["regs", "next", "conditions"], ["regs", "prev", null],
      ["conditions", "next", "map"], ["conditions", "prev", "regs"],
      ["map", "next", null], ["map", "prev", "conditions"],
    ]);
  });
  it("within a narrower set keeps the one order (the Conditions tab's reach offers Regulations and Conditions)", () => {
    const two: WaterView[] = ["conditions", "regs"];            // given out of order: the order is still the strip's
    expect([stepView("conditions", "prev", two), stepView("conditions", "next", two), stepView("regs", "next", two),
            stepView("map", "prev", two), stepView("regs", "next", ["regs", "map"])])
      .toEqual(["regs", null, "conditions", null, "map"]);
  });
});
