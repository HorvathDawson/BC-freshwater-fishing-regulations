/** T584 (T580 F6): the reader's span and model pick are the same on every way into a water (a remount keeps them). */
import { beforeEach, describe, expect, it } from "vitest";
import { act, renderHook } from "@testing-library/react";
import { resetChartPrefs, useChartPrefs } from "./chartPrefs";

beforeEach(resetChartPrefs);

describe("chart prefs", () => {
  it("start at 72 h with no model pick", () => {
    const { result } = renderHook(() => useChartPrefs());
    expect([result.current.span, result.current.pick]).toEqual(["72h", { model: null, also: [], on: null }]);
  });
  it("a new mount (a new water, a deep link, a search) reads what the last one set", () => {
    const a = renderHook(() => useChartPrefs());
    act(() => { a.result.current.setSpan("year"); a.result.current.setPick((p) => ({ ...p, model: "CLEVER" })); });
    a.unmount();
    const b = renderHook(() => useChartPrefs());
    expect([b.result.current.span, b.result.current.pick.model]).toEqual(["year", "CLEVER"]);
  });
});

import { holdHeight } from "./heldHeight";
describe("heldHeight (the block over the route map)", () => {
  it("keeps the tallest height for one key and starts again on another", () => {
    const a = holdHeight({ key: "", h: 0 }, "w:3", 180);
    const b = holdHeight(a, "w:3", 120);
    expect([a.h, b.h, holdHeight(b, "w:3", 200).h, holdHeight(b, "w:4", 90)]).toEqual([180, 180, 200, { key: "w:4", h: 90 }]);
  });
});
