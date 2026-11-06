/**
 * T29 — the pin store's own contract, over vedder_1010's anchors and measured line heights (fixtures/pin-geometry.json, app
 * coordinates): notify only on a change, a stable lines reference, the tab in view, a press, the stack height. (T439: the
 * replay of every golden state's bars, with golden-pins.json, is gone; core pinChain's own tests are core/src/regs/pins.test.ts.)
 */
import { describe, expect, it } from "vitest";
import type { PinAnchor } from "@app/core";
import { createPinStore, pinLineKey } from "./pinStore";
import slice from "./fixtures/pin-geometry.json";

type Slice = { heights: Record<string, number>;
  states: Record<string, { viewport: { height: number }; anchors: PinAnchor[] }> };
const P = slice as unknown as Slice;
const HEIGHTS = new Map(Object.entries(P.heights));

describe("pin store contract", () => {
  const vedder = P.states.vedder_1010!;
  const mk = () => {
    const store = createPinStore({ heights: HEIGHTS });
    let n = 0;
    store.subscribe(() => { n++; });
    store.setViewport(584);
    store.setAnchors(vedder.anchors);
    return { store, count: () => n };
  };

  it("notifies only when the bar or the tab changes; the lines keep one reference while they stand", () => {
    const { store, count } = mk();
    const before = count();
    store.setScroll(128);
    const first = store.getLines();
    expect([count() - before, first.map((l) => l.text)]).toEqual([1, ["Trout and char · 2 a day from streams"]]);
    for (const y of [130, 140, 150, 160, 170]) store.setScroll(y);
    expect([count() - before, store.getLines() === first]).toEqual([1, true]);
    store.setScroll(240);
    expect([count() - before, store.getLines().map((l) => l.text)])
      .toEqual([2, ["Trout and char · 2 a day from streams", "↳ of those, 1 over 50 cm (2 if one is a hatchery steelhead)"]]);
  });

  it("a tier line carries its tier (the size line wears the rule)", () => {
    const { store } = mk();
    store.setScroll(240);
    expect(store.getLines().map((l) => [l.cls, l.tier ?? null])).toEqual([["g two", null], ["c", "size"]]);
  });

  it("a measured line height (PIN_HC) recomputes; a zero height (not laid out) is ignored", () => {
    const store = createPinStore();
    let n = 0;
    store.subscribe(() => { n++; });
    store.setAnchors(vedder.anchors);
    store.setScroll(240);
    const g = store.getLines()[0]!;
    const k = n;
    store.setLineHeight(g, 0);
    store.setLineHeight(g, -3);
    expect(n).toBe(k);
    expect(pinLineKey(g)).toBe("g two|Trout and char · 2 a day from streams|(of 4 across Region 2)");
  });

  it("the tab in view (prototype mini): the last section whose top reached the bar; the end of the page is the licence", () => {
    const { store } = mk();
    store.setContentHeight(3300);
    store.setSections({ gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700 });
    const at = (y: number) => { store.setScroll(y); return store.getTab(); };
    expect([at(0), at(1487), at(1488), at(2088), at(2688), at(2716)]).toEqual(["keep", "keep", "gear", "methods", "licence", "licence"]);
  });

  it("a press selects at once and notifies; the next scroll re-reads the tab in view", () => {
    const { store, count } = mk();
    store.setContentHeight(3300);
    store.setSections({ gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700 });
    const k = count();
    store.selectTab("licence");
    expect([store.getTab(), count() - k]).toEqual(["licence", 1]);
    store.setScroll(10);
    expect(store.getTab()).toBe("keep");
  });

  it("the stack height is the bar's while it shows lines, 0 when empty", () => {
    const { store } = mk();
    store.setScroll(128);
    store.setStackHeight(43);
    expect(store.getStackHeight()).toBe(43);
    store.setScroll(0);
    expect(store.getStackHeight()).toBe(0);
  });
});
