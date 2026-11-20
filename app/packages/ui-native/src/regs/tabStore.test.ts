/**
 * T29 — the tab store's own contract: the tab in view, a press, notify only on a change. (T810 cutover) The pin bar's
 * half (its lines, their heights, the stack) left with the bar (owner 2026-11-17 23:29); core tabInView's own tests are
 * core/src/regs/tabScroll.test.ts.
 */
import { describe, expect, it } from "vitest";
import { createTabStore } from "./tabStore";

describe("tab store contract", () => {
  const mk = () => {
    const store = createTabStore();
    let n = 0;
    store.subscribe(() => { n++; });
    store.setViewport(584);
    store.setContentHeight(3300);
    store.setSections({ gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700 });
    return { store, count: () => n };
  };

  it("the tab in view (prototype mini): the last section whose top reached the top; the end of the page is the licence", () => {
    const { store } = mk();
    const at = (y: number) => { store.setScroll(y); return store.getTab(); };
    expect([at(0), at(1487), at(1488), at(2088), at(2688), at(2716)]).toEqual(["keep", "keep", "gear", "methods", "licence", "licence"]);
  });

  it("notifies only when the tab changes", () => {
    const { store, count } = mk();
    const k = count();
    store.setScroll(10); store.setScroll(20);
    expect(count() - k).toBe(0);
    store.setScroll(1600);
    expect([store.getTab(), count() - k]).toEqual(["gear", 1]);
  });

  it("a press selects at once and notifies; the next scroll re-reads the tab in view", () => {
    const { store, count } = mk();
    const k = count();
    store.selectTab("licence");
    expect([store.getTab(), count() - k]).toEqual(["licence", 1]);
    store.setScroll(10);
    expect(store.getTab()).toBe("keep");
  });
});
