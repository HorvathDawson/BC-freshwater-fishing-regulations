/**
 * T29 — the anchors: that the Keep tab registers none (T810 cutover: the pin bar is gone, owner 2026-11-17 23:29), and how
 * measured boxes become the tab row's section tops. jsdom has no layout, so the boxes here are hand-set.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import type { RegsPage } from "@app/core";
import { LIGHT } from "../theme";
import { KeepTab } from "./KeepTab";
import { NO_ACTIONS } from "./LineText";
import { AnchorContext, sectionsOf, type AnchorKey, type AnchorSink } from "./useAnchors";
import vedder1010 from "./fixtures/vedder_1010.json";
import vedder515 from "./fixtures/vedder_515.json";

afterEach(cleanup);
const page = vedder1010 as unknown as RegsPage;
const page515 = vedder515 as unknown as RegsPage;

/** what the Keep tab registers, by id */
function registered(p: RegsPage) {
  const keys = new Map<string, AnchorKey>();
  const sink: AnchorSink = { ref: (id, key) => { keys.set(id, key); return () => {}; }, changed() {} };
  render(<AnchorContext.Provider value={sink}>
    <KeepTab keep={p.keep} dateLabel={p.header.date} palette={LIGHT} actions={NO_ACTIONS} />
  </AnchorContext.Provider>);
  return keys;
}

describe("anchors", () => {
  // (T810 cutover) retired, the three cases over the Vedder's trout group (its article, tier headers and rows as pin
  // anchors; boxes → PinAnchor; a day change re-nesting an id): the pin bar is gone (owner 2026-11-17 23:29)
  it("(T810 cutover) the Vedder's Keep tab, both days, registers no pin anchor: the table draws no group, tier or row anchor", () => {
    expect([[...registered(page).keys()], [...registered(page515).keys()]]).toEqual([[], []]);
  });
  it("section tops and the methods height come from their boxes", () => {
    const s = (tab: "gear" | "methods" | "licence", top: number, height: number) =>
      [`section:${tab}`, { key: { k: "section" as const, tab }, box: { top, height } }] as const;
    expect(sectionsOf(new Map([s("gear", 1500, 600), s("methods", 2100, 400), s("licence", 2700, 500)])))
      .toEqual({ gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700 });
    expect(sectionsOf(new Map([s("gear", 1500, 600)]))).toEqual({ gear: 1500, methods: null, methodsHeight: 0, licence: null });
  });
});
