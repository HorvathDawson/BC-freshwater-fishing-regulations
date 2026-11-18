/**
 * T29 — the anchors: what the Keep tab registers (GroupCard through AnchorContext), and how measured boxes become the
 * pin store's `PinAnchor[]` (prototype pinTops / secEnd) and the tab row's section tops. jsdom has no layout, so the
 * boxes here are hand-set; the geometry the golden prototype measured is replayed in pinStore.test / PinBar.test.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import type { RegsPage } from "@app/core";
import { LIGHT } from "../theme";
import { KeepTab } from "./KeepTab";
import { NO_ACTIONS } from "./LineText";
import { AnchorContext, anchorsOf, sectionsOf, type AnchorKey, type AnchorSink } from "./useAnchors";
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
  // (T810 cutover) retired, the three cases over the Vedder's trout group ("vedder_1010 registers its article, head block,
  // each tier's header …", "boxes → PinAnchor: …", "a day change that re-nests a reused element id …"): only a Keep-tab
  // group card registers pin anchors, and no Keep tab draws one since the cutover (the trout table has no pin bar; the
  // groups left are in the Other fish list). No table test covers them: the pin bar has no subject on a real page.
  // PinBar.test / pinStore.test still hold the bar over their own lines.
  it("(T810 cutover) the Vedder's Keep tab, both days, registers no pin anchor: the table draws no group, tier or row anchor", () => {
    expect([[...registered(page).keys()], [...registered(page515).keys()]]).toEqual([[], []]);
  });
  it("a group not laid out yet has no anchor; section tops and the methods height come from their boxes", () => {
    expect(anchorsOf(page.keep.groups, new Map())).toEqual([]);
    const s = (tab: "gear" | "methods" | "licence", top: number, height: number) =>
      [`section:${tab}`, { key: { k: "section" as const, tab }, box: { top, height } }] as const;
    expect(sectionsOf(new Map([s("gear", 1500, 600), s("methods", 2100, 400), s("licence", 2700, 500)])))
      .toEqual({ gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700 });
    expect(sectionsOf(new Map([s("gear", 1500, 600)]))).toEqual({ gear: 1500, methods: null, methodsHeight: 0, licence: null });
  });
});
