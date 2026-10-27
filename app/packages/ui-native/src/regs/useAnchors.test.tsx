/**
 * T29 — the anchors: what the Keep tab registers (GroupCard through AnchorContext), and how measured boxes become the
 * pin store's `PinAnchor[]` (prototype pinTops / secEnd) and the tab row's section tops. jsdom has no layout, so the
 * boxes here are hand-set; the geometry the golden prototype measured is replayed in pinStore.test / PinBar.test.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { useRef } from "react";
import type { View } from "react-native";
import type { PinAnchor } from "@app/core";
import type { RegsPage } from "@app/core";
import { LIGHT } from "../theme";
import { KeepTab } from "./KeepTab";
import { NO_ACTIONS } from "./LineText";
import { AnchorContext, anchorsOf, sectionsOf, useAnchors, type AnchorKey, type AnchorSink, type Box } from "./useAnchors";
import type { PinStore } from "./pinStore";
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
  it("vedder_1010 registers its article, head block, each tier's header (with the sections it lies in) and each row", () => {
    expect(Object.fromEntries(registered(page))).toEqual({
      g0: { k: "group", row: 0 },
      p0: { k: "pin", row: 0 },
      "h0:t3": { k: "head", row: 0, tid: "t3", tids: ["t3"] },
      "h0:t4": { k: "head", row: 0, tid: "t4", tids: ["t3", "t4"] },
      "box:0:t4": { k: "box" },
      "k0:.0.0.0": { k: "krow", row: 0, tids: ["t3", "t4"] },
      "k0:.0.0.1": { k: "krow", row: 0, tids: ["t3", "t4"] },
      "k0:.0.0.2": { k: "krow", row: 0, tids: ["t3", "t4"] },
      "h0:t2": { k: "head", row: 0, tid: "t2", tids: ["t2"] },
      "k0:.1.0": { k: "krow", row: 0, tids: ["t2"] },
    });
  });

  it("boxes → PinAnchor: the group's strings and form from the VM, a tier's end = the bottom of its last row or tier head", () => {
    const boxes = new Map<string, { key: AnchorKey; box: Box }>();
    const put = (id: string, top: number, height: number) => boxes.set(id, { key: registered(page).get(id)!, box: { top, height } });
    put("g0", 100, 1400); put("p0", 100, 120);
    put("h0:t3", 230, 0); put("h0:t4", 230, 60); put("k0:.0.0.0", 290, 80); put("k0:.0.0.1", 370, 80); put("k0:.0.0.2", 450, 90);
    put("h0:t2", 560, 70); put("k0:.1.0", 630, 100);
    cleanup();
    const g = page.keep.groups[0]!;
    expect(anchorsOf(page.keep.groups, boxes)).toEqual([{
      row: "r0", group: true, tree: true, formA: true, headlineN: 2, top: 100, height: 1400, groupLine: g.pinLine,
      pin: { top: 100, height: 120 },
      tiers: [
        // the folded share: its header has no height, so its end is its rows' (540), its number the stored 2
        { tid: "t3", tier: "share", n: 2, head: { top: 230, height: 0 }, end: 540, short: "2 of them from streams" },
        { tid: "t4", tier: "size", n: 1, head: { top: 230, height: 60 }, end: 540, short: "of those, 1 over 50 cm (2 if one is a hatchery steelhead)" },
        { tid: "t2", tier: "own", n: 4, head: { top: 560, height: 70 }, end: 730,
          short: "4 hatchery rainbow trout a day under this river’s rule, not part of the 2 from streams" },
      ],
    }]);
  });

  it("a group not laid out yet has no anchor; section tops and the methods height come from their boxes", () => {
    expect(anchorsOf(page.keep.groups, new Map())).toEqual([]);
    const s = (tab: "gear" | "methods" | "licence", top: number, height: number) =>
      [`section:${tab}`, { key: { k: "section" as const, tab }, box: { top, height } }] as const;
    expect(sectionsOf(new Map([s("gear", 1500, 600), s("methods", 2100, 400), s("licence", 2700, 500)])))
      .toEqual({ gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700 });
    expect(sectionsOf(new Map([s("gear", 1500, 600)]))).toEqual({ gear: 1500, methods: null, methodsHeight: 0, licence: null });
  });

  it("a day change that re-nests a reused element id: the anchors use the NEW page's tier sections, not the first ones", () => {
    // the same ids come back on vedder_515 with other tiers: k0:.0.0.0 is in [t3,t4] on 10-10 and in [t2,t3] on 5-15
    const k1010 = registered(page), k515 = registered(page515);
    cleanup();
    expect(k1010.get("k0:.0.0.0")).not.toEqual(k515.get("k0:.0.0.0"));
    // a fixed geometry per id: every element measures the same on both days
    const ids = [...k515.keys()];
    const geo = (id: string): Box => ({ top: 100 + ids.indexOf(id) * 100, height: id.startsWith("g") ? 5000 : 80 });
    const fake = (id: string) => ({
      measureLayout: (_c: unknown, ok: (x: number, y: number, w: number, h: number) => void) => {
        const b = geo(id); ok(0, b.top, 0, b.height);
      },
    }) as unknown as View;
    let last: readonly PinAnchor[] = [];
    const store = { setSections() {}, setBoxes() {}, setAnchors(a: readonly PinAnchor[]) { last = a; } } as unknown as PinStore;
    function Harness({ p }: { p: RegsPage }) {
      const content = useRef<View | null>({} as View);
      const { sink } = useAnchors(p, content, store);
      // hands the real sink a node with a layout (jsdom has none); one wrapper per id, like a stable ref
      const wrap = useRef(new Map<string, (n: View | null) => void>()).current;
      const faked: AnchorSink = {
        ref(id, key) {
          const f = sink.ref(id, key);
          if (!wrap.has(id)) wrap.set(id, (n) => f(n ? fake(id) : null));
          return wrap.get(id)!;
        },
        changed: sink.changed,
      };
      return <AnchorContext.Provider value={faked}>
        <KeepTab keep={p.keep} dateLabel={p.header.date} palette={LIGHT} actions={NO_ACTIONS} />
      </AnchorContext.Provider>;
    }
    vi.useFakeTimers();
    try {
      const r = render(<Harness p={page} />);
      act(() => { vi.advanceTimersByTime(50); });
      r.rerender(<Harness p={page515} />);
      act(() => { vi.advanceTimersByTime(50); });
    } finally { vi.useRealTimers(); }
    const boxes = new Map([...k515].map(([id, key]) => [id, { key, box: geo(id) }] as const));
    expect(last).toEqual(anchorsOf(page515.keep.groups, boxes));
  });
});
