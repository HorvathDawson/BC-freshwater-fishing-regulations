/**
 * T29 — where the Keep tab's articles, group heads and tier sections are, and where the Gear / Methods / Licence sections
 * start, as CONTENT offsets of the regs ScrollView (the prototype's `pinTops` + the boxes `pinUpdate` and `mini` read),
 * handed to the pin store.
 *
 * Every anchored element registers a ref through `AnchorContext` (GroupCard: the article, its head block, each tier's
 * header, each kind / release row with the tier sections it sits in; RegsScreen: the section wrappers). After a layout
 * change the hook measures every registered node against `contentRef` (`measureLayout`: react-native-web through
 * bounding rects; native Fabric with a ref) and rebuilds the `PinAnchor[]` from the page's groups (`anchorsOf`) and the
 * section tops (`sectionsOf`). A section is measured with the rest rather than from its own onLayout: on the web its
 * onLayout fires only when its SIZE changes, not when the Keep tab above it grows and moves it.
 *
 * Nothing here reads a regulation: the strings and numbers are the VM's (GroupVM.pinLine, TierVM.short / n,
 * head.form / leadN). A measuring pass in which nothing has a height is dropped (jsdom has no layout; a test injects
 * anchors itself).
 */
import { createContext, useCallback, useEffect, useMemo, useRef } from "react";
import type { LayoutChangeEvent, View } from "react-native";
import type { GroupVM, PinAnchor, PinTierAnchor, RegsPage, Tab, TreeNodeVM } from "@app/core";
import type { PinSections, PinStore } from "./pinStore";

export type SectionTab = Exclude<Tab, "keep">;

/** What an anchored element is. `tids`: the tier sections it lies in, outermost first (a tier's header includes its own). */
export type AnchorKey =
  | { readonly k: "group" | "pin"; readonly row: number }
  | { readonly k: "head"; readonly row: number; readonly tid: string; readonly tids: readonly string[] }
  | { readonly k: "krow"; readonly row: number; readonly tids: readonly string[] }
  | { readonly k: "section"; readonly tab: SectionTab };

export interface AnchorSink {
  /** a stable ref callback for the element `id` (unique on the page) */
  ref(id: string, key: AnchorKey): (node: View | null) => void;
  /** an anchored element laid out: measure again soon */
  changed(): void;
}
export const AnchorContext = createContext<AnchorSink | null>(null);

export interface Box { readonly top: number; readonly height: number }
export type Boxes = ReadonlyMap<string, { readonly key: AnchorKey; readonly box: Box }>;

/** The anchors of the page's groups from measured boxes (pure; exported for the test). Document order = keep.groups. */
export function anchorsOf(groups: readonly GroupVM[], boxes: Boxes): PinAnchor[] {
  const list = [...boxes.values()];
  const of = (k: "group" | "pin", row: number) => list.find((b) => b.key.k === k && b.key.row === row)?.box ?? null;
  const headOf = (row: number, tid: string) =>
    list.find((b) => b.key.k === "head" && b.key.row === row && b.key.tid === tid)?.box ?? null;
  // prototype secEnd: the bottom of the section's last kind row or tier head (one with a height)
  const endOf = (row: number, tid: string): number | null => {
    let b = -Infinity;
    for (const { key, box } of list)
      if ((key.k === "krow" || key.k === "head") && key.row === row && key.tids.includes(tid) && box.height > 0)
        b = Math.max(b, box.top + box.height);
    return b === -Infinity ? null : b;
  };
  const out: PinAnchor[] = [];
  for (const g of groups) {
    const art = of("group", g.row);
    if (!art) continue;
    const tiers: PinTierAnchor[] = [];
    const walk = (ns: readonly TreeNodeVM[]) => {
      for (const n of ns) if (n.t === "tier") {
        tiers.push({ tid: n.tid, tier: n.tier, n: n.n, head: headOf(g.row, n.tid), end: endOf(g.row, n.tid), short: n.short });
        walk(n.children);
      }
    };
    walk(g.tree ?? []);
    out.push({ row: `r${g.row}`, group: true, tree: g.pinLine !== null, formA: g.head.form === "a", headlineN: g.head.leadN,
      top: art.top, height: art.height, groupLine: g.pinLine, pin: of("pin", g.row), tiers });
  }
  return out;
}

/** The tab row's section tops from measured boxes (pure; exported for the test). */
export function sectionsOf(boxes: Boxes): PinSections {
  let gear: number | null = null, methods: number | null = null, methodsHeight = 0, licence: number | null = null;
  for (const { key, box } of boxes.values()) {
    if (key.k !== "section") continue;
    if (key.tab === "gear") gear = box.top;
    else if (key.tab === "licence") licence = box.top;
    else { methods = box.top; methodsHeight = box.height; }
  }
  return { gear, methods, methodsHeight, licence };
}

/**
 * The anchor sink for the page's Keep tab and the section wrappers' ref + layout handler. `contentRef` wraps every
 * anchored element (measureLayout against a non-ancestor fails on native).
 */
export function useAnchors(page: RegsPage, contentRef: React.RefObject<View | null>, store: PinStore): {
  sink: AnchorSink;
  section: (tab: SectionTab) => { ref: (node: View | null) => void; onLayout: (e: LayoutChangeEvent) => void };
  remeasure: () => void;
} {
  const nodes = useRef(new Map<string, { key: AnchorKey; node: View }>()).current;
  const refs = useRef(new Map<string, (node: View | null) => void>()).current;
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const groupsRef = useRef(page.keep.groups);
  groupsRef.current = page.keep.groups;

  const measure = useCallback(() => {
    timer.current = null;
    const content = contentRef.current;
    if (!content) return;
    const all = [...nodes.entries()];
    const boxes = new Map<string, { key: AnchorKey; box: Box }>();
    let pending = all.length;
    const done = () => {
      if (--pending > 0) return;
      // no layout (jsdom): nothing has a height — keep what the store has
      if (![...boxes.values()].some((b) => b.box.height > 0)) return;
      store.setSections(sectionsOf(boxes));
      store.setAnchors(anchorsOf(groupsRef.current, boxes));
    };
    for (const [id, { key, node }] of all) {
      try {
        node.measureLayout(content as never, (_x, y, _w, h) => { boxes.set(id, { key, box: { top: y, height: h } }); done(); },
          () => done());
      } catch { done(); }
    }
  }, [contentRef, nodes, store]);

  const changed = useCallback(() => {
    if (timer.current === null) timer.current = setTimeout(measure, 16);
  }, [measure]);
  useEffect(() => () => { if (timer.current !== null) clearTimeout(timer.current); }, []);
  // a new page (another day, part or profile): measure again
  useEffect(() => { changed(); }, [page, changed]);

  const sink = useMemo<AnchorSink>(() => ({
    ref(id, key) {
      let f = refs.get(id);
      if (!f) {
        f = (node) => { if (node) nodes.set(id, { key, node }); else nodes.delete(id); };
        refs.set(id, f);
      }
      return f;
    },
    changed,
  }), [refs, nodes, changed]);

  const section = useCallback((tab: SectionTab) => ({
    ref: sink.ref(`section:${tab}`, { k: "section", tab }),
    onLayout: () => changed(),
  }), [sink, changed]);

  return { sink, section, remeasure: changed };
}
