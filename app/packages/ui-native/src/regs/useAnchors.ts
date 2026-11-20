/**
 * T29 — where the Gear / Methods / Licence sections start, as CONTENT offsets of the regs ScrollView, handed to the tab
 * store (the tab row's section in view and a tab press's scroll).
 *
 * Every anchored element registers a ref through `AnchorContext` (RegsScreen: the section wrappers). After a layout change
 * the hook measures every registered node against `contentRef` (`measureLayout`: react-native-web through bounding rects;
 * native Fabric with a ref) and rebuilds the section tops (`sectionsOf`). A section is measured with the rest rather than
 * from its own onLayout: on the web its onLayout fires only when its SIZE changes, not when the Keep tab above it grows and
 * moves it. A measuring pass in which nothing has a height is dropped (jsdom has no layout).
 *
 * (T810 cutover, owner 2026-11-17 23:29 "Remove it (Recommended)") The Keep group's anchors (its article, head block, tier
 * headers, rows and size boxes, for the pin bar) left with the bar: no Keep tab draws a group card since the cutover.
 */
import { createContext, useCallback, useEffect, useMemo, useRef } from "react";
import { Dimensions, type LayoutChangeEvent, type ViewInstance } from "react-native";
import type { RegsPage, Tab } from "@app/core";
import type { TabSections, TabStore } from "./tabStore";

export type SectionTab = Exclude<Tab, "keep">;

/** What an anchored element is: a section wrapper of the page. */
export type AnchorKey = { readonly k: "section"; readonly tab: SectionTab };

export interface AnchorSink {
  /** a stable ref callback for the element `id` (unique on the page) */
  ref(id: string, key: AnchorKey): (node: ViewInstance | null) => void;
  /** an anchored element laid out: measure again soon */
  changed(): void;
}
export const AnchorContext = createContext<AnchorSink | null>(null);

export interface Box { readonly top: number; readonly height: number }
export type Boxes = ReadonlyMap<string, { readonly key: AnchorKey; readonly box: Box }>;

/** The tab row's section tops from measured boxes (pure; exported for the test). */
export function sectionsOf(boxes: Boxes): TabSections {
  let gear: number | null = null, methods: number | null = null, methodsHeight = 0, licence: number | null = null;
  for (const { key, box } of boxes.values()) {
    if (key.tab === "gear") gear = box.top;
    else if (key.tab === "licence") licence = box.top;
    else { methods = box.top; methodsHeight = box.height; }
  }
  return { gear, methods, methodsHeight, licence };
}

/**
 * The anchor sink and the section wrappers' ref + layout handler. `contentRef` wraps every
 * anchored element (measureLayout against a non-ancestor fails on native).
 */
export function useAnchors(page: RegsPage, contentRef: React.RefObject<ViewInstance | null>, store: TabStore): {
  sink: AnchorSink;
  section: (tab: SectionTab) => { ref: (node: ViewInstance | null) => void; onLayout: (e: LayoutChangeEvent) => void };
  remeasure: () => void;
} {
  const nodes = useRef(new Map<string, ViewInstance>()).current;
  // the key each id was LAST registered with: an id comes back on another page (day, part) with other tier nesting,
  // and its ref callback is stable (React does not call it again), so the key is looked up at measure time
  const keys = useRef(new Map<string, AnchorKey>()).current;
  const refs = useRef(new Map<string, (node: ViewInstance | null) => void>()).current;
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);

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
    };
    for (const [id, node] of all) {
      const key = keys.get(id)!;
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
  // (T40) anchors are layout, never kept across a change of the window or the OS text size: react-native emits a
  // Dimensions change carrying the new fontScale (a text-size change re-lays out every line), the web a resize — measure
  // again (the elements whose own size changed report through onLayout too; this catches the ones that only moved)
  useEffect(() => {
    const sub = Dimensions.addEventListener("change", () => changed());
    return () => sub.remove();
  }, [changed]);

  const sink = useMemo<AnchorSink>(() => ({
    ref(id, key) {
      keys.set(id, key);
      let f = refs.get(id);
      if (!f) {
        f = (node) => { if (node) nodes.set(id, node); else nodes.delete(id); };
        refs.set(id, f);
      }
      return f;
    },
    changed,
  }), [refs, nodes, keys, changed]);

  const section = useCallback((tab: SectionTab) => ({
    ref: sink.ref(`section:${tab}`, { k: "section", tab }),
    onLayout: () => changed(),
  }), [sink, changed]);

  return { sink, section, remeasure: changed };
}
