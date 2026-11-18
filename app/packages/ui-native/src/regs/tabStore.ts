/**
 * T29 — the tab row's state, OUTSIDE React (no React state on the scroll path).
 *
 * `onScroll` at 60 Hz through `useState` would re-render the whole page; this store recomputes the tab in view (core
 * `tabInView`) from the scroll offset and notifies its subscribers only when it changed, so `useSyncExternalStore`
 * re-renders the tab row alone. No words and no decisions here: the tab is tabInView's over the section tops
 * `useAnchors` measured. Coordinates are CONTENT offsets of the ScrollView at scroll 0.
 *
 * (T810 cutover, owner 2026-11-17 23:29 "Remove it (Recommended)") This was the pin store: the pin bar's lines and the
 * size boxes it bounded are gone with the bar (no Keep tab draws a group card since the cutover); the tab row stays.
 *
 * Pure TypeScript over @app/core (no react-native), so it runs in the logic project too.
 */
import { tabInView, type Tab } from "@app/core";

export interface TabSections {
  readonly gear: number | null; readonly methods: number | null; readonly methodsHeight: number; readonly licence: number | null;
}

export interface TabStore {
  subscribe(fn: () => void): () => void;
  getTab(): Tab;
  getSections(): TabSections;
  setScroll(y: number): void;
  setViewport(height: number): void;
  setContentHeight(h: number): void;
  setSections(tops: TabSections): void;
  /** a tab press: selected at once (prototype 2000), the scroll follows */
  selectTab(t: Tab): void;
}

const NO_SECTIONS: TabSections = { gear: null, methods: null, methodsHeight: 0, licence: null };

export function createTabStore(): TabStore {
  const subs = new Set<() => void>();
  let sections = NO_SECTIONS, y = 0, viewport = 0, content = 0, tab: Tab = "keep";
  const notify = () => { for (const f of [...subs]) f(); };
  const recompute = () => {
    const t = tabInView(sections, y, viewport, content, 0);
    if (t !== tab) { tab = t; notify(); }
  };
  return {
    subscribe(fn) { subs.add(fn); return () => { subs.delete(fn); }; },
    getTab: () => tab,
    getSections: () => sections,
    setScroll(v) { y = v; recompute(); },
    setViewport(h) { viewport = h; recompute(); },
    setContentHeight(h) { content = h; recompute(); },
    setSections(s) { sections = s; recompute(); },
    selectTab(t) { if (t !== tab) { tab = t; notify(); } },
  };
}
