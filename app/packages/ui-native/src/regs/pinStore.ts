/**
 * T29 — the pin bar's and the tab row's state, OUTSIDE React (no React state on the scroll path).
 *
 * `onScroll` at 60 Hz through `useState` would re-render the whole page; this store recomputes the bar's lines (core
 * `pinChain`) and the tab in view (core `tabInView`) from the scroll offset and notifies its subscribers only when either
 * changed (string-compared, the prototype's PIN_LAST), so `useSyncExternalStore` re-renders `PinBar` and the tab row
 * alone. No words and no decisions here: the lines are pinChain's over the anchors `useAnchors` measured; the tab is
 * tabInView's over the section tops.
 *
 * Coordinates are CONTENT offsets of the ScrollView at scroll 0. The header is a sibling above the ScrollView (T23), so the
 * bar's top line is the scroll view's own top: `header: 0`.
 *
 * Measured line heights (the prototype's PIN_HC): each drawn line reports its height (`setLineHeight`, PinBar onLayout),
 * cached by its words; an undrawn line counts `pinLineEstimate`. A changed height recomputes the chain — the prototype's
 * "until the stack stands still" loop, converging because the cache is keyed by words.
 *
 * Pure TypeScript over @app/core (no react-native), so the golden replay of every state runs in the logic project too.
 */
import { pinChain, pinLineEstimate, PIN_LH, PIN_ROW, tabInView, type PinAnchor, type PinLine, type Tab } from "@app/core";

export interface PinSections {
  readonly gear: number | null; readonly methods: number | null; readonly methodsHeight: number; readonly licence: number | null;
}

export interface StickyBox { readonly top: number; readonly height: number }

export interface PinStore {
  subscribe(fn: () => void): () => void;
  /** stable reference until the lines change */
  getLines(): readonly PinLine[];
  getTab(): Tab;
  /** the bar's measured height, 0 when it is empty (v33 --pin-h) */
  getStackHeight(): number;
  getSections(): PinSections;
  setScroll(y: number): void;
  setViewport(height: number): void;
  setContentHeight(h: number): void;
  setAnchors(a: readonly PinAnchor[]): void;
  setSections(tops: PinSections): void;
  setStackHeight(h: number): void;
  /** (owner 2026-10-27) the size boxes, content offsets by anchor id (`useAnchors`): what a stuck header is bounded by */
  setBoxes(b: ReadonlyMap<string, StickyBox>): void;
  getBoxes(): ReadonlyMap<string, StickyBox>;
  /** a drawn line's measured height (ignored when ≤ 0: not laid out) */
  setLineHeight(l: PinLine, h: number): void;
  /** a tab press: selected at once (prototype 2000), the scroll follows */
  selectTab(t: Tab): void;
}

const NO_SECTIONS: PinSections = { gear: null, methods: null, methodsHeight: 0, licence: null };
export const pinLineKey = (l: PinLine): string => `${l.cls}|${l.text}|${l.sub ?? ""}`;
const keyOf = (ls: readonly PinLine[]): string => ls.map(pinLineKey).join("\n");

/** `heights`: line heights known in advance (by `pinLineKey`) — a test's measured oracle; the app starts empty. */
export function createPinStore(o: { heights?: ReadonlyMap<string, number> } = {}): PinStore {
  const subs = new Set<() => void>();
  const heights = new Map(o.heights ?? []);
  const heightOf = (l: PinLine) => heights.get(pinLineKey(l)) ?? pinLineEstimate(l);
  let anchors: readonly PinAnchor[] = [], sections = NO_SECTIONS;
  let y = 0, viewport = 0, content = 0, stack = 0;
  let boxes: ReadonlyMap<string, StickyBox> = new Map();
  let lines: readonly PinLine[] = [], linesKey = "", tab: Tab = "keep";

  const notify = () => { for (const f of [...subs]) f(); };
  const recompute = () => {
    const next = pinChain(anchors, y, { header: 0, LH: PIN_LH, ROW: PIN_ROW }, heightOf);
    const k = keyOf(next), t = tabInView(sections, y, viewport, content, 0);
    let changed = false;
    if (k !== linesKey) { lines = next; linesKey = k; changed = true; }
    if (t !== tab) { tab = t; changed = true; }
    if (changed) notify();
  };

  return {
    subscribe(fn) { subs.add(fn); return () => { subs.delete(fn); }; },
    getLines: () => lines,
    getTab: () => tab,
    getStackHeight: () => (lines.length ? stack : 0),
    getSections: () => sections,
    setScroll(v) { y = v; recompute(); },
    setViewport(h) { viewport = h; recompute(); },
    setContentHeight(h) { content = h; recompute(); },
    setAnchors(a) { anchors = a; recompute(); },
    setSections(s) { sections = s; recompute(); },
    getBoxes: () => boxes,
    setBoxes(b) {
      const same = b.size === boxes.size && [...b].every(([k, v]) => boxes.get(k)?.top === v.top && boxes.get(k)?.height === v.height);
      if (!same) { boxes = b; notify(); }
    },
    setStackHeight(h) { if (h !== stack) { stack = h; notify(); } },
    setLineHeight(l, h) {
      if (!(h > 0)) return;
      const k = pinLineKey(l);
      if (heights.get(k) === h) return;
      heights.set(k, h);
      recompute();
    },
    selectTab(t) { if (t !== tab) { tab = t; notify(); } },
  };
}
