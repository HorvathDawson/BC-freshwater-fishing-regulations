# T29 — RN: anchors, `PinBar`, tab-in-view, tab scroll-to

Phase 2 · Depends on T28 (`pinChain`, `appPinLines`, `tabInView`, `tabScrollY`, `anchoredScrollY`, `PinAnchor`), T23 (screen,
header tabs), T26 (`onAnchor` events), T27 (KeepTab) · Blocks T34/T35 (their sections feed `tops`), T41 · Decisions D1 (a),
D2 (a), D12 (a) · Size 1 day

Target **regs v33 @ 04765ab5**: `pinTops` 1614–1618, `pinUpdate` 1637–1655 (incl. v33 `--pin-h` 1655), `mini` 1656–1664, scroll
listener 1665, tab press 1904–1905; CSS 756–762 (+ v33 758/760: lines wrap), 854 (20h bar surface), 867 (v33 scroll margins).

## Goal
While the reader scrolls the Keep tab, a bar directly under the header carries the chain of sections they are inside —
"Trout and char · **2** a day from streams" with "(of **4** across Region 2)" under it, then "↳ of those, **1** over 50 cm (2 if one
is a hatchery steelhead)" while inside the size section — exactly the lines the prototype shows at the same scroll offset. The
header tab follows the section in view; pressing a tab scrolls to its section. Scrolling re-renders only the bar and the tab row,
never the page.

## Files
- create `app/packages/ui-native/src/regs/pinStore.ts` — the external store (no React state on the scroll path)
- create `app/packages/ui-native/src/regs/useAnchors.ts` — `onAnchor` events → `PinAnchor[]` (content offsets)
- create `app/packages/ui-native/src/regs/PinBar.tsx`
- create `app/packages/ui-native/src/regs/PinBar.test.tsx`
- modify `app/packages/ui-native/src/regs/RegsScreen.tsx` — create the store, wire `onScroll`/`onLayout`/`onContentSizeChange`,
  mount `PinBar`, tab press → `scrollTo`, header `selected` from the store
- modify `app/packages/ui-native/src/regs/RegsHeader.tsx` — `selected` read through `useSyncExternalStore(store.subscribe,
  store.getTab)` in a tiny `TabRow` child (so only the tabs re-render on scroll)
- modify `app/packages/ui-native/src/regs/KeepTab.tsx` — pass `onAnchor` to `Group` (T26 already forwards it)

## Signatures
```ts
// pinStore.ts
import { pinChain, appPinLines, tabInView, PIN_LH, PIN_DROP, type PinAnchor, type PinLine, type Tab } from "@app/core";
export interface PinStore {
  subscribe(fn: () => void): () => void;
  getLines(): readonly PinLine[];            // stable reference until the lines change (string-compare, like PIN_LAST)
  getTab(): Tab;
  getStackHeight(): number;                  // the bar's measured height (0 when empty) — v33 --pin-h
  setScroll(y: number): void;                // from onScroll; recomputes lines + tab, notifies only on change
  setViewport(height: number): void;        // the ScrollView's own height (onLayout)
  setContentHeight(h: number): void;         // onContentSizeChange
  setAnchors(a: readonly PinAnchor[]): void;
  setSections(tops: { gear: number | null; methods: number | null; methodsHeight: number; licence: number | null }): void;
  setStackHeight(h: number): void;           // PinBar onLayout
  selectTab(t: Tab): void;                   // a press: selected at once (prototype 1905), scroll follows
}
export function createPinStore(): PinStore;
// lines = appPinLines(pinChain(anchors, y, { header: 0, height: viewport, LH: PIN_LH, DROP: PIN_DROP }))

// useAnchors.ts
export function useAnchors(page: RegsPage, contentRef: React.RefObject<View>, store: PinStore): {
  onAnchor: (a: AnchorEvent) => void;           // pass to KeepTab → Group → TreeNodes
  onSection: (tab: Exclude<Tab, "keep">) => (e: LayoutChangeEvent) => void;   // gear/methods/licence wrappers
};

// PinBar.tsx
export function PinBar(p: { store: PinStore; palette: Palette }): JSX.Element | null;
```

### Anchors: onLayout → content offsets
- Every anchored element reports through `onAnchor` (T26's `AnchorEvent` kinds `group`, `pin`, `tier`, `head`, `worked`) from its
  `onLayout`. Because `onLayout`'s `y` is relative to the parent, `useAnchors` resolves the content offset by
  `node.measureLayout(contentRef.current, (x, y, w, h) => …)` — `contentRef` is a `View` wrapping everything inside the
  `ScrollView` (react-native-web implements `measureLayout` via bounding rects; native Fabric supports a ref). The event therefore
  carries the element's ref too: extend `AnchorEvent` with `ref: React.RefObject<View>` (T26 forwards it; add now if T26 landed
  without). Re-measure all anchors once on `onContentSizeChange` (fonts loading, a fold opening) — the prototype's `pinTops` on
  resize/fonts.ready.
- Build `PinAnchor` per group (`keep.groups[i]`), `row: "r" + group.row`:
  `group: true, tree: group.tree != null, formA: group.head.form === "a", headlineN` = the share lead's number (T17 exposes
  `head.leadN: number | null`; golden `head.leadN`), `groupLine: group.pinLine` (T28), `pin` = the `pin` event box, `top/height`
  = the `group` box, `worked` = the `worked` box, and `tiers` in document order (pre-order walk of `group.tree`): `{tid, tier,
  n: tier's first num run as a number, top/height: "tier" box, head: "head" box, short: "↳ " + tier.short, nextHead}` with
  `nextHead` from the tree (T28 Notes: next sibling node is a tier → its head top; a kind/release row → null; last child → the
  parent tier's next sibling by the same rule, one level; top level → null).
- Plain rows (`keep.plainRows`) produce an anchor `{group: false, tree: false, …}` from their `onLayout` — so the app shows
  **no** lines over them (`appPinLines` → `[]`), the one intentional divergence from the prototype (which keeps stale lines; T28).
- Section tops for the tabs: `onSection("gear"|"methods"|"licence")` on the T23 section wrappers → `store.setSections` (content
  offsets from `e.nativeEvent.layout.y` — they are direct children of `contentRef`, so `y` is already a content offset).

### PinBar drawing (tokens.md §5 "PinBar")
- Mounted in `RegsScreen` as the LAST child of the `View` that holds the `ScrollView` (so it overlays the scroll's top edge,
  directly under the fixed header — whatever height the header + FaceBar have, D2): `position: "absolute", top: 0, left: 0,
  right: 0`, `backgroundColor: card` (20h: one surface), `borderBottomWidth: 1, borderBottomColor: line` (20h's `box-shadow 0 1px 0
  var(--line)`), `pointerEvents="none"`, `aria-hidden` / `importantForAccessibility="no-hide-descendants"` (the prototype's `#pins`
  is `aria-hidden="true"` — the full headers in the page are what a screen reader reads). Renders `null` when there are no lines.
  `onLayout` → `store.setStackHeight(h)`.
- Lines (`useSyncExternalStore(store.subscribe, store.getLines)`):
  - `g` / `g two`: `Text TYPE.pinGroup ink`, padding `3 GUTTER`; the first number bold: `boldFirst(text)` → `pre` + nested
    `Text style={{fontFamily: FACE.textHeavy}}` `n` + `post`; `sub` (form a) → a second `Text TYPE.pinSub ink` on its own line
    (block, inside the same padded View), its own first number bold in `textHeavy`.
  - `c`: `Text TYPE.pinLine ink`, padding `3 GUTTER 3 26`, its first number bold (`textHeavy` at 12.48).
  - v33: lines WRAP (no `numberOfLines`); the stack stays ≤ 90 px on the golden states (largest golden height 87).
- Line height budget: `PIN_LH = 26` is what `pinChain` assumes per line; the drawn lines are 19.5 + 6 = 25.5 (group) and 16.22 + 6
  = 22.2 (tier) — the prototype's own numbers; do not "fix" either side.

### Scroll, tab in view, tab press
- `ScrollView` `scrollEventThrottle={16}` `onScroll={e => store.setScroll(e.nativeEvent.contentOffset.y)}`,
  `onLayout={e => store.setViewport(e.nativeEvent.layout.height)}`, `onContentSizeChange={(w, h) => store.setContentHeight(h)}`.
- The tab row reads `store.getTab()` (= `tabInView(tops, y, viewport, contentH)`); RN `aria-selected` follows it.
- Tab press: `store.selectTab(t)`; `scrollRef.current?.scrollTo({ y: tabScrollY(t, tops), animated: true })` (prototype 1904:
  smooth; keep → 0).
- Any anchored scroll the app adds later (a sheet's "show me this row", a deep link to a tier) uses
  `anchoredScrollY(targetTop, store.getStackHeight())` — v33 `r20i-scroll-margins`: nothing lands under the stack.
- Reduced motion: `animated: !reduceMotion` (`AccessibilityInfo.isReduceMotionEnabled`, as the app's other animations do).

## Ports
`pinTops` (measure after layout → `useAnchors`), `pinUpdate` (→ T28 `pinChain` + `PinBar` drawing incl. the `<b>` on the first
number of text and of sub, 1653), v33 `--pin-h` (→ `getStackHeight`), `mini` (→ T28 `tabInView`), the scroll listener 1665 (→
`onScroll`), the `data-sec` press 1904–1905 (→ `tabScrollY` + `selectTab`).

## Reads
`page.keep.groups[]` (`row`, `head.form`, `head.leadN`, `pinLine`, `tree[].{tid, tier, short, line}`), `page.keep.plainRows`, the
section wrappers' layout.

## Tests (`PinBar.test.tsx`; jsdom has no layout, so anchors are injected into the store from golden `pins.anchors` converted to
app coordinates: every `top` (article, pin, tier, head, worked, `nextHead`) minus `viewport.header` (119), and the viewport
`height = 703 − 119 = 584`, `header = 0` — exact, since both sides shift by the same 119 and the scroll offset is unchanged)
1. **vedder_1010 replay through the component**: mount `RegsScreen` (vm-fixture page) with an injected `PinStore`; set anchors;
   for each golden sample `y`, `fireEvent.scroll(scrollEl, { target: { scrollTop: y } })` (react-native-web's ScrollView reads
   `scrollTop` from the DOM node) and assert the bar's line texts:
   - y 0 → no bar;
   - y 128 → "Trout and char · 2 a day from streams" + sub "(of 4 across Region 2)";
   - y 368 → those + "↳ of those, 1 over 50 cm (2 if one is a hatchery steelhead)";
   - y 496 → group line only; y 1056 → + "↳ plus up to 4 hatchery rainbow trout under this river’s rule"; y 1096 → group line only;
   - y 1600 → no bar.
   Each line's first number is a nested Text in `FACE.textHeavy` ("2", "4" in the sub, "1", "4").
2. **glenn_501**: y 280 → "Trout and char · 2 a day from streams" + "(of 5 across Region 4)"; y 704 → + "↳ plus up to 5 rainbow
   trout under the Duncan River’s rule" (r20h-pin-drops: the share's "↳ … from streams" never appears — form a); y 768 → group only.
3. **klwa_1010** (form null): y 128 → one line "Trout and char · This lake’s rule: 2 a day", no sub; y 744 → none.
4. **All 109 states without pins.error**: drive `store.setScroll` (no DOM events, fast) through every golden sample `y` and assert
   `getLines()` = the sample's lines — expect 109/109; the 25 error states are covered by T28's skip test (the app shows `[]`).
5. **Re-render scope**: wrap `KeepTab` in a render counter (`vi.fn` in a test-only `Profiler`); 20 scroll events change the bar
   but the KeepTab render count stays 1.
6. **Tab in view**: set sections `{gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700}`, viewport 584, content 3300;
   scroll 1488 → the "Gear" tab has `aria-selected="true"`; 0 → "What you can keep".
7. **Tab press**: spy `ScrollView.prototype.scrollTo` (or the ref); press "Gear" → called with `{ y: 1502, animated: true }` and
   "Gear" selected immediately; press "What you can keep" → `{ y: 0 }`.
8. **PinBar surface**: the bar View has `backgroundColor: LIGHT.card`, `borderBottomWidth: 1`, `borderBottomColor: LIGHT.line`,
   `position: absolute`, `top: 0`; it is `aria-hidden`.
9. **Stack height** (r20d-pinned-stack, render form): with jsdom heights unavailable, assert structurally that a state's maximum
   line count ≤ 3 and that no line has `numberOfLines` (v33 wraps); the ≤ 90 px measurement is in T41's Playwright walk.

## Acceptance
```sh
pnpm -C app exec vitest run packages/ui-native/src/regs/PinBar packages/core/src/regs/pins
pnpm -C app check
pnpm -C app dev:web   # Vedder Oct 10: scroll slowly — the bar appears as the group head slides under the header, the size line
                      # appears inside the size section and goes when < ~110 px of it is left or the rainbow row's head is in view
```
T41 adds the Playwright side-by-side: the same scroll offsets in the prototype and the app web build, bar texts equal, stack ≤ 90 px.

## Notes
- **Why an external store**: `onScroll` at 60 Hz through `useState` re-renders the whole page; `useSyncExternalStore` with a
  string-compared snapshot re-renders only `PinBar` and the tab row, and only when the lines/tab change (the prototype's
  `PIN_LAST`).
- The bar sits at the scroll view's top edge, not at a measured header height: the header is a sibling above the ScrollView
  (T23), so `top: 0` of the scroll container IS "under the header" — no `--hdr-h` equivalent needed. That is also why the app's
  `pinChain` viewport uses `header: 0`.
- At 200 % text (T40), the prototype makes the header static (`.ts2`); the app's header stays fixed and the bar keeps working —
  check the stack height at 200 % in T40 (the 90 px budget will be exceeded; the owner may want the bar to show only the group
  line there — not decided, listed).
- `measureLayout` against a non-ancestor throws on native; `contentRef` must wrap every anchored element (it does: the whole
  scroll content).
