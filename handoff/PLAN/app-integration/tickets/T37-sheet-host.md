# T37 — RN: `SheetHost` + the sheet body components

Status: DONE — c1bffeef, c3566bc2, d91d9ba3 (the taller-sheet item O5 moved to T82).

Phase 4 · Depends on T36 (SheetVM), T23 (RegsScreen owns `sheet` state), T22 (TYPE/SPACE), T20 (fish image map), T34
(`RuleBookLink`, `SourceLine`) · Blocks T38, T41, T42, T43 · Decisions D6 (book links), D7 (the day sheet stays T23's
`DaySheet`, not a SheetKey) · Size 1 day

> **T37 AS BUILT (2026-10-21):** `SheetHost.tsx` (the one `Sheet`: title, sub, scroll, back layer, drag from the handle),
> `SheetBody.tsx` (`SheetBlockView` over every `SheetBlock` kind, incl. T36 part 2's `group` → `GroupCard titled` and
> `rowTitle`), `sheet/{Fold,Para,RuleRow,LadderView,SourcesView,WeekBox,FishImage,PartsChoice,TermSection}.tsx`.
> T23's `RegsScreen` is not on this branch: the open key lives in **`RegsView`** (T21 slice) — move it when T23 lands.
> `Sheet.tsx`: optional `scroll` / `sub` / `scrollRef` (no change for existing callers) + the title is a header;
> the scroll area is **0.68** of the window, not 0.82 (0.82 covered the water's header on 390 × 844). **Shell.tsx**: the
> water body now sits inside the `BackLayerContext` provider (before, only LayersSheet did — the browser's back left the
> water with the sheet still open). LineText is reused (T26/T34 had it). `adipose` images are T38's (plain grounds until
> then). Tests: SheetHost 14 (incl. G1 drag/back, golden mount: all vedder_1010 sheets draw every golden line in order),
> SheetBody 25. Web: handoff/PLAN/app-integration/T37-shots/.

## Goal
Any press on a ⓘ, a rule line, "Source ›", "How to tell ›", a fish picture, an Other-fish row, the possession row or
"See every regulation line ›" opens ONE bottom sheet over the regs screen, drawn from `page.sheet(key)`. A press inside a
sheet opens the next sheet in its place (the prototype replaces; there is no back stack). "Done", the scrim, and the
hardware back close it. The header of the water stays mounted and visible above the scrim (`howtell-current-water`).
Folds ("In the book’s words", "Technical detail", "Overruled (N)", "Rules that don’t apply here (N)") start closed.

## Files
- create `app/packages/ui-native/src/regs/SheetHost.tsx` — the host (state lives in `RegsScreen`)
- create `app/packages/ui-native/src/regs/SheetBody.tsx` — `SheetBlockView` switch over `SheetBlock.k`
- create `app/packages/ui-native/src/regs/sheet/{Para,Quote,Fold,TermSection,RuleRow,LadderView,SourcesView,WeekBox,FishImage,PartsChoice}.tsx`
- create `app/packages/ui-native/src/regs/LineText.tsx` **only if T26/T34 did not already** (renders a `Line`: text, num, term → ⓘ button, book → `RuleBookLink`, link → press)
- create `app/packages/ui-native/src/regs/SheetHost.test.tsx`, `app/packages/ui-native/src/regs/SheetBody.test.tsx`
- modify `app/packages/ui-native/src/regs/RegsScreen.tsx` (T23): `const [sheet, setSheet] = useState<SheetKey | null>(null)`; pass `onOpen={setSheet}` down every tab; render `<SheetHost …/>` last
- modify `app/packages/ui-native/src/Sheet.tsx`: add an optional `scroll?: boolean` prop — **no behaviour change for existing callers** (default false)

## Signatures
```ts
// SheetHost.tsx
import type { RegsPage, SheetKey, SheetVM } from "@app/core";
export interface SheetHostProps {
  page: RegsPage;                       // page.sheet(key) is lazy + pure (T36)
  open: SheetKey | null;
  onOpen: (k: SheetKey) => void;        // nested open: replaces
  onClose: () => void;
  onPart?: (partIx: number) => void;    // parts sheet rows
  onTab?: (t: Tab) => void;             // "Steelhead stamp ›" (r20i-duty-line) when it sits in a sheet
  palette: Palette;
  fish: FishImages;                     // T20 map code → ImageSourcePropType (registered by apps/mobile)
  adipose: { wild: ImageSourcePropType; clipped: ImageSourcePropType };   // T38
}
export function SheetHost(p: SheetHostProps): JSX.Element | null;   // null when open == null or page.sheet(open) == null (a stale key closes silently)

// SheetBody.tsx
export function SheetBody(p: { vm: SheetVM; palette: Palette; fish: FishImages; adipose: SheetHostProps["adipose"];
  onOpen: (k: SheetKey) => void; onPart?: (i: number) => void; onTab?: (t: Tab) => void }): JSX.Element;
export function SheetBlockView(p: { b: SheetBlock } & Omit<Parameters<typeof SheetBody>[0], "vm">): JSX.Element;

// sheet/Fold.tsx — a closed-by-default disclosure (the prototype's <details>)
export function Fold(p: { summary: string; palette: Palette; children: React.ReactNode; testID?: string }): JSX.Element;
//   Pressable header: accessibilityRole "button", aria-expanded={open}, label = summary; body rendered only when open
```
Sheet.tsx change (the existing component has **no ScrollView inside** — facts B):
```ts
export function Sheet({ open, onClose, title, palette, children, scroll = false, sub }: { …existing…; scroll?: boolean; sub?: string | null })
//   scroll: wraps children in <ScrollView style={{ maxHeight: windowHeight * 0.82 }} contentContainerStyle={{ paddingHorizontal: GUTTER, paddingBottom: SPACE.s4 }}
//           keyboardShouldPersistTaps="handled">; sub: a TYPE.small `sub` line under the title row (rule / src sheets)
```
SheetHost renders `<Sheet open title={vm.title} sub={vm.sub} scroll palette onClose>` + `<SheetBody>`; on `open` change it
scrolls the inner ScrollView to 0 (`ref.scrollTo({y:0, animated:false})`) so a nested open starts at its top.

## Ports (regs-v11.html @ 04765ab5)
- `head` 1668 → `Sheet` title row (title `<h2>`, "Done" `data-close`, sub `<p>`); grab bar already in `Sheet`.
- the global click handler (≈1885–1910): `data-open` → open, `data-close` → close, scrim press → close, `data-gl` →
  `gl:`, `data-rule` → `rule:`, `data-howtell` → `hatch`, `data-fish` → `sp:`, `data-src` → `src:` — in the app every one of
  those is a `Run`/`SheetBlock` with `open: SheetKey` and the component calls `onOpen(open)`.
- CSS (`.sheet`, `.shead`, `.sbody`, `.glsays`, `.glex`, `.bookw`, `.exact`, `.lbl`, `.prule`, `.appl`, `.srow`, `.lvh`,
  `.cred`, `.tips`, `.wkstat`, `.ladder2`) — styles from `tokens.md` §5 census roles `Sheet · title|says|example|fold`
  (read the exact values from `port/golden/tokens.json` `.census.light` where `role` starts with `Sheet ·`).
- page v36 `openSheet` 794 (focus to Done on open → `accessibilityViewIsModal` on the sheet container; on web the Modal
  traps focus).

## Reads
`SheetVM.{title, sub, blocks}`; each block's fields (T36 contract); `FishImages[code]`; `Palette` `card ink sub accent line line2 scrim tint`.

## Tests
**C — `SheetHost.test.tsx`** (render project; `vm-fixtures.ts` `sheetPage()` builds a `RegsPage` whose `sheet()` returns
T36's fixture VMs; `afterEach(cleanup)`; `LIGHT`):
1. `opens one sheet by key` — `open="gl:bait_ban"` → `getByRole("heading", {name: "Bait ban"})`; exactly one `Done` button.
2. `fold closed by default` — the `Fold` "In the book’s words" has `aria-expanded="false"`; the quote text
   `"Bait Ban: the use of natural bait is prohibited in waters with a bait ban."` is NOT in the document; press → it is (`sheet-book-words`).
3. `nested open replaces` — in `pos:pos`, press "Source ›" → `onOpen("rule:z2:possession_quota::possession_quota.r1")`;
   rerender with that key → heading is the rule title, the "Possession" heading is gone (one sheet at a time).
4. `Done / scrim close` — pressing "Done" and pressing the scrim (`accessibilityLabel "Close"`) each call `onClose` once.
5. `stale key closes` — `open="other:o99"` (sheet() → null) renders nothing and does not throw.
6. `sheet over the regs screen keeps the header mounted` — mount `RegsScreen` (T23) with `open="hatch"`:
   `getByRole("heading", {name: "Hatchery or wild?"})` AND the header name `"Vedder River"` and date `"Oct 10"` are still in
   the document (`howtell-current-water`).
7. `book links` — every `RuleBookLink` in the `rule:zp:conduct::conduct.r1` sheet has text matching `/^Rule book p\.\d+ ↗$/`;
   in `gl:bait_ban` the line reads `"Rule book"` + `"p.4 ↗"` + `"p.8 ↗"` (bare runs) (`rule-book-label`).
8. `pictures press → sp:` — in `other:o0` (vedder_1010), the `Kokanee ›` picture button calls `onOpen("sp:KO")`.
**C — `SheetBody.test.tsx`**: one case per block kind (para, example, head, quote, list, fold, term, rule, value, weekBox,
ladder, sources, pictures, image, adipose, pages, source, link, choice) — renders without a colour literal, every text
from the block appears exactly once, every pressable has `accessibilityRole="button"` and a label; `ladder` marks a
`today` line with the visible word "Today" (not colour only).
**C — golden mount**: for vedder_1010, mount `SheetHost` for each of its 63 sheet keys (store present → real page via the
T12 harness; absent → skip) and assert the rendered text (whitespace-normalised, in order) contains every golden line of
that sheet (`golden.states.vedder_1010.sheets[k]`, `"@"` → `golden.sheets[k]`) — text order, not layout.

## Acceptance
```sh
pnpm -C app vitest run packages/ui-native/src/regs/SheetHost packages/ui-native/src/regs/SheetBody
pnpm -C app check
# D (manual, T41's tool): node app/tools/compare-regs.mjs --state vedder_1010 --sheet pos:pos   → side-by-side PNG; sheet height < 300 px (r20g-possession-fold)
```

## Notes
- `Sheet` is shared (Layers, Spots, Day). Adding `scroll`/`sub` as optional props keeps every caller unchanged; do not
  move the regs sheet to a second component — one scrim, one sheet look (tokens.md: keep `opacity.ui.scrim` .28).
- The prototype's "Done" is a word button, not uppercase-tracked: the existing `Sheet` draws `TYPE.section` caps "Done".
  Keep the app's (it is the app's sheet chrome; `r20c-style-tokens` allows caps on kickers/labels only — "Done" is chrome
  outside `#keep`, the census does not cover it).
- `Modal` on react-native-web renders into a portal: render tests query `screen` (document), not the container.
- Android hardware back: `Modal onRequestClose` already calls `onClose`.
- Day picking is not a sheet key (D7): `DaySheet` stays T23's.
