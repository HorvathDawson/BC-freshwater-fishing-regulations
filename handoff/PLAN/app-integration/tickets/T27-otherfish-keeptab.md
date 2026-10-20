# T27 — RN: `OtherFish`, `KeepTab` composition, Keep render gates

Phase 1 · Depends on T19 (`OtherFishRowVM`, `PlainRowVM`), T21 (`KeepVM` with `order`, Keep parity), T23, T24, T25, T26 · Blocks
T29, T41 · Decisions D4 (a), D12 (a) · Size 1 day

Target **regs v33 @ 04765ab5** (prototype `buildKeep` Other-fish rewrite ≈ 1296–1321, CSS 78–82, 168–170, 301–305, 764, 865).

## Goal
The whole "What you can keep" tab, in `KeepVM.order`: strip, inherited line, banner, every group, the plain (card-less) rows,
"Other fish" (a 1.5 px ink rule, the section head, one row per non-trout fish: thumbnail · name (+ weekday split) · value
"15 a day" / "release" (amber, bold) / "closed" (red) / "no limit", v33 "· size limit" hint, chevron → its sheet), the possession
row, "Where you can’t fish". Mounted in `RegsScreen`'s keep slot. Plus the render tests that stand in for the prototype's
visual checks on the Keep tab.

## Files
- create `app/packages/ui-native/src/regs/OtherFish.tsx` (+ `PlainRow`)
- create `app/packages/ui-native/src/regs/KeepTab.tsx`
- create `app/packages/ui-native/src/regs/KeepTab.test.tsx` (the render gates below)
- modify `app/packages/ui-native/src/regs/RegsScreen.tsx`: render `<KeepTab keep={page.keep} …/>` in `regs-keep`
- modify `app/packages/core/src/regs/testing/vm-fixtures.ts`: `keepOf(stateId): KeepVM` (assembles T24–T26 fixtures + other fish +
  plain rows + `order` from golden `keep.order` selectors: `div.wstrip`→"strip", `div.inh`→"inherited", `div.banner`→"banner",
  `article.row.ledger.g28`→`{group:i}`, `article.row.ledger`→`{plain:i}`, `section.sec`→"otherFish", `section.posrow`→"possession",
  `section.nofish`→"whereNot") — and a T21-backed `pageOf(stateId)` when the province store is present

## Signatures
```ts
export function KeepTab(p: { keep: KeepVM; dateLabel: string; palette: Palette; fish: FishImages; h: LineHandlers;
                             onAnchor?: (a: AnchorEvent) => void }): JSX.Element;
export function OtherFish(p: { rows: readonly OtherFishRowVM[]; palette: Palette; fish: FishImages; onOpen: (k: SheetKey) => void }): JSX.Element;
export function PlainRow(p: { row: PlainRowVM; palette: Palette }): JSX.Element;
```
`OtherFishRowVM` (contracts §2.2 + v33): `{ thumb: string|null; name: string; value: string; tone: "keep"|"release"|"closed"|
"nolimit"; week: string|null; sizeHint: string|null; open: SheetKey }` — CONTRACT CHANGE: applied to contracts.md (§2.2 `OtherFishRowVM.tone`).

## Drawing
- **KeepTab**: `View testID="regs-keep"`; maps `keep.order` → `YearStrip` (`dateLabel` = `page.header.date`), `InheritedLine`,
  `ClosedBanner`, `Group` (with `onAnchor`), `PlainRow`, `OtherFish`, `PossessionRow`, `WhereNot`. Nothing else: no headings
  of its own, no spacing beyond each block's own margins (tokens.md §5).
- **OtherFish** (`section.sec` as rewritten at 1321): `paddingTop: 14, paddingHorizontal: GUTTER, paddingBottom: 4,
  borderTopWidth: 1.5, borderTopColor: ink`; head `Text TYPE.sectionHead ink accessibilityRole="header"` `words.OTHER_FISH`
  ("Other fish") marginBottom 8; list (`role="list"`); each row: `View` row, gap 10, `borderTopWidth: 1, borderTopColor: line`,
  `alignItems: "center"`:
  - thumb (`.th`): 64 wide, minHeight 44, centred; `Image fish[thumb]` 64×32 contain, no background (r20h-picture-surface); no
    picture → silhouette 40×20 `line2`; `aria-hidden` (the row's button carries the name).
  - button (`.obtn`): `Pressable` flex 1, row, gap 10, `alignItems: "center"`, minHeight 44, `paddingVertical: 9`,
    `accessibilityRole="button"`, `accessibilityLabel={name + ": " + value}` (+ ", " + week when set) → `onOpen(open)`:
    column flex 1: `Text TYPE.regsBody ink` `{name}`; `week` → `Text TYPE.metaSemi sub` marginTop 2.
    value: `Text TYPE.regsBodyBold` colour `{keep: ink, nolimit: ink, release: restricted, closed: closed}[tone]` (v33: release
    bold); `sizeHint` → nested `Text TYPE.metaSemi sub` `{" " + sizeHint}` on the same baseline (CSS 865).
    chevron `›` 14 px `sub`.
- **PlainRow** (golden `keep.plainRows`, e.g. cow1_1010's release-only "Char"): an `article`-like `View` `paddingHorizontal:
  GUTTER, paddingTop: 12, borderTopWidth: 1.5, borderTopColor: ink`; line 0 `TYPE.lead ink`, the rest `TYPE.regsBody ink`
  (meta-looking lines are not distinguished in `PlainRowVM.lines: string[]` — accept; listed in Notes).

## Ports
prototype `buildKeep` (Other-fish rewrite at 1296–1321: `thumb`, `.owk`, `.osz`, `CHEV`), page v36 `rowsHtml` 692 (T19 owns
the words); CSS above.

## Reads
`page.keep` (`order, strip, inherited, banner, groups, plainRows, otherFish, possession, whereNot`), `page.header.date`.

## Tests (`KeepTab.test.tsx`)
Gates named after the prototype checks they replace (check.mjs @04765ab5; tests.md maps them):
1. **r20h-one-surface** — mount `KeepTab` for vedder_1010, x35p_1010, klwa_1012, light and dark (6 mounts, as the check).
   Walk every element inside the groups (`[data-testid^="group-"] *`): any element with a non-transparent computed
   `backgroundColor` must be `palette.card` OR be a chip (`testID` in `size-chip`, `also-chip`) painted `palette.tint`. Zero
   others. (jsdom has no layout, so "wider than 200 and taller than 40" from the check becomes "any surface that is not a chip".)
2. **r20h-picture-surface** — the same 6 mounts: every fish `Image` in the Keep tab (kind pictures + Other-fish thumbs) and its
   parent have no background colour; at least 12 pictures across the mounts (the check's floor).
3. **r20f-pictures-first** — every kind row with pictures (`testID="kind-pics"`): the siblings before it are only `kind-says` /
   `kind-meta`; nothing of `kind-says|kind-meta` after it; ≥ 12 rows across vedder_1010, glenn_501, klwa_1010, bc1_501 (count the
   rows; the check's floor). Captions ≤ 2 lines → `numberOfLines === 2` on every caption Text.
4. **r20b-number-once (render)** — for every tier and kind line in those states: the count of nested numeral Texts equals the
   VM's `num` runs, and each number's digits occur in the normalised outer text exactly as often as in `plain(line)` (no number
   drawn twice by the component). A folded kind (`folded: true`) draws no sentence (vedder_1010 own-tier rainbow row: only the
   chip "50 cm (20 in) or less" and "Rainbow trout ›").
5. **header-label-two-lines** — mount `RegsScreen` for cow1_1010 with the place measurer mocked to 2 lines → the label shows
   "From the CNR Mile 66 Trestle up to the CNR Bridge (mile 70.2)"; mocked to 3 lines → "CNR Mile 66 Trestle → CNR Bridge (mile
   70.2)"; the label box style has `height: 32.44` in both; the action button sits in row 1 (same parent as the name), not on the
   label row.
6. **header-one-shape** — mount vedder_1010, cow1_1010, klwa_1010, bc1_501: the header subtree has the same testIDs in the same
   order in all four (place slot present even when empty), and after `fireEvent.scroll` on the scroll view its text is unchanged.
7. **Order** — for every golden state, the rendered block testIDs in order map 1:1 to `golden.keep.order` (125/134; the 9
   states with an unmodelled block are listed exclusions — see Notes).
8. **Other fish** — bc1_501: rows "Kokanee release" (value `LIGHT.restricted`, `textBold`), "Whitefish 15 a day" (ink), "Bass
   closed" (`LIGHT.closed`); klwa_1010 row 0: name "Kokanee", week "Weekends: keep 5 · Weekdays: release" (`metaSemi` sub), value
   "5 a day"; pressing it calls `onOpen("other:klwa_1010|o0")`. The section's top border is 1.5 `LIGHT.ink`, head "Other fish".
9. **Golden text sweep** — for every state (T21 pages when the store is present, else the golden-built fixture), the Keep tab's
   normalised `textContent` equals `golden.states[id].keep.lines.join(" ")` normalised, modulo the strip's status mark ("✓ "/"✕ "
   is CSS content in the prototype, not in `lines`) — strip it before comparing. Expect 125/134 (same 9 exclusions as test 7);
   any further exclusion is listed with its reason in the test.

## Acceptance
```sh
pnpm -C app exec vitest run packages/ui-native/src/regs
pnpm -C app check
pnpm -C app dev:web   # Vedder Oct 10, Glenn May 1, Bella Coola p1 May 1, Kootenay LWA Oct 10 — side by side with the prototype
```

## Notes
- The Other-fish **size hint** is v33 (r20i-other-size-hint): the prototype decides it by regex over the group JSON; T19 must
  decide it in core (`sizeHint`), never the component.
- `PlainRowVM` carries flat strings, so a plain row's sub-lines all draw as body text — the prototype draws them in the old card
  styles (kicker, "How this was decided" fold). 25 states have one. If the owner minds, T19 gives `PlainRowVM` typed lines;
  this ticket then maps types to tokens.md styles (½ day). Listed as an open problem.
- CONTRACT CHANGE: applied to contracts.md (§2.2 — `details.inpart.hot` / `div.caveats.top` are T15b `CaveatVM` blocks `{ caveat: n }`, §6 #2). Drawing: `caveats` = margin `8 GUTTER 0`, padding
  `6 0 6 10`, left 3 `restricted`, label `kicker` `sub` (20g), items `regsBody` + `meta sub` where; `inpart` = a fold (`Pressable`
  head `metaBold` + body lines `regsBody`).
- The status mark is drawn by `YearStrip` from `words.STATUS_MARK`; golden `lines` omit it (CSS `::before`) — test 9 strips it.
- Six mounts × light/dark in jsdom are cheap; do not shrink the state list below the check's (the floors are the point).
