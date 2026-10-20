# T26 — RN: `GroupHead`, `OriginLine`, `TierHeader`, `SizeBand`, `KindRow`, `FishStrip`, `NotKeptRow`, `HowItAddsUp`

Phase 1 · Depends on T17 (`GroupVM`/`GroupHeadVM`/`OriginVM`), T18 (`TreeNodeVM`, `TierVM`, `KindVM`, `FishPicVM`), T19
(`NotKeptVM`, worked), T20 (fish image map), T22, T23, T25 (`LineText`, `LineHandlers`) · Blocks T27, T29 · Decisions D4 (a),
D12 (a) · Size 1 day

Target **regs v33 @ 04765ab5** (CSS 454–512, 660–693, 701, 733–762, 788–816, 849–868; script `buildKeep` tier/kind parts —
inventory names `buildKeep.tnode`, `buildKeep.kt`, `buildKeep.sizeOf`, `buildKeep.bigWords`, `buildKeep.dutyWords`).

## Goal
One trout-and-char group drawn as the prototype draws it after 20h: ONE surface (the card) — the group starts with a 1.5 px ink
rule and its head (name · scope kicker, then the total line: form a "**2** a day per angler from streams" over "of the 4 a day
per angler across Region 2, lakes included"); each tier a display-face header with its big number (no left bar, 20f pick c);
the size section framed by two `line2` hairlines (no tint); kind rows "sentence → size chip + qualifier → the fish pictures →
also / duty / presence"; pictures 80–132 px wide, 2:1, no tile, captioned "<name> ›" and opening the fish sheet; not-kept rows;
"How it adds up" under a 1.5 px ink rule. No logic: every string is in the VM.

## Files
- create `app/packages/ui-native/src/regs/GroupHead.tsx` (+ `OriginLine` exported from it)
- create `app/packages/ui-native/src/regs/Tree.tsx` — `TreeNodes` (recursion), `TierHeader`, `SizeBand`, `KindRow`, `ReleaseRow`
- create `app/packages/ui-native/src/regs/FishStrip.tsx`
- create `app/packages/ui-native/src/regs/NotKeptRow.tsx`, `app/packages/ui-native/src/regs/HowItAddsUp.tsx`
- create `app/packages/ui-native/src/regs/Group.tsx` — `Group` = head + origin + tree + not-kept + worked (what T27 lists)
- create `app/packages/ui-native/src/regs/Group.test.tsx`
- modify `app/packages/core/src/regs/testing/vm-fixtures.ts`: `groupOf(stateId, i): GroupVM` from golden `keep.groups[i]`
  (`head`, `tree` with `big[]` → `num` runs, `pages` → `book` runs, `pics`, `notKept`, `worked`)

## Signatures
```ts
export interface GroupProps { group: GroupVM; palette: Palette; fish: FishImages; h: LineHandlers;
  /** T29: report content offsets; absent in T26/T27 tests */ onAnchor?: (a: AnchorEvent) => void }
export function Group(p: GroupProps): JSX.Element;
export function GroupHead(p: { group: GroupVM; palette: Palette; h: LineHandlers }): JSX.Element;
export function OriginLine(p: { origin: OriginVM; palette: Palette; h: LineHandlers }): JSX.Element;
export function TreeNodes(p: { nodes: readonly TreeNodeVM[]; palette: Palette; fish: FishImages; h: LineHandlers; depth: number;
                               onAnchor?: (a: AnchorEvent) => void }): JSX.Element;
export function TierHeader(p: { tier: TierVM; palette: Palette; h: LineHandlers; first: boolean }): JSX.Element;
export function SizeBand(p: { children: React.ReactNode; palette: Palette }): JSX.Element;
export function KindRow(p: { kind: KindVM; palette: Palette; fish: FishImages; h: LineHandlers; inBand: boolean; first: boolean }): JSX.Element;
export function FishStrip(p: { pics: readonly FishPicVM[]; palette: Palette; fish: FishImages; onOpen: (k: SheetKey) => void }): JSX.Element;
export function NotKeptRow(p: { row: NotKeptVM; palette: Palette; fish: FishImages; h: LineHandlers }): JSX.Element;
export function HowItAddsUp(p: { worked: NonNullable<GroupVM["worked"]>; palette: Palette }): JSX.Element;
// AnchorEvent is T29's (declared there; T26 only forwards onLayout through `onAnchor` props so T29 does not re-open these files):
export type AnchorEvent =
  | { k: "group"; row: number; y: number; height: number }            // the article (Group root View)
  | { k: "pin"; row: number; y: number; height: number }              // the head block (GroupHead root)
  | { k: "tier"; row: number; tid: string; y: number; height: number } // a tier node (TierHeader + its children)
  | { k: "head"; row: number; tid: string; y: number; height: number } // the TierHeader alone
  | { k: "worked"; row: number; y: number; height: number };
```
`y` values are **relative to the reporting View's parent** (RN `onLayout`); T29 converts to content offsets.

## Drawing (tokens.md §5 "GroupHead … HowItAddsUp"; colours by `Palette` field)
- **Group** root: `paddingHorizontal: GUTTER, paddingBottom: 12, marginTop: 12` (CSS 745), `testID={"group-" + row}`. Children in
  order: `GroupHead`, `origin && OriginLine` (the `.ghd.gsub` block: padding `8 0 4`), `TreeNodes nodes={tree} depth={0}`,
  `notKept.map(NotKeptRow)`, `quiet && Text meta sub`, `worked && HowItAddsUp`.
- **GroupHead** (`.ghd.gpin`): full-bleed `marginHorizontal: -GUTTER, paddingHorizontal: GUTTER, paddingVertical: 8`,
  `backgroundColor: card` (20h: no band), `borderTopWidth: 1.5, borderTopColor: ink`. Line 1: `Text TYPE.lead ink
  accessibilityRole="header"` `{title}` + (scope) nested `Text TYPE.kicker sub` `{words.KICKER_GAP + scope}` (inline, CSS 458
  margin-left 8; a nested Text cannot take margin, so the gap is two spaces kept in words.ts). Then the total block:
  - `head.form === "a"` (D12 a): `LineText line={head.lead} style={{...TYPE.metaBold, lineHeight: 19.97}} numStyle={TYPE.numeralMid}
    colour={ink}`; under it `Text` `{...TYPE.metaSemi, marginTop: 1}` `sub`: `words.OF_THE` + " " + `LineText line={head.words}`
    whose numeral is `metaBold` `ink` (CSS 860: `.gof .ghn` is NOT big in form a), terms ⓘ after.
  - `form === "b"`: the words line big (numeral `numeralMid`) then `head.shareOf` (`"↳ 2 of them from streams"`) as `metaBold
    ink`, its number `numeralMid` (CSS 861 `.g2`).
  - `form === null`: `LineText line={head.words} style={{...TYPE.metaBold, lineHeight: 19.97}} numStyle={TYPE.numeralMid}`.
  - `whyHere` → `Text {...TYPE.metaSemi, lineHeight: 16.85} sub` marginTop 2. `nolimit` → the words carry it (VM).
- **OriginLine** (`.gorigin` / `.tier .torigin`): row `flexWrap: "wrap"`, `alignItems: "center"`, `columnGap: 6`, marginTop 4;
  `LineText says` `metaSemi ink`; `tag` → bordered chip (1.5 `ink`, padding `2 5`, textHeavy 11.04/13.25 caps, letterSpacing .4);
  `howtell` → link "How to tell ›" (words.HOW_TO_TELL) `metaBold accent` minHeight 32 → `h.onOpen("hatch")`; `book` via LineText.
- **TreeNodes**: for each node: `tier` → `View testID={"tier-" + tid}` (`onLayout` → `onAnchor({k:"tier"})`) containing
  `TierHeader` (own `onLayout` → `{k:"head"}`) then `TreeNodes children depth+1`; if `tier.band` the whole node is wrapped in
  `SizeBand`. `kind` → `KindRow`; `release` → `ReleaseRow` (names `regsBodySemi ink` + `FishStrip`). `first` = index 0 in its list.
- **TierHeader** (`.tier`): `kicker` → `Text TYPE.kicker sub` paddingTop 8; line → `LineText line={tier.line}` with
  `style = tier.tier === "size" || tier.tier === "count" ? TYPE.tierSmall : TYPE.tier`, `numStyle={TYPE.numeral}`, colour `ink`,
  padding: size `12 0 8`, count `8 0 8`, others `16 0 8` (`2 0 8` after a kicker) — **no left border** (20f); the numeral
  margin `0 1` → nested Text cannot margin: the VM's text runs already carry the spaces. The size line's second number (`size:
  "big2"`) is the same `numeral` style. `sub` → `Text {...TYPE.meta, lineHeight: 16.85} sub` marginTop 4. `origin` → `OriginLine`.
- **SizeBand** (`section.tnode[data-tnode=size]`, 20h CSS 855): `marginHorizontal: -GUTTER, paddingHorizontal: GUTTER,
  paddingBottom: 8, marginTop: 12, borderTopWidth: 1, borderBottomWidth: 1, borderColor: line2`, **no backgroundColor**
  (r20h-one-surface). Kind rows inside take `inBand` → their top rule is `line2`.
- **KindRow** (`.krow.kt`): `paddingVertical: 8`, `borderTopWidth: first ? 0 : 1`, `borderTopColor: inBand ? line2 : line`.
  Order is fixed (20f "pictures first", r20f-pictures-first): (1) `says` → `LineText style={TYPE.regsBodySemi}` colour `kind.zero ?
  restricted : ink` (`folded` → no sentence); (2) meta row (`kmeta`: row wrap, `rowGap 4, columnGap 8`, marginTop 4): `size` chip
  (`TYPE.regsChip`, or textBold 15/19.5 when the VM marks a size rule — T18's `sizeRule: boolean`; `backgroundColor: tint`,
  `paddingVertical 3, paddingHorizontal 8`) and `qualifiers` (`TYPE.metaSemi sub`); (3) `FishStrip pictures` (marginTop 4);
  (4) `also[]` chips (`TYPE.metaBold`-size 12.48/16.85, `ink`, `backgroundColor: tint`, padding `3 8`, marginTop 4); (5) `duty`
  (`TYPE.duty ink`, marginTop 4 — v33 full weight; its "Steelhead stamp ›" link is a `link` run in a `Line` if T18 made duty a
  Line, else a trailing `Pressable` → `h.onOpen("tab:licence")`… see Notes); (6) `presence` → `LineText style={TYPE.meta}` `sub`
  marginTop 4. **Nothing but (1)/(2) may come before the pictures.**
- **FishStrip** (`.kpics`/`.kp`): row `flexWrap: "wrap"`, `columnGap: 8`. Each pic: `Pressable` `accessibilityRole="button"`
  `accessibilityLabel={words.FISH_LOOK(pic.name)}` ("What a Brook trout looks like" — prototype `fishBtn`), `minWidth: 80, maxWidth:
  132, minHeight: 44, paddingVertical: 4, gap: 2, alignItems: "flex-start"` → `Image source={fish[pic.code]} style={{ width: 80,
  height: 40 }} resizeMode="contain"` with **no backgroundColor** (r20h-picture-surface; D4 (a): the photo's own ground in dark too)
  and `accessibilityIgnoresInvertColors`; caption `Text {...TYPE.metaSemi, lineHeight: 14.98} accent numberOfLines={2}`
  `{pic.caption}` ("Brook trout ›" — the VM carries the nbsp before ›). No picture in `fish` for a code → the silhouette (the
  prototype's `SIL` path, `react-native-svg` 56×28, `fill={line2}`), not a button. `onPress` → `onOpen(pic.open)` (`sp:EB`).
- **NotKeptRow** (`.nkr`): row gap 12: `mark` (`TYPE.answer closed`) when present; column: `says` `TYPE.regsBodyBold` colour
  `status === "closed" ? closed : restricted`; `whose` `LineText meta sub` marginTop 2; `presence` `LineText meta sub`; `FishStrip`.
- **HowItAddsUp** (`.worked`): `marginTop: 16, paddingTop: 8, borderTopWidth: 1.5, borderTopColor: ink` (the 1.5 px ink rule).
  Head `Text TYPE.kicker sub` `words.HOW_IT_ADDS_UP` ("How it adds up") padding `6 0 4`, `accessibilityRole="header"`; `says`
  `TYPE.meta sub` margin `2 0 4`; list (`role="list"`): each `li` paddingVertical 6, `borderTopWidth: i ? 1 : 0`, `line`; q and a in
  ONE `Text` (they wrap together): `at === "here"`: q `TYPE.meta sub` + ":" (VM) then a `textBold 12.48/17.72 ink`;
  `"elsewhere"`: q `textSemi 12.48/17.72 ink` then a `TYPE.meta sub` (tokens.md §5, prototype 1200, CSS 688).

## Ports
prototype `buildKeep` group part (head, `tnode`, `tierH`, `tierWords`, `kt`, `nk`, `worked`, `dutyWords` — inventory names
@04765ab5; the v32→v33 diff touched `buildKeep.sizeOf`, `buildKeep.kt`, `buildKeep.bigWords`, added `dutyWords`), `fishBtn`
(inventory). The CSS rounds 20c–20i listed above; the census rows of tokens.md §5.

## Reads
`GroupVM` (`title, scope, head{form, lead, words, ofTotal, whyHere, shareOf, terms, nolimit}, origin, tree, notKept, worked,
quiet`), `TierVM`, `KindVM`, `ReleaseRowVM`, `FishPicVM {code, caption, open}` (+ `name` — see Notes), `NotKeptVM`.

## Tests (`Group.test.tsx`; VMs = `groupOf(id, i)`; texts compared whitespace-normalised)
1. **vedder_1010 group 0 head (form a)**: header text "Trout and char"; a `Text` with normalised content "2 a day per angler from
   streams" whose nested "2" has `fontSize: 17`; then "of the 4 a day per angler across Region 2, lakes included" whose "4" is
   NOT 17 px (12.48); the head View has `borderTopWidth: 1.5`, `borderTopColor: LIGHT.ink`, `backgroundColor: LIGHT.card`.
2. **Tier lines**: share kicker "All Region 2 streams"; line "Only 2 of the 4 may be from streams, not counting hatchery rainbow
   trout" with exactly one 26 px nested Text "2"; its origin "From streams, keep hatchery fish only — wild trout and char go back."
   + link "How to tell ›" + link "Rule book p.21 ↗"; size line "Only 1 of the fish in this box may be over 50 cm (20 in) — 2 if one
   is a hatchery steelhead" with two 26 px numerals "1" and "2" and font size 15 (tierSmall); own line "This river’s rule: up to 4
   hatchery rainbow trout a day Rule book p.22 ↗".
3. **r20b-number-once (render)**: within a tier line's outer `Text`, exactly one nested Text per VM `num` run and its digits
   appear once (the line's normalised text contains the number once where golden `big` has one entry).
4. **One line box (tokens.md §2.4)**: every nested numeral `Text` has no `lineHeight` in its style; every outer line `Text`
   has one (`TYPE.tier` 21.25 / `tierSmall` 18.75 / `regsBodySemi` 20.25).
5. **SizeBand** (r20h-one-surface, render form): the band View has `borderTopWidth: 1, borderBottomWidth: 1`, colour
   `LIGHT.line2`, and NO `backgroundColor` (or `transparent`); the size chips inside have `backgroundColor: LIGHT.tint`.
6. **KindRow order (r20f-pictures-first)**: for the steelhead row (vedder_1010 size band kind 3): children order = sentence
   ("Up to 2 may be hatchery steelhead only — wild steelhead go back How to tell ›"), meta (chip "over 50 cm (20 in)", qualifier
   "rainbow trout"), pictures ("Steelhead ›"), duty ("10 a year · Stop fishing the Vedder River after 2 a day · Record each on your
   licence right away Steelhead stamp ›", `TYPE.duty`), presence ("A rainbow trout over 50 cm (20 in) here counts as a steelhead.
   ⓘ"). Assert by `testID` order: `kind-says`, `kind-meta`, `kind-pics`, `kind-duty`, `kind-presence`.
7. **FishStrip (r20h-picture-surface)**: brook/brown/cutthroat row → 3 buttons named "What a Brook trout looks like", …; each
   `img` (react-native-web renders `Image` as a div with a background-image + an `img`) and its wrappers have no
   `backgroundColor` other than transparent, light AND dark; image box 80×40; captions "Brook trout ›", "Brown trout ›", "Coastal
   cutthroat trout ›"; pressing the first calls `onOpen("sp:EB")`. A code missing from `fish` renders the silhouette and no button.
8. **Size chip (v33)**: "keep 30 cm (12 in) or longer; release the rest" chip text present.
9. **NotKeptRow vedder_515**: "Rainbow trout and cutthroat trout: release every one" in `LIGHT.restricted`, `textBold`; whose "This
   river’s rule · Rule book p.22 ↗"; pictures "Rainbow trout ›", "Coastal cutthroat trout ›".
10. **HowItAddsUp vedder_1010**: top border 1.5 `LIGHT.ink`; head "How it adds up"; says "Your own limit, counted across every
    Region 2 stream you fish today."; 4 items; item 1 = "If you keep 4 hatchery rainbow trout: that’s your trout and char for the day,
    everywhere." with the answer in `textBold` ink; item 3 ("Kept 1 at a lake?") question in `textSemi` ink, answer in `sub`.
11. **Golden sweep**: for all 97 golden groups (96 states), the Group's normalised `textContent` equals the golden group's
    `lines` joined (`groups[i].lines` — the generator's per-group `lines()`), modulo the documented space rule. Expected 97/97.
12. Dark: vedder_1010 with `DARK` — no `backgroundColor` other than `DARK.card`/`DARK.tint`/transparent anywhere in the group.

## Acceptance
```sh
pnpm -C app exec vitest run packages/ui-native/src/regs/Group
pnpm -C app check
```

## Notes
- CONTRACT CHANGE: applied to contracts.md (§2.2 `FishPicVM.name`; `KindVM.sizeRule` = `size.rule`, §6 #4; `Run.link {tab}` §1 already; `AnchorEvent` §2.7).
- 20h removed every band: do NOT paint the group head or the size band with `tint` (the task list's "SizeBand (tint)" predates
  20h). The only tint left in a group: size chip, also-counts chip.
- "Pictures first": the prototype check fails on any `.kac/.duty/.kpres/.ksn` before `.kpics`. The component order above is
  the whole guarantee — test 6 pins it.
- Captions ≤ 2 lines (`numberOfLines={2}`); the prototype balances (`text-wrap: balance`) — RN has no balance; accept greedy
  wrapping, the 132 px max width keeps most captions on one line.
- `onAnchor` is wired now (cheap) so T29 needs no edits here; when absent, no `onLayout` handlers are attached.
