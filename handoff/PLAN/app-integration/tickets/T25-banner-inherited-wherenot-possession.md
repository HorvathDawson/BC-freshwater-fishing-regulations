# T25 — RN: `ClosedBanner`, `InheritedLine`, `WhereNot`, `PossessionRow` (+ the shared `LineText`)

Status: DONE (built in T21) — 3665f151 (KeepBlocks.tsx, LineText.tsx).

Phase 1 · Depends on T14 (`BannerVM`), T15 (`InheritedVM`, `WhereNotVM`, possession), T00 (`Line`/`Run`, `words.ts`), T22, T23 ·
Blocks T26 (reuses `LineText`), T27 · Decisions D6 (Rule book link opening) · Size ½ day

Target **regs v33 @ 04765ab5** (CSS 118–121, 263–264, 330–336, 553–559, 714–718, 764).

## Goal
The Keep tab's non-group blocks, drawn from their VM slices: the closed-day banner (red rule, the big phrase, the sentence, the
chain of closing rules each with "Source" and its Rule book page, "See the rules from then ›" that moves the day), the
"By another water’s rule" line (accent rule + faint accent ground), "Where you can’t fish" (kicker + small items with their
book pages), and the Possession row ("Possession · 2 days’ quota ›" opening its sheet). Plus `LineText`: the ONE component that
draws a `Line` (text / num / term ⓘ / book / link runs), used by every later RN ticket.

## Files
- create `app/packages/ui-native/src/regs/LineText.tsx` — `Line` → nested `Text`s
- create `app/packages/ui-native/src/regs/RuleBookLink.ts` — `openBook(printed, pdf)` (D6 a: `Linking.openURL(SYNOPSIS_URL)`
  native, `SYNOPSIS_URL + "#page=" + pdf` web — a `.native.ts`/`.web.ts` pair per check-platform; `SYNOPSIS_URL` from
  `core/src/regs/words.ts` or T34's constant if it landed first)
- create `app/packages/ui-native/src/regs/ClosedBanner.tsx`, `InheritedLine.tsx`, `WhereNot.tsx`, `PossessionRow.tsx`
- create `app/packages/ui-native/src/regs/KeepBlocks.test.tsx`
- modify `app/packages/core/src/regs/testing/vm-fixtures.ts`: `bannerOf(id)`, `inheritedOf(id)`, `whereNotOf(id)`, `possessionOf(id)`
  — golden → VM (text runs + `book` runs parsed from "Rule book p.N ↗" + golden `pages[].pdf`)

## Signatures
```ts
// LineText.tsx — the only renderer of core's Line
export interface LineHandlers {
  onOpen: (k: SheetKey) => void;              // term → `gl:<id>` (one id) or `gl:<a,b>` (2+), link → its `open`
  onDay?: (d: Day) => void;                   // link with { day }
}
export function LineText(p: {
  line: Line; palette: Palette; style: TextStyle; colour: string;   // the sentence's TYPE + colour; runs inherit them
  numStyle?: TextStyle;                                            // the big numeral face (default TYPE.numeral; T26 passes numeralMid where the census says so)
  h: LineHandlers;
}): JSX.Element;
// run → element:
//  text  → raw string child
//  num   → <Text style={size === "inline" ? textBoldAtSize : numStyle}>{s}</Text>   (NO lineHeight on the inner Text — tokens.md §2.4)
//  term  → <Text accessibilityRole="button" aria-label={…} onPress hitSlop={{top:8,bottom:8,left:8,right:8}} style={{color: accent}}>ⓘ</Text>
//          one id: label `What does ${title} mean?` (title from the VM: T00 gives `term` runs a `titles` field, or words.GL_LABEL(id));
//          2+ ids: text words.WHAT_THESE_MEAN ("What these words mean"), TYPE.metaSemi, underline, accent
//  book  → <Text accessibilityRole="link" onPress={() => openBook(printed, pdf)} style={{...TYPE.metaBold, color: accent}}>{words.BOOK(printed)}</Text>   // "Rule book p.21 ↗"
//  link  → <Text accessibilityRole="button" onPress={…} style={{fontFamily: FACE.textBold, color: accent}}>{s}</Text>
// runs are joined with " " between element boundaries exactly as `plain()` does (contracts §1), so the rendered textContent
// whitespace-normalised equals plain(line).

export function ClosedBanner(p: { banner: BannerVM; palette: Palette; h: LineHandlers }): JSX.Element;
export function InheritedLine(p: { inherited: InheritedVM; palette: Palette; h: LineHandlers }): JSX.Element;
export function WhereNot(p: { whereNot: WhereNotVM; palette: Palette; h: LineHandlers }): JSX.Element;
export function PossessionRow(p: { possession: { label: string; value: string; open: SheetKey }; palette: Palette; onOpen: (k: SheetKey) => void }): JSX.Element;
```
VM slices (contracts §2.2; the presenter tickets own them): `BannerVM {big: string[]; paras: string[]; chain: {text; scope;
rule: RuleKey|null; book: Line}[]; reopen: {text; day}|null}`, `InheritedVM {label; lines: {says: Line; caution}[]}`,
`WhereNotVM {heading; scope; items: {text: Line}[]}`.

## Drawing (tokens.md §5)
- **ClosedBanner**: `View` margin `8 GUTTER 0`, padding `10 0 4`, `borderTopWidth: 1.5`, `borderTopColor: closed`. Each `big[]` →
  `Text TYPE.lead closed`; each `paras[]` → `Text TYPE.regsBody ink` marginVertical 4; chain → a list (`accessibilityRole="list"`
  on web via `role="list"`), each item a row `minHeight: 44`, gap 8, `alignItems: "center"`, `borderTopWidth: 1, borderTopColor:
  line`: `Text TYPE.regsBody ink` flex 1 (`text`; `scope` is part of the golden text — the VM keeps it separate so the component
  appends it as a nested `Text` after a space), then (when `rule`) a "Source" chip — `Pressable` minHeight 32, `paddingHorizontal: 6`,
  `backgroundColor: tint` (CSS 715), `Text metaBold sub` `words.SOURCE` → `onOpen("rule:" + rule)` — then `LineText line={book}`.
  `reopen` → `Pressable` minHeight 44 `accessibilityRole="button"`, `Text regsBodyBold accent` `{reopen.text}` → `h.onDay(reopen.day)`.
- **InheritedLine**: `View` margin `8 GUTTER 0`, padding `6 0 6 10`, `borderLeftWidth: 3, borderLeftColor: accent`, `overflow:
  "hidden"`; first child an absolute fill `View { ...StyleSheet.absoluteFillObject, backgroundColor: accent, opacity: 0.06 }`
  (= CSS `color-mix(accent 6%, card)`, tokens.md §1.2). `label` → `Text TYPE.kicker accent`; each line: `LineText says regsBody
  ink` marginTop 2; `caution` → `Text TYPE.meta sub` marginTop 2.
- **WhereNot**: `View` margin `6 GUTTER 0`, padding `10 0 6`, `borderTopWidth: 1, borderTopColor: line2`. Head row (baseline,
  wrap, gap 8): `Text TYPE.kicker sub accessibilityRole="header"` `{heading}` + (scope) `Text TYPE.metaSemi sub`. Items: each
  `LineText text style={{...TYPE.meta, lineHeight: 17.47}} colour={sub}` paddingVertical 3; book runs inline with marginLeft 6.
- **PossessionRow**: `View` margin `8 GUTTER 0`, `borderTopWidth: 1.5, borderTopColor: ink`; one `Pressable` row, padding `9 0`,
  minHeight 44, gap 10, `accessibilityRole="button"`, `accessibilityLabel={label + ": " + value}`: `Text regsBody ink` flex 1
  `{label}`, `Text regsBodyBold ink` `{value}`, chevron (the app's `›` glyph in `sub`, 14 px — the prototype's `CHEV` svg) →
  `onOpen(open)`.

## Ports
page v36 `closedBanner` 520–541 / `closersAt` 517 + prototype dedupe (`buildKeep` banner part), `inheritedHtml` 611,
`whereHtml` 616, `possessionHtml` 626 — the words are the presenters' (T14/T15); T25 draws. CSS refs above.

## Reads
`page.keep.banner`, `.inherited`, `.whereNot`, `.possession`; `Line` runs.

## Tests (`KeepBlocks.test.tsx`; VMs from golden via vm-fixtures; compare `textContent` whitespace-normalised)
1. **vedder_615 banner**: texts "No fishing for any game fish Jun 1–30.", a list item containing "Jun 1–30. This water", a
   button "Source", a link "Rule book p.22 ↗"; a button "See the rules from then ›" — pressing it calls `onDay` with the VM's day.
   The block's top border colour is `LIGHT.closed`.
2. **chw0_1010 banner**: `big` "Closed all year" drawn in `TYPE.lead` `LIGHT.closed`.
3. **glenn_501 inherited**: kicker "By another water’s rule" (rendered upper-case via `textTransform`, textContent keeps the
   data's case), the line "Open Apr 1–Jun 14: the Duncan River’s exemption from the spring stream closure covers its
   tributaries. Rule book p.36 ↗", the caution "Trout Lake’s row, nearer this stream, prints no such exemption; …"; a child
   with `opacity: 0.06` and `backgroundColor: LIGHT.accent`; left border `LIGHT.accent` 3 px.
4. **vedder_615 whereNot**: heading "Where you can’t fish"; 2 items; each ends with a link "Rule book p.9 ↗"; pressing it calls
   `openBook(9, 11)` (mock `RuleBookLink`).
5. **vedder_1010 possession**: a button named "Possession: 2 days’ quota"; pressing it calls `onOpen("pos:vedder_1010|pos")`
   (the golden `open`).
6. **Golden sweep**: for every state, mount each present block and compare its normalised `textContent` to the golden
   `lines` of that block (`banner.lines`, `inherited` → label + lines + cautions, `whereNot.items[].text`, possession
   `label + value`): banner 27 states (12 with a `big` phrase), inherited 2, whereNot 133, possession 107 (27 states have none).
7. `LineText` unit: a line `[text "Up to", num "2" big, text "may be hatchery steelhead", link "How to tell ›" → hatch]`
   renders ONE outer `Text` whose style has `lineHeight`, an inner `Text` "2" whose style has `fontSize: 26` and **no**
   `lineHeight`; pressing "How to tell ›" calls `onOpen("hatch")`; `textContent` normalised = `plain(line)`.
   A `term` run with ids `["steelhead"]` renders a button "What does Steelhead mean?" (label from the VM) with text "ⓘ"; with 2
   ids renders "What these words mean".

## Acceptance
```sh
pnpm -C app exec vitest run packages/ui-native/src/regs/KeepBlocks
pnpm -C app platform && pnpm -C app check
```

## Notes
- `LineText` is the single place runs become elements — T26, T27, T34, T35, T37 import it. Never re-implement run drawing.
- Hit areas: the CSS grows `.book`/`.srcbtn`/inline `.linkbtn` hit boxes with a `::before` (CSS 717–718) without moving the
  line; RN: `hitSlop={{top: 12, bottom: 12, left: 4, right: 4}}` on the nested pressable `Text` — no padding (padding would
  grow the line box).
- The ⓘ must not inflate its line (CSS 749): no padding on the nested `Text`, hitSlop only.
- D6 (a): no `#page=` on native (the label already says the printed page); the pair `RuleBookLink.native.ts` /
  `RuleBookLink.web.ts` keeps check-platform's "pairs complete" rule.
