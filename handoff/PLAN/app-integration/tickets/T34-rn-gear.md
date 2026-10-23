# T34 — RN: GearLines, ChangesLine, SourceLine, RuleBookLink, ClosedNote, YesNoList, BaitList

Status: DONE — f93594ee.

Phase 3 · Depends on T27 (RN Keep tab: the Line renderer, `TermButton`, sheet opening, theme/type tokens from T22), T30,
T31 · Blocks T35 · Decisions **D6** (Rule book links: `Linking.openURL(SYNOPSIS_PDF)` on native, `#page=<pdf>` on web) ·
Size ≤ 1 day

## Goal
The Gear tab drawn in React Native from `GearVM` with no logic: the day line, the mode line, Lines · Hooks · Weight on the
line (label left, values big right), each line's quiet changes line ("Next" chip, untinted, dates bold, the data's words,
"· in force today" when on) and source line ("Province · Region 2 · This water’s ›" that opens the Sources sheet, then
"Rule book p.8 ↗" links), the closed sentence, and the Bait item (note with ⓘ, dated lines, ✓/✕ list, source line).

## Files (all under `app/packages/ui-native/src/regs/`)
- create `openBook.ts` with T43's exact signatures (`SYNOPSIS_PDF`, `bookUrl(pdf, os)`, `openBook(pdf)`) — the one
  implementation; T43 later adds `bare`/`pdf: null` to `RuleBookLink` and tests, nothing else
- create `GearLines.tsx`, `ChangesLine.tsx`, `SourceLine.tsx`, `RuleBookLink.tsx`,
  `ClosedNote.tsx`, `YesNoList.tsx`, `BaitList.tsx`, `GearTab.tsx`
- create `gear.render.test.tsx` (project `render`)
- modify `app/packages/core/src/regs/testing/vm-fixtures.ts` — add `GEAR_VEDDER_1010`, `GEAR_VEDDER_515`, `GEAR_CLOSED`
  (hand-copied from golden v33, frozen) 
- modify `app/packages/ui-native/src/regs/index.ts` — export the components
- modify `app/packages/core/src/regs/words.ts` — `BOOK: (p: number) => \`Rule book p.${p} ↗\``, `SYNOPSIS_PDF` (if T00 did not add them)

## Signatures
```tsx
type Open = (key: SheetKey) => void;
export function GearTab(p: { vm: GearVM; palette: Palette; onOpen: Open }): JSX.Element;      // closed ? <ClosedNote/> : day + GearLines + BaitList
export function GearLines(p: { mode: GearVM["mode"]; lines: readonly GearLineVM[]; palette: Palette; onOpen: Open }): JSX.Element;
export function ChangesLine(p: { change: ChangeVM; palette: Palette }): JSX.Element;
export function SourceLine(p: { src: SourceLineVM; palette: Palette; onOpen: Open; pages?: boolean /* default true; Licence passes false */ }): JSX.Element;
export function RuleBookLink(p: { printed: number; pdf: number | null; bare?: boolean; palette: Palette }): JSX.Element;  // T43's signature; T34 implements the non-bare, pdf≠null path
export function ClosedNote(p: { text: string; palette: Palette }): JSX.Element;
export function YesNoList(p: { list: YesNoVM; palette: Palette }): JSX.Element;
export function BaitList(p: { vm: BaitVM; palette: Palette; onOpen: Open }): JSX.Element;
```

## Ports (@04765ab5 — DOM/CSS only, the words are in the VM)
- `buildGear` gear section + `dayLine` → `GearTab`: `<section>` heading "Gear" (`accessibilityRole="header"`, TYPE.section
  style per tokens.md role `#gear h3`), `p.gday` (TYPE.small, `palette.sub`), then the mode line (`.gmodew .gmode .gval`:
  TYPE.answer weight, its ⓘ), `ul.glines` rows (`.glab` TYPE.small `palette.sub`, left column; `.gval` values stacked, each
  bold, the row's ⓘ after the last value), each row: ChangesLine(s) then SourceLine — order fixed by round 20g.
- `gnote` → `ChangesLine`: chip text `vm.chip` ("Now"/"Next") as a small caps label (`.chgi`, `palette.sub`, NO background,
  NO border tint — "untinted"), then `<b>{when}:</b> {says}` and, when `inForce`, `<em>· in force today</em>`. No link, no ⓘ,
  no Rule book page on the change (the change's rule is in the item's SourceLine). `accessibilityLabel` = the plain sentence.
- `itemSrc` → `SourceLine`: a Pressable text button `vm.who` (already ends " ›"), `accessibilityRole="button"`,
  `accessibilityLabel={vm.aria}`, `onPress={() => onOpen(vm.open)}`; then one `RuleBookLink` per page, wrapping.
- `RuleBookLink` (`a.book`): label `BOOK(printed)`; `accessibilityRole="link"`, `accessibilityLabel` `Rule book page ${printed}, opens the PDF`;
  onPress → `openBook(pdf)` (D6 (a): web `#page=<pdf>`, native the bare PDF URL — `bookUrl` owns the `Platform.OS` branch;
  no inline `Linking`/`Platform` in components).
- `p.foot` → `ClosedNote`: TYPE.body, `palette.sub`.
- `yesNo` → `YesNoList`: per column a small header (`.ynh` "Allowed"/"Not allowed", TYPE.section caps) then rows: mark glyph
  (`✓` `palette.open`, `✕` `palette.closed`; `aria-hidden`), name bold, note small under it (`palette.sub`). One list (not two
  columns) — round 20g. Row `accessibilityLabel` = `${head}: ${name}${note ? ", " + note : ""}`.
- bait `.gxc[data-k=bait]` → `BaitList`: label "Bait" (`.gxl`), notes (Line renderer — the ⓘ is a `term` run), dated
  ChangesLines, YesNoList, SourceLine.

## Reads
`RegsPage.gear` only.

## Tests (`gear.render.test.tsx`, `@testing-library/react` via react-native-web; imitate `WaterScreen.test.tsx`)
1. `GEAR_VEDDER_1010`: `getByRole("header", {name:"Gear"})`; text `"Today, Oct 10"`; row labels `Lines`, `Hooks`,
   `Weight on the line`; values `"1 line"`, `"One hook, lure or fly"`, `"Single barbless hook, no trebles"`, `"Up to 1 kg (2.2 lb)"`;
   the Hooks changes line reads `"Next May 1–31: fly fishing only"` (chip + text) and has no link inside
   (`within(change).queryByRole("link")` null); the Hooks source button `getByRole("button", {name: vm.src.aria})` (aria is not in the golden; T30 builds it as "Sources: " + words joined ", ")
   with text `"Province · Region 2 · This water’s ›"`; three links `"Rule book p.8 ↗"`, `"Rule book p.21 ↗"`, `"Rule book p.22 ↗"`.
2. Pressing that source button calls `onOpen("srcs:gl|hook")`; pressing `"Rule book p.21 ↗"` calls a mocked
   `openBook` with `23`; `bookUrl(23, "web")` = `SYNOPSIS_PDF + "#page=23"`, `bookUrl(23, "ios")` = `SYNOPSIS_PDF`.
3. Bait: ✓ rows `Worms`, `Roe (fish eggs)` + `"the fish or fish parts ban excepts it"`, `Water insects and crayfish` + `"on streams"`;
   ✕ `Fish or fish parts`; dated `"Next May 1–31: bait ban"`; source `"Province ›"`.
4. `GEAR_VEDDER_515`: mode `"Fly fishing only"` with one ⓘ button for term `fly_fishing` (its accessible name is the Line renderer's
   "What does <title> mean?" from T27); bait dated `"Now May 1–31: bait ban · in force today"`; the Weight row has no source button.
5. `GEAR_CLOSED` (`vedder_615`): only `"No fishing here until Jul 1."`; no "Today" line, no rows, no bait.
6. **Golden mount**: `GearTab` over the frozen `vedder_1010` GearVM fixture (written by T35's `gml.parity.test.ts` with
   `UPDATE_REGS_FIXTURES=1` — render tests cannot open the store) — the rendered text lines (block
   elements → lines, inline → space) equal `golden.states.vedder_1010.gear.lines_` exactly
   (`["Gear","Today, Oct 10","Lines 1 line","Province › Rule book p.8 ↗","Hooks One hook, lure or fly","Single barbless hook, no trebles ⓘ","Next May 1–31: fly fishing only", …,"Province › Rule book p.8 ↗"]`).
7. Style-token gate (T40's rule applied early): no colour literal; chip has no `backgroundColor`.

## Acceptance
```sh
pnpm -C app check
pnpm -C app exec vitest run --project render packages/ui-native/src/regs/gear
```

## Notes
- Components never compute: no `filter`/`join` of data, no `startsWith` — if a test needs a derived string, it belongs in T30/T31.
- `accessibilityState` is banned (check-platform); use `aria-*`.
- The day line is absent when `vm.day == null` (closed) — that is the VM's job; the component only checks `vm.closed`.
- D6 (b) changes only `openBook.ts` (a viewer + `deps.md` line); `RuleBookLink`'s signature stays.
