# T23 — RN: `RegsScreen` in `WaterScreen`, `RegsHeader`, the one scroll

Status: DONE — 830312a5.

Phase 1 · Depends on T11 (`useRegsStore`, `useRegsTarget`, `useRegulations`, `useRegsPage`, `regsDay`, `useAnglerProfile`), T16 (`HeaderVM`), T21 (`buildRegsPage` Keep
slice), T22 (tokens) · Blocks T24–T27, T29, T34, T35, T37 · Decisions D1 (a), D2 (a), D7 (a), D11 (a) · Size 1 day

Target **regs v33 @ 04765ab5**. Line refs: `app/design/regs-v11.html` @ 04765ab5 (CSS 1–880, script from 892).

## Goal
Opening a water shows the Regulations face: the existing `FaceBar` (Conditions | Regulations, D2 a) as the first row, then the
regs header — back ‹, the water's name, the date pill ("Today ▾ / Oct 10" with the status dot), the Map (or Parts) button, the
part's place label on two lines, the four tabs "What you can keep · Gear · Methods · Licence" — fixed above ONE `ScrollView`
that holds the four tab sections (Keep drawn by T24–T27; Gear/Methods/Licence slots filled in phase 3). The day is the Shell's
`mapDay` (null = today), shared with the map's legend (D7 a); the pill opens a small "Which day?" sheet with the existing
`MapDay` stepper. `RegulationsPlaceholder` is no longer mounted.

## Files
- create `app/packages/ui-native/src/regs/RegsScreen.tsx` — layout: header + scroll + (T29) pin bar; owns `sheet`, `tab`, `partIx`
- create `app/packages/ui-native/src/regs/RegsHeader.tsx` — the header (no logic beyond the label fit)
- create `app/packages/ui-native/src/regs/DaySheet.tsx` — `Sheet` + `MapDay` (D7 a)
- create `app/packages/ui-native/src/regs/RegsHeader.test.tsx`, `app/packages/ui-native/src/regs/RegsScreen.test.tsx`
- modify `app/packages/ui-native/src/WaterScreen.tsx` — mount `RegsScreen` in place of the `ScrollView`/`Section`/
  `RegulationsPlaceholder` block; keep the loading/failed branches and `CrossLink` (moved to the end of the scroll content as the
  last child, unchanged words); drop the duplicate `export type Face` (:30) — `Faces.tsx` owns it
- modify `app/packages/ui-native/src/WaterScreen.test.tsx` — case 1 now asserts the header (below) instead of "Regulations are
  coming"; cases 2–3 unchanged
- modify `app/packages/ui-native/src/Shell.tsx` — pass the day and the map action (exact edits below)
- modify `app/apps/mobile/src/App.tsx` — pass `fish` (the image map from T20) to `Shell` (the `regs` URL is T01's, `openRegsDb` T11's)
- keep `app/packages/ui-native/src/RegulationsPlaceholder.tsx` (still the fallback when the regs store is absent/failed — see
  Notes); it is no longer the normal path

## Signatures
```ts
// RegsScreen.tsx
import type { RegsPage, Tab, SheetKey } from "@app/core";      // T00/T21 re-export from core's index
export interface FishImages { readonly [code: string]: ImageSourcePropType }   // T20 defines it; re-exported here if T20 put it in core as a string map
export interface RegsScreenProps {
  page: RegsPage;
  palette: Palette;
  fish: FishImages;
  /** The selected day; null = today (Shell `mapDay`). */
  day: Date | null;
  onDay: (d: Date | null) => void;
  onBack?: () => void;
  /** D1 (a): frame this water on the Map tab. Absent → the action is not drawn even when header.action.kind === "map". */
  onMap?: () => void;
  /** The FaceBar's Conditions press (WaterScreen passes its first-reach callback). */
  onConditions?: () => void;
  /** The part being shown; RegsScreen calls it from the Parts sheet (T37). */
  onPart?: (partIx: number) => void;
  /** Phase-3 sections; absent → that tab section is not drawn and its tab is still listed (header-one-shape). */
  gear?: React.ReactNode; methods?: React.ReactNode; licence?: React.ReactNode;
  /** The last child of the scroll: WaterScreen passes its private `CrossLink` ("and the conditions") — self-check: `CrossLink`
   *  is a non-exported function in WaterScreen.tsx (:141), so RegsScreen cannot import it. */
  footer?: React.ReactNode;
}
export function RegsScreen(p: RegsScreenProps): JSX.Element;

// RegsHeader.tsx
export interface RegsHeaderProps {
  header: HeaderVM; palette: Palette;
  selected: Tab;
  onBack?: () => void;
  onDate: () => void;
  onAction: (kind: "map" | "parts") => void;
  onTab: (t: Tab) => void;
}
export function RegsHeader(p: RegsHeaderProps): JSX.Element;
/** fitLabel (prototype 1001–1004): the full label when it lays out in ≤ 2 lines, else the short one. Pure; exported for the test. */
export function pickPlace(place: { full: string; short: string }, measuredHeight: number | null, lineHeight: number): string;
//   measuredHeight null (not laid out yet, or jsdom 0) → full;  else measuredHeight <= lineHeight * 2 + 1 ? full : short

// DaySheet.tsx
export function DaySheet(p: { open: boolean; onClose: () => void; palette: Palette; day: Date | null;
                              onDay: (d: Date | null) => void }): JSX.Element;   // title words.DAY_SHEET_TITLE = "Which day?"

// WaterScreen.tsx — new props (all optional so existing callers and tests compile)
export function WaterScreen(p: {
  source: RegsSource; item: ItemId; palette: Palette; onBack?: () => void;
  onConditions?: (section: SectionId) => void;
  regsUrl?: string;                // T01: the regs store URL; absent → RegulationsPlaceholder (store not served)
  openRegsDb?: RegsOpenDb;         // T11: the injected opener
  day?: Date | null; onDay?: (d: Date | null) => void; section?: SectionId | null;   // T01
  onMap?: () => void;
  fish?: FishImages;
}): JSX.Element;
```
WaterScreen body when `useWater` is ready (hooks exactly as T11 defines them; contracts.md §2.8):
```tsx
const storeA = useRegsStore(source, regsUrl, openRegsDb);       // Async<RegsStore | null>
const targetA = useRegsTarget(storeA.value, item, section ?? null);   // Async<RegsTarget | null> (T09 targetFor: the tapped part)
const [picked, setPartIx] = useState<number | null>(null);      // a Parts-sheet choice overrides the tapped part
const target = picked == null ? targetA.value : { kind: "part" as const, item, partIx: picked };
const inputs = useRegulations(storeA.value, target, day ?? null);   // Async<RegsInputs | null>; null day = today
const [who] = useAnglerProfile();                                // D8 (a)
const { today } = regsDay(day ?? null);
const page = useRegsPage(inputs.value, who, today);              // RegsPage | null — buildRegsPage in T11's useMemo, never here
// page → <RegsScreen page={page} … onConditions={() => first && onConditions?.(first)} onPart={setPartIx} />
// any of the three Async loading → FishSpinner (as today); failed, or !(regsUrl && openRegsDb) → the old ScrollView with RegulationsPlaceholder (unchanged words)
```

### Shell.tsx edits (exact)
- props: add `fish?: FishImages;` to `Shell`'s destructuring and type (beside `feed`). `regs`, `openRegsDb` and the water mount's
  `regsUrl`/`openRegsDb`/`day`/`onDay`/`section` are already passed by T01/T11.
- the water mount (:423): add
  `fish={fish}`
  `onMap={() => { const id = item; setItem(null); setPicked(id); setTab("map"); }}` — the same "frame it on the map" path
  search uses (`setPicked(id); setTab("map")`, :450), minus re-opening the page.
- nothing else: `TabBar` stays under the water screen (D1 a, :675); the Map tab's `MapDay` (:663) reads the same `mapDay`, so a day
  chosen on the page colours the map and vice versa.

### Layout (RegsScreen)
```
<View style={{ flex: 1, backgroundColor: palette.card }}>
  <View style={{ paddingHorizontal: 18, paddingTop: 14, paddingBottom: 8 }}>      // D2 (a): FaceBar first, NO onBack
    <FaceBar palette face="regulations" onFace={f => f === "conditions" && onConditions?.()} />
  </View>
  <RegsHeader … />                                     // fixed sibling: never inside the ScrollView (header-one-shape)
  <View style={{ flex: 1 }} onLayout={…viewport height → T29}>
    <ScrollView ref={scroll} scrollEventThrottle={16} onScroll={…T29} contentContainerStyle={{ paddingBottom: 32 }}>
      <View nativeID="regs-keep">   {/* KeepTab (T27) */}   </View>
      {gear && <View nativeID="regs-gear">{gear}</View>}
      {methods && <View nativeID="regs-methods">{methods}</View>}
      {licence && <View nativeID="regs-licence">{licence}</View>}
      {footer}                                          // WaterScreen's CrossLink, passed in (it stays private there)
    </ScrollView>
    {/* <PinBar/> absolute top 0 — T29 */}
  </View>
  <DaySheet …/>  {/* SheetHost — T37 */}
</View>
```
ONE ScrollView: no nested vertical scroll anywhere in the page (the fish strip wraps; it does not scroll sideways).

### RegsHeader drawing (tokens.md §5 "RegsHeader")
- root `View`: `backgroundColor: card`, `borderBottomWidth: 1.5`, `borderBottomColor: ink`.
- row 1 (height 48, `paddingLeft: 4, paddingRight: 8`, gap 8, `alignItems: "center"`):
  back `Pressable` 44×44, `accessibilityRole="button"`, `accessibilityLabel={words.BACK_TO_SEARCH}` ("Back to search", prototype
  1014) — text "‹" textBold 22 `accent`; drawn only when `onBack`.
  name `Text` `TYPE.headerName` `ink`, `numberOfLines={2}`, `accessibilityRole="header"`, flex 1.
  date pill `Pressable` `accessibilityRole="button"`, `accessibilityLabel={header.dateAria}` ("Change date, now Oct 10"), minHeight
  44, `paddingHorizontal: 6`, row gap 6: dot `View` 8×8 `backgroundColor` = `{open: palette.open, closed: palette.closed,
  faint: palette.sub}[header.dot]`; column: `Text TYPE.metaHeavy accent` `{header.dayWord}` + nested `Text` 10 px " ▾"
  (words.DATE_CARET), `Text metaSemi(lh 14.98) sub` `{header.date}`.
  action (only when `header.action` and, for `map`, `onMap`): `Pressable` 44×44 min, column, gap 1, `accessibilityRole="button"`,
  `accessibilityLabel={header.action.aria}`; icon = the TabBar's map glyph (`icons.tsx` `<Icon name="map" colour={palette.accent} size={20} />`)
  or, for Parts, a new `"list"` entry in `icons.tsx` `IconName`/`SHAPES` (today: map·search·conditions·spots only; frame
  `M4 6h15M4 11.5h15M4 17h15`, prototype `placeGo`);
  label `Text metaBold(lh 13.73) accent` `{header.action.label}`.
- place row: `paddingLeft: 56, paddingRight: 12, paddingBottom: 6`, marginTop −2. **Always present** (an empty slot of the same
  height when `header.place === null`, D11 a — the header is one size everywhere). Visible `Text metaSemi sub`,
  `numberOfLines={2}`, fixed `height: 32.44` (2 × 16.22), text = `pickPlace(place, measured, 16.22)`. The measuring twin: an
  absolutely positioned `Text` with the same style and width, `{place.full}`, `opacity: 0`, `aria-hidden`,
  `pointerEvents="none"`, `onLayout={e => setMeasured(e.nativeEvent.layout.height)}` — works on native and web (react-native-web
  does not implement `onTextLayout`, so do not use it). The full label is the visible `Text`'s `accessibilityLabel` whatever is
  shown.
- tabs row: `paddingHorizontal: 16`; `View accessibilityRole="tablist" accessibilityLabel={words.SECTION}` ("Section"), row,
  gap 16. Each tab: `Pressable accessibilityRole="tab" aria-selected={selected === t.tab}` (check-platform bans
  `accessibilityState`), minHeight 34, `paddingTop: 2`, `borderBottomWidth: 3`, `borderBottomColor: on ? ink : "transparent"`;
  `Text TYPE.regsTab` `on ? ink : sub` `{t.label}`. `onPress={() => onTab(t.tab)}`. Hit area: `hitSlop={{top: 5, bottom: 5}}`
  (34 + 10 = 44).

## Ports
- prototype `hdr` 1012–1020, `placeRow` (1001-ish, inventory `placeRow`), `placeGo`, `fitLabel` (inventory names; v32 refs
  994/996/1001 moved +7 in v33 — read `port/golden/inventory.json`). The header's words/labels are T16's (`HeaderVM`); T23 only
  draws them. `fitLabel`'s "≤ 2 lines at lineHeight + 1 px" becomes `pickPlace`.
- prototype `sheetHtml` `date` 1686–1688 → `DaySheet` (title only; the body is the app's `MapDay`, not the prototype's snapshot
  day list — the app reads any day).
- CSS: 54–57, 441–454, 515–537, 770 (header), 78–82 (sections).

## Reads
`page.header` (`HeaderVM`: `name, dayWord, date, dateAria, dot, place{full, short}, action{kind,label,aria}, tabs[{tab,label}]`),
`page.keep` (passed on to T27), `page.status` (nothing else).

## Tests
`RegsHeader.test.tsx` (render project; VMs built from golden headers — `core/src/regs/testing/vm-fixtures.ts` gains
`headerOf(stateId)` that maps `golden.states[id].header` to `HeaderVM`: `day→dayWord`, `place.{full,short}`, `action`,
`tabs[].sec→tab`, `tabs[].text→label`):
1. **vedder_1010**: heading "Vedder River"; a button named "Change date, now Oct 10" containing text "Today" and "Oct 10"; a
   button named "View on map: where these rules apply, Mouth → Vedder Crossing Bridge" with text "Map"; text
   "Mouth → Vedder Crossing Bridge"; `getAllByRole("tab")` texts = `["What you can keep","Gear","Methods","Licence"]`; the first
   has `aria-selected="true"`, the rest `"false"`; the dot's background is `LIGHT.open`.
2. **vedder_615**: pill texts "Mon", "Jun 15"; dot `LIGHT.closed`.
3. **bc1_501**: action button named "Other parts — now 4 side channels, joining 55–59.1 km from the mouth", text "Parts"; place
   shows "4 side channels, joining 55–59.1 km from the mouth" (jsdom measures 0 → full).
4. **klwa_1010**: `place === null` → the place row exists with no text (an element with `testID="regs-place"` and empty text),
   so the header has the same rows as vedder_1010 (**header-one-shape**: same child count / testIDs).
5. **header-label-two-lines** (pure): `pickPlace({full:"From the CNR Mile 66 Trestle up to the CNR Bridge (mile 70.2)", short:
   "CNR Mile 66 Trestle → CNR Bridge (mile 70.2)"}, 33.44, 16.22)` → full; with 48.66 → short; with `null` → full; `0` → full
   (jsdom). (Golden cow1_1010 `place.shown` = full.)
6. Press "Gear" → `onTab("gear")`; press the pill → `onDate()`; press "Map" → `onAction("map")`; press "‹" → `onBack()`.
7. Static: no `fontWeight`, no `palette.faint/wash` (T22 test 4 now covers these files).

`RegsScreen.test.tsx`:
8. **header-one-shape**: mount with the vedder_1010 page (T21 fixture or a hand VM), capture `RegsHeader`'s rendered text;
   fire `scroll` on the `ScrollView` (react-native-web `ScrollView` is a `div`; `fireEvent.scroll(el, {target:{scrollTop:900}})`)
   → the header's text and testIDs are unchanged, and the header element is NOT a descendant of the scroll element.
9. **One scroll**: exactly one element with `overflow-y: auto|scroll` style under the screen (react-native-web renders
   `ScrollView` with `overflowY: "auto"`): `container.querySelectorAll('[style*="overflow-y"]')` filtered to scroll → length 1.
10. FaceBar: a radiogroup with "Conditions" and "Regulations"; pressing Conditions calls `onConditions`.
11. The pill opens a dialog titled "Which day?" containing the `MapDay` "One day earlier" button; pressing it calls
    `onDay(<yesterday's Date>)`.

`WaterScreen.test.tsx` case 1 (fixture source, no `regs` prop) keeps passing unchanged — no regs URL → the placeholder path; add a
case with `regsUrl` + `openRegsDb` set and stubs `useRegsStore`/`useRegsTarget`/`useRegulations` ready and `useRegsPage` (vi.mock `@app/ui` → the vm-fixtures page) asserting the heading
"Vedder River" and no "Regulations are coming".

## Acceptance
```sh
pnpm -C app exec vitest run packages/ui-native/src/regs/RegsHeader packages/ui-native/src/regs/RegsScreen packages/ui-native/src/WaterScreen
pnpm -C app check
pnpm -C app dev:web   # open Vedder: header 48 + place 36 + tabs 37 ≈ 121 px under the FaceBar; scroll — the header does not move
```

## Notes
- **D2 (a) cost**: FaceBar (~44 px + 22 padding) sits above the header, so the scroll area is ~66 px shorter than the
  prototype's 703 px. The pin bar's position is the scroll area's top (T29 measures, nothing hard-coded), so this never breaks
  the pins — only how much list a screenful shows.
- The FaceBar is given **no `onBack`**: the regs header's ‹ is the one back (two backs in two rows would be the first thing a
  reader reports).
- `header.action.kind === "map"` but no `onMap` (e.g. a test or a host without a map) → draw nothing in the slot, but keep the 44
  px width so the name does not reflow between waters.
- The tabs are `role=tab` over ONE scroll (D1 a); they are not a pager. T29 wires press → scroll-to and scroll → selected; until
  then `selected` is local state set by press.
- `RegulationsPlaceholder` remains for "no regs store served" (`regs` null — the fixture bundle, CI) and "store refused"
  (`failed`, e.g. digest mismatch) — the honest "check the synopsis" sentence, never a blank.
- `useRegulations` must key on the day's calendar date, not the `Date` object (a new `Date()` each render would refetch forever)
  — T11's concern; this ticket passes `day` (null for today) straight through, and `today` comes from T11 `regsDay`.
