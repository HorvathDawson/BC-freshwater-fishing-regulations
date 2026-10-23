# T35 — RN: WaysList, Boats, ReferenceFold, WhoSelector, LicenceAnswer; Gear/Methods/Licence golden parity

Status: DONE — 28e6d404, aeeb52de.

Phase 3 · Depends on T11 (`useAnglerProfile`), T21 (`buildRegsPage`), T27, T30–T34 · Blocks T36 (sheets from these
links), T41 · Decisions **D8** (profile per session, default B.C. resident 16+), D6 (via `SourceLine`/`RuleBookLink`) ·
Size ≤ 1 day

## Goal
The Methods and Licence tabs drawn from `MethodsVM` / `LicenceVM`, and the proof that the three Phase-3 tabs equal the
prototype on every golden state: Ways to fish (one ✓/✕ list, conditions small), Boats, the **Reference** fold closed by
default (label "Reference", title "Handling fish", sub "21 rules that apply everywhere"), and the Licence tab: "Who is fishing? ⓘ", two
radio groups, the profile label, the answer blocks, the source line without pages.

## Files
- create `app/packages/ui-native/src/regs/{WaysList,Boats,ReferenceFold,MethodsTab,WhoSelector,LicenceAnswer,LicenceTab}.tsx`
- create `app/packages/ui-native/src/regs/methods-licence.render.test.tsx`
- create `app/conformance/src/regs/gml.parity.test.ts` — parity over `buildRegsPage` (all states × profiles), T12 harness (tests.md §1.2)
- modify `app/packages/core/src/regs/testing/vm-fixtures.ts` — `METHODS_BC1_501`, `LICENCE_VEDDER_1010_ADULT`, `LICENCE_VEDDER_1010_U16_NON`, `LICENCE_CLOSED`
- modify `app/packages/ui-native/src/regs/index.ts`; the Regs screen composition (T23's `RegsScreen`) to mount the three tabs

## Signatures
```tsx
export function MethodsTab(p: { vm: MethodsVM; palette: Palette; onOpen: Open }): JSX.Element;
export function WaysList(p: { vm: NonNullable<MethodsVM["ways"]>; palette: Palette; onOpen: Open }): JSX.Element;   // label + YesNoList + SourceLine
export function Boats(p: { vm: NonNullable<MethodsVM["boats"]>; palette: Palette; onOpen: Open }): JSX.Element;
export function ReferenceFold(p: { vm: NonNullable<MethodsVM["reference"]>; palette: Palette; onOpen: Open; initiallyOpen?: false }): JSX.Element;
export function LicenceTab(p: { vm: LicenceVM; palette: Palette; onOpen: Open; onWho: (w: Who) => void; who: Who }): JSX.Element;
export function WhoSelector(p: { vm: LicenceVM; who: Who; palette: Palette; onWho: (w: Who) => void; onOpen: Open }): JSX.Element;
export function LicenceAnswer(p: { blocks: readonly LicenceBlockVM[]; palette: Palette; onOpen: Open }): JSX.Element;
```

## Ports (@04765ab5 — DOM/CSS only)
- `WAYS` 1573 / `.gxc[data-k=methods]` → `WaysList`: label "Ways to fish" (`.gxl`), `YesNoList` (T34), `SourceLine`.
- `.gxc[data-k=boats]` → `Boats`: label, plain list (`ul.gxi.plain`), `SourceLine`.
- `details.gref` → `ReferenceFold`: a Pressable summary row — small label `vm.label` ("Reference"), bold `vm.title`, em `vm.sub`;
  `aria-expanded` (not `accessibilityState`), local `useState(false)`; open: each group's head (`.hh`, TYPE.section) and its
  items as a plain list, then `SourceLine`. Closed: the body is not rendered (golden `open:false`; T41 checks the toggle).
- `licence()` 1381–1426 `.lchoose` → `WhoSelector`: the question as a small label with the ⓘ (`questionTerms`, Line
  renderer `term` run); two `Choice` (`ui-native/src/Sheet.tsx`: `role="radiogroup"`, each option `role="radio"`,
  `aria-checked`) — `label = axis.aria` ("Who is fishing?") for both; options `{k: value, t: text}`; `value` = the checked
  option; age onChange → `onWho({...who, age: k as "adult" | "u16"})`, residency → `onWho({...who, residency: k})`.
  When `vm.axes` is empty (closed day) render nothing. Then `p.resfor` = `vm.label` (TYPE.small, `palette.sub`).
- licence blocks → `LicenceAnswer`: a mechanical `switch (b.cls)`: `classbox` (bold head + ⓘ, body, "Source" text button →
  `onOpen(b.source)`), `classbox youth`, `flag` ("?" mark + text), `scopenote`, `needs` (label "You need"; rows: name bold
  + ⓘ, `when` small, `price` mono-tabular per tokens.md `.needs li > .price`; then the `onlyIf` head "Also, only if…" and its
  rows; then the price note), `paper` (lead + bold strong + tail), `nymbox`, `muted`, `lostlist lopen` (small heading, then
  each card: "For every angler:" small, label, paths separated by "or").
- `LicenceTab`: heading "Licence", WhoSelector, LicenceAnswer, `SourceLine` with `pages={false}` (licence `src` has no pages;
  it opens `licsrc:<profileKey>`, e.g. `licsrc:u16_res`; golden key via `goldenKeyOf`).
- Wiring (T23 `RegsScreen`): `const [who, setWho] = useAnglerProfile()` (D8 (a): Shell-level, in memory, default
  `{age:"adult", residency:"resident"}`); `useRegsPage(inputs, who, today)` (T11) rebuilds the page from the same inputs on change (no reload); `buildLicence` is the only part that reads `who`.

## Reads
`RegsPage.methods`, `RegsPage.licence`, `RegsPage.who`.

## Tests
### Page parity — `app/conformance/src/regs/gml.parity.test.ts` (project `logic`; skipped when `provinceStore()` is null; golden v33)
For every `{id, g, page}` of `cases("gear")`/`cases("methods")`/`cases("licence")` (`page(who)` = `buildRegsPage` for `requestOf(id)`; keys compared via `goldenKeyOf(key, id)`):
`eqPlain(page.gear, golden.states[id].gear)` (T30 + T31 fields), `eqPlain(page.methods, golden.states[id].methods)` (T32 fields);
then for each of the six profiles (`adult|resident`, `adult|non_resident`, `adult|non_resident_alien`, `u16|resident`,
`u16|non_resident`, `u16|non_resident_alien`) rebuild with that `who` and compare `page.licence` to
`golden.states[id].licence[profile]` (T33 fields). **Expected counts: 134 gear (107 open + 27 closed), 134 methods
(107 + 27; 14 with boats; 107 references), 804 licence (642 answers + 162 closed-empty); 0 exclusions until T42 lands, then x31p_1010 is excluded from gear + methods (D20: the golden carries the prototype's tidal closedUntil bug).** A failure prints
the state id, the profile and the first differing path.

### Render — `methods-licence.render.test.tsx` (project `render`)
1. `METHODS_BC1_501`: ✓ rows `"Angling (rod and line)"`, `"Spear or bow — not for trout or other game fish"` (note
   `"not for game fish, salmon or protected species · burbot allowed"`), `"Crayfish traps"`; ✕ `"Set lines"`, `"Nets"`, `"Snagging"`,
   `"Chumming"`; Boats item `"No powered boats — between the Goat Creek confluence and the Talchako River confluence"`,
   source button text `"This water’s ›"`.
2. ReferenceFold: summary text `"Reference Handling fish 21 rules that apply everywhere"`, `aria-expanded="false"`, and
   `queryByText("Gently, back in the water")` is null; after `fireEvent.click(summary)`: `aria-expanded="true"`, heads
   `When you catch a fish`, `When you release a fish`, `When you keep a fish`, `Carrying fish`, `Never` and the source `"Province ›"`.
3. `LICENCE_VEDDER_1010_ADULT`: `getByText("Who is fishing?")` + one ⓘ (term `under_16`); `getAllByRole("radiogroup")` length 2,
   each named "Who is fishing?"; radio `"16 or older"` and `"B.C. resident"` have `aria-checked="true"`, the other three
   `"false"`; label `"B.C. resident, 16 or older"`; `"Also, only if…"` precedes `"Steelhead Conservation Surcharge Stamp"`;
   price `"$41.15 a year · $11.43 a day · $22.86 for 8 days"`; the source button `"Province · This water ›"` has no Rule book
   links; pressing it calls `onOpen("licsrc:adult")`.
4. Pressing radio `"Under 16"` calls `onWho({age:"u16", residency:"resident"})`; pressing `"From outside Canada"` calls
   `onWho({age:"adult", residency:"non_resident_alien"})`.
5. `LICENCE_VEDDER_1010_U16_NON`: label `"Canadian, other province, under 16"`; the youth box text starts
   `"Under 16: no licence needed if you fish with a licensed adult"`; needs rows `"Nothing, if you fish with a licensed adult (16 or older)"`
   and `"Basic angling licence"`; no `"Also, only if…"`.
6. `LICENCE_CLOSED`: only the heading "Licence" (no radiogroup).
7. **Golden mounts** (one per tab; the VM is a frozen JSON fixture written by `gml.parity.test.ts` with `UPDATE_REGS_FIXTURES=1` into `core/src/regs/testing/`, since render tests cannot open the store): `MethodsTab` over `vedder_1010` → rendered lines equal `golden.states.vedder_1010.methods.lines`
   with the fold opened (golden reads hidden fold text); `LicenceTab` for each of the six profiles of `vedder_1010` → lines
   equal `golden.states.vedder_1010.licence[p].lines`
   (adult|resident starts `["Licence","Who is fishing? ⓘ","16 or older Under 16","B.C. resident Canadian, other province From outside Canada","B.C. resident, 16 or older","You need", …]`
   and ends `"Province · This water ›"`).

## Acceptance
```sh
pnpm -C app check
pnpm -C app exec vitest run --project logic conformance/src/regs/gml.parity
pnpm -C app exec vitest run --project render packages/ui-native/src/regs/methods-licence
```

## Notes
- `Choice`'s visual (pills) differs from the prototype's segmented `.segm.lsel`; restyle via tokens.md role `.lsel` or add a
  `variant="segmented"` prop to `Choice` — do NOT fork it (one radiogroup implementation, check-platform's aria rule).
- The ⓘ after the question opens `gl:under_16` (T36 sheet).
- D8 (b) (persisted profile) changes only `useAnglerProfile`; nothing here.
- The golden is regenerated by `sync.mjs`; on a prototype bump the parity test's failures name the tickets to revisit
  (README §1). Golden `meta.version` must read `v33` for the strings quoted in T30–T35.
