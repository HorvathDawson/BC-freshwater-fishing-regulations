# T39 — dark theme + the palette render gate (light / dark / CVD)

Status: DONE — e4a5bef9, 2ebe4055.

Phase 4 · Depends on T22 (tokens, D3 sub), T27 (Keep tab), T35 (Gear/Methods/Licence), T37 (sheets) · Blocks T41 ·
Decisions D3 (sub), D4 (dark fish pictures) · Size 1 day

## Goal
The regs page draws correctly on `LIGHT`, `DARK` and `CVD` (`paletteFor(theme)`), and a gate fails the build when a
component breaks the prototype's 20g/20h/20i colour rules: per tab at most **6 / 5 / 5 / 4** distinct text colours
(Keep / Gear / Methods / Licence), every text colour a palette token, status colours (`open closed restricted`) only on
status words, numbers in `ink`, one grouping tint (`tint`) only on chips, the card the one surface inside a group and the
pin bar, fish pictures with no ground, `sub` on `card` ≥ 6.5:1 and "release" bold (glare). Nothing visible changes in
light mode if T22–T35 were right; dark mode gets a measured census.

## Files
- create `app/packages/ui-native/src/regs/palette.render.test.tsx` (C, render project)
- create `app/packages/ui-native/src/regs/testing/census.ts` — the jsdom census walker (test-only; imported by T40 too)
- create `app/tools/regs-census.mjs` (D) — the prototype's `app/design/regs-v11/tools/census.mjs` CENSUS expression run
  against Expo web (`--url http://localhost:8081`, `--theme light|dark|cvd`, states from `port/reader/states.json` T41),
  writes `out.json` and prints the per-tab colour counts next to `port/golden/tokens.json` `.census` for the same roles
- modify `app/tools/cvd.test.ts`: add `it("reads secondary text on the card at 6.5:1 or better, light and dark")`
- modify `app/packages/ui-native/src/theme.test.ts`: add `it("keeps sub and faint at the prototype's value (D3)")` — only if T22 did not
- (no theme value edits here: D3's `sub` change is T22's; this ticket proves it)

## Signatures
```ts
// testing/census.ts
export type Role = "status" | "number" | "chip" | "button" | "text";
export interface CensusHit { el: HTMLElement; text: string; color: string; token: keyof Palette | null; bg: string | null; bgToken: keyof Palette | null; weight: number; size: number; role: Role }
/** Reverse map: an rgb() string → the Palette key(s) holding that colour (several keys may share one value: v33 sub == faint). */
export function tokenOf(palette: Palette, rgb: string): (keyof Palette)[];
/** Every element with its own non-empty text node under `root`, with its inline style (react-native-web renders inline styles in jsdom — FishSpinner.test.tsx reads `.style.backgroundColor` the same way). */
export function census(root: HTMLElement, palette: Palette): CensusHit[];
/** role from the testID convention every regs component follows (T23–T35): testID prefixes "st:" status word, "n:" number, "chip:", "btn:" */
export function roleOf(el: HTMLElement): Role;
export const MAX_COLOURS: Readonly<Record<Tab, number>> = { keep: 6, gear: 5, methods: 5, licence: 4 };
```
CONTRACT CHANGE: applied to contracts.md (§2.7 testID prefixes).

## Ports
- `check.mjs` @ 04765ab5 `r20g-palette` (L941), `r20h-one-surface` (L863), `r20h-picture-surface` (L864), `r20i-glare`
  (20i block, L784–846) — the rules, restated per role above.
- `app/design/regs-v11/tools/census.mjs` `CENSUS` — D tool body (copy the expression; it reads computed styles on any DOM,
  so it runs unchanged on Expo web's DOM except the selectors, which become `[data-testid^="st:"]` etc.).
- regs-v11.html CSS dark overrides (`:root[data-theme=dark]`, `@media (prefers-color-scheme: dark)` blocks in lines 1–880)
  — tokens only; the app's `DARK` palette is already `themes/dark.json` (tokens.md §1).

## Reads
`Palette` keys `page card tint line line2 ink sub faint accent onAccent open closed restricted`; `port/golden/tokens.json`
`.tokens.dark`, `.census.dark` (expected per-role colour token), `.compare`.

## Tests
**C — `palette.render.test.tsx`** — for each theme in `[LIGHT, DARK, CVD]` × states `vedder_1010, klwa_1012, bc1_501
(bellacoola 501 p1), glenn_615, vedder_515` (the r20g-palette set; VMs from the T12 harness when the store is present, else
the `vm-fixtures.ts` Vedder page only) × tab:
1. `≤ N text colours per tab` — `new Set(census(tabRoot).map(h => h.color)).size <= MAX_COLOURS[tab]`.
2. `every text colour is a token` — every hit's `token` ∈ `{ink, sub, accent, open, closed, restricted, onAccent, card}`.
3. `status colours only on status` — a hit whose token ∈ `{open, closed, restricted}` has role `status`
   (`.wstrip`, Other-fish verdicts, yes/no marks, `.krow .nm/.kn` in the prototype ↔ `st:` testIDs).
4. `card / onAccent text only on buttons` — role `button`.
5. `numbers in ink` — every role `number` hit has token `ink`.
6. `one tint, only on chips` — every hit with `bgToken === "tint"` has role `chip`; no other non-card background token.
7. `one surface` (r20h) — inside each group container (`testID="group:<i>"`) and the pin bar (`testID="pins"`): no
   element with a `backgroundColor` other than `card` / transparent, except chips.
8. `pictures transparent` (r20h) — every `Image` under `testID^="pic:"` and its parent have no `backgroundColor` (D4 a).
9. `release is bold` (r20i-glare) — every `st:release` hit has `weight >= 700`.
**A — `tools/cvd.test.ts`** new case: `contrast(LIGHT.sub, LIGHT.card) >= 6.5 && contrast(DARK.sub, DARK.card) >= 6.5`
(v33 values: light `#555C62` on `#FFFFFF` 6.79:1, dark `#B0B8BF` on `#15181B` 8.87:1 — D3 a); and the CVD theme's status
trio still passes the existing "keeps the three status colours apart" case with the regs strip's legend colours.
**D — `node app/tools/regs-census.mjs --theme dark out.json`** (manual, recorded in the PR): per tab colour counts equal the
prototype's census (`r20h_report.md`: 6 / 5 / 5 / 4 light and dark) and the owner looks once at the dark Keep tab with the
fish pictures (D4).

## Acceptance
```sh
pnpm -C app vitest run packages/ui-native/src/regs/palette tools/cvd packages/ui-native/src/theme
pnpm -C app check
node app/tools/regs-census.mjs --theme dark /tmp/census-dark.json   # needs pnpm -C app/apps/mobile web running
```

## Notes
- CVD: the regs page has no hue-only meaning — every status colour sits on a word (`Open`, `Closed`, `✓`, `✕`); test 3
  plus the existing `cvd.test.ts` cases cover it. Do not add a CVD-specific style in regs components.
- `sub` and `faint` share one value at v33 (`#555C62`/`#B0B8BF` in the prototype) — `tokenOf` returns both keys; rules
  accept either. If D3 flips to (b), test 2 still passes (sub is a token either way); only the cvd 6.5:1 case changes
  (it would fail on the app's `#6C737A` 4.80:1 — then it becomes `it.todo` with a pointer to decisions.md D3).
- The scrim stays the app's .28 in dark (tokens.md: `opacity.ui.scrim`) — not a regs decision.

## As built (2026-10-22)
- `MAX_COLOURS` is `{ keep: 7, gear: 5, methods: 5, licence: 4 }`: the design target is v38, whose check.mjs r20g BOUND gives
  Keep 7 (UR7, owner B: the size section's `rule` head and numbers). `rule` is a text token; a number under `tier:size` /
  `pin:size` is held to `rule`, every other number to `ink`.
- States: the frozen pages (render tests cannot open a store) — vedder_1010, vedder_515, bc1_501, klwa_1010 and glenn_501 (for
  klwa_1012 / glenn_615, which have no frozen page), plus the closed vedder_615; × LIGHT / DARK / CVD × the four tabs.
- The components carry the §2.7 testIDs now (contracts.md "AS BUILT"); the group containers keep `keep-group` /
  `keep-plain` and the pin bar `pin-bar` (renaming them would break T21/T29 tests).
- Keyed exceptions (census.ts `ALLOW`, each with its reason): the caveat block's `restricted` left rule (T42), the closed
  banner's `closed` top rule, the licence paper note's `tint` ground (`note:paper`, prototype `.gtopic .paper`). A key
  excuses its own element (a border: its one side) only, never what the element holds (review fix: a `closed` rule on a
  banner chain row, or a tinted run inside the paper note, is a fault).
- An Other-fish row with no picture (thumb null: Crayfish, Protected species) draws the prototype's `.th.sil` silhouette
  (a `line2` fish shape, no ground) as `pic:none` — it was a filled `line2` tile no rule saw (review fix).
- Rule 3 also covers rules (borders): a status colour on a border only on a status element or a keyed exception.
- `theme.test.ts` unchanged: T22 already holds `sub` at the prototype's value (`tools/regs-tokens.test.ts`); `faint` is
  NOT the prototype's in the app (it stays the app-wide token, banned as regs text by D3 — regs-tokens' lint + rule 2).
- D tool run (own ports, Expo web over the live release r-f734cbb93724): Vedder River + Bella Coola River, today (Oct 22),
  light / dark / CVD — Keep 7 / Gear 5 / Methods 5 / Licence 4 (Vedder), 6 / 4 / 5 / 4 (Bella Coola), no faults.
  Dark Keep shot with the fish pictures for the owner (D4): `../T39-shots/vedder-keep-dark.png`; census `../T39-shots/census-dark.json`.
