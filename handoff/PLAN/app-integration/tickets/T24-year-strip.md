# T24 — RN: `YearStrip`

Phase 1 · Depends on T13 (`StripVM`, v33 shape in T13), T22 (tokens), T23 (screen) · Blocks T27 · Decisions — · Size ½ day

Target **regs v33 @ 04765ab5** (CSS 84–113, 265–267, 699; script `flagBar`/`flagLegend` — inventory names, v33 ≈ 945–953).

## Goal
The first block of the Keep tab: the status word big ("✓ Open" / "✕ Closed" in the status colour), the day words ("today",
"on Jun 15", "today (Sat)"), the soon chip ("Opens Jul 1" in green, "Closed Jun 1–30", "Trout release only until Nov 30"), the
365-day bar with its hatches (closed, release, fly fishing only, flies only, bait ban), the today marker with the date above it,
the month initials and the legend in the data's words. Pure drawing of `StripVM`.

## Files
- create `app/packages/ui-native/src/regs/YearStrip.tsx`
- create `app/packages/ui-native/src/regs/StripSwatch.tsx` (the run/legend fill, react-native-svg; shared by the bar and the legend)
- create `app/packages/ui-native/src/regs/YearStrip.test.tsx`
- modify `app/packages/core/src/regs/testing/vm-fixtures.ts`: `stripOf(stateId): StripVM` (golden `keep.strip` → VM, below)
- modify `app/packages/core/src/regs/words.ts`: `STATUS_MARK = { open: "✓ ", closed: "✕ " }` (the prototype's CSS `::before`
  content, CSS 87 — a page word, so it lives in words.ts, not in the component)

## Signatures
```ts
// YearStrip.tsx
export function YearStrip(p: { strip: StripVM; palette: Palette; /** header.date, the marker's label ("Oct 10") */ dateLabel: string }): JSX.Element;
// StripSwatch.tsx
export type SwatchFill = "open" | "closed" | "rel" | "fly" | "flies";
/** prototype flagBar `style(f)` as a fill + overlay: closed if bit 1; else the LAST of rel(2) < fly(4) < flies(8) present
 *  (CSS order 107–110: later class wins), default open; bait (16) is an overlay band. bits null → as 0 (the prototype's
 *  `null & 1` is 0). Pure; exported for the test. */
export function swatchOf(bits: number | null): { fill: SwatchFill; bait: boolean };
export function StripSwatch(p: { bits: number | null; palette: Palette; width?: number | "100%"; height: number; idPrefix: string }): JSX.Element;
```
`StripVM` (T13, v33): `status: "open"|"closed"`; `line: {kind: "status"|"when"|"soon"|"soonOpen"; text}[]`;
`runs: {bits: number|null; days}[]` (Σ days = 365); `todayAt: number` (0..1); `months: string[]`;
`legend: {bit; mark: "✓"|"✕"|null; label}[]`.

## Drawing (tokens.md §5 "YearStrip"; every colour a `Palette` field, no literal)
- block: `paddingTop: 10, paddingHorizontal: GUTTER, paddingBottom: 2`.
- status line: `View` row, `flexWrap: "wrap"`, `alignItems: "baseline"`, gap 8.
  - `kind: "status"` → `Text TYPE.answer` colour `strip.status === "closed" ? closed : open`, text `STATUS_MARK[status] + text`,
    `accessibilityRole="header"`.
  - `kind: "when"` → `Text TYPE.meta` `sub`.
  - `kind: "soon" | "soonOpen"` → `View` marginLeft "auto", `alignSelf: "center"`, `borderWidth: 1.5`, `paddingVertical: 2,
    paddingHorizontal: 8`, border + text colour = `soonOpen ? open : ink` (CSS 89–90: `.wsoon` inherits ink; `.wsoon.open` the
    open green); `Text TYPE.metaBold`.
- bar: `View` row, `height: 10`, `gap: 2`, `marginTop: 22`, `position: "relative"`, `importantForAccessibility=
  "no-hide-descendants"` + `aria-hidden` (the words above and the legend carry the meaning). Each run: `View style={{ flexGrow:
  days, flexShrink: 0, flexBasis: 0, height: 10 }}` `testID={"strip-run " + classOf(bits)}` containing `StripSwatch width="100%"
  height={10}`.
- swatch fills (react-native-svg `Svg`/`Defs`/`Pattern`/`Rect`; `patternUnits="userSpaceOnUse"`; ids `${idPrefix}-${fill}` with
  `idPrefix` = `useId()` stripped of non-alphanumerics, so web `url(#…)` resolves):
  | fill | CSS | SVG |
  |---|---|---|
  | open | `.f-open{background:var(--open)}` | `Rect` 100 % `fill={open}` |
  | closed | `repeating-linear-gradient(135deg, closed 0 3px, mix(closed 30 %, card) 3px 6px)` | `Rect fill={card}`; pattern 6×6 `patternTransform="rotate(45)"`: `Rect x0 w3 h6 fill={closed}` + `Rect x3 w3 h6 fill={closed} opacity={0.3}` |
  | rel | `repeating-linear-gradient(90deg, restricted 0 3px, transparent 3px 5px)` | pattern 5×10: `Rect x0 w3 h10 fill={restricted}` (gaps show the card) |
  | fly | `repeating-linear-gradient(90deg, open 0 3px, card 3px 5px)` | pattern 5×10: `Rect w3 fill={open}` + `Rect x3 w2 fill={card}` |
  | flies | `repeating-linear-gradient(45deg, open 0 2px, card 2px 4px)` | pattern 4×4 `patternTransform="rotate(-45)"`: `Rect w2 h4 fill={open}` + `Rect x2 w2 h4 fill={card}` |
  | + bait | `box-shadow: inset 0 -5px 0 restricted` | an extra `Rect y={height-5} height={5} fill={restricted}` over the fill |
- today marker: absolute `View` `left: \`${todayAt*100}%\``, `top: -6`, `width: 2`, `height: 22`, `marginLeft: -1`, `backgroundColor:
  ink`; its label `Text` (metaSemi size, `lineHeight: 12.48`, `ink`) absolutely above (`bottom: "100%"`, centred with
  `transform: [{translateX: "-50%"}]` → in RN use a fixed-width (80) box centred: `left: -40, width: 80, textAlign: "center"`),
  `marginBottom: 2`, text = `dateLabel`.
- months: row `justifyContent: "space-between"`, `marginTop: 5`; each `Text TYPE.month` `sub`.
- legend: row wrap, `rowGap: 2, columnGap: 12`, `marginTop: 6`; item: row, `alignItems: "center"`, gap 5: `StripSwatch
  bits={bit} width={bit <= 1 ? 14 : 12} height={10}`, then `Text TYPE.meta sub` `{(mark ? mark + " " : "") + label}`.
  Legend `bits`: 0 → open, 1 → closed, 2 → rel, 4 → fly, 8 → flies, 16 → open + bait band (prototype 951: `f-open f-bait`).

## Ports
prototype `flagBar` / `flagLegend` (inventory names; @04765ab5 ≈ 945–953) — the class choice becomes `swatchOf`; CSS 84–113
(`.wstrip`, `.wsline`, `.wsstat`, `.wsoon`, `.sbar`, `.smonths`, `.slegend`, `.f-*`), 265–267 (`.sbar` margin, the today
marker and its label), 699 (months weight).

## Reads
`page.keep.strip` (`StripVM`), `page.header.date`.

## Tests (`YearStrip.test.tsx`; `stripOf(id)` maps golden `keep.strip`: `line[].cls` `"wsstat …"`→status, `"muted small"`→when,
`"wsoon open"`→soonOpen, other `"wsoon…"`→soon; `bar[].cls` → bits (`f-closed` 1; `f-rel` 2; `f-fly` 4; `f-flies` 8; `f-bait`
16; OR'd); `bar[].flex` → days; `today` `left:calc(X% - 1px)` → X/100; `legend[].text` → mark + label; `months` as is)
1. **vedder_1010**: text "✓ Open" (one `Text`, colour `LIGHT.open`), "today"; four runs with testIDs in order
   `strip-run f-open`, `strip-run f-open f-fly f-bait`, `strip-run f-closed`, `strip-run f-open` and `flexGrow` 120, 31, 30, 184;
   marker `left: 77.26%` with text "Oct 10"; month texts `J F M A M J J A S O N D`; legend texts `["✓ open","✕ closed","fly fishing
   only","bait ban"]`.
2. **vedder_615**: "✕ Closed" colour `LIGHT.closed`; "on Jun 15"; chip "Opens Jul 1" whose border and text are `LIGHT.open`;
   marker `left: 45.21%`.
3. **vedder_515**: chip "Closed Jun 1–30" in `LIGHT.ink` (v33 span chip).
4. **iron_720**: chip "Trout release only until Nov 30"; runs `[181,153,31]`.
5. **klwa_1012**: one run `strip-run f-open` flexGrow 365; legend `["✓ open"]`; when "on Oct 12 (Mon)".
6. **Golden sweep** (every state whose `keep.strip` is non-null): the rendered run count = `bar.length`, each run's testID =
   `"strip-run " + collapse(bar[i].cls)`, each `flexGrow` = `bar[i].flex`, the legend texts = `legend[].text`, and the line texts
   (status with its mark stripped) = `line[].text`. Expect 133/134 (x31p_1010 tidal → strip null, not mounted).
7. `swatchOf` unit — closed ignores every other bit, bait included (prototype `f & 1 ? 'f-closed' : …`):
   `swatchOf(17) → {fill:"closed", bait:false}`; `swatchOf(4|16) → {fly, bait:true}`; `swatchOf(2|4) → {fly}` (fly beats rel);
   `swatchOf(4|8) → {flies}`; `swatchOf(null) → {open, bait:false}`.
8. **No literal colours**: every `fill`/`backgroundColor` in the rendered SVG/DOM equals one of `LIGHT.{open,closed,restricted,card,
   ink}` (walk `container.querySelectorAll('[fill]')`); repeat with `DARK`.
9. Dark: mount vedder_1010 with `DARK` → status word `DARK.open`, closed stripes `DARK.closed`.

## Acceptance
```sh
pnpm -C app exec vitest run packages/ui-native/src/regs/YearStrip
pnpm -C app check
```
Manual: `pnpm -C app dev:web`, Vedder Oct 10, compare against `app/design/regs-v11/port/golden` shots / the prototype at
`#w=vedder&d=1010` — same hatch directions (closed "/", flies "\", fly vertical).

## Notes
- The bar is **365 days** (T13: the book's common year; `todayAt = bookIndex/365`). Do not use `dayOfYear` (leap) here.
- react-native-svg `Pattern` + `patternTransform` render on web through the svg web build the vitest alias already uses; if a
  platform drops `patternTransform`, the fallback is unrotated stripes (still distinguishable) — note it, do not hand-draw lines.
- Run widths: `flexBasis: 0` + `flexGrow: days` with a 2 px gap reproduces CSS `flex: n 0 0` + `gap: 2px` exactly (the gaps eat
  width before the share is split, as in CSS).
- The status word's mark comes from `words.STATUS_MARK`, never typed in the component (the no-hand-facts render test, T40).
