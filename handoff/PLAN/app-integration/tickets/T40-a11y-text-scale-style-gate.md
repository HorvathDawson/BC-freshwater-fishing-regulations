# T40 — accessibility, 200 % text, hit areas, roles, and the style-token gate

Status: DONE — d9cb96fa, cdc1edb8.

Phase 4 · Depends on T23–T29 (every regs component), T35, T37, T39 (`testing/census.ts`) · Blocks T41 · Decisions D1
(header tabs are a tablist), D7 (date pill) · Size 1 day

## Goal
Every regs screen works with a screen reader and at the OS's largest text size: text scales (`allowFontScaling` on,
`maxFontSizeMultiplier` 2 on numbers and the header only), layout re-measures (pin anchors, header label fit) after a
font-size change, every pressable has a ≥ 44×44 hit area (via `hitSlop` where the visible box is smaller), roles are
right (tablist/tab, radiogroup/radio, button, heading, image), and containers carry no labels that would swallow their
children. A static style gate ports `r20c-style-tokens`: on the Keep tab ≤ 4 font sizes and ≤ 4 weights, one link colour,
no underlined links, and ALL-CAPS only on kickers/labels.

## Files
- create `app/packages/ui-native/src/regs/a11y.test.tsx` (C), `app/packages/ui-native/src/regs/style-tokens.test.tsx` (C)
- create `app/packages/ui-native/src/regs/hit.ts` — `hitSlopFor(w, h)` (pure, exported for the test)
- modify the regs components that fail the tests (expected: `LineText` term ⓘ button, `RuleBookLink`, `FishPic` caption
  button, `ChangesLine` chip, `PinBar` lines, `Fold` header, `RegsHeader` date pill + action)
- modify `app/packages/ui-native/src/regs/RegsScreen.tsx` / `useAnchors` (T29): re-measure on `Dimensions` `change` and on
  `AccessibilityInfo`/`PixelRatio.getFontScale()` change (web: `resize`) — anchors are layout, never cached across a scale change

## Signatures
```ts
// hit.ts
export const MIN_HIT = 44;
/** the hitSlop that grows a w×h box to at least 44×44, split evenly; zero when already big enough */
export function hitSlopFor(w: number, h: number): { top: number; bottom: number; left: number; right: number };
// usage: <Pressable hitSlop={hitSlopFor(…measured or the style's known box…)} …>  — for inline Text links (ⓘ, book, How to tell ›)
//        the box is the line height × the glyph run; use hitSlopFor(24, TYPE.<role>.lineHeight)

// a11y conventions every regs component follows (written down here, enforced by a11y.test.tsx):
//   header tabs     <View accessibilityRole="tablist">  each <Pressable accessibilityRole="tab" aria-selected={on}>   (no accessibilityState — check-platform bans it)
//   who selectors   existing <Choice> (radiogroup / radio / aria-checked)
//   group title, tab section heads, sheet title    accessibilityRole="header" (react-native-web → role="heading")
//   fish picture    <Pressable accessibilityRole="button" accessibilityLabel={`${caption} — what it looks like`}> with the Image accessibilityIgnoresInvertColors
//   ⓘ              accessibilityRole="button" accessibilityLabel={`What does ${term} mean?`} (one id) / "What these words mean" (2+) — contracts §1 `term`
//   book link       accessibilityRole="link" accessibilityLabel={`Rule book page ${printed}, opens the PDF`}
//   fold            accessibilityRole="button" aria-expanded={open}
//   strip           the bar is accessibilityRole="image" with the status line as its label; the legend is text
//   containers      NO accessibilityLabel on a View that has text children (a label hides the children from VoiceOver/TalkBack)
```

## Ports
- `check.mjs` @ 04765ab5 `r20c-style-tokens` (L1085; `round20c` 1008): font sizes/weights/link colour/caps/hit ≥ 32 (the
  app raises the floor to 44, the platform guideline); `header-label-two-lines` action ≥ 44×44 (L1912).
- regs-v11.html `fitLabel` (≈1001) re-run on resize — T23's `pickPlace(measuredHeight)` already takes the measured height;
  this ticket wires `onTextLayout` to re-run it after a font-scale change.
- `port/golden/tokens.json` `.census` — the per-role size/weight table the gate compares to.

## Reads
`TYPE.*` (T22), `Palette`, `tokens.json .census.light[role].{size, weight, transform}`.

## Tests
**C — `a11y.test.tsx`** (vedder_1010 page via vm-fixtures / harness; `screen` queries):
1. `tabs` — `getByRole("tablist")` holds 4 `tab`s named `"What you can keep"`, `"Gear"`, `"Methods"`, `"Licence"`; exactly one
   `aria-selected="true"` (`r20e-four-tabs`).
2. `headings` — `getAllByRole("heading")` includes `"Vedder River"` and the group title `"Trout and char"`.
3. `who selectors` — Licence: two `radiogroup`s; selecting "Under 16" sets `aria-checked="true"` on it only.
4. `every pressable is labelled and big enough` — for every `[role=button], [role=link], [role=tab], [role=radio]`: an
   accessible name (text or aria-label) is non-empty, and its style box + `hitSlop` (read from the element's
   `data-hitslop` test attribute that `Pressable` wrappers in regs set — see Notes) is ≥ 44 in both axes.
5. `no container labels` — no element with `aria-label` has a descendant text node that is not inside a pressable
   (the "container swallows children" rule).
6. `font scaling` — every `Text` in the Keep tab has `allowFontScaling !== false`; numbers (`testID^="n:"`) and the header
   name carry `maxFontSizeMultiplier === 2`.
7. `re-measure on scale` — render `RegsScreen`, fire the font-scale change handler (mock `PixelRatio.getFontScale` 1 → 2 and
   emit `Dimensions` `change`): `useAnchors` `measure` is called again and the PinBar reads the new anchors (spy).
**C — `style-tokens.test.tsx`** (port of `r20c-style-tokens`; states vedder_1010, x35p_1010, klwa_1012, glenn_615 Keep tab;
`census()` from T39, excluding the strip months/legend and the year bar):
1. `≤ 4 font sizes` and 2. `≤ 4 font weights` on Keep text (`new Set(hits.map(h => h.size)).size <= 4`, same for weight);
3. `one link colour` — all `role=link`/`button` text colours one value (`accent`), none `textDecorationLine: underline`;
4. `caps only on kickers` — a hit with `textTransform: uppercase` has testID prefix `kicker:` (scope kicker, "Other fish"
   head, Reference head, not-kept label, where-not head) — the prototype's `.lab, .scope, .bkh, .nkt .lab, .nofish h3`.
**A — `hit.test.ts`**: `hitSlopFor(24, 21)` → `{top: 12, bottom: 11, left: 10, right: 10}`; `hitSlopFor(50, 50)` → zeros.

## Acceptance
```sh
pnpm -C app vitest run packages/ui-native/src/regs/a11y packages/ui-native/src/regs/style-tokens packages/ui-native/src/regs/hit
pnpm -C app check
# D (T41 tool): node app/tools/compare-regs.mjs --state vedder_1010 --font-scale 2   → shoots Expo web with html{font-size:200%}
#   and the RN text multiplier; the owner checks nothing is clipped (header 2-line label, pin bar, tier numbers)
```

## Notes
- jsdom has no layout, so test 4 cannot measure boxes: every regs `Pressable` goes through a tiny `RegsPress` wrapper (T26
  or this ticket) that takes `box: {w, h}` (its known style size) and sets both `hitSlop={hitSlopFor(box.w, box.h)}` and
  `dataSet={{hitslop: …}}` (react-native-web renders `data-hitslop`). On device the real size is the style's.
- `accessibilityState` is banned in shared code (`check-platform`) — use `aria-selected`, `aria-checked`, `aria-expanded`,
  `aria-disabled` (react-native 0.81 + react-native-web 0.21 both map them).
- Native screen-reader pass (VoiceOver + TalkBack, one water, every tab, one sheet) is a MANUAL step recorded in the PR —
  jsdom proves names and roles, not reading order on device.
