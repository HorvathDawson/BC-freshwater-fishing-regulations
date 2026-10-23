# T22 — theme + type tokens for the page

Status: DONE — 24e63e82, 8f010a17, 44a09ac2.

Phase 1 · Depends on — (pure token work; may start any time) · Blocks T23–T29, T34, T35, T37, T39, T40 · Decisions D3 (palette
`sub`), D9 (one more font file) · Size ½ day

Target **regs v33 @ 04765ab5**; every value from `app/design/regs-v11/port/golden/tokens.json` via `tokens.md`.

## Goal
The app's theme and type scale carry everything the Regulations page paints with, so every later RN ticket writes its
`StyleSheet` mechanically from `tokens.md` §5: `color.ui.sub` takes the prototype's value (D3 default (a)), `FACE` gains Archivo
800, `TYPE` gains the 25 regs entries of `tokens.md` §2.3, `SPACE`/`GUTTER` exist. A static gate stops regs components from
reading `faint`/`wash` (folded in 20g) or writing a `fontWeight`. Visible change: every secondary-grey text in the app is one
step darker (6.79:1 light, 8.87:1 dark).

## Files
- modify `app/packages/map/style/themes/light.json`: `"color.ui.sub": "#555C62"` (was `#6C737A`)
- modify `app/packages/map/style/themes/cvd.json`: `"color.ui.sub": "#555C62"` (was `#6C737A`; cvd is the light ground)
- modify `app/packages/map/style/themes/dark.json`: `"color.ui.sub": "#B0B8BF"` (was `#98A0A7`)
- regenerate `app/packages/map/style/style.json`, `style.meta.json`, `style.sha256` (`pnpm -C app style:build`)
- modify `app/packages/ui-native/src/type.ts`: `FACE.textHeavy`, the `TYPE` entries of tokens.md §2.3 verbatim
- modify `app/packages/ui-native/src/theme.ts`: `SPACE`, `GUTTER`
- no `index.ts` change: it already has `export * from "./type"` (:32) and `export * from "./theme"` (:38)
- modify `app/apps/mobile/src/App.tsx`: import `Archivo_800ExtraBold` from `@expo-google-fonts/archivo` and add it to `useFonts`
  (:117–121)
- create `app/packages/ui-native/src/regs/tokens.test.ts` (project `render` picks up `packages/ui-native/**/*.test.ts`)
- create `app/packages/ui-native/src/regs/.gitkeep` only if the directory would otherwise be empty (it will not be: the test lives there)

## Signatures
```ts
// type.ts
export const FACE = { /* existing … */, textHeavy: "Archivo_800ExtraBold" } as const;
export const TYPE = { /* existing … */,
  answer, numeral, numeralMid, lead, sectionHead, headerName, tier, tierSmall,
  regsBody, regsBodySemi, regsBodyBold, regsChip, duty,
  meta, metaSemi, metaBold, metaHeavy, kicker, regsTab, pinGroup, pinLine, pinSub, month,
} as const satisfies Record<string, TextStyle>;   // values: tokens.md §2.3, copied exactly

// theme.ts
/** The page's spacing scale (regs v33 CSS 697 --s1..--s4). */
export const SPACE = { s1: 4, s2: 8, s3: 12, s4: 16 } as const;
/** The page's side gutter (.row.ledger / .wstrip / .sec / #pins .pl padding). */
export const GUTTER = 16;
```
No `Palette` change: the regs page reads only `page card tint line line2 ink sub accent open closed restricted scrim` (+ `r`).

## Ports
CSS tokens only: `:root` 17–26 (colours, `--t1..--t4`), 697 (`--w-*`, `--s*`), 817–823 (the 20g roles + the faint/wash fold),
v33 lines 17/20/22 (`--sub`). No script function.

## Reads
`tokens.json` `.tokens.light/.dark`, `.compare`, `.census` (for the test fixture below).

## Tests (`regs/tokens.test.ts`)
1. **Theme follows the prototype's sub (D3 a)**: `LIGHT.sub === tokens.tokens.light["--sub"]` and `DARK.sub ===
   tokens.tokens.dark["--sub"]` (read `app/design/regs-v11/port/golden/tokens.json` with `fs` — it is JSON, under `app/design/`,
   not scanned by the colours gate). If D3 flips to (b), this case becomes `expect.not` + a comment pointing at decisions.md.
2. **Every same-valued token still agrees**: for each row of `tokens.compare` with `same: true`, the `Palette` field named in
   tokens.md §1.1 equals the prototype's light and dark value (`page, card, tint, line, line2, ink, accent, open, restricted,
   closed`) — catches a theme edit that drifts the app from the page.
3. **The type scale is the prototype's**: `TYPE.regsBody.fontSize === 15 && lineHeight === 21.3`; `TYPE.meta.fontSize === 12.48`;
   `TYPE.answer.fontSize === 26`; `TYPE.lead.fontSize === 17`; derive `--t1..--t4` from `tokens.tokens.light["--t1"]` etc.
   (`parseFloat(rem) * 16`) and assert `TYPE.answer/lead/regsBody/meta` sizes equal them, so a prototype bump that changes a
   size fails here first.
4. **No weight props, no folded colours, no literal radii in regs components** (static): read every file under
   `packages/ui-native/src/regs/**/*.tsx` (none yet → the test passes vacuously and starts biting at T23) and assert none matches
   `/fontWeight\s*:/`, `/palette\.(faint|wash|quiet)\b/`, `/borderRadius\s*:\s*\d/`. (T40 widens this into the style-token gate.)
5. **Every TYPE face is loaded**: `FACES_REQUIRED` contains `FACE.textHeavy` (App.tsx imports it; a grep in the test of
   `apps/mobile/src/App.tsx` for `Archivo_800ExtraBold` keeps the two from drifting).
Existing gates that must stay green: `tools/cvd.test.ts` "holds the light and dark themes to the same text contrast" (sub ≥ 4.5
on card: now 6.79 / 8.87 / cvd 6.79), `packages/ui-native/src/theme.test.ts`, `tools/check-colours.test.ts`.

## Acceptance
```sh
pnpm -C app style:build                      # regenerates style.json / style.meta.json / style.sha256
pnpm -C app style && pnpm -C app colours     # build-style duplicate/ΔE/contrast rules + no colour literals
pnpm -C app exec vitest run packages/ui-native/src/regs/tokens tools/cvd packages/ui-native/src/theme
pnpm -C app check
```
Then `pnpm -C app dev:web` once and look at Search + a water + the Layers sheet: secondary text one step darker, nothing else
moved (a manual glance, not a gate).

## Notes
- **Do not** alias `color.ui.faint` → `@color.ui.sub` or `color.ui.wash` → `@color.ui.tint` "to match the prototype": it repaints
  every other screen. The fold lives in the regs components (test 4). An identical hex (not an alias) would also fail
  build-style's "two meanings, one colour" rule.
- `style:build` output is hash-checked by `pnpm style` (`git diff --exit-code packages/map/style/`) — commit the generated files
  with the theme edit, in the same commit (`git commit -- <paths>`).
- `@expo-google-fonts/archivo` ^0.4.2 ships `Archivo_800ExtraBold`; no new dependency, no `deps.md` line. If a web size budget
  (D9) is ever enforced on fonts, the fallback is `textHeavy = textBold` (one line) — the census's 800s become 700s.
- Never set `fontWeight` alongside a custom `fontFamily`; react-native-web synthesises a second bold.
