# tokens.md: colour, type and spacing on the Regulations page

The values live in code: colours in `app/packages/map/style/tokens.json` and `themes/{light,dark,cvd}.json` (read as the
`Palette` in `app/packages/ui-native/src/theme.ts`), type in `app/packages/ui-native/src/type.ts` (`FACE`, `TYPE`),
spacing in `theme.ts` (`SPACE`, `GUTTER`, `RADIUS`). This page holds the rules every regs component's `StyleSheet`
follows. The scale is the regs v33 prototype's, measured once; the prototype and its `tokens.json` extract are retired.

## 1. Colour

### 1.1 Colour roles

| `Palette` field | used for |
|---|---|
| `ink` | text, every number, the 1.5 px structure rules (header bottom, group top, "How it adds up", possession row), the tab underline |
| `sub` | secondary text, labels, kickers, month initials, legend, captions, the changes line |
| `accent` | links ("How to tell ›", "Rule book p.N ↗", "Source ›"), the ⓘ, the date pill words, Map / Parts, the fish caption, the inherited left rule |
| `open` / `closed` / `restricted` | ONLY status: the strip's word, bar, legend and soon chip, ✓ / ✕ marks, a not-kept row's words, Other-fish release / closed values, the banner, the header dot |
| `tint` | the one grouping tint: the size chip, the also-counts chip, the changes chip |
| `card` | every surface (header, scroll, pin bar, group head) and text on an ink fill |
| `line` / `line2` | 1 px hairlines between rows / the size-section frame and section rules |
| `rule` | the size section's numbers (UR7, owner B) |

- **No colour literal in any component** (`app/tools/check-colours.mjs`); an opacity is a number and is allowed, and so
  is `transparent`. A new colour is a token in `tokens.json` and all three themes, then `pnpm -C app style:build`.
- **`palette.faint`, `palette.wash` and `palette.quiet` are never read in `ui-native/src/regs/`**: the prototype folded
  them into `sub` and `tint` (`app/tools/regs-tokens.test.ts`).
- `sub` is the prototype's `#555C62` light (cvd too) / `#B0B8BF` dark, at least 6.5:1 on `card` (D3; `tools/cvd.test.ts`).
- The scrim is the shared `Sheet`'s (`opacity.ui.scrim`, 0.28 in dark too): no regs-only scrim.
- The inherited line's ground is `accent` at opacity 0.06 under its content; the strip's closed hatch is `closed` at
  opacity 0.3 over `card`. No new token for either.
- Per tab at most 7 / 5 / 5 / 4 text colours (Keep, Gear, Methods, Licence), all from `TEXT_TOKENS`, status colours on
  status words only: `regs/testing/census.ts` (`palette.render.test.tsx`).

## 2. Type

### 2.1 The scale

Answer and numerals 26 px, section heads and the lead 17 px, body 15 px, meta 12.48 px (the prototype's `--t1..--t4` at
16 px root); body line height 1.42. Faces: display Bricolage Grotesque 800, text Archivo 400 / 600 / 700 / 800, figure
JetBrains Mono 500. Pinned by `app/tools/regs-tokens.test.ts`.

### 2.2 Faces

`FACE.displayHeavy`, `text`, `textSemi`, `textBold` and `textHeavy` (`Archivo_800ExtraBold`: the date-day word, the
size chip, pin numbers, the ✓ / ✕ marks, and the one 900 the prototype used). A new face goes into `FACE` (so
`FACES_REQUIRED` grows) and the app's `useFonts`. **Never set `fontWeight` on a regs `Text`**: the weight is the
`fontFamily`, and a custom face plus a weight makes react-native-web synthesise a double bold (`regs-tokens.test.ts`).

### 2.3 `TYPE` entries

The regs entries are `type.ts`, copied exactly from the prototype's computed sizes (`answer`, `numeral`, `numeralMid`,
`lead`, `sectionHead`, `headerName`, `tier`, `tierSmall`, `regsBody*`, `regsChip`, `duty`, `meta*`, `kicker`, `regsTab`,
`pin*`, `month`). Regs components use these, never the app's `TYPE.body`, `small` or `section`, which differ. Bold
inside a run is a nested `Text` in the bold face of the same size, never `fontWeight`.

### 2.4 One line box for a big numeral

A 26 px figure never makes its line taller: the sentence is ONE outer `Text` with the sentence's style and line height,
and the numeral is a NESTED `Text style={TYPE.numeral}` with no line height of its own. Never wrap the numeral in a
`View` (it breaks the line and the baseline). Render tests assert the outer style carries the line height and the inner
none; at 200 % text, `app/tools/regs-text-scale.test.ts` lays it out in Chrome.

## 3. Spacing, radii, rules

- `SPACE = { s1: 4, s2: 8, s3: 12, s4: 16 }` and `GUTTER = 16`, the page's side padding. Measured gaps off the scale
  are literals in the component.
- **Radii are 0 everywhere** (`RADIUS`); never a literal `borderRadius` (`regs-tokens.test.ts`).
- Rules: 1.5 px `ink` for structure, 1 px `line` between rows, 1 px `line2` for the size section and the where-not
  block; `1`, never `StyleSheet.hairlineWidth`.

## 4. When a token changes

A colour change is app-wide: decide it with the owner, edit the three theme files (cvd follows light unless the token is
a status hue), run `pnpm -C app style:build`, then `pnpm -C app style && pnpm -C app colours` and
`pnpm -C app exec vitest run tools/cvd.test.ts packages/ui-native/src/theme.test.ts`. A new token needs a `Palette`
field (`paletteFor`) and every theme (the build fails on a theme missing one). A type or spacing change edits `type.ts`
/ `theme.ts` and `regs-tokens.test.ts` together. Commit the theme JSON, the regenerated `style.json` / `style.meta.json`
/ `style.sha256` and the code in one change.

## 5. Per-component styles

Each component's `StyleSheet` is the source of its values; the census and the render tests hold them to §1 and §2.

- **RegsHeader** (`RegsHeader.tsx`): the header is a fixed sibling above the scroll, 1.5 `ink` bottom rule; the name
  `headerName`, two lines; the date pill `metaHeavy` / `metaSemi`; tabs `regsTab`, the selected one `ink` with a 3 px
  `ink` underline; every press target at least 44 × 44.
- **YearStrip** (`YearStrip.tsx`): the status word `answer` in its status colour; notes `meta` `sub`; the bar 10 px with
  2 px run gaps; months `month`; the legend `meta`.
- **ClosedBanner, InheritedLine, WhereNot, PossessionRow** (`KeepBlocks.tsx`): the banner's top rule `closed`, its big
  words `lead`; the inherited box's left rule `accent`; where-not `meta` `sub` under a `line2` rule; the possession row
  under a 1.5 `ink` rule.
- **GroupHead … HowItAddsUp** (`GroupCard.tsx`): the head under a 1.5 `ink` rule, no band; the name `lead`; tier lines
  `tier` / `tierSmall` with nested numerals; the size band framed in `line2`, full bleed; chips in `tint`; pictures on
  no ground; "How it adds up" under a 1.5 `ink` rule.
- **OtherFish, section head**: rows 44 px high under 1 px `line`; values `regsBodyBold` in the tone's colour.
- **PinBar**: `card` with a 1 px `line` bottom, at most 90 px; numbers in `textHeavy` at the line's size.
- **Shared atoms** (`LineText.tsx`, `ChangesLine.tsx`): the ⓘ takes the surrounding style in `accent` with a 44 px hit
  box; book links and "Source ›" `metaBold` `accent`.
- **Gear, Methods, Licence, Sheet** (`sheet/Para.tsx`, `sheet/Fold.tsx`, `SheetBody.tsx`): the same rules; says text
  Archivo 400 15, examples 12.48 `sub`, a fold's summary Archivo 700.
