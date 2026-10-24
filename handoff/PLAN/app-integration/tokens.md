# tokens.md — the prototype's tokens, type and spacing → the app's theme, `TYPE`, `SPACE`

Target **regs v33 @ 04765ab5** (decisions D5). Every value on this page is copied from
`app/fixtures/golden/tokens.json` (`meta.sha = 04765ab5`, `meta.version = v33`). When the two disagree,
**tokens.json wins** and this page is stale — follow §4. Prototype line refs are `app/design/regs-v11.html` @ 04765ab5
(CSS is lines 1–880; the round-20c/20g/20h/20i token blocks are at 697, 817–823, 849–861, 862–868).

Who applies what: **T22** makes the theme/type/spacing edits in §1–§3; **T23–T29** write each component's `StyleSheet` from
the §5 table; **T39** turns §1's census into a render gate; **T40** the style-token gate.

---

## 1. Colour

### 1.1 The prototype tokens, computed, and where each one lands

The prototype reads ten colour roles (UI-NOTES v31 / CSS 817–821: "`--ink` text and every number · `--sub` secondary text,
labels, the changes line · `--accent` links, buttons, tabs · `--open`/`--closed`/`--restricted` ONLY status · `--tint` the one
grouping tint · `--line`/`--line2` rules"). Two more (`--faint`, `--wash`) are **folded** into `--sub`/`--tint` at CSS 822–823,
so their computed value equals the token they fold into.

| prototype token | light (computed) | dark (computed) | app theme key (`themes/*.json`) | `Palette` field | app light | app dark | same? |
|---|---|---|---|---|---|---|---|
| `--page` | `#EFEFEC` | `#0A0B0D` | `color.ui.page` | `page` | `#EFEFEC` | `#0A0B0D` | yes |
| `--card` | `#FFFFFF` | `#15181B` | `color.ui.card` | `card` | `#FFFFFF` | `#15181B` | yes |
| `--tint` | `#EEEFEB` | `#1E2227` | `color.ui.tint` | `tint` | `#EEEFEB` | `#1E2227` | yes |
| `--line` | `#E5E6E1` | `#252A2F` | `color.ui.line` | `line` | `#E5E6E1` | `#252A2F` | yes |
| `--line2` | `#D3D5CF` | `#343B42` | `color.ui.line2` | `line2` | `#D3D5CF` | `#343B42` | yes |
| `--ink` | `#15181C` | `#F0F2F0` | `color.ui.ink` | `ink` | `#15181C` | `#F0F2F0` | yes |
| `--accent` | `#5F26E0` | `#A97CFF` | `color.ui.accent` | `accent` | `#5F26E0` | `#A97CFF` | yes |
| `--open` | `#1D7741` | `#48b07a` | `color.status.open` | `open` | `#1D7741` | `#48b07a` | yes |
| `--restricted` | `#8f5c00` | `#e8a733` | `color.status.restricted` | `restricted` | `#8f5c00` | `#e8a733` | yes |
| `--closed` | `#940018` | `#FC4164` | `color.status.closed` | `closed` | `#940018` | `#FC4164` | yes |
| **`--sub`** | **`#555C62`** | **`#B0B8BF`** | `color.ui.sub` | `sub` | `#6C737A` | `#98A0A7` | **NO → D3** |
| `--faint` (folded → `--sub`) | `#555C62` | `#B0B8BF` | (`color.ui.faint`) | — never read by regs | `#6E757B` | `#7C858C` | folded, see 1.2 |
| `--wash` (folded → `--tint`) | `#EEEFEB` | `#1E2227` | (`color.ui.wash`) | — never read by regs | `#F7F7F5` | `#101215` | folded, see 1.2 |
| `--on-accent` | `#FFFFFF` | `#100A22` | `color.ui.on-accent` | `onAccent` | `@color.ui.card` (= `#FFFFFF`) | `#100A22` | same value; alias only |
| `--shadow` | `rgba(21,24,28,.9)` | `rgba(0,0,0,.85)` | `color.ui.shadow` × `opacity.ui.shadow` | `lift.boxShadow` | ink × 0.9 | `#000000` × 0.85 | same value; not drawn by regs |
| **`--scrim`** | `rgba(21,24,28,.28)` | **`rgba(0,0,0,.4)`** | `color.ui.shadow` × `opacity.ui.scrim` | `scrim` | ink × 0.28 | `#000000` × **0.28** | dark differs → 1.2 |

Prototype-only tokens the regs page does not draw (map overlay of the prototype's poster, not the app's regs face): `--water`
`#4A90E2`/`#38B6DE`, `--lake` `#64B5F6`/`#205579`, `--land` `#F8F8F2`/`#0A0B0D`, `--st-base` `#0072B2`/`#56B4E9`, `--st-own`
`#B37B00`/`#F0E442`, `--st-water`, `--st-lake`, `--st-closed` (= `--closed`). The app's map draws these from its own tokens;
nothing to map.

### 1.2 The diff rows — decision and default

| row | prototype | app | decision | **default (what T22 does)** |
|---|---|---|---|---|
| `--sub` | `#555C62` / `#B0B8BF` (v33 `r20i-glare`: ≥ 6.5:1 — 6.79:1 on white, 8.87:1 on `#15181B`) | `#6C737A` / `#98A0A7` (4.80:1 / 6.72:1) | **D3** | **D3 (a)**: set `color.ui.sub` to the prototype value in `themes/light.json` **and** `themes/cvd.json` (cvd is the light ground) = `#555C62`, `themes/dark.json` = `#B0B8BF`. App-wide (every `palette.sub` text darkens one step). Always copy the value from `tokens.json` `tokens.light["--sub"]` / `tokens.dark["--sub"]` at sync time — it moved v32 → v33 (`#5F666C`/`#A3ABB2` → now). Checked: neither hex is used by another token in any theme, so the build-style "two meanings, one colour" rule (`tools/build-style.mjs` ~:220) passes; ui tokens are outside the ΔE rule; `tools/cvd.test.ts` "same text contrast" (sub ≥ 4.5 on card) passes with room. |
| `--faint` | folded into `--sub` (CSS 822) | `color.ui.faint` `#6E757B`/`#7C858C` | none needed | **Do not change the theme.** Setting `color.ui.faint` to the sub hex would trip build-style's duplicate rule; aliasing it (`"@color.ui.sub"`) would repaint every other screen. The fold is done in the **components**: every census row whose colour is `--faint|--sub` paints `palette.sub`. `palette.faint` is **banned** in `ui-native/src/regs/**` (T22 adds the static test; T40 keeps it). |
| `--wash` | folded into `--tint` (CSS 822) | `color.ui.wash` `#F7F7F5`/`#101215` | none needed | Same: regs components paint `palette.tint` where the census says `--tint|--wash`; `palette.wash` banned in `regs/**`. (20h "one surface": the only tinted things left on the page are the size chip `.szc`, the also-counts `.kac`, the changes chip `.chgi` — all `tint`.) |
| `--on-accent` | `#FFFFFF` / `#100A22` | `@color.ui.card` / `#100A22` | none | Identical values. Census rows `--card\|--on-accent` (light) / `--card` (dark) are **text on an ink fill** (the pressed `WhoSelector` option, the header tab once pressed in the old segmented style) → paint `palette.card` (the dark census names `--card`, not on-accent). |
| `--shadow` | ink@.9 / black@.85 | identical via `translucent()` | none | Only the prototype's phone-frame `.app` slab uses it; the app has no frame. Nothing to do. |
| `--scrim` | ink@.28 / **black@.40** | ink@.28 / black@**.28** | owner-minor (no D id; sheets are T37) | **Keep the app's** `opacity.ui.scrim` 0.28 in dark: the scrim belongs to the shared `Sheet` (Layers, date, spots use it); a regs-only scrim would be a second scrim. Listed so T37/T39 do not "fix" it silently. If the owner wants .40: one value in `themes/dark.json` `opacity.ui.scrim`, app-wide. |
| `.inh` ground | `color-mix(in srgb, var(--accent) 6%, var(--card))` (CSS 553) | — | none | No new token: `InheritedLine` (T25) draws an absolutely-filled `View` `backgroundColor: palette.accent, opacity: 0.06` under its content (identical arithmetic to the mix: card shows through 94 %). |
| closed hatch | `color-mix(in srgb, var(--closed) 30%, var(--card))` (CSS 97/107) | — | none | `YearStrip` (T24) draws a `palette.card` rect then the `palette.closed` stripe at `opacity 0.3` — the same mix; no literal. |

Colour literals: **none** in any regs component — `tools/check-colours.mjs` scans `packages/**`; opacity is a number, not a
colour, and is allowed. `transparent` is allowed (the gate's NAMED set excludes it).

### 1.3 Colour roles in the components (the rule every StyleSheet follows)

| census colour | `Palette` field | used for |
|---|---|---|
| `--ink` | `ink` | text, every number, the 1.5 px rules (header bottom, group top, How it adds up, posrow), tab underline |
| `--faint\|--sub`, `--sub` | `sub` | secondary text, labels, kickers, month initials, legend, captions under rows, the changes line |
| `--accent` | `accent` | links ("How to tell ›", "Rule book p.N ↗", "Source ›", "See the rules from then ›"), ⓘ, the date pill words, Map/Parts, the fish caption, the inherited left rule |
| `--open` / `--closed` / `--restricted` | `open` / `closed` / `restricted` | ONLY status: strip status word + bar + legend swatches + soon chip, ✓/✕ marks, a not-kept row's words, release/closed values in Other fish, the banner rule + big words, the header dot |
| `--tint\|--wash`, `--tint` | `tint` | the size chip, the also-counts chip, the changes chip |
| `--card\|--on-accent`, `--card` | `card` | every surface (header, scroll, pin bar, group head), text on an ink fill |
| `--line` / `--line2` | `line` / `line2` | 1 px hairlines between rows (`line`), size-section frame + section rules (`line2`) |

---

## 2. Type

### 2.1 The prototype scale

`html{font-size:16px}`; CSS 25: `--t1 1.625rem` = **26 px** (the answer, numerals) · `--t2 1.0625rem` = **17 px** (section heads,
the lead) · `--t3 .9375rem` = **15 px** (body) · `--t4 .78rem` = **12.48 px** (meta, labels; the comment says "12.5").
Weights (CSS 697): `--w-body 400`, `--w-emph 600`, `--w-head 700`, `--w-num 800` (one 900: `ReferenceFold · title`, T35 — maps
to 800). Body line-height **1.42** (CSS 29: `font: var(--t3)/1.42 Archivo`). Families: display **Bricolage Grotesque** (700/800),
text **Archivo** (400/500/600/700 loaded by the prototype's Google Fonts link; **800 is used** by the census — the browser
synthesised it), figure **JetBrains Mono** (500; the months ask 600, CSS 699 — synthesised).

### 2.2 Faces — `FACE` changes (T22)

| prototype | app `FACE` | note |
|---|---|---|
| Bricolage 800 | `displayHeavy` `BricolageGrotesque_800ExtraBold` | exists |
| Bricolage 700 | `display` | exists (regs does not use it) |
| Archivo 400 / 600 / 700 | `text` / `textSemi` / `textBold` | exist |
| Archivo 500 | `textMedium` | exists; 20c turned every 500 into 600 — regs never uses it |
| **Archivo 800** (date-day word, size chip, pin numbers, ✓/✕ marks, how-it-adds-up "a" was 700) | **new `FACE.textHeavy = "Archivo_800ExtraBold"`** | add to `FACE` (so `FACES_REQUIRED` grows) and to `useFonts` in `apps/mobile/src/App.tsx` (`Archivo_800ExtraBold` from `@expo-google-fonts/archivo`, already a dependency — no `deps.md` change). Cost: one font file (~60 KB woff2 on web) — inside D9's budget note. |
| Archivo 900 | `textHeavy` | the only 900 (Methods' Reference title) |
| JetBrains Mono 600 (months) | `figure` (500) | not loaded at 600; the prototype itself synthesises it. Keep 500 — no new face for 12 one-letter labels. |

RN rule (all tickets): **never set `fontWeight`** on a regs `Text` — the weight IS the `fontFamily` (a custom face plus
`fontWeight: "700"` makes react-native-web synthesise a double bold). `TYPE` entries carry no weight.

### 2.3 New `TYPE` entries (T22 adds them to `ui-native/src/type.ts`, exactly)

Line heights are the census's computed px (font-size × CSS line-height), unrounded except where the CSS said `normal` (then the
Archivo/Bricolage normal metric rounded: noted).

```ts
// --- the Regulations page (regs v33 @ 04765ab5; tokens.md §2). Sizes are the prototype's rem × 16. ---
/** THE answer: the strip's "Open"/"Closed" (.wsstat, --t1 800 /1, tracking -0.5). */
answer:      { fontFamily: FACE.displayHeavy, fontSize: 26, lineHeight: 26, letterSpacing: -0.5 },
/** A big numeral INSIDE a sentence (.tn/.kn/.ghn at --t1): NO lineHeight — the outer Text's lineHeight is the line box (§2.4). */
numeral:     { fontFamily: FACE.displayHeavy, fontSize: 26 },
/** A head numeral inside the group's total line (.ghn/.gln/.ghn2 at --t2, line-height 1). */
numeralMid:  { fontFamily: FACE.displayHeavy, fontSize: 17 },
/** The lead: a group's name (.rtitle --t2/1.2), a section head (.sech h2, tracking -0.2 added at the call site via `sectionHead`). */
lead:        { fontFamily: FACE.displayHeavy, fontSize: 17, lineHeight: 20.4 },
sectionHead: { fontFamily: FACE.displayHeavy, fontSize: 17, lineHeight: 20.4, letterSpacing: -0.2 },
/** The water's name in the regs header (.ttl b --t2/1.15, two lines max). */
headerName:  { fontFamily: FACE.displayHeavy, fontSize: 17, lineHeight: 19.55 },
/** A tier header (.tier .tl, 20f pick c: display face --t2/1.25) and the size/count head (--t3/1.25). */
tier:        { fontFamily: FACE.displayHeavy, fontSize: 17, lineHeight: 21.25 },
tierSmall:   { fontFamily: FACE.displayHeavy, fontSize: 15, lineHeight: 18.75 },
/** Body, --t3/1.42. regsBody* are 15 px; the app's TYPE.body (14.5/21) is NOT the prototype's body. */
regsBody:     { fontFamily: FACE.text,      fontSize: 15, lineHeight: 21.3 },
regsBodySemi: { fontFamily: FACE.textSemi,  fontSize: 15, lineHeight: 20.25 },   // .kt .ks (1.35)
regsBodyBold: { fontFamily: FACE.textBold,  fontSize: 15, lineHeight: 21.3 },    // values (.orow b, .posrow b), .golink
regsChip:     { fontFamily: FACE.textHeavy, fontSize: 15, lineHeight: 19.5 },    // .szc (1.3); [data-size-rule] uses textBold
duty:         { fontFamily: FACE.textBold,  fontSize: 15, lineHeight: 20.25 },   // v33 .k28 .duty (--t3/1.35, 700)
/** Meta, --t4. `meta` = /1.42 (notes, legend, captions); the /1.3 forms for labels and short lines. */
meta:        { fontFamily: FACE.text,      fontSize: 12.48, lineHeight: 17.72 },
metaSemi:    { fontFamily: FACE.textSemi,  fontSize: 12.48, lineHeight: 16.22 },
metaBold:    { fontFamily: FACE.textBold,  fontSize: 12.48, lineHeight: 16.22 },
metaHeavy:   { fontFamily: FACE.textHeavy, fontSize: 12.48, lineHeight: 14.98 },  // the date pill's "Today"/"Fri" (1.2)
/** ALL-CAPS kickers only (.lab, .scope, .bkh, .tkick, .nofish h3): --t4 700, tracking 1 (1.2 for .lab). */
kicker:      { fontFamily: FACE.textBold, fontSize: 12.48, lineHeight: 16.22, letterSpacing: 1, textTransform: "uppercase" },
/** The four header tabs (.segm[role=tablist] button: 700 --t4, tracking .2, sentence case; line-height normal → 15). */
regsTab:     { fontFamily: FACE.textBold, fontSize: 12.48, lineHeight: 15, letterSpacing: 0.2 },
/** The pin bar: the group line (--t3 700 /1.3), a tier line (--t4 700 /1.3), the form-a sub-line (--t4 600 /1.3). */
pinGroup:    { fontFamily: FACE.textBold, fontSize: 15, lineHeight: 19.5 },
pinLine:     { fontFamily: FACE.textBold, fontSize: 12.48, lineHeight: 16.22 },
pinSub:      { fontFamily: FACE.textSemi, fontSize: 12.48, lineHeight: 16.22 },
/** The strip's month initials (.smonths: JetBrains Mono .625rem/1). */
month:       { fontFamily: FACE.figure, fontSize: 10, lineHeight: 10 },
```
`TextStyle` already allows every key used. Existing `TYPE.small` (12.5/18) and `TYPE.section` (11/14, tracking 1.4) are **not**
the prototype's meta/kicker (12.48/17.72, tracking 1) — regs components use `meta`/`kicker`; the rest of the app keeps
`small`/`section` unchanged. (Pixel deltas if someone "simplifies": 0.02 px size, 0.28 px line — invisible, but the
`style-token` gate (T40) compares against this table, so pick one and keep it: **this table**.)

Bold-in-run: where the prototype bolds part of a line (`<b>` in a pin line, the value in a changes line, a `num` run with
`size:"inline"`), the run is a nested `Text` with the bold face of the same size (`textHeavy` for pin numbers, `textBold`
elsewhere) — never `fontWeight`.

### 2.4 One line box for a big numeral (CSS 701, 20c #4)

`.tier .tn, .ghd .num .ghn, .kt .ks .kn { line-height:.7; vertical-align:-1px }` — a 26 px figure never makes its line taller.
In RN: the sentence is ONE outer `Text` with the sentence's style (e.g. `TYPE.tier`, lineHeight 21.25); the numeral is a
**nested** `Text style={TYPE.numeral}` (no lineHeight of its own). On iOS/Android a nested span inherits the paragraph's
lineHeight, so the line box stays 21.25; on web react-native-web renders a `<span>` whose line-height inherits the parent's
px value — same result. **Do not** wrap the numeral in a `View` (that breaks the line and the baseline). Render tests assert
the outer `Text` style carries the lineHeight and the inner has none (T26).

### 2.5 Header-scale sizes the census reports (for T23)

| part | style | colour |
|---|---|---|
| back "‹" | `textBold` 22 (lineHeight 26) — 44×44 hit box | `accent` |
| name | `TYPE.headerName`, `numberOfLines={2}` | `ink` |
| date day ("Today"/"Fri") + " ▾" (10 px) | `TYPE.metaHeavy` | `accent` |
| date ("Oct 10") | `metaSemi` lineHeight 14.98 | `sub` |
| Map / Parts label | `metaBold` lineHeight 13.73 (1.1) | `accent` |
| place label | `metaSemi` (16.22), box height 32.44 (2 lines) | `sub` |
| tab | `TYPE.regsTab` | `sub`; selected `ink` + 3 px `ink` bottom border |

---

## 3. Spacing, radii, rules

- **`SPACE`** (T22 adds to `ui-native/src/theme.ts`, exported from the package index):
  `export const SPACE = { s1: 4, s2: 8, s3: 12, s4: 16 } as const;` (CSS 697 `--s1..--s4`). Plus `export const GUTTER = 16;` — the
  page's side padding (`.row.ledger`, `.wstrip`, `.sec`, `#pins .pl`, `.banner`/`.inh`/`.nofish`/`.posrow` margins).
- Other prototype gaps that are not on the scale (use the literal, they are measured): header `r1` gap 8, tabs gap 16, place
  label left inset 56, `wsline` gap 8, legend gaps 2 × 12, `kpics` gap 0 × 8, `sbar` run gap 2, `kmeta` gaps 4 × 8, pin
  tier-line left inset 26.
- **Radii: 0 everywhere** — `palette.r.box/chip/pill` are all 0 (`RADIUS`); never a literal `borderRadius`.
- **Rules**: 1.5 px `ink` (structure: header bottom, group head top, How it adds up top, posrow top, pin bar bottom is a 1 px
  `line` shadow since 20h → in RN `borderBottomWidth: 1, borderBottomColor: palette.line`), 1 px `line` (row separators),
  1 px `line2` (size-section top/bottom frame, `nofish` top). Hairlines use `StyleSheet.hairlineWidth`? **No** — the prototype
  is 1 px CSS; use `1`.

---

## 4. When `tokens.json` changes (a prototype bump)

1. `node app/design/regs-v11/port/sync.mjs --ref <commit>`; its report lists "changed token values" and the census rows whose
   style moved.
2. For each **colour** token that changed:
   - one of the "same? yes" rows in §1.1 → it changed in the prototype only: decide with the owner whether the app follows
     (it is app-wide). If yes: edit `themes/light.json` / `dark.json` / `cvd.json` (cvd follows light unless the token is a
     status hue — cvd has its own), run `pnpm -C app style:build`, then `pnpm -C app style && pnpm -C app colours &&
     pnpm -C app exec vitest run tools/cvd.test.ts packages/ui-native/src/theme.test.ts`.
   - `--sub` → D3 already decided: copy the new hex into `color.ui.sub` (light + cvd, dark) and run the same commands; update
     the §1.1 row and the contrast numbers in §1.2.
   - a new token → add a §1.1 row; if a component paints it, it needs a `color.ui.*` token in `tokens.json` + all three themes
     (the build fails on a theme missing a token) and a `Palette` field in `theme.ts` (`paletteFor`).
3. For each **type/spacing** token (`--t1..--t4`, `--w-*`, `--s1..--s4`) that changed: update §2.3/§3 and the matching `TYPE`/
   `SPACE` entry (lineHeight = new size × the role's CSS line-height factor).
4. For each **census** row that changed (`tokens.json census.light[i]` / `.dark[i]`, matched by `sel` + `role`): update its §5
   row; the component named in the role's left part owns the StyleSheet change; re-run that ticket's render test.
   Diff mechanically: `node -e` over the old and new `census` arrays keyed by `sel|role|i` (the census can carry several rows per
   selector — one per state variant, e.g. `.ghd .num .ghn` at 12.48 and 17 px).
5. Commit `app/fixtures/golden/` + theme JSON + `style.json`/`style.meta.json`/`style.sha256` + `type.ts`/`theme.ts` + this page together
   ("regs vNN tokens").

v32 → v33 (the bump this spec absorbed): `--sub` `#5F666C/#A3ABB2` → `#555C62/#B0B8BF`; census: `.k28 .duty` 12.48/600 →
**15/700** (lineHeight 20.25), a second `.kt .szc` row at **700** (`[data-size-rule]` chip), a second `.linkbtn` row at
12.48/700/16.85 (the duty line's "Steelhead stamp ›"); pin lines now wrap (`white-space:normal`, CSS 758/760) — the stack still
≤ 90 px.

---

## 5. Per-component style table (from `tokens.json census`, v33)

Columns: role (census `role`) · selector · `TYPE` key · colour (`Palette` field; dark differs only where noted) · background ·
border · padding (CSS order top right bottom left → RN `paddingTop/…`). "—" = none/transparent. Rows the census found on no
state (`n: 0`) are taken from the CSS line given.

### RegsHeader (T23)
| role | selector | TYPE | colour | bg | border | padding / box |
|---|---|---|---|---|---|---|
| header | `.hdr` (CSS 54) | — | — | `card` | bottom 1.5 `ink` | fixed sibling above the scroll; total height ≈ 119 (golden `pins.viewport.header`) |
| row 1 | `.r1` (CSS 526) | — | — | — | — | height 48, `paddingLeft 4, paddingRight 8`, gap 8, align center |
| back | `.back` | textBold 22/26 | `accent` | — | — | 44 × 44 |
| name | `#hdr .ttl b` | `headerName` | `ink` | — | — | flex 1, 2 lines |
| date day | `#hdr .hdate .hd2 b` | `metaHeavy` | `accent` | — | — | pill `paddingHorizontal 6`, minHeight 44, gap 6; dot 8×8 |
| date | `#hdr .hdate .hd2 > span` | `metaSemi` (lh 14.98) | `sub` | — | — | column under the day |
| Map/Parts | `#hdr .hgo span` | `metaBold` (lh 13.73) | `accent` | — | — | column, icon 20×20 stroke 1.7, minWidth/minHeight 44, `paddingHorizontal 4`, gap 1 |
| place label | `#hdr [data-place]` | `metaSemi` | `sub` | — | — | `paddingLeft 56, paddingRight 12, paddingBottom 6`, marginTop −2; box height 32.44 |
| tab | `#hdr .segm button` | `regsTab` | `sub` | — | bottom 3 transparent | `paddingTop 2`, minHeight 34 |
| tab (selected) | `…[aria-pressed=true]` | `regsTab` | `ink` | — | bottom 3 `ink` | same |
| tab row | `.hdr .r2` + `.segm[role=tablist]` (CSS 450, 770) | — | — | — | — | `paddingHorizontal 16`, tabs gap 16 |

### YearStrip (T24)
| role | selector | TYPE | colour | bg | border | padding |
|---|---|---|---|---|---|---|
| block | `.wstrip` (CSS 84) | — | — | — | — | 10 16 2 |
| status word | `.wstrip .wsstat` | `answer` | `open` / `closed` | — | — | prefix "✓ " / "✕ " (words) |
| status note | `.wstrip .wsline .muted` | `meta` | `sub` | — | — | row baseline-aligned, gap 8, wrap |
| soon chip | `.wsoon` (CSS 89) | `metaBold` | `ink`; `.open` → `open`; `.closed` → `closed` | — | 1.5 currentColor | 2 8, marginLeft auto |
| bar | `.sbar` (CSS 92, 265) | — | — | — | — | height 10, run gap 2, marginTop 22 |
| today marker | `.sbar .today` (CSS 266–267) | label `metaSemi` lh 12.48 | `ink` | `ink` | — | 2 × 22 at top −6, x = todayAt − 1; label centred above, marginBottom 2 |
| month | `.wstrip .smonths > *` | `month` | `sub` | — | — | row space-between, marginTop 5 |
| legend | `.wstrip .slegend > span` | `meta` | `sub` | — | — | wrap, gaps 2 × 12, marginTop 6; swatch 12×10 (open/closed 14×10), gap 5 |

### ClosedBanner, InheritedLine, WhereNot, PossessionRow (T25)
| role | selector | TYPE | colour | bg | border | padding |
|---|---|---|---|---|---|---|
| banner | `.banner` (CSS 118) | — | — | — | top 1.5 `closed` | margin 8 16 0, padding 10 0 4 |
| banner big | `.banner .big` (CSS 119; n 0) | `lead` (lh normal → 20.4) | `closed` | — | — | — |
| para | `.banner > p` | `regsBody` | `ink` | — | — | margin 4 0 |
| chain item | `.closechain .cd` | `regsBody` | `ink` | — | li: top 1 `line` | li minHeight 44, row, gap 8 |
| link | `.golink` | `regsBodyBold` | `accent` | — | — | minHeight 44 |
| inherited box | `.inh` (CSS 553) | — | — | `accent` @ 0.06 over card | left 3 `accent` | margin 8 16 0, padding 6 0 6 10 |
| inherited label | `.inh .ltag` (CSS 554) | `kicker` | `accent` | — | — | — |
| says | `.inh [data-inherited]` | `regsBody` | `ink` | — | — | marginTop 2 |
| caution | `.inh [data-caution]` | `meta` | `sub` | — | — | marginTop 2 |
| where-not | `.nofish` (CSS 330) | — | `sub` | — | top 1 `line2` | margin 6 16 0, padding 10 0 6 |
| head | `.nofish h3` | `kicker` (lh normal → 16.22) | `sub` | — | — | marginBottom 2 |
| item | `.nofish li` | `meta` (lh 17.47 = 1.4) | `sub` | — | — | 3 0 |
| possession row | `.posrow` (CSS 764) | — | — | — | top 1.5 `ink` | margin 8 16 0; button padding 9 0, minHeight 44 |
| label | `.posrow .on` | `regsBody` | `ink` | — | — | flex 1 |
| value | `.posrow b` | `regsBodyBold` | `ink` | — | — | — |

### GroupHead … HowItAddsUp (T26)
| role | selector | TYPE | colour | bg | border | padding |
|---|---|---|---|---|---|---|
| article | `.row.ledger` (CSS 745) | — | — | — | — | 0 16 12, marginTop 12 |
| head band | `.ghd.gpin` | `regsBody` | `ink` | `card` (20h: no band) | top 1.5 `ink` | 8 16 (full-bleed: marginHorizontal −16) |
| name | `.ghd.gpin .rtitle` | `lead` | `ink` | — | — | — |
| scope | `.ghd.gpin .rtitle .scope` | `kicker` | `sub` | — | — | inline after the name, marginLeft 8 |
| total line words | `.ghd .num .ghu` | `metaBold` (lh 14.98) | `ink`; form a "of the" words `sub` | — | — | — |
| total line numeral | `.ghd .num .ghn` | `numeralMid` nested (§2.4) | `ink` | — | — | outer line `metaBold` lh 19.97 (CSS 748: 1.6) |
| share lead (form a) | `[data-share-lead]` | `metaBold` lh 19.97 | `ink` | — | — | block |
| share lead numeral | `[data-share-lead] .gln` | `numeralMid` nested | `ink` | — | — | — |
| of the total (form a) | `[data-of-total]` | `metaSemi` | `sub` (its numeral `metaBold` `ink`) | — | — | marginTop 1 |
| why here | `[data-why-here]` (n 0; `.ghr` CSS 468) | `metaSemi` lh 16.85 | `sub` | — | — | marginTop 2 |
| origin | `.gorigin` (`.ghd.gsub`) | `metaSemi` | `ink` | — | — | sub-block padding 8 0 4; row wrap gap 0 × 6; its links `metaBold` `accent` minHeight 32 |
| origin tag | `.ghd .gtag` (n 0; CSS 464) | textHeavy 11.04/13.25 caps .4 | `ink` | — | 1.5 `ink` | 2 5 |
| tier kicker | `.tier .tkick` | `kicker` | `sub` | — | — | paddingTop 8 |
| tier line (root/share/own) | `.tier .tl` | `tier` | `ink`; own-tier numeral `ink` (20g) | — | — | paddingTop 16 (2 after a kicker), paddingBottom 8; no left bar (20f) |
| tier line (size/count) | `.tnode[size/count] > .tier .tl` | `tierSmall` | `ink` | — | — | size: 12 0 8; count: 8 0 |
| tier numeral | `.tier .tl .tn` | `numeral` nested | `ink` | — | — | margin 0 1 |
| tier why | `.tier .tsub` | `meta` lh 16.85 | `sub` | — | — | marginTop 4 |
| size band | `.tnode[data-tnode=size]` | — | — | — (20h) | top + bottom 1 `line2` | full-bleed (marginHorizontal −16, paddingHorizontal 16), paddingBottom 8, marginTop 12; its rows' top rule `line2` |
| kind row | `.krow.kt` | — | — | — | top 1 `line` (inside a size band `line2`; first row none) | 8 0, gap 12 |
| kind sentence | `.kt .ks` | `regsBodySemi` | `ink`; zero kind (`kzero`) `restricted` | — | — | — |
| kind numeral | `.kt .ks .kn` | `numeral` nested | `ink`; `.kn.none` `restricted` | — | — | — |
| size chip | `.kt .szc` | `regsChip` (`[data-size-rule]`: textBold 15/19.5) | `ink` | `tint` | — | 3 8 |
| qualifier | `.kt .kq` | `metaSemi` | `sub` | — | — | in `kmeta` row (wrap, gaps 4 × 8, marginTop 4) |
| pictures | `.kpics` / `.kp` | — | — | — (20h: no tile) | — | row wrap gap 0 × 8; tile minWidth 80 maxWidth 132, padding 4 0, minHeight 44; image width 80, aspect 2:1, `resizeMode="contain"` |
| caption | `.kcap` | `metaSemi` lh 14.98 | `accent` | — | — | "<caption> ›" (nbsp before ›), ≤ 2 lines |
| also counts | `.kac` | `metaBold` lh 16.85 | `ink` | `tint` | — | 3 8, marginTop 4 after pictures else 8 |
| duty (v33) | `.k28 .duty` | `duty` | `ink` | — | — | marginTop 8 (4 right after pictures) |
| presence | `.kpres` | `meta` | `sub` | — | — | marginTop 3 (4 after pictures) |
| How to tell › (inline) | `.linkbtn` in `.ks` | inherits the sentence (`regsBodySemi`) | `accent` | — | — | hitSlop 12 vertical, 4 horizontal (CSS 718) |
| not-kept words | `.nkr .nm` | `regsBodyBold` (lh normal → 19.5) | `closed` / `restricted` (release) | — | — | — |
| not-kept whose | `.nkr .s2` | `meta` | `sub` | — | — | marginTop 2 |
| not-kept mark "✕" | `.nkr .kn` | `answer` (census: Bricolage 26/26 800 — the later `.kn` rule beats CSS 666's `--t2`) | `closed` | — | — | — |
| How it adds up | `.worked` | `regsBody` | `ink` | — | top **1.5 `ink`** | marginTop 16, paddingTop 8 |
| head | `.worked .bkh` | `kicker` | `sub` | — | — | 6 0 4 |
| says | `.worked .wsays` | `meta` | `sub` | — | — | margin 2 0 4 |
| q | `.worked li span` | `at:"here"` (`li.wh`, prototype 1200): `meta`; `at:"elsewhere"` (`li.wq`, CSS 688): textSemi 12.48/17.72 | here `sub`; elsewhere `ink` | — | li top 1 `line` (first none) | li 6 0 |
| a | `.worked li b` | here: textBold 12.48/17.72; elsewhere: `meta` | here `ink`; elsewhere `sub` | — | — | — |

### OtherFish, section head (T27)
| role | selector | TYPE | colour | bg | border | padding |
|---|---|---|---|---|---|---|
| section | `.sec` (CSS 78) | — | — | — | `.sec + .sec` top 1.5 `ink` | 18 16 4 |
| section head | `.sech h2` | `sectionHead` | `ink` | — | — | marginBottom 8 |
| row | `.orow` / `.obtn` | — | — | — | top 1 `line` | 9 0, minHeight 44, gap 10, align center |
| thumb | `.th` (CSS 301) | — | — | — (20h) | — | 64 wide, image 64 × 32 contain; no picture → silhouette `line2` 40 × 20 |
| name | `.olist .on` | `regsBody` | `ink` | — | — | flex 1 |
| week | `.olist .owk` | `metaSemi` | `sub` | — | — | marginTop 2 |
| value | `.olist .orow b` | `regsBodyBold` | `ink`; release `restricted` (v33 bold); closed `closed` | — | — | — |
| size hint (v33) | `.orow .osz` | `metaSemi` | `sub` | — | — | marginLeft 6, same baseline as the value |

### PinBar (T29)
| role | selector | TYPE | colour | bg | border | padding |
|---|---|---|---|---|---|---|
| bar | `#pins .pinbar` | — | — | `card` (20h) | bottom 1 `line` (20h `box-shadow 0 1px 0 var(--line)`) | absolute top = header bottom, left/right 0; ≤ 90 px |
| group line | `#pins .pl.g` | `pinGroup` | `ink` | — | — | 3 16; wraps (v33) |
| tier line | `#pins .pl.c` | `pinLine` | `ink` | — | — | 3 16 3 26 |
| number | `#pins .pl b` | textHeavy at the line's size (15 / 12.48) nested | `ink` (20g) | — | — | first `\b\d+\b` only |
| total sub-line | `#pins .pl .pls` | `pinSub` | `ink` | — | — | own line under the group line; its first number bold |

### Shared atoms (T26/T34 — listed so `Line` rendering is one component)
| role | selector | TYPE | colour | note |
|---|---|---|---|---|
| ⓘ term | `.gl` | the surrounding style (15 400/600/700) | `accent` | hit box 44 (hitSlop), no line growth |
| "What these words mean" | `.glk` | `metaSemi` + underline | `accent` | — |
| Rule book link | `a.book` | `metaBold` | `accent` | "Rule book p.N ↗"; hitSlop 12 |
| Source › | `.isrc .srcs` | `metaBold` | `accent` | T34 |

Everything in §5 Gear/Methods/Licence/Sheet (census roles `GearLines`, `ChangesLine`, `YesNoList`, `ReferenceFold`,
`ClosedNote`, `WhoSelector`, `LicenceAnswer`, `Sheet`) is read by T34/T35/T37 directly from `tokens.json` with the same
column rules; their TYPE keys are already in §2.3 (`lead` for `.gpart > h3`, `metaBold` labels, `regsBodyBold` 17 for
`.gval b` → add `value: textBold 17/22.95` in T34).
