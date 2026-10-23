# T151 — The water's Map view: the bold emphasis makes the regulations legend hard to follow

Status: DONE — 410dbc63 (map cue, tokens, labels, shots), 08b842d0 (state-handle pin, fixtures); adversarial review 56c68c19 (mark labels in the loaded Archivo_400Regular face, pinned). Built with Q123's default (dim the others).

Source: owner 2026-10-23: "the bold in the section map view is also weird and makes the reg legend hard to follow".

## Work
1. Shot the water Map view (a many-part water: Fraser, Kootenay Lake, Chilliwack/Vedder; light + dark) and find every
   "bold": heavier line widths on the selected/current part, casing/halo, bold legend or label text.
2. Read as the owner meant: emphasis must not change how a status colour reads against its legend swatch. Default:
   the selected part is marked WITHOUT thickening the coloured line (a thin outline/halo or dimming the other parts),
   every part's line drawn at one width so the legend maps 1:1 to the map, legend text at normal weight with only
   the current part's legend row marked.
3. Before/after shots; the colour gate and status-colour tests still pass.

Owner question: the selection cue (default: dim the other parts, same line width). Size: S.

## As built
Every "bold" found in the water Map view (`T151-shots/before--*--map.jpg`):
1. the selected part's stream floored at `width.stream.highlight` 3.2 px against the rest of the water's 2.4 px;
2. the selection CASING under it (`color.highlight`, 2.5 px each side — magenta in light, pink in dark: the Fraser's
   blue "open" part read pink-and-blue) and a 3 px RING on a selected lake (Kootenay Lake's arm);
3. the route-arrows (◀ in `color.highlight`) drawn along the selected part — they follow `selected`;
4. the boundary marks' labels at weight 700 ("Tamihi Rapids Bridge", "the CPR Bridge at Mission").
The water Map view draws no legend of its own; the legend the owner reads against is the Map tab's (one line width per
status, `LegendCount line`), so the fix is that the map's lines match it.

The fix — ONE decision, `SelectionCue` in `@app/map` `adapters/contract.ts`:
- "outline" (default; the browse map's tapped / searched water): unchanged — `selected`, casing, ring, 3.2 px floor.
- "dim" (the water's Map view, `WaterMapView` → `MapScreen selection="dim"` → `Map` → `adapter.highlight(..., cue)`):
  the part is lit `part` (a new feature-state key, in `HIGHLIGHT_KEYS` so it clears per feature like the others), so no
  casing, no ring and no route-arrows; the part and the water's other parts share ONE floor (`width.stream.water`
  2.4 px — `["any", part, water]`); the other parts (`water`) draw at `opacity.water.others` (new token, 0.4 in every
  theme) times their own opacity — `dimTheOthers`, applied to every per-section line and fill and the edge that
  follows it (lake shorelines). The colour of every part is its mode's, untouched.
- Mark labels: weight 400 (web `markElement`, native `Archivo_400Regular`).
- The parts list under the map (T150) is the Map view's per-part key: normal weight, the current part's row marked
  (✓, `aria-checked`) — see T150.
- Tests: map `parity.test.ts` — the "dim" cue's feature-state calls whole (and the default stays `selected`); the
  stream floor whole (`selected` 3.2, `part`/`water` 2.4); the dimming whole on the six per-section layers in all three
  themes and on no other layer, no colour reading `part` / `water`; `sections.test.ts` (clears `part`);
  `WaterMapView.test.tsx` (the map is handed `selection: "dim"`; the list's text is the regular face).
- Mutations run (each fails the named test, then reverted): highlight ignoring the cue → parity "T151: a WATER'S MAP
  VIEW…"; the part back on its own floor → parity "P15 / T151: the stream's width floor…"; `dimTheOthers` not applied →
  parity "T151: the water's OTHER parts are dimmed…"; `WaterMapView` passing "outline" → WaterMapView.test "T151: the
  map is told…".
- Not pinned by a test: the mark labels' weight (`markElement` is not exported and there is no marks render test);
  seen in the after shots.
- Colour gates: `pnpm check` (colours, style, cvd, status-colour tests) — see Checks. No colour token changed.

Shots (own Expo port 39561, the live release through :39217), `T151-shots/`:
`before--` / `after--{fraser,kootenay,chilliwack}--{phone,desktop}--{light,dark}--map.jpg`, and the Q123 alternative
`alt-halo--{fraser,kootenay,chilliwack}--phone--{light,dark}--map.jpg` — a thin halo (1 px casing each side, 1.5 px
ring, the part at the same 2.4 px width, nothing dimmed), built as a temporary local change for the shots only (not
committed). The open parts list: `T150-shots/after--*--map-parts.jpg`. `shots.mjs` re-takes them.

## Checks (T150 + T151, 2026-10-23)
`REGS_STORE_REQUIRED=1 pnpm -C app check` via scripts/heavy.sh (on 08b842d0): every gate before `test` green (tickets,
boundaries, platform, style, colours, spinner, fixture, gauge-policy, calendar, fish, answers, pack, deps, typecheck);
vitest 8033 passed, 4 failed, 24 skipped — the 4 are exactly the known x4p_1010 failures from T100 (Shuswap's map page
in its book links; being fixed on the main checkout): caveats.parity "130/130 states" and "1. x4p_1010", keep.parity
"130/130 states", sheets.parity "x4p_1010". The first run also caught three things this change owed (fixed in
08b842d0): the frozen regs VM fixtures still carried `header.action`, the state-handle pin lacked `part`, and the T40
text-scale snapshot (the name row at 2× on a phone is shorter without the 44 px slot).

## Ships with
App build only (style.meta.json regenerated by `pnpm style:build` for the new token). No pipeline, bundle, release or
tile change.

## Owner question (default built)
- Q123: how the Map view marks the part you are on. Built: dim the water's other parts to 40 %, every part at one width,
  no outline. Alternative shot: a thin halo, nothing dimmed (`alt-halo--*`). Undo / switch: the "dim" cue in
  `contract.ts` (`highlight`, `dimTheOthers`) and `opacity.water.others`.

## Left / follow-ups
- Only the water's OWN other parts dim; unrelated waters around it (tributaries with their own rows) stay at full
  strength and their natural width, so a thin tributary can still sit beside the part at full colour. Dimming every
  other water too needs a map-wide paint flag (or `global-state`), not feature-state.
- The browse map's tapped-water selection (casing + 3.2 px floor) is unchanged; the owner's words were about the
  section map view. If the same "bold" bothers there, the same cue applies.

## Adversarial review (2026-10-23)
Held against "the bold in the section map view is also weird and makes the reg legend hard to follow" (one line width
for every part; the part marked by dimming the others, Q123 default; legend text normal weight). Verdict: as built is
right; one defect fixed.
- **Fixed — the mark labels were not Archivo at all on the web:** `markElement` set `font:400 12px/1.25 Archivo`, but the
  app registers Archivo per weight (`Archivo_400Regular`, expo-google-fonts) and no family named "Archivo" — live,
  `document.fonts` lists only `Archivo_400Regular/600/700/800` — so every label fell back to the browser's sans. One
  value now, `@app/map` marks.ts `MARK_LABEL_FONT` (`Archivo_400Regular`, weight 400), read by both maps; verified live
  (computed `font-family: Archivo_400Regular, sans-serif`, weight 400). This also closes the builder's "Not pinned by a
  test": marks.test pins the value and `app/tools/one-parts-list.test.ts` that both maps read it and neither spells a
  face or weight (mutation: a literal `font:700 12px … Archivo` in Map.web fails it).
- One width: read `paintFor` — `part` and `water` share `width.stream.water` (`["any", PART, WATER]`); only `selected`
  (the browse map) keeps the 3.2 px floor. The builder's four mutations (cue ignored, part on its own floor, dimming
  off, "outline" passed) are each pinned by a named parity / WaterMapView test.
- Shots: `T151-shots/review--{fraser,kootenay,chilliwack}--{phone,desktop}--{light,dark}--map.jpg` — every part at one
  width, the others at 40 %, no casing, no ring, labels regular.
- Owner question stays Q123 (dim vs the thin-halo alternative `alt-halo--*`).
- Checks: `REGS_STORE_REQUIRED=1 pnpm -C app check` via scripts/heavy.sh (on the review change): every gate before `test`
  green (boundaries now clean again); vitest 8039 passed, 4 failed, 24 skipped — the same 4 known x4p_1010 failures from
  T100 (caveats.parity ×2, keep.parity, sheets.parity), nothing else.
- Commit: 56c68c19.
