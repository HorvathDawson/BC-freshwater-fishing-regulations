# T165 — No public access: streams dotted grey (like under-lake streams, a bit stronger); the warning lives in the banner

Status: DONE — 429173ad (dotted grey layer, grey tokens, warning token, legend, gates, shots). Built as the owner
decided (dotted grey); Q150 below records the details built by default.

Source: owner 2026-10-23 (after T146): "the no access color is still not aggressive enough. it is too benign. should be
more warning maybe it could take the yellow or orange color then has its own regulation gets a different color?"
Supersedes Q137/Q138 (T146's spruce / dash questions).

## OWNER DECISION 2026-10-23 (supersedes the amber swap below)
"maybe the stream color through no access areas should be dotted grey? like under lakes but a little more visible?" —
chosen over amber lines and dotted amber: streams through no-access land draw as a DOTTED GREY line (the under-lake
stream style, a little stronger); no-access lake fills grey; the no-access LAND fill stays; the regulations page keeps
the T147 "No public access" block in the amber WARNING colour; "own regulations" KEEPS its amber on the map. The dash is a
second channel by the owner's choice (it was Q138; the `stream-nobaseline` layer is the precedent: its own layer because
line-dasharray cannot vary per feature).

## Work (as decided)
1. Map: a `stream-noaccess` dotted layer (grey, a little stronger than `under_lake`), lake fills inside no-access grey;
   status.no_access becomes that grey for lines/fills in every theme; the colour gates updated (no_access grey must stay
   apart from base, water.ungauged and the under-lake grey — say how it reads apart, the dot pattern being the main cue).
2. The page: the T147 block's bar/tint/heading in the warning amber (status.own's hue or a dedicated warning token,
   whichever the gates prefer); the legend says "No public access" with the dotted grey swatch.
3. status.own stays amber; no new colour needed.
4. Shots: Coquitlam / Seymour / Capilano watersheds, light + dark, phone + desktop; the under-lake style beside it.

## Earlier plan (superseded)
1. status.no_access := the amber status.own uses today (Okabe & Ito orange in light, yellow in dark; the cvd theme's
   warning hue) — it must read as a warning at a glance.
2. status.own gets a NEW colour, apart from closed (crimson), the new no_access (amber), base (blue), the selection
   outline (color.highlight, magenta) and the forest/park ground, in light, dark and cvd, measured with the repo's gates
   (status-colours-distinct, water-status-cvd, apartFromSelection; ΔE floors as T146 set them). Search candidates
   (e.g. violet, teal, deep green) and show 3 options as shots (Fraser + Coquitlam, light and dark, phone) — build the
   best by the gates; the owner can swap (Q150).
3. Everything that names these colours follows: tokens.json comments, the legend words (the legend says "no public
   access" in amber and "own regulations" in the new colour), the T147 no-access block (bar and tint), the T54 partial /
   permit notes, depth-contour and selection gates (T158), the colouring engine (one pathway), every theme.
4. Mutation pins: the old spruce/cyan no_access fails "no access reads as a warning"; own in amber fails "own is not the
   warning colour".
Ships: app (style rebuild). Size: S–M.

## As built (lane-T165, 2026-10-23)
The amber swap was started (a search for a new `own` colour: teal / violet-cyan / olive options were shot) and STOPPED
when the owner chose dotted grey; none of it shipped, and `own` keeps its amber. What shipped:

- **Map — `stream-noaccess`** (runtime-style.ts, after `stream-nobaseline`): its own line layer on the stream
  source-layer, because `line-dasharray` cannot vary per feature. Painted by the adapter with the stream's own mode and
  day (`adapters/contract.ts` `noaccessPaint` / `noaccessOpacity` / `noAccessShown`): opacity 1 where the profile mode's
  day puts the section in `no_access` (@app/core `dayProfiles` labels) or the categorical mode's `no_access` member, 0
  in every other mode and before a day is installed. `paintFor` hides the `stream` line there (`["case", <no_access>,
  0, …]` inside the zoom curve), so the gaps are gaps. One colouring pathway: no second access test anywhere.
  Look: `color.status.no_access`, 1.2 → 2.8 px (z6 → z16) at full strength, the route's own dash (`dash.route`); the
  under-lake route is 0.8 → 2.4 px at `opacity.route` 0.35 — the same idea, stronger.
- **Tokens:** `color.status.no_access` is a GREY — light / cvd `#5E565E` (a dark warm grey), dark `#9C9A98` (a mid
  grey). No-access lake fills follow (the lake layer's `no_access` category). `color.ui.warning` (new) =
  `@color.status.own` in every theme — a reference, never a second hex. tokens.json comments rewritten.
- **The page (T147 block):** bar and tint (`palette.warning`, `noAccessBand` = the warning laid at 14 % over the card)
  in the amber; the swatch beside the title is the legend's dotted grey (`DottedSwatch`). The heading text stays INK:
  the light amber is 3.0:1 on the card (#D08404), under the 4.5:1 words need and only just the 3:1 of large text — see Q150.
- **Legend:** core `WATER_STATUS_LINE` (no_access: "dotted"); the map legend strip and the theme miniature (More →
  theme) draw no public access as a dotted grey line; `LegendCount dotted`.
- **The grey, searched** (`T165-shots/grey.mjs`): every near-neutral (RGB spread ≤ 16), held by the style build's ΔE76
  12 against the other map colours, 3:1 on the card, ≥ 15 from every basemap ground, ≥ 20 (normal) from open,
  restricted, ink; best worst-reader minimum against closed / own / base. Measured, as built (worst of normal + protan /
  deutan / tritan, Viénot + Machado; tokens / thin lines / lake fills):

  | theme | grey | vs closed | vs own | vs base | normal vs base (tok/line/lake) | vs unmapped | vs route as drawn |
  |---|---|---|---|---|---|---|---|
  | light | #5E565E | 25.1 / 15.0 / 17.4 | 31.5 / 19.1 / 28.8 | 28.2 / 14.3 / 14.3 | 31.7 / 17.6 / 17.6 | 22.5 | 22.0 |
  | dark  | #9C9A98 | 12.8 / 8.5 / 12.3 | 22.9 / 19.2 / 23.4 | 24.7 / 19.5 / 24.6 | 33.6 / 24.3 / 31.0 | 8.3 | 26.3 |
  | cvd   | #5E565E | 25.1 / 15.0 / 23.9 | 31.5 / 19.1 / 31.0 | 27.2 / 15.9 / 25.2 | 34.8 / 16.0 / 29.5 | 22.5 | 27.0 |

  vs `water.ungauged` ≥ 20 normal in every theme (light 41, dark 35, cvd 42).

## Gates (mutation → the test that fails)
- `tools/status-colours-distinct.test.ts`: access floors re-measured (rounded down to 0.5), every one under 20 with its
  reason — `WHY.greyAccess` (a neutral parts from the statuses by lightness only; the dots are the main cue; dark's rose
  closed sits at the grey's lightness for a protanope, 8.5 as lines), `WHY.greyPermit`, `WHY.greyBoundary`. The T146
  hue-off-base gate is replaced by **"no public access is a dotted grey; own keeps its amber (T165)"**: a neutral (RGB
  spread ≤ 16), apart from base (token / line / lake, normal), the ungauged grey (20), the solid missing grey and the
  under-lake route as drawn, light / dark / cvd. Pins: T54's indigo, T146's spruce and ice cyan → "is a hue, not a grey";
  a grey painted `water.ungauged` / `water.unmapped` → 0.0 against it; **own moved off the amber (teal #207050) →
  "own … is not the amber"** and "the warning … is not own's amber". `apartFromSelection` skips the hue line for a grey
  (chroma < 10); the magenta pin stands. The T158 contour pin now paints the ice cyan it was written against.
- `tools/water-status-cvd.test.ts` `ACCESS_FLOOR` light 16.5 → 25, dark 28 → 12.5 (recorded: dark grey vs rose closed,
  protan).
- `packages/map/src/noaccess.test.ts` (new): a no-access section draws on the dotted layer (whole paint, every theme)
  and the stream line is hidden there; dots off in plain / standing / no day; categorical mode; the layer sits right after
  `stream-nobaseline` and is built with no dots; **pin: a line left solid under the dots fails** (removing the hide in
  `paintFor` fails 2 tests — run); own keeps `color.status.own` on the map's day match.
- `LegendStrip.test.tsx`: the regulations key's no-access swatch is dotted in the grey, the rest solid; pin: a line swatch
  not asked to be dotted is solid. `AccessNotice.test.tsx`: block bar/tint in `LIGHT.warning` (= own), swatch dots in
  the grey. `theme.test.ts`: the band is the warning laid at NO_ACCESS_BAND; ink 7:1 on it in every theme.
  `layer-order.test.ts`: `stream-noaccess` is a water line over the boundaries. `profile-style.test.ts`: 11 layers
  validated with the real paint (the dots added).

## Shots (`../T165-shots/`, this worktree's Expo web on :39611 against the owner's :39217, GET only)
`built--{coquitlam,seymour,capilano}--{phone,desktop}--{light,dark}.jpg` — the three watersheds;
`built--underlake-coquitlam-lake--{phone,desktop}--{light,dark}.jpg` — Coquitlam Lake at z13.2: the no-access dashes
beside the faint under-lake thread. Scripts: `shots.mjs`, `grey.mjs`.
NOT shot: the T147 block on the live regs store — the main checkout's `regs.sqlite` is mid re-cut ("rows section version
32, this app reads 33 — refused"), so every Regulations page shows that refusal; the block is held by
`AccessNotice.test.tsx` (render) instead.

## Checks
`pnpm style:build` (style.meta.json, style.sha256 regenerated; the build's own ΔE76 12 clean in every theme).
`REGS_STORE_REQUIRED=1 pnpm -C app check` via `scripts/heavy.sh appcheck 30` (`.venv` and `data/generated/*` symlinked
read-only from the main checkout, removed after): every gate before `test` green (tickets, boundaries, platform, style
intact sha db9b8d89640d, colours, spinner, fixture, gauge-policy, calendar, fish, answers, pack, deps, typecheck);
vitest 4387 passed, 399 failed in 44 files — ALL the regs-store pairing on the main checkout ("regs store: rows section
version 32, this app reads 33 — refused", the fixture mid re-cut): 290 `regs.fixture`, the conformance parity / release
suites, and the UI suites that open a water's page or map (back, WaterMode, WaterMapView, Spots.shell, MapDay.shell,
WaterScreen.*, ui regs / waterMap / regs.render, the T44 water-frame suites): their rendered DOM shows that refusal, and
WaterMapView's one store-free test ("no release") passes. No colour, map-style or legend suite fails; the targeted run of
tools/ + packages/map + ui-native theme / regs / LegendStrip / menu + core status is green (1445 tests, the same
store-paired files aside).

## Adversarial review (2026-10-23)
- **Does no access read apart from base and closed on the real map?** Light: yes — dark grey dashes on the crimson hatch,
  beside solid blue / crimson / amber (`built--seymour--desktop--light.jpg`, `built--coquitlam--phone--light.jpg`).
  Dark: yes for normal vision (light-grey dashes vs rose / yellow / deep blue). For a protanope in dark the grey and the
  rose closed are one lightness (8.5 as lines): the dash is then the only cue — recorded, not fixed (a lighter grey
  meets the boundary #B8B8B8 and the missing grey, a darker one the ground).
- **Does it read as a warning?** It no longer tries to: the owner moved the warning to the page. On the map it reads as
  "not water for you" (dashed, grey), next to the crimson no-access hatch, which still says closed land.
- **Confusables found:** (1) the dark solid missing grey `water.unmapped` #93A0AB is 8.3 from the dark grey in normal
  vision — told apart by solid vs dashed; it is only tidal / outside-B.C. water. (2) In light the no-access LAKE fill
  (#5E565E at `opacity.lake` 0.6 over water) is a heavy slate on the map (Coquitlam, Capilano, Seymour lakes) — reads
  as "something different" but darker than a stream's dashes suggest; Q150 (b). (3) The dashes use `dash.route` [4,3]
  — in light/dark they read DASHED rather than dotted (the under-lake route has the same look, which is what the owner
  pointed at); cvd's `dash.route` is [1,2.5], truly dotted. Q150 (c). (4) MU boundaries are dashed greys too, but wider,
  lighter and drawn under the water; the no-access line is darker and on top (gate `WHY.greyBoundary`).
- **Second copies of hex values?** None: the warning is a reference to `status.own`; `noAccessBand` is computed; the grey
  lives only in the theme files (the old #00604C / #50E4FF / #705864 appear only as encoded mutation pins in tests).
- **Selection:** a selected no-access stream keeps the magenta casing around the hidden solid line's width, so the
  outline still shows around the dashes (casing paints under the stream; the dots are above).
- Not run on a native device (the layer is in the runtime style both renderers take).

## Owner question (default built)
- **Q150** (re-used for the dotted-grey details): (a) the block's HEADING stays ink — the amber is the bar and the tint,
  because the light amber is 3.0:1 on the card (4.5:1 needed for the body, 3:1 barely met for the title); amber heading text would need a darker amber (a new token) —
  default: ink. (b) No-access lakes are filled grey at the lake opacity (dark slate in light); lighter / outline-only? —
  default: as built. (c) The line uses the under-lake route's own dash (dashes in light/dark, dots in cvd); a true dot
  ([0.1, 2] with round caps) instead? — default: the route's dash, as the owner pointed at the under-lake style.
