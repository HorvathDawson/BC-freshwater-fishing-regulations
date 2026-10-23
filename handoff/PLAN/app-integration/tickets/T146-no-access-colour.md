# T146 — The "no public access" colour reads too close to base regulations

Status: DONE — 158391f5 (tokens, gate, option shots), 03ef3c76 (review: light is spruce, not the selection's magenta). Built with the Q-T146a default; Q-T146b (spruce vs selection) default built.

Source: Owner 2026-10-23 (app walk). "the no public access color is awkward too. it is very similar to base regs". Related: T54 (no-access
streams, Q1–Q3), T47 (status colours you can tell apart; Palette D stands).

## Work
Pick a no-access colour clearly apart from status.base (and every other status colour) in light and dark, measured
(ΔE / the colour gate T47 uses, colour-blind sims), on map lines, lake fills and the legend. Keep Palette D otherwise.
Show 3 options as shots; build the best (owner can swap). Size: S.

## What was measured (2026-10-23)
T54's light indigo `#2A00E0` cleared every gate (23.4 ΔE2000 from base as tokens for the worst reader) and still read
as base: both are BLUE, only 23° of LCh hue apart (normal-vision line 24.4). The gate's ΔE could not see a shared hue
family. The search (`T146-shots/frontier.mjs`, `pick.mjs`, sRGB cube step 4, the gate's own maths from `app/tools/cvd.ts`):

- **Light: no colour is both a different hue family from base AND colour-blind-safe.** Every colour ≥ 40° of hue off
  the light base stands at most **12.2** from closed, own or base for some dichromat as a thin line; only an indigo beside
  base clears 20 (red, orange and blue already take the dichromat's space). So light trades the dichromat floor for the
  normal-vision one; the `cvd` theme (colour-blind) keeps its mauve, 14.5+ from every water status. → Q-T146a.
- **Dark: there is a strictly better colour.** An ice cyan `#50E4FF` stands 22.6 from every water status as a thin line
  for every reader (T54's ochre: 12.7 from closed) and is 71° off the deep-blue base.

## The three options (shots: `T146-shots/sheet-{ns,cq}-{light,dark}.jpg`; `today` = T54's palette, first column)
| option | light | dark | worst reader vs closed/own/base (light, line) | normal vision vs base (light: token/line/lake, hue) |
|---|---|---|---|---|
| A ultramarine (colour-blind first) | `#0C00D4` | `#50E4FF` | 22.4 | 34.1 / 25.1 / 27.4, 23° — still reads as base |
| B magenta (built first; replaced in review: the selection's hue) | **`#F01C84`** | **`#50E4FF`** | 12.0 | 39.3 / 32.3 / 37.3, 75° |
| **C spruce (BUILT after review, as `#00604C`)** | `#006048` | `#447000` (olive) | 11.4 | 42.3 / 27.2 / 27.2, 116° — reads as forest / park; dark olive is muddy |
(The shots are of `#FC1C88`, the first magenta; the built `#F01C84` is the same hue nudged to clear the 15-from-ground
floor on the flavour's forest, `water-status-cvd.test.ts`. `T146-shots/built--*.jpg` are the built app, legend included.)

## As built
(Superseded in light by the adversarial review below: light is now spruce `#00604C`; the rest stands.)
- Tokens: `color.status.no_access` light `#2A00E0` → `#F01C84` (magenta), dark `#985400` → `#50E4FF` (ice cyan), cvd
  unchanged (`#705864`). One token per theme: the map lines, lake fills, legend swatch and the T147 block all read it.
- Gate (`app/tools/status-colours-distinct.test.ts`): access floors re-measured (rounded down to 0.5): light token /
  line / lake / sat 11.5 / 12 / 12 / 11.5, dark 28 / 22.5 / 27 / 16.5; permit and boundary floors moved likewise. Every
  floor under 20 recorded with its reason (`WHY.lightAccess`, `accessBoundary`, `darkAccessPermit`); dark accessToken /
  Line / Lake are no longer exceptions. NEW: "no public access is not base (T146)" — at least 45° of LCh hue off base and
  35 / 30 / 30 normal-vision ΔE (token / line / lake), light and dark. Mutation pins: T54's indigo fails it with all four
  whole sentences (23.2°, 32.8, 24.4, 26.7); dark no_access painted base's own blue fails all four.
- `app/tools/water-status-cvd.test.ts` `ACCESS_FLOOR` light 20 → 11.5 (recorded: the magenta is 11.9 from own under
  tritan Machado), dark 15 → 28.
- `tokens.json` `$comment` updated; `pnpm style:build` (style.meta.json, style.sha256).
- Shots: `T146-shots/` (options + built, scripts `options-shots.mjs`, `sheet.py`, search scripts).

## Ships with
App build only (tokens and style are compiled into the app). No pipeline, bundle or release rebuild.

## Owner questions (default built)
- **Q-T146a**: light no access (now spruce, after the review) is 11.5 from closed/own/base for a dichromat as a line (the
  indigo was 20+ but read as base; the magenta was 12.0 but the selection's hue). Keep (colour-blind readers use the
  `cvd` theme), or go back to an indigo (A) and accept it looks like base, or add a second channel (a dash on no-access
  lines; colour alone carries status today by an earlier owner rule)? Default: keep.

## Adversarial review (2026-10-23)
Reproduced on this worktree's app (Expo web, own port 39541, the live release through :39217), light and dark, the
North Shore / Coquitlam map and the Coquitlam River's water Map view after a tap.

**Defect (fixed, 03ef3c76): the built magenta was the selection's colour.** Light `#F01C84` stood **10.4** (normal vision)
from `color.highlight` `#c2187a` — the selection outline (`casingPaint` / `ringPaint`) — LCh hue 359 against 351. The
tokens comment claimed the highlight had been searched against; no gate measured it (`pick.mjs` / `frontier.mjs` never
read `color.highlight`, and the status gate holds only statuses, boundaries and ink). On screen
(`T146-shots/review--defect--watermap-coquitlam-river-light.png`): tapping the Coquitlam River and opening its Map, the
selected part runs up through the Coquitlam Watershed, so its no-access stretch is magenta cased in magenta (no outline
to see), and every unselected no-access tributary beside it reads as selected — the owner's complaint about base, moved
onto the selection (`adapters/contract.ts` already records the owner's earlier "read as magenta" complaint about the
selection). Dark (`#50E4FF` vs `#ff77b8`) and cvd (`#705864` vs `#F55C00`) are far apart.
- Fix: light `color.status.no_access` → **spruce `#00604C`** (LCh 36/30/171: 113° off base, 180° off the selection,
  80° off every status; 7.6:1 on the card). Searched (`T146-shots/selection.mjs`, sRGB step 4, pick.mjs's rules plus:
  ≥ 45° of hue off base AND the highlight, ≥ 20 normal ΔE from the highlight, the T146 normal-vision base floors, the
  boundary line): the spruce is the best for the worst reader against closed / own / base — **11.5**, against the
  magenta's 12.0. It is the builder's option C nudged (`#006048` → `#00604C`). Shots: `review--fixed--map-cq-{light,dark}.png`,
  `review--fixed--watermap-coquitlam-river-{light,dark}.png` (the selected part's magenta casing now reads around a
  green line; the no-access tributaries are plainly not selected), and the T147 block in it (`T147-shots/review--*`).
- Gate: NEW `apartFromSelection` in `tools/status-colours-distinct.test.ts`: no access ≥ 45° of LCh hue off
  `color.highlight`, ≥ 20 as tokens, and a thin no-access line ≥ 20 from the casing beside it over every ground (normal
  vision, light and dark). MUTATION PIN: the magenta fails it with three whole sentences (7.6°, 10.4, 10.3).
- Floors re-measured to the spruce (rounded down to 0.5): light access token / line / lake / sat 16.5 / 11.5 / 11.5 / 10,
  accessPermit 21 / 11.5 / 11, boundary 14.5 / 1.5 (the spruce is 1.8 from the grey boundary as lines for a protanope;
  recorded in `WHY.accessBoundary` — dashed, wider, drawn under). `water-status-cvd` `ACCESS_FLOOR` light 11.5 → 16.5 (16.8
  off closed, protan Viénot — checked by setting 17). The T146 normal-vision base floor for line / lake 30 → **26**: the
  spruce stands 26.5 / 26.5; 30 was the magenta's own number. The indigo pin still fails on hue, token and line (24.4 < 26;
  its lake sentence, 26.7, no longer fails and was removed from the pin).
- Checked and held: the light "no hue 40°+ off base clears ~12 for a dichromat" claim (re-run with the selection added:
  best 11.5). Dark ice cyan sits **4.2** from `color.ui.live` (UI chrome, not on the regulations map) and **4.7** from
  `color.contour.line` (the depth layer's lake contours) — see Left.

**Not a defect, recorded:** the T146 option sheets compared options on the plain status map only; no sheet showed a
selection. Any future colour search for a map line includes `color.highlight` (now gated).

## Owner questions (review)
- **Q-T146b**: light no access is SPRUCE `#00604C` because the magenta was the selection's hue. A dark green on the
  map can read as "forest / park" and, to some, as "go". Keep spruce (default built), or keep the magenta and move the
  SELECTION off magenta instead (touches every map's selection, its gates and T52's change ramp)?

## Left / follow-ups
- Dark ice cyan vs the depth contours (`color.contour.line` `#4FD0E8`, ΔE 4.7): on a no-access LAKE with the Depth layer
  on, its contours vanish into the fill. Not gated; the contour colour is the one to move if it matters.
- A second channel (dash / casing) for no-access lines would lift the dichromat case in light; it changes the
  "colour alone carries status" rule and the one colouring pathway on web + native — an owner decision (Q-T146a).
- Native map (T74) not shot on a device; it reads the same token.
