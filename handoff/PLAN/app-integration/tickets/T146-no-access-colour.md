# T146 — The "no public access" colour reads too close to base regulations

Status: DONE — 158391f5 (tokens, gate, option shots); ticket in the following commit. Built with the Q-T146a default.

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
| **B magenta (BUILT)** | **`#F01C84`** | **`#50E4FF`** | 12.0 | 39.3 / 32.3 / 37.3, 75° |
| C spruce | `#006048` | `#447000` (olive) | 11.4 | 42.3 / 27.2 / 27.2, 116° — reads as forest / park; dark olive is muddy |
(The shots are of `#FC1C88`, the first magenta; the built `#F01C84` is the same hue nudged to clear the 15-from-ground
floor on the flavour's forest, `water-status-cvd.test.ts`. `T146-shots/built--*.jpg` are the built app, legend included.)

## As built
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
- **Q-T146a**: light no-access magenta is 12 from closed/own/base for a dichromat (the indigo was 20+ but read as base).
  Keep (colour-blind readers use the `cvd` theme), or go back to an indigo (A) and accept it looks like base, or add a
  second channel (a dash on no-access lines; colour alone carries status today by an earlier owner rule)? Default: keep.

## Left / follow-ups
- A second channel (dash / casing) for no-access lines would lift the dichromat case in light; it changes the
  "colour alone carries status" rule and the one colouring pathway on web + native — an owner decision (Q-T146a).
- Native map (T74) not shot on a device; it reads the same token.
