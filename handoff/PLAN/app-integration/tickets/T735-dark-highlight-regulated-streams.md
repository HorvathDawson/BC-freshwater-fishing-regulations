# T735 — The part you are on is hard to pick out in dark mode on a regulated stream

Status: IN PROGRESS (before closed testing; OWNER PICKED LOOK C 2026-11-16 11:51, "C: dark edge + soft glow (Recommended)"; BUILT on branch `app/t735-dark-look-c` @ dfa9047d3 by a Sonnet lane, not reviewed by the app chat, no pictures yet, not merged)

Source: owner 2026-11-14, phone, dark palette, build 31/32, verbatim: "The highlighting isn't very good in dark mode on regulated streams."
His shot: the Fraser River's Map tab, "Part 3 of 19".

## Problem
He did not say what is wrong, so this ticket measures first and shows him looks. Nothing is changed in the app yet.

## What dark draws today (`paint.ts`, `themes/dark.json`)
- The part you are on in a water's Map view (feature-state `part`): its status line at full strength, a casing of `CASING_PX` 2.5 each side in
  `color.highlight` #ff77b8 (pink), or `color.ui.attention` #F0F2F0 (near white) when an in-season change covers the section.
- The water's other parts (`water`): the status colour at `opacity.water.others` 0.25. They get NO casing, except where an in-season
  change covers them: that casing is opacity 1 and is not dimmed with the line.
- Status colours, dark: open #A7FBB2, restricted #BE8DC2, closed #FF3B6D, own #FFD307, base #4C70FC, no_access #9C9A98.

## Measured (WCAG ratios; `scratchpad` script, grounds: basemap earth #141414, land as drawn #3B3B3B, basemap water #333333)
- Others at 0.25 over the ground: 1.2 to 2.0 : 1 (open 1.9-2.0, own 1.8-1.9, restricted 1.5, closed 1.3, base 1.3). All under 3:1.
- The Fraser (every part in-season): the others are a near-white casing at full strength with a faint line inside, so they read pale and
  as bright as the part you are on; the casing is the same on the selected part, so the selection is told only by the line's fill.
- Status line against its own white casing: open 1.09, own 1.28, restricted 2.4, closed 3.06, base 3.7. Open and own (mint, yellow) nearly
  vanish into a white casing: the casing, not the status, draws the line's edge.
- On the pink casing (streams with no in-season change): own 1.7, open 2.0, closed 1.4, restricted 1.1 : status and ring are
  close in lightness; separation is by hue only.
- Satellite ground in dark: a bright field (~#7A7A5A) leaves the others at 1.0-1.4 : 1 and the casing at 1.8-3.9.

## Looks built (dark only, an uncommitted patch, `?dl=a|b|c` on the web)
a: dark casing under the part (status colour carries), in-season casing on the others at 0.35.
b: today's ring, thinner (1.25 px), others at 0.12, in-season casing on others 0.25.
c: thin (1.5 px) dark casing and a soft light glow outside it, in-season casing on others 0.35.
Shots and side-by-side sheets: `~/bcfish-session-backup-2026-11-12/dark-highlight/`; patch `options.patch` there.

## Acceptance
- Owner picks a look (or says what is wrong); it is applied to dark only, through palette tokens, light unchanged.
- Dark: the part you are on measures at least 3:1 against the ground and its own edge, for every status; the others at least 1.5:1.
- `tools/status-colours-distinct.test.ts` and the map contrast checker (T470) pass; web shots on 3 waters, 6 statuses, plus satellite.

## Owner's pick, 2026-11-16 11:51 (question dialog in the main chat, with the two sheets on screen)
"C: dark edge + soft glow (Recommended)". To build (app chat): look c from `options.patch`, dark only, through palette
tokens, light unchanged; then the acceptance lines above. An earlier note said the glow can be stronger if he asks.

## Look C built on a branch, 2026-11-16 13:00 (Sonnet lane's report; NOT yet read or re-run by the app chat)
Branch `app/t735-dark-look-c` @ dfa9047d3 (from the shared head a8caa4c4b), 12 files under `app/packages/map` and
`app/tools/layer-order.test.ts`.
- Five new palette tokens (dark / light and cvd): `width.selection.casing` 1.5 / 2.5; `flag.selection.edge` 1 / 0
  (the switch; "flag." is a new prefix); `color.selection.edge` = `color.outside`; `opacity.selection.glow` 0.4 / 0;
  `opacity.selection.casing.others` 0.35 / 1.
- `paint.ts` `casingPaint` + new `glowPaint`; `runtime-style.ts` adds layer `selection-glow` directly under the
  casing, in dark only; `adapters/contract.ts` carries the glow for the native map (same `part` / `selected` state).
- Light unchanged, as reported: `style/style.json` has no diff; only `style.meta.json` and `style.sha256` changed.
- Reported checks: `vitest run packages/map` 53 files, 403 tests pass; status-colours-distinct, map-contrast (T470),
  layer-order, style-valid, style-spec pass; `tsc -b` clean. New `selection-look.test.ts`.
- Reported contrast of each dark status colour against the new edge #0A0B0D: open 16.0, restricted 7.3, closed 5.7,
  own 13.7, base 4.7, no_access 7.0 (all at least 3:1).
STILL OWED: read the diff; web pictures before / after on 3 waters and satellite (kept script
`~/bcfish-session-backup-2026-11-12/dark-highlight/shoot.mjs` + `looks.sh`; the `?dl=` switch is gone, compare the
branch with the shared head), sent to the owner in chat; "the others at least 1.5:1" is not measured; the water-copy
line and the lake / wetland rings were left as they are; an emulator look in dark; then the merge with benches.

## Checks and benches, 2026-11-16 18:05 (app chat)
Branch `app/t735-dark-look-c`, merged with the head. Two fixes for the full check: the look-C test reads the casing colour
from the palette (1bf670eb9); the style tests the dark glow layer moved, updated on purpose (fd0b004e6: the mid-zoom
fixture's dark `selection-casing` colour and width only, hand-carried, nothing else; 7f1da560a: handover gains
`selection-glow`, the profile-style count is 21 in dark and 20 in light and cvd). `pnpm check` rc 0. Benches on the
`t735` APK: install, memory, tap-stalls, regs-open (twice; first taps 33-85 ms), bg-install, crash-bed: all PASS.
Pictures sent to the owner 2026-11-16 ~13:31 (four before/after sheets).
