# T726 — iOS: safe areas, Dynamic Island, portrait lock, dark/light, Dynamic Type, VoiceOver (IOS-9)

Status: OPEN — stopped 2026-11-22 by lane L8: simulator shots on four iPhones, large text and a VoiceOver walk (M). The two code findings in Results are already fixed: the ↗ emoji tile (T723 item 7, `textPresentation.ts`) and the photo viewer's top inset (T815).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T726 | iOS: safe areas, Dynamic Island, portrait lock, dark/light, Dynamic Type, VoiceOver (IOS-9) | OPEN | — | owner 2026-11-14 (iOS reopened, simulator only); stub IOS-9 in docs/agents/START-HERE-ios.md |

Source: owner 2026-11-14, verbatim: "Maybe we could make a handoff to start improving the ios versions? Making sure it is
rock solid too?" and "We want to do as much work as possible without the Apple account". Stub IOS-9 in
`docs/agents/START-HERE-ios.md` (approved to start there, "Unattended day"). Simulator only; no Apple account.

## Work
**IOS-9. Layout: safe areas, Dynamic Island, rotation, dark/light, Dynamic Type, VoiceOver.** Acceptance: shots on iPhone
17 / 17e / Air / 17 Pro Max sims in light and dark; large text; VoiceOver reaches every card and button (T336 class).
Size: M.
  Sub-steps: **lock portrait on iPhone** (`expo.orientation`/`ios` Info.plist; check Android's setting and leave it);
  shots on iPhone 17 / 17e / Air / 17 Pro Max in light and dark; largest Dynamic Type; VoiceOver tree walk like T336
  (every card/button reachable); Dynamic Island / home-indicator insets; port `a11y-walk.mjs`. Mirrors: `check-colours`,
  `regs-text-scale.test.ts`, `a11y-walk.mjs`.

## Acceptance
shots on iPhone 17 / 17e / Air / 17 Pro Max sims in light and dark; large text; VoiceOver reaches every card and button (T336 class).

Depends: T718. Size: M. Owner: no (rotation answered 2026-11-14: portrait only).

## Results
- **Found on the 2026-11-14 baseline (0.7.9, iPhone 17, iOS 26.5), not fixed yet:** the "This water in the rule book p.N ↗"
  link draws its arrow as a blue EMOJI tile on iOS (U+2197 takes emoji presentation there; Android draws the plain
  arrow). Every "↗" in the app's text is suspect. Fix to try: the text variation selector (U+FE0E) after the arrow, or the
  app's own icon; check Android stays the same. Shot: baseline `006-tap-water-…png`.
- **Orientations in the built Info.plist:** Portrait and PortraitUpsideDown (Expo's `"portrait"`). iPhones without a
  home button ignore upside-down, so this is portrait only in practice; tighten to Portrait alone only if a run shows
  the app turning over.

## Found before this ticket started (2026-11-15, merged-branch simulator run)
- **Photo viewer:** its "Photo 1 of 1" line sits under the iPhone status-bar clock (no top safe-area inset). Seen on
  `ios/integration` and on the base alike, so not from the merge. Shot in the integration lane's session 3 folder
  (`integ-ios-3`, the modal-back photo step). To fix here: the viewer's header takes the top inset; check the Dynamic
  Island models too.
