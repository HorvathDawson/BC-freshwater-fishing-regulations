# T726 — iOS: safe areas, Dynamic Island, portrait lock, dark/light, Dynamic Type, VoiceOver (IOS-9)

Status: OPEN.

Source: owner 2026-11-14, verbatim: "Maybe we could make a handoff to start improving the ios versions? Making sure it is
rock solid too?" and "We want to do as much work as possible without the Apple account". Stub IOS-9 in
`handoff/PLAN/ios/START-HERE-ios.md` (approved to start there, "Unattended day"). Simulator only; no Apple account.

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
(filled in as the work runs)
