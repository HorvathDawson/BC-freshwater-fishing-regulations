# T721 — iOS: left-edge swipe back is dependable on every layer (IOS-4)

Status: OPEN.

Source: owner 2026-11-14, verbatim: "Maybe we could make a handoff to start improving the ios versions? Making sure it is
rock solid too?" and "We want to do as much work as possible without the Apple account". Stub IOS-4 in
`handoff/PLAN/ios/START-HERE-ios.md` (approved to start there, "Unattended day"). Simulator only; no Apple account.

## Work
**IOS-4. Back and swipe.** Goal: left-edge swipe back is dependable by thumb and by the test bed; sheets, menu pages,
walkthrough, rule book, salmon/run-timing screens all leave predictably. Acceptance: `back-layers`-style walk on
simulator; real-iPhone verdict (needs owner) recorded; if a fast mid-height drag still stays, fix the gesture or its
hit area. Size: M.
  Sub-steps: port `edge.mjs` and the `edge-back*.yaml` flows; sweep drag height/speed grid, record which go back; walk every
  layer (water page, sheets, menu pages, walkthrough, rule book, salmon/run timing, search) one step per swipe; fix hit
  area/threshold in `EdgeBack.tsx` if the grid fails (iOS-only file); confirm Android untouched. Mirrors: `edge`,
  `back-layers.test.ts`.

## Acceptance
`back-layers`-style walk on simulator; real-iPhone verdict (needs owner) recorded; if a fast mid-height drag still stays, fix the gesture or its hit area.

Depends: T718. Size: M. Owner: yes (a thumb on a real iPhone is the judge; waits for T88).

## Results
(filled in as the work runs)
