# T989 — Opened from a Google result, "back" goes to the default map instead of the water

Status: IN PROGRESS — lane/085-t989; left (Claude): merge + combined review. The web start-link landing already frames the map (now pinned); the phone link/tap entry (openRequest) did not land: fixed.

Source: owner 2026-11-21: "when a user opens a water body page from Google itself it is on regulation page but if they go
back at top right it goes to default location instead of the location of the water body they were viewing".

## Work
- Reproduce on the website: open a water's page by its own URL in a fresh tab (no history, as from a search result),
  then the top-right back/close. Today the map opens at the default place.
- Fix: leaving a page entered by a deep link lands on the map at that water (its extent / the part linked), as if the
  reader had opened it from the map. One load path (owner 11-03): the same place whatever the way in. Check the phone
  app's link entry the same way.
- Pin with a test: deep link → back → the camera is on the water.

## Acceptance
From a cold deep link, back shows the map at the water, on web and in the app.

## Repro on the live website (Claude, 2026-11-21)
Live site, phone size, /water/big-lagoon cold: first-run disclaimer → I understand → top-right close lands on Big
Lagoon; walkthrough Skip keeps it; with location granted (Vancouver) close still lands on the water. The website
does NOT show the bug. Most likely the owner's case is the installed app opened from a Google result (Android
App Links / iOS universal links → `openRequest`, no `landOn`) — the path lane/085-t989 fixes. Confirm in the
emulator walk: `adb shell am start -a android.intent.action.VIEW -d https://canifishthis.ca/water/big-lagoon`,
then close → camera on the water.
