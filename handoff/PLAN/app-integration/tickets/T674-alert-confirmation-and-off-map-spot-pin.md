# T674 — The alert confirmation stays on screen; a spot off the map is not in the accessibility tree

Status: IN PROGRESS — both built and pinned in component tests; left (Claude): see both on the emulator on the next APK (`saved-spots` scenario)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T674 | After "Save alert" the confirmation stays on screen; a saved spot off the map has no screen-reader button | IN PROGRESS (both built, component tests; left (Claude): emulator check on the next APK) | 04572e8d3 | T603 emulator run 2026-11-11; (a) `AlertMe` scrolls its block into the pane's view after a save; (b) `spotsInView`, both maps |

Source: T603's recorded emulator run 2026-11-11 ("Seen on the way"), two app bugs the test bed found.

## Bugs
a. After "Save alert" (Coquitlam River → Conditions → Alert me, Save low on the screen) "Saved. Manage alerts in More →
   Notifications." and "1 alert on this gauge" were above the fold. Shot: `../T603-shots/t603-alert-saved-off-screen.jpg`.
b. A saved spot's pin stayed in the accessibility tree while off the map (spot at Hope, map on Coquitlam: a 3 px button
   "Open spot <title>", uiautomator `spot-mark:spot:…`, bounds [0,136][3,139]).

## Done
- a (04572e8d3): `AlertMe` brings its block into view after each save (`coachScroll.bringIntoView` through the pane's
  `CoachScrollContext`; unmeasurable → retried once after 250 ms, then `reportOnce`). Tests: `ui-native/src/menu/Notifications.test.tsx` "(T674)".
- b: `spotsInView` (`core/spotPins.ts`) — both maps give a screen-reader button only to marks in the camera's box
  (`Map.native.tsx`, `web/useSpots.tsx`). Tests: `core/spotPins.test.ts` "spotsInView", `map/adapter-contract.render.test.tsx` "T674".
- Emulator check 2026-11-20 (L4, `saved-spots`, build of 124d79efa): a PASS (now a failing check in `saved-spots`; shot
  `handoff/PLAN/app-integration/L4-android-shots-2026-11-20/T674/a-after-save-alert-confirmation-on-screen.png`);
  b PASS (step "T674 b": `b-map-away-no-spot-mark.png`, `b-map-at-spot.png`).

## Left
- The boundary marks' screen-reader buttons (`Map.native.tsx`, T317) are built the same way (every mark of the open
  water, wherever the camera is). They belong to the water on screen, so they were left; say if they should follow.

Depends: T603. Size: S. Owner: no.
