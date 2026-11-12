# T674 — The alert confirmation stays on screen; a spot off the map is not in the accessibility tree

Status: IN PROGRESS — both built and pinned in component tests; left (Claude): see both on the emulator on the next APK (`saved-spots` scenario)

Source: T603's recorded emulator run 2026-11-11 ("Seen on the way"), two app bugs the test bed found.

## Bugs
a. **After "Save alert" the confirmation can be above the fold.** Steps: Coquitlam River → Conditions → Alert me → A level
   for the day → scroll so Save sits low on the screen → Save alert. Saving collapses the editor; the pane keeps its
   scroll place and shows what is under the gauge card, so "Saved. Manage alerts in More → Notifications." and
   "1 alert on this gauge" are above the fold. Shot: `../T603-shots/t603-alert-saved-off-screen.jpg`.
b. **A saved spot's map pin stays in the accessibility tree while the spot is off the map.** With the spot at Hope and
   the map on Coquitlam the map still held a 3 px button "Open spot <title>" at its top-left corner (uiautomator
   `spot-mark:spot:…`, bounds [0,136][3,139]): a screen reader reached it and a tap-by-label opened the spot.

## Built
a. `AlertMe` brings its whole block (the button row, the count, the confirmation) into its page's view after each save
   (`coachScroll.bringIntoView`, through the pane's own `CoachScrollContext` scroller: above the fold scrolls back up,
   below it scrolls down, in view moves nothing). A block that cannot be measured is looked at once more after 250 ms,
   then reported (`reportOnce`), never passed over. No new words. Tests: `ui-native/src/menu/Notifications.test.tsx`
   "(T674)" (3; the first two failed before the fix).
b. One core function, `spotsInView` (`core/spotPins.ts`): the marks whose face reaches into the camera's box. Both maps
   give a screen reader a button only for those (`Map.native.tsx`: the settled camera and the map's own size;
   `web/useSpots.tsx`: also on `moveend` / `resize`). A map that has not been sized holds none. What is drawn and what a
   tap does are unchanged. Tests: `core/spotPins.test.ts` "spotsInView" (3), `map/adapter-contract.render.test.tsx`
   "T674" on web and native (both failed before the fix).

## Not done / to check
- Neither was seen on a device after the fix. Next emulator run: (a) the steps above with Save at the foot of the screen,
  the confirmation and count must be on screen at rest; (b) with a spot at Hope and the map on Coquitlam, no
  `spot-mark:` node in the uiautomator dump; pan to Hope and it is there.
- (a) trusts the pane's scroll offset as last reported by its scroll events. If Android clamps the offset when the
  content shrinks without a scroll event, the move could be off by the clamp; the emulator check covers it.
- The boundary marks' screen-reader buttons (`Map.native.tsx`, T317) are built the same way (every mark of the open
  water, wherever the camera is). They belong to the water on screen, so they were left; say if they should follow.

Depends: T603. Size: S. Owner: no.
