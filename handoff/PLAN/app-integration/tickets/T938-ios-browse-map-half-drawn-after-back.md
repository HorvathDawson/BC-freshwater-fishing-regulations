# T938 — iPhone: after search → water → Back, the browse map draws only its top part

Status: IN PROGRESS — lane L6: the sideways case (the T928 tester's picture) reproduced on the iPhone simulator and fixed in 3a8d5333b (a hard home re-renders the edge view so React commits 0); the grey lower part not reproduced on this build; left (Claude): the Android emulator walk

Source: found 2026-11-20 by the main chat's T929/T928 lane on the iOS simulator (iPhone 17e, 390 pt), while trying to
reproduce T928. Reproduces every time, on the 0.8.3 build (1f989bb48) and on lane/t929-t928.
Shot: lane/t929-t928 `handoff/PLAN/app-integration/tickets/T928-shots/ios-browse-map-part-drawn-after-search-water-back.png`.

## What happens
Search a water, open its page, press Back: the browse map draws only its top ~60%; below it is flat grey. It stays so
through pans and a tab change. No backgrounding needed.

## Likely cause (lane's reading, unproven)
The browse map is resized while hidden under the water page (`opacity: 0`) and its drawing surface does not follow the new
frame. May or may not be the tester's T928 case (theirs was shifted sideways, the day bar too).

## Work
Prove the cause on the simulator; make the map's surface follow its frame when it is shown again (or never resize it while
hidden). A test where one can pin it; before/after simulator shots to the owner. Check Android (emulator) for the same walk.

## Acceptance
Search → water → Back draws the whole browse map on iPhone (and Android stays right), light and dark.

## Lane L6, 2026-11-20: the sideways case reproduced and fixed; the grey lower part not reproduced

Bench: `scenarios-ios/t938.mjs` (`run-ios.mjs t938-ios`), iPhone 17e, iOS 26.5, light and dark. Cultus Lake from search,
its page (it opens on its Map view), Back three ways: the page's ‹; the left-edge swipe tried mid, high, then low; one
low edge swipe alone. Two checks after Back and after a pan: the map's lower band (60-82 % down) is not one flat
colour, and its left strip (0-25 % across, 30-80 % down) is not one flat colour (the layer left off to the right).

- **The grey lower part (this ticket's first report) did not reproduce** on 418f6f3a1, 6fdb3d8db or 3e8189d79 (these
  carry T928's EdgeBack changes c71429593 and 54b256e06): Back by the ‹ draws the whole map, light and dark, three runs.
- **The sideways case DID reproduce, every time, light and dark** (6fdb3d8db and the probe build): after an edge swipe
  that went back, the browse map and its day bar stood ~75 % to the right (where the finger let go), white or black on
  the left, through a pan: the T928 tester's picture. The probe's shot after each drag showed how: on the page's Map
  view the mid and high drags land on the page's own map and pan it (EdgeBack's recogniser begins, then is cancelled);
  the low one goes back. One low swipe alone came home cleanly on the same build.
- **Cause.** A cancelled edge drag springs the offset home with the native driver, which makes the offset a native
  value for good. The drag itself moves the offset from JS (`setValue` every frame), and a `setValue` on a native value
  goes to the native side only: React Native does not flush it back to React, so React's copy keeps the drag's offset.
  The first cancelled drag of a session arms it; the next edge swipe that goes back leaves the layer off home.
- **Fix (3a8d5333b).** `EdgeBack.tsx`: every hard home (`homeNow`: after a back's render, after the hold, on the app
  going to the background and back, after a spring lands) re-renders the edge view after `setValue(0)`, so React reads
  the offset again (0) and commits it, whatever the driver. The spring stays on the native driver.
  - First try, 3e8189d79: the spring home on the JS driver, so the offset never turns native. It passed on the
    simulator (6 of 6, 13:05-13:38) but breaks the project's guard `tools/no-per-frame-animation.test.ts` (phone
    components hand every animation to the platform, so nothing stutters while JS is busy), so it was replaced.
  - Test: `EdgeBack.native-value.test.tsx` makes the offset behave as a native value after a swipe that went back
    (`setValue` stores it and tells the view nothing, as on a phone): after the hold the view is home. It fails without
    the re-render (checked: left at 120 px). The web tests (`gestures.test.tsx`) run every value JS-driven and could not
    see this.
- **After the fix** (build 3a8d5333b, run started 13:48): 6 of 6 PASS, light and dark (the ‹, the three-drag edge walk,
  one low edge swipe). The three-drag walk that failed now ends with the whole map: left strip 50 % (light) and 85 %
  (dark) its commonest colour, against 100 % before; the same after a pan.
- Shots (`handoff/PLAN/ios/shots/t938/`): before `before-fix-light-1-before.jpg`,
  `before-fix-light-2-first-edge-drag-pans-page-map.jpg`, `before-fix-light-3-after-edge-back.jpg`,
  `before-fix-light-4-after-pan.jpg`, `before-fix-dark-3-after-edge-back.jpg`; after
  `after-fix-light-3-after-edge-back.jpg`, `after-fix-dark-3-after-edge-back.jpg`; the ‹ walk
  `ios-light-chevron-after-back.jpg`, `ios-dark-chevron-after-back.jpg`.
- Same pattern, no symptom seen: `ViewSwipe.tsx` also mixes a JS `setValue` drag with a native spring home; its view
  goes away with the water page, so nothing was left standing. Not changed.
- Left (Claude): the same walk on the Android emulator (EdgeBack is iOS-only, so the sideways case cannot happen there;
  the grey-band check is still owed).

## Review leftover (lane LF, 2026-11-20, 4df30d481)
`ViewSwipe.tsx` has the same native spring + `setValue` pattern, but homes only by a spring (whose end re-renders the
view), so there is no live defect. A comment at its `home` now points at EdgeBack's `homeNow` / `sync()`: a hard home
added there must re-render too. Comment only; no behaviour change, no test.
