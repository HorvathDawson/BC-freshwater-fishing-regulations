# T938 — iPhone: after search → water → Back, the browse map draws only its top part

Status: IN PROGRESS — lane L6: the sideways case (the T928 tester's picture) reproduced on the iPhone simulator and fixed in 3a8d5333b (a hard home re-renders the edge view so React commits 0); the grey lower part not reproduced on this build; left (Claude): the Android emulator walk

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T938 | iPhone: after search → water → Back, the browse map draws only its top part (found chasing T928) | IN PROGRESS — lane L6: the sideways case (T928's picture) reproduced and fixed (a hard home re-renders the edge view so React commits 0); the grey lower part not reproduced; left (Claude): the Android emulator walk | 3e8189d79, 3a8d5333b | main chat lane 2026-11-20, simulator; in 0.8.3 too |

Source: found 2026-11-20 by the main chat's T929/T928 lane on the iOS simulator (iPhone 17e, 390 pt), while trying to
reproduce T928. Reproduces every time, on the 0.8.3 build (1f989bb48) and on lane/t929-t928.
Shot: lane/t929-t928 `handoff/PLAN/app-integration/tickets/T928-shots/ios-browse-map-part-drawn-after-search-water-back.png`.

## What happens
Search a water, open its page, press Back: the browse map draws only its top ~60%; below it is flat grey. It stays so
through pans and a tab change. No backgrounding needed.

## Done (lane L6, 2026-11-20; bench `scenarios-ios/t938.mjs`, `run-ios.mjs t938-ios`, iPhone 17e, iOS 26.5)
- **The grey lower part did not reproduce** on 418f6f3a1, 6fdb3d8db or 3e8189d79 (they carry T928's EdgeBack changes
  c71429593 and 54b256e06): Back by the ‹ draws the whole map, light and dark, three runs.
- **The sideways case (the T928 tester's picture) reproduced every time and is fixed.** Cause: a cancelled edge drag
  springs the offset home with the native driver, which makes the offset a native value for good; a later JS
  `setValue` goes to the native side only, so React's copy keeps the drag's offset and the next edge swipe that goes
  back leaves the layer ~75 % to the right. Fix (3a8d5333b), `EdgeBack.tsx`: every hard home (`homeNow`) re-renders the
  edge view after `setValue(0)`; the spring stays on the native driver (the first try, 3e8189d79, a JS-driver spring,
  breaks `tools/no-per-frame-animation.test.ts`). Test: `EdgeBack.native-value.test.tsx` (fails without the re-render:
  left at 120 px). After the fix 6 of 6 PASS, light and dark. Shots: `handoff/PLAN/ios/shots/t938/`.
- `ViewSwipe.tsx` has the same native spring + `setValue` pattern but homes only by a spring (whose end re-renders), so
  no live defect; a comment at its `home` points at EdgeBack's `homeNow` / `sync()` (lane LF, 4df30d481).

## Work (left, Claude)
The same walk on the Android emulator (EdgeBack is iOS-only, so the sideways case cannot happen there; the grey-band
check is still owed).

## Acceptance
Search → water → Back draws the whole browse map on iPhone (and Android stays right), light and dark.
