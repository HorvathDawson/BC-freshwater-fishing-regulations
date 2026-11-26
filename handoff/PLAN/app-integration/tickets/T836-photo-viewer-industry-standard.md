# T836 — The spot photo viewer behaves like the standard phone photo viewers

Status: IN PROGRESS — merged 00edd4889 (2026-11-17: all four behaviours, tests); lane L6: the iPhone simulator bench 9 of 9 light and dark after one fix (a swipe also hid the buttons: 6fdb3d8db, tap maxDistance); left (Claude): the Android bench (open/close from the thumbnail, drag down, strip, tap vs double tap, that the buttons' layer passes touches), pinch rubber and fling glide feel; the counter over a light photo is T976

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T836 | The spot photo viewer behaves like the standard phone photo viewers (swipe down to close, tap to hide, counter, opens from its thumbnail) | IN PROGRESS — iPhone simulator 9 of 9 after the swipe-is-no-tap fix (6fdb3d8db, lane L6); left (Claude): the Android bench and the pinch / fling feel; counter readability is T976 | 00edd4889, 6fdb3d8db | Android + iPhone simulator bench (light/dark) |

Source: owner 2026-11-17 ~11:15, in the app chat, verbatim: "the photo viewer could also use a little improvement to
make it industry standard". Asked which of four standard (Apple / Google Photos) behaviours to add; picked all four
(11:17): "Counter + smooth motion, Tap to hide the buttons, Swipe down to close, Opens from its thumbnail".

## Done (merged 00edd4889, 2026-11-17; branch `app-fix/t836-photo-viewer`)
All four behaviours, no new dependency: `Animated` on the native driver (`motion.ts` `NATIVE_DRIVER`), not Reanimated;
no frame is asked of JS. Numbers in `@app/core` `photoViewer.ts` (`photoDragKind`, undecided until 6 px;
`photoStripFollow` / `photoStripSettle`: 30 % of the width or a 400 px/s flick moves on; `photoDismissFollow` /
`photoDismissEnds` / `photoDismissLook`: down 15 % of the height or a 600 px/s flick closes; `photoFromThumb` /
`photoThumbShown`); `photoZoom.ts` gains `photoZoomLoose`, `rubberPhotoView`, `photoGlide` (a fling carries 0.25 s of
its speed; past 5x it settles back). `usePhotoZoom` hands each view to `show(view, how)`. `PhotoViewer`: the counter
(`photoCounter`, hidden for one photo), tap to hide (`useExclusiveGestures`), the strip, open from / close into
`SpotPhotos`' `thumb(i)` (a fade when the cell is off screen, zoomed, or not read within 150 ms); Reduce Motion
(`motion.ts` `reduceMotionNow`): a fade. Fixed with tests: react-native-web drops an INLINE `pointerEvents` style
(`StyleSheet` `PASS.through` now). Tests: `core/photoViewer.test.ts` (28), `PhotoViewer.test.tsx`,
`PhotoViewer.motion.test.tsx`, `PhotoViewer.insets.test.tsx`; mutations killed.

## Android bench 2026-11-20 (L4, `photo-zoom` extended, emulator, build of 124d79efa)
`photo-zoom` now runs, after its old steps: the counter and tap-to-hide, the strip following the finger, a short and a long drag down, and
the open from / close into the thumbnail, light and dark (screenrecord cut into 10 frames a second). All PASS; the pinch step now drives
(adbd as root for the touchscreen). Shots in `handoff/PLAN/app-integration/L4-android-shots-2026-11-20/T836/`: tap hides every button
(`T836-1-chrome-shown.png`, `T836-2-chrome-hidden.png`); the strip follows the finger (`strip-follows-finger-two-photos.png`); a short drag
down springs back (`T836-7-drag-short-mid.png`), a long one shrinks the photo over the fading page and closes (`T836-9-drag-long-mid.png`;
the buttons stay at full strength: T970); the photo grows from its thumbnail in about three frames and shrinks back into it
(`T836-light-open-sheet.png`, `T836-dark-open-sheet.png`, `T836-light-close-sheet.png`); the buttons' layer passes touches on Android.
`pinch-spread-after-lift.png`: after a spread the photo rests with its bottom edge showing dark ground (one frame, not clamped; the
settle was not filmed). Not covered: Reduce Motion, the fling glide's feel, a thumbnail off screen, the iPhone.


## iPhone simulator bench and one fix (lane L6, 2026-11-20; 91d27a5a7, 6fdb3d8db)
- Bench `scenarios-ios/l6-shots.mjs` `t836-ios` (a spot seeded with three photos turned 0 / 90 / 180 degrees).
- Found (build 418f6f3a1): every swipe also hid the buttons (the gesture handler's tap has no distance limit by default
  on iOS and Android). Fix 6fdb3d8db: the tap and the double tap fail once the finger has moved
  `PHOTO_VIEWER.decideAfter` (6 px): `maxDistance` on both; test in `PhotoViewer.test.tsx` "(T836) a finger that moves
  as far as a drag is decided is no tap …". Shared code, so Android gets it too.
- After the fix (build 6fdb3d8db): 9 of 9 PASS, light and dark. Shots (`handoff/PLAN/ios/shots/t836/`):
  `T836-ios-1-thumbs-light.jpg`, `T836-ios-3-open-light.jpg`, `T836-ios-4-hidden-light.jpg`, `T836-ios-6-next-light.jpg`,
  `T836-ios-6-next-dark.jpg`, `T836-ios-7-closing-light.jpg`; before the fix: `before-fix-T836-ios-6-next-light.jpg`.
- The counter over a white photo is hard to read: T976.

## Left
- The Android bench on a build with 6fdb3d8db (open/close from the thumbnail, drag down, strip, tap vs double tap, that
  the buttons' layer passes touches); pinch rubber and fling glide feel; not yet covered anywhere: Reduce Motion, a
  thumbnail off screen; the Android Modal's window offset is measured, not assumed (check the photo lands on the cell).

## Owner

- [ ] 2026-11-20 — Look at the photo viewer on the emulator (`handoff/PLAN/app-integration/L4-android-shots-2026-11-20/T836/`: open and close the sheets, drag down) and say if it behaves like the standard phone photo viewers.
