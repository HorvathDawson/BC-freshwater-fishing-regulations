# T836 — The spot photo viewer behaves like the standard phone photo viewers

Status: IN PROGRESS — merged 00edd4889 (2026-11-17: all four behaviours, tests); lane L6: the iPhone simulator bench 9 of 9 light and dark after one fix (a swipe also hid the buttons: 6fdb3d8db, tap maxDistance); left (Claude): the Android bench (open/close from the thumbnail, drag down, strip, tap vs double tap, that the buttons' layer passes touches), pinch rubber and fling glide feel; the counter over a light photo is T976

Source: owner 2026-11-17 ~11:15, in the app chat, verbatim: "the photo viewer could also use a little improvement to
make it industry standard". Asked which of four standard (Apple / Google Photos) behaviours to add; picked all four
(11:17): "Counter + smooth motion, Tap to hide the buttons, Swipe down to close, Opens from its thumbnail".

Today (T284, T454, T815): the photo whole (contain), ✕, previous / next and a swipe between a spot's photos; pinch,
wheel and double-tap zoom with drag pan (`@app/core` photoZoom, `usePhotoZoom`); a swipe changes photo only at the fit;
the ✕ clear of the notch (T815).

## Build (all four)
1. **Swipe down to close:** at the fit, dragging the photo down shrinks and fades it with the backdrop; let go past a
   distance or a flick closes, else it springs back. Not while zoomed (a pan then).
2. **Tap to hide the buttons:** one tap hides ✕, arrows and counter (the photo alone, full screen); a tap brings them
   back. A double tap still zooms (the single tap waits for it).
3. **Counter + smooth motion:** "2 of 5" at the top (words from the words file); zoom and pan carry momentum and spring
   back at the edges; between photos the strip follows the finger and settles on the next or back. Native driver /
   Reanimated on the UI thread: no per-frame JS (tools/no-per-frame-animation.test.ts).
4. **Opens from its thumbnail:** the photo grows from the thumbnail tapped (its window box) to full screen and shrinks
   back into it on close (to the current photo's thumbnail if it is on screen, else a fade).
Reduce Motion: the open / close becomes a fade (iOS and Android convention). Android back and the iOS edge swipe close it
as today. Tests per behaviour (gesture logic as pure functions where possible, mutation-checked); pictures and short
clips on Android and the iPhone simulator, light and dark.

## Built — 2026-11-17 (branch `app-fix/t836-photo-viewer`, off `redesign/stream-sections` with T819 + T815; not merged)

All four, with no new dependency: `Animated` on the native driver (`motion.ts` `NATIVE_DRIVER`), not Reanimated
(`deps.md` keeps RNGH on JS callbacks). A finger's movement sets an `Animated.Value` (no re-render); every let-go,
open, close, settle and the buttons' fade is an animation handed to the platform. No frame is asked of JS.

- **Numbers in core** — `@app/core` `photoViewer.ts` (new): `photoDragKind` (one finger: zoomed a pan, at the fit
  sideways the strip, down the dismiss), `photoStripFollow` / `photoStripSettle` (follows the finger, rubber past the
  first / last; let go: 30 % of the width or a 400 px/s flick moves on, never while zoomed or off the ends),
  `photoDismissFollow` / `photoDismissEnds` / `photoDismissLook` (down 15 % of the height or a 600 px/s flick closes, a
  flick back up keeps it; the ground is gone by half the height, the photo shrinks to 0.6), `photoFromThumb` /
  `photoThumbShown` (the fitted photo laid over its thumbnail, covering it as the thumbnail does; only into a thumbnail
  at least half on screen). `photoZoom.ts` gains `photoZoomLoose`, `rubberPhotoView` (the platform rubber band past the
  zoom and edge limits) and `photoGlide` (let go: a fling carries 0.25 s of its speed, held by the edges; past 5x it
  settles back about the centre). `photoSwipe` is gone (the strip's settle replaces it).
- **`usePhotoZoom`** (`@app/ui`) no longer holds the view in React state: it hands each view to `show(view, how)` —
  "follow" (a finger on it), "settle", "glide" — and keeps the free view while fingers are down.
- **`PhotoViewer`** — the photo is now full screen under the bars; the ✕, the counter and the arrows sit over it inside
  the window's insets (T815 kept). 1. *Swipe down to close:* follows the finger, shrinks, the ground fades; closes or
  springs back. Not while zoomed (a pan). 2. *Tap to hide:* one tap hides the ✕, arrows and counter (they stop taking
  touches, `aria-hidden`); a tap brings them back; the single tap waits for the double tap (`useExclusiveGestures`).
  3. *Counter + smooth motion:* "2 of 5" at the top (`photoCounter` in the spots words; none for one photo); the strip of
  photos (the open one and its neighbours) follows the finger and eases onto the next or back; the arrows ease it too;
  zoom and pan rubber past the limits and settle or glide when let go; a double tap eases in and out. 4. *Opens from its
  thumbnail:* `SpotPhotos` hands `thumb(i)` (each cell measured in the window); the photo grows from the cell tapped and
  shrinks back into the current photo's cell on close (✕, Android back, the iOS edge swipe, the drag down), or fades
  when that cell is off screen, the photo is zoomed in, or its size / place cannot be read within 150 ms.
  **Reduce Motion:** asked at the moment of opening and closing (`motion.ts` `reduceMotionNow`, the hook's one catch
  moved there): open and close are a fade.
- Small calls made: the counter is hidden for a single photo; the drag down also follows the finger sideways (as
  Photos does); the buttons sit on a dim disc so they read over a bright photo; growing, the photo fades over the first /
  last fifth of the motion (it covers its square thumbnail, so a wider photo overhangs the cell, and the fade hides the
  overhang as it lands).
- Two faults the web shots caught, both fixed with a test that fails without the fix: (a) react-native-web drops an
  INLINE `pointerEvents` style, so the insets' full-window layer under the buttons took every tap and drag (the
  `StyleSheet` `PASS.through` now; test: every layer between a button and the chrome's root passes touches); (b) the
  recogniser's first update can carry no movement, and 0 vs 0 read as sideways sent a drag down along the strip
  (`photoDragKind` is undecided until 6 px; core + component tests).

**Tests.** `core/photoViewer.test.ts` (new, 28: every threshold and guard; 13 guards mutation-checked, each killed),
`PhotoViewer.test.tsx` (zoom, strip, drag down closes / springs back / pans when zoomed, tap hides and returns, double
tap leaves the buttons, counter, ✕ closes; 7 wiring mutations killed), `PhotoViewer.motion.test.tsx` (new, real
`Animated` sampled: grows from the thumbnail and shrinks back, fades when the thumbnail is off screen, Reduce Motion
fades both ways; 4 mutations killed), `PhotoViewer.insets.test.tsx` (the arrows, not the photo, inside the insets).
`pnpm -C app run check:fast` passes.

**Owed (bench).** Android and the iPhone simulator, light and dark: the open / close from the thumbnail (the Android
Modal's window offset is measured, not assumed — check the photo lands on the cell), the drag down, the strip, the tap
timing against the double tap, pinch rubber and fling glide feel; on Android, that the buttons' layer passes touches
(it rests on `pointerEvents` in a style on the safe-area view). Web shots (390 px, light and dark; Spots is the phone
app's only, so the shots forced the web notice off locally, uncommitted): the lane's scratchpad `t836-shots/`.

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
- **Bench** `scenarios-ios/l6-shots.mjs` `t836-ios`: a spot seeded with three different photos (one picture turned
  0 / 90 / 180 degrees, so a moved strip is seen), then in light and dark: open from the thumbnail and the counter, a tap
  hides the buttons and a second brings them back, a swipe to the next photo, a drag down closes it.
- **Found (build 418f6f3a1, 11:56-12:00): every swipe also hid the buttons.** The strip moved on (the picture 77 % changed)
  but the counter and buttons were gone. Cause: the gesture handler's tap has no distance limit by default on iOS and
  Android, so a swipe faster than the tap's 500 ms was ALSO a tap. **Fix (6fdb3d8db):** the tap and the double tap
  fail once the finger has moved `PHOTO_VIEWER.decideAfter` (6 px, where a drag is decided): `maxDistance` on both.
  Test: `PhotoViewer.test.tsx` "(T836) a finger that moves as far as a drag is decided is no tap …" reads the limit
  sent to the native handler on both taps (`fireGestureHandler` skips thresholds); it fails without the fix (checked).
- **After the fix** (build 6fdb3d8db, 12:21-12:26): 9 of 9 PASS, light and dark: opened from the thumbnail at "1 of 3";
  tap hides, tap brings back; a swipe moves to the next photo with the buttons up and "2 of 3"; a drag down closes it.
- Shots (`handoff/PLAN/ios/shots/t836/`): `T836-ios-1-thumbs-light.jpg`, `T836-ios-3-open-light.jpg`,
  `T836-ios-4-hidden-light.jpg`, `T836-ios-6-next-light.jpg`, `T836-ios-6-next-dark.jpg`, `T836-ios-7-closing-light.jpg`;
  before the fix: `before-fix-T836-ios-6-next-light.jpg`.
- Seen in the shots, not changed: the counter is white text with no backing, so over a white photo (the seeded fish
  drawing) "2 of 3" is hard to read. Phone viewers put a soft shade behind it; a small follow-up if wanted.
- Not covered here: the Android bench, pinch rubber and fling glide (feel), and on Android that the buttons' layer
  passes touches. The fix is shared code, so Android gets it too (its taps had the same no-limit default).
