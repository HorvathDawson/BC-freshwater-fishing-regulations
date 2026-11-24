# T970 — Photo viewer: while the photo is dragged down to close, the buttons stay at full strength over the fading page

Status: PARTIAL — the chrome (✕, counter, arrows) now fades with the drag down, gone by the time the ground is half gone (`photoDismissLook().chrome`, `PhotoViewer.tsx`); a spring-back brings it back; pinned in `PhotoViewer.motion.test.tsx` (mutation-checked). Code 8c5dfce63. left (Claude): the Android look, light + dark.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T970 | Photo viewer: the buttons stay at full strength while the photo is dragged down to close | PARTIAL — built + render-tested (8c5dfce63); left: the Android look, light + dark | 8c5dfce63 | L4 Android looks 2026-11-20 |

Source: L4 Android device-looks pass 2026-11-20 (T836 bench, emulator, `photo-zoom`), found on the shot taken mid-drag.

## What was seen
A long drag down at the fit (T836 behaviour 1): the photo follows the finger and shrinks and the black ground fades, so the
spot page shows through. The ✕, the "2 of 2" counter and the ‹ arrow keep their full strength (the arrow's grey disc sits
over the page's PHOTOGRAPHS thumbnails and the ✕ disc over the page's "Delete"). Photos and Google Photos fade the buttons
with the ground. Shot: `handoff/PLAN/app-integration/L4-android-shots-2026-11-20/T836/T836-9-drag-long-mid.png`.

## Work
Fade the chrome (✕, counter, arrows) with the same drag value as the ground (`photoDismissLook`), so it is gone by the
time the ground is half gone; a spring-back brings it back. Test beside the existing drag-down tests. Look at one shot,
Android, light and dark.

Depends: T836. Size: S. Owner: no (a look; he sees the shot).

## Note (L8, 2026-11-22)
The chrome's opacity already multiplied the ground's fade in the source (T836), yet the 11-20 shot showed it at full strength mid-drag; the tests now sample the real Animated mid-drag and pass, so the shot may predate the build or be a native-driver effect. The device look decides.


State 2026-11-22 (stopped for credits): review fixes done (6 items, see PR #43); left: nothing from the review, device looks/shots still owed; branch fix/T970-ui-smalls.
