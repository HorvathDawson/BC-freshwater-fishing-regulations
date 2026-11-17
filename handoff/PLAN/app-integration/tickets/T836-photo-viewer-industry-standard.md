# T836 — The spot photo viewer behaves like the standard phone photo viewers

Status: OPEN (app chat; after T819 lands: both change `app/packages/ui-native/src/spot/PhotoViewer.tsx`).

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
