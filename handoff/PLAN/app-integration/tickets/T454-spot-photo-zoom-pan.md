# T454 — Spot photos: zoom and pan in the viewer

Status: IN REVIEW — built in 4ede32a69 (owner phone check pending; web and emulator not yet looked at). Owner 2026-11-06, chat 5.

Owner: "the spot viewer needs a better image viewer to look at images on it so we can zoom and pan etc; right now it is
a fixed size."

## What
`ui-native/src/spot/PhotoViewer.tsx` (T284) shows a photo full screen, fitted to the screen, with close, previous/next
and a PanResponder swipe. You cannot zoom or pan it. Make it work like a standard phone photo viewer:
- pinch to zoom (about 1× to 5×, zooming in around the fingers), drag to pan while zoomed (the photo's edges stop at the
  screen edge), double-tap to zoom in at that point or back to fit;
- a swipe moves to the previous/next photo only at 1×; while zoomed, a drag pans;
- the zoom resets on a photo change, on rotation, and on close; Back (`useBackLayer`) still closes;
- web: mouse wheel / trackpad pinch zooms, drag pans, double-click toggles zoom.

## Work
1. Build on `react-native-gesture-handler` v3 (already in; `usePinchGesture` / `usePanGesture` / `useTapGesture`, T250).
   If a well-established zoom viewer package does it better, it may be used instead (memory: better package over
   minimal), with a line in `app/deps.md`.
2. Keep the zoom/pan maths (clamping, focal-point zoom) in a hook or in core, not in the component (rule 25). A split
   between web and native is a `.web`/`.native` file pair (rule 34a).
3. Tests: the maths (clamp at the edges, focal zoom, reset on a photo change), and the swipe working at 1× but not while
   zoomed. Emulator check with a test-bed photo on a spot; send the owner a short screen recording.

Related: T284 (the viewer). Size: S–M. Owner: phone check.

## Emulator check (2026-11-06)

New scenario `photo-zoom` (`app/tools/testbed/run.mjs photo-zoom`) on the n1_pixel emulator, release APK built from 4ede32a69. A spot with two photos, viewer opened, each step judged on the photo box's pixels against the step before. Results: double-tap zoom in 32 % changed; one-finger drag pans 34 %; sideways swipe while zoomed moves the picture (10 %) and stays on "Photo 1 of 2"; double-tap back to the fit 28 %; swipe at the fit goes to "Photo 2 of 2". No crash. NOT verified: pinch (adb cannot inject multi-touch here: `sendevent` is permission denied on the Play-image emulator), so the pinch gesture still needs the owner's phone. Shots + 30 s recording: `app/tools/testbed/out/t454/` (git-ignored, local).
