# T454 — Spot photos: zoom and pan in the viewer

Status: OPEN (owner 2026-11-06, chat 5).

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
