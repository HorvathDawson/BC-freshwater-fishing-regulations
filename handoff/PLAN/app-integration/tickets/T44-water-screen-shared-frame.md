# T44 — one frame for the water screen's views (no shift between Regulations, Conditions and Map)
Phase 4 · Depends on T23 (RegsScreen), one-water-screen (openWater / WaterViews), P15 (WaterMapView) · Size 1 day

## Owner (2026-10-21)
"in desktop switching between regulation and condition tab makes the margin on the left change and everything move. it
feels wrong. these should be linked together better I think. maybe each is a sub component of the main component
setting the spacing etc?"

## Goal
The water screen is ONE parent frame that owns the layout every view shares: the outer gutters, the content max-width
and its centring on wide screens, the safe-area insets, the header row (FaceBar: back + view strip) and its height, and
the background. Regulations, Conditions and Map are children that fill the frame's content box and set no outer margins
of their own. Switching views (tap, swipe, back) moves nothing that both views draw: the back button, the view strip,
the water's name and the left edge of the content stay at the same pixel positions.

## Work
- Measure today: Expo web at 390x844, 768x1024 and 1440x900. Record the x/y of the back button, the strip, the title and
  the content's left edge on each view, and the differences. Screenshots go in `T44-shots/before/`.
- Add `WaterFrame` (ui-native), owned by WaterScreen/WaterViews, with the layout tokens (gutter, maxWidth, header height)
  in theme tokens, not literals. RegsScreen, the Conditions view and WaterMapView drop their own outer padding and
  header row and render inside it. The FaceBar is drawn ONCE by the frame (today each view draws its own).
- The Map view may go full-bleed under the frame's header. That is the one allowed difference, and the header still does
  not move.
- Keep the owner rule: one water screen, one opener (openWater). This ticket changes layout, not routing.

## Acceptance
- A render test per breakpoint: the back button, strip, title and content-left of all three views are equal
  (whole-number px) after a view switch. A mutation (a child adds its own gutter) fails it.
- A house check: no view under `water/` or `regs/RegsScreen` sets an outer horizontal padding/margin or draws a FaceBar.
- Web shots after: `T44-shots/after/` at the three sizes, each view. A frame-by-frame swipe recording shows no jump.
- Phone layout unchanged, pixel for pixel, apart from where today's views disagree.
