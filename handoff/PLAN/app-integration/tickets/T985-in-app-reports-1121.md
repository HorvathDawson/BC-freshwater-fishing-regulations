# T985 — In-app reports 2026-11-21 (4 items)

Status: OPEN — joins the 0.8.5 batch (restart page step 1: "any new item joins the batch").

Source: `GET /reports` on the production feeds worker, read 2026-11-21 12:50. The fifth report (2026-11-20 23:38,
Squamish River, "make the map a lot bigger") is T979.

## Items
1. 2026-11-21 17:48Z, Android 0.8.4 (42), Conditions tab, Chipmunk Creek, bug. Verbatim: "First load of this page from
   conditions map when I clicked button to change model from default for forecast the chart flickered hard for a second
   before it settled". Reproduce; the chart should keep its old lines until the new model's are ready (no blank frame).
2. 2026-11-21 07:36Z, iOS 0.8.4, Map tab, Chilliwack River, bug. Verbatim: "No downstream label on this stream".
   Reproduce: which stretch, which label (the downstream end / direction label of the water Map tab).
3. 2026-11-21 07:35Z, iOS 0.8.4, Map tab, Chilliwack River. Verbatim: "Went to map from conditions and the map is
   uncoloured here until I go upstream then back downstream or until I pan or zoom on map". A T451-type regression:
   reproduce on the test bed (Conditions → Map), fix, pin with a test bed walk.
4. 2026-11-21 07:34Z, iOS 0.8.4, Conditions tab, feature. Verbatim: "The chart gestures do not work very well. When
   checking specific date no way to drag the bar only click and zooming in and panning would interfere with that etc".
   Proposal for the owner (design): drag the date bar to scrub; pan/zoom stays two-finger or on the axis.

## Acceptance
#1–#3 reproduced, fixed, and checked on the platform reported. #4 is a design proposal: the owner picks.

## Owner pick 2026-11-21 (#4)
B (a handle on the date bar to scrub, pan on an axis strip under the chart, pinch zooms, one-finger drag on the chart scrolls the page), owner verbatim: "b but have it also use the hold and drag gesture on top of it" (touch-and-hold anywhere, then drag, also scrubs).

## Owner pick 2026-11-21 (#2)
Label the mouth end: the map labels "Flows into <water>" at the downstream end of every water with an exit; the mouth's place goes in the release.
