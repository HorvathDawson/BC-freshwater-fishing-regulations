# T985 — In-app reports 2026-11-21 (4 items)

Status: DONE — shipped 0.8.5; emulator walk PASS #1/#3 (2ccf1ea45, 2026-11-21).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T985 | In-app reports 2026-11-21 (4 items: chart flicker on model change, Chilliwack downstream label, map uncoloured after Conditions, drag the chart date bar) | DONE (shipped 0.8.5; emulator walk PASS #1/#3) | 2ccf1ea45 | prod /reports, read 2026-11-21 |

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

Status #2 (lane/085-mouth, 2026-11-21): BUILT, not merged. The bundle's `water_exit` carries the mouth's place (`lon` /
`lat`, `topology.mouth_place`: the mouth part's main stem nearest the receiving part); the release ships it in `water` /
`water-<r>` as microdegrees, ADDITIVE (still water/3, water-slice/4: app 0.8.4 reads the new release; a bump would have
raised minAppVersion to 0.8.5). The app reads the row's own columns: a release before them draws no mouth label. The Map
labels "Flows into <water>" at the mouth on the mouth part (ring elsewhere; tap = the bar's link; tidal said, not linked).
Tests: vitest (waterMap, navReads, mini release), pytest test_part_geo, mini release re-cut, fast-fail app gates.
Left: the release build REFUSES a bundle built before the columns, so the 0.8.5 data needs a bundle rebuild
(`python -m pipeline.deliver`); `test_the_chilliwack_flows_into_the_vedder_at_vedder_crossing` (needs_bundle) then pins
the Chilliwack's mouth at Vedder Crossing. Not seen on a device or the web map (no bundle with the columns yet).
