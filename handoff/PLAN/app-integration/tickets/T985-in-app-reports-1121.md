# T985 — In-app reports 2026-11-21 (4 items)

Status: IN PROGRESS. Lane 085-ui reproduced on the web build (staging data, 390 px): #1 NOT reproduced (Chipmunk Creek, Conditions: a legend model click drew the new line with no spinner and no dim; the only blank-ish path is the dimmed held chart, opacity 0.45, when a model's series is not in hand: native cold start only). #2 REPRODUCED in part: Chilliwack part 1 labels only its upstream end (Tamihi Rapids Bridge); the mouth end has no mark, so no label (the group's `exit` carries no coordinates; text 'Flows into Vedder River' and the step button exist). A fix needs the mouth's place in the release: data + owner pick, not a UI guess. #3 NOT reproduced on web (Conditions -> Map coloured at 0.5 s and 4.5 s); the T451 colouring queue is native-only, so it needs the Android emulator / iOS sim (not run). left (Claude): emulator check for #1/#3, owner pick for #2.

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
