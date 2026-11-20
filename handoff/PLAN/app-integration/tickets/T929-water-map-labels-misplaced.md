# T929 — Water map ("Show all parts"): boundary labels don't line up with the place they name

Status: OPEN.

Source: a tester on iPhone relayed by the owner 2026-11-19 (`T928-shots/tester-2026-11-19-iphone.png`, top shot: Chilliwack
River, Map, part 3 of 4 "100 m below the Slesse Creek confluence → Chilliwack River Ecological Reserve boundary"; the label
boxes float away from the river, and the view shows Ross Lake / the Skagit, south-east of the part). Tester, verbatim:
"when I click 'see all segments' such as on the chilliwack the descriptions don't line up with the body of water or land it's
describing".

## Work
Reproduce on Android, iOS and web (Chilliwack River, Map, parts 1–4): (a) is the camera framed on the part (it looks pushed
south-east)? (b) are the boundary labels anchored at their cut points, with a leader to the point when moved off it to avoid
overlap? Related: T465 (labels overlap zoomed out). Fix so each label sits at (or visibly points to) its place and the part
fills the frame, on every platform.
## Acceptance
Shots of Chilliwack parts 1–4 on the three platforms, labels on their points; a test pinning label anchors = cut points.
Depends: none. Size: M. Owner: no (pictures sent).

## Tester, later the same evening (relayed by the owner), verbatim
"the labels for the sections just move around with you as you pan around the map ... If you zoom or move off the section
they aren't in the correct area for their description anymore and can just be over a random piece of land or body of
water". So the boundary labels are placed in SCREEN space (laid out once for the framed view) and do not follow the map on
pan/zoom: they must be anchored to their cut points (a map symbol / marker at the coordinate) or re-laid out on every camera
move, on every platform.
