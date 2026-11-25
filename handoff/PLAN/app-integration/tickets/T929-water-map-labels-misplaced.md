# T929 — Water map ("Show all parts"): boundary labels don't line up with the place they name

Status: DONE — 65691d11d, 752b5596e, 819419ebb; boundary labels anchored at their cut points; merged via e97c6932a; pictures approved by the owner 11-20.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T929 | Water map: boundary labels don't line up with the place they name; the part looks mis-framed (Chilliwack) (tester via owner) | DONE | 65691d11d, 752b5596e, 819419ebb | — |

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

## iOS simulator repro 2026-11-19 20:35 (main chat's lane-3 agent; iPhone 17 / iOS 26.5, 0.8.3 build 40 from 1f989bb48)
Shots: scratchpad (session cf82a6c9) `ios-t928-t929/` (cmp-0809.png, cmp-pan.png). REPRODUCED, two causes:
1. Framing (native): picking a row in "Show all parts" shrinks the map to 48 pt and grows it back in the same render; the
   refit runs before iOS applies the new frame (log: `fit h=48`, refit at `h=259` 1 ms later, camera zoom 4 instead of ~10;
   `Map.native.tsx:686-704, :958-964`; the binding fits to the current frame and clips padding, `CameraUpdateItem.m`
   `_makeCamera` :61-86, `_clippedPadding` :112-133). Stepping with ▲/▼ frames right. Fix: `easeTo` with the centre/zoom JS
   already computes (`fitZoomOf`), or defer the refit a frame after `onLayout`. Android not checked.
2. Labels "move with you" (shared, web too): `shiftedLabels` (`point-marks.ts:56-65`) pulls every label to `labelShift(x)`
   (`marks.ts:9-10`) at each settle, so a mark off screen gets its label pinned 100 px inside the edge and it follows the
   camera. Fix: shift only marks on screen; a leader line when a label is moved off its mark.
Not verified: web run (same pinning by code; framing right via `syncSize`); the tester's exact south-east view.

## Fixed 2026-11-20 (lane t929-t928: 65691d11d; review follow-up 819419ebb)
- **Labels (web and phone):** a label's point is now always its mark's own cut point (`marksData`); near an edge only its
  `text-anchor` changes (`labelAnchor`, `marks.ts`: bottom-left / bottom-right within 100 px of an edge, else bottom),
  so the box's corner sits over its ring and it never needs a leader. It moves with the map and leaves the screen with
  its mark. Only `project` is asked now (`native/labelShift.ts` → `native/labelAnchor.ts`, the T734 guard kept).
  Pinned: `point-marks.test.ts` (label coordinates = cut points whatever the camera), `marks.test.ts`, the web contract
  test (anchor changes at `moveend`, point unchanged).
- **Framing (phone):** the fit is sent as a centre and a zoom worked out from the laid-out size (`fitCamera`, core
  `ladder.ts`, via `easeTo`), never the binding's `fitBounds` (which fitted to iOS's not-yet-grown 48 pt frame). Android
  takes the same path. Pinned: `ladder.test.ts`, the native contract test (48 pt fit, refit at 259 pt lands at the
  259-pt zoom, no fitBounds sent). Web unchanged (its `useFit` measures the canvas first).
- **Pictures (iPhone 17e simulator, 390 pt, light; before = 0.8.3 build at 1f989bb48, after = the lane build 6bf58c8cf, c71429593 once rebased):**
  `T929-shots/ios-part3-pick-before-after.png` (part 3 picked in "Show all parts": z4 / 100 km before, the part framed at
  5 km after), `T929-shots/ios-pan-labels-before-after.png` (after a pan: the Slesse label pinned at the left edge
  before, gone with its mark after), `T929-shots/ios-edge-label-before-after.png` (Tamihi Rapids Bridge near the right
  edge: label slid left off its ring before, hanging from it after).
- Not checked: Android emulator and a web run (unit/contract tests only). Seen: part 3's lower mark sits under the
  "Whole river" button at that framing (its label shows above it); not changed here.

## Owner approval and review follow-up, 2026-11-20
- **Owner approved the pictures** (dialog 2026-11-20 ~06:34, OWNER-TODO "iPhone map T929"). The independent review found
  it ready.
- Review follow-up (819419ebb): a ring just OFF the map hung its label inward (up to 180 px of label on the map with no
  ring). Off screen it now hangs outward: x < 0 → bottom-right, x > width → bottom-left (`labelAnchor`, `marks.test.ts`).
