# T821 — iOS: the back swipe flashes the previous menu for a moment before going back

Status: DONE (f7b41c208; landed 2026-11-17 in land/app-1117, proven on the simulator; owner re-tests on the 0.7.12 build).

Source: owner 2026-11-16, in the app chat (~21:40, on iPhone build 36), verbatim: "when i swiper to close on apple it flashes the menu back for a second before it
goes back."

## 2026-11-16 21:45 — cause and fix (branch `app-fix/ios-1116`, app chat lane)
Both iOS edge-back swipes put the dragged view back at x = 0 the instant the back was decided, before the back had
drawn anything: `modalEdge.ts` (menu pages, sheets, the PDF and photo viewers) did `x.setValue(0)` and then the Modal's
own close ran (a menu page's slide, a sheet's leave), and `EdgeBack.tsx` (a water's page) did the same before the route
change rendered. So the page just swiped away stood whole again for as long as the close took: "it flashes the menu back
for a second before it goes back". Not a double back (the tests count one back per swipe). Fix: a modal's swipe carries
the content on off the right edge (160 ms, native driver) while the modal closes; nothing resets it (a closed Modal
unmounts its content). A screen's swipe holds the view where the finger let go and comes home in a layout effect after
the render the back caused (the screen it went back to, before it is painted). Tests: `ModalBack.native.test.tsx` and
`gestures.test.tsx` (T821 cases; both fail on the old code). Android and the web: untouched (neither draws the edge
swipe; their back is the system's / the browser's).
