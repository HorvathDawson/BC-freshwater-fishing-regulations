# T928 — iPhone: the main map fills only 2/3 of the screen after tab changes

Status: IN REVIEW — owner: OWNER-TODO "T928/T929 pictures", then main chat review + merge (lane/t929-t928: c71429593, 54b256e06). Sideways shift not reproduced; drag fixed; the part-drawn browse map found is T938.

Source: a tester on iPhone (TestFlight 0.8.3) relayed by the owner 2026-11-19 (`T928-shots/tester-2026-11-19-iphone.png`,
middle shot: the map drawn in the right two-thirds, a dark empty strip on the left, the day bar and legend at full width).
Tester, verbatim: "And now my map is only 2/3 my screen" / "Even when I change tabs" / "Like map conditions and runs" / "I
just restarted it and it's fine though". Owner: "That is definitely an apple only thing ... I haven't tested apple much yet".

## Work
Reproduce on the iOS simulator (lane 3): open a water's card, go Regulations / Conditions / Map / Runs and back, rotate,
background/foreground; find what leaves the native map view at a stale width (a layout measured during a transition, a
`MLRNMapView` frame not updated). Fix in the iOS half (AGENTS 34a: platform file), never a shared-code branch. A
test-bed scenario that walks the tabs and asserts the map's frame equals the screen width.
## Acceptance
The scenario passes on the simulator; a shot after the same tab walk shows the full-width map.
Depends: none. Size: M. Owner: no.

## More from the tester (2026-11-19, `T928-shots/tester-2026-11-19-restart.png`)
Owner asked for a report from More → Report a problem; tester, verbatim: "I restarted the app.... Ooops" / "No it reverted
back to normal. All try to brake it again but I'm not sure if I can reach the button to report a problem". So no report came
in (checked the production and staging workers 2026-11-19 evening: none from the tester). While reproducing, also check
whether More → Report a problem is reachable while the map is narrow (the tab bar / More screen at the right width).

## iOS simulator try 2026-11-19 20:35 (main chat's lane-3 agent; iPhone 17 / iOS 26.5, 0.8.3 build 40)
NOT reproduced: bottom-tab walks, water Regulations/Conditions/Map tabs, slow/fast/partial/double edge swipes, salmon mode,
background/foreground. Reading the tester's shot: the map AND the day bar are shifted right by the same ~135 pt while the
legend stays full width, so the whole browse layer is pushed sideways, not a narrow map frame. The only movable sideways
offset in that subtree is the edge-back drag (`EdgeBack.tsx:23-57`, native driver): suspect a drag left part-way (its
animated value not settled back to 0). Next: read the drag's value back on re-render / reset it on focus; try an
interrupted swipe on a real device. Report a problem (More) renders in that same layer: likely shifted but reachable.

## Lane t929-t928, 2026-11-20 (c71429593; review follow-up 54b256e06)
**The sideways shift: NOT reproduced** (iPhone 17e simulator, 390 pt, 0.8.3 build at 1f989bb48): a slow edge swipe on a
water page with the app sent to the background half-way (Settings launched over it at 3.5 s of a 4 s swipe) went back
normally; nothing stood sideways. **Fixed anyway in `EdgeBack.tsx`** (the only sideways offset in that layer): the
"finger down" flag is one ref for the view (each handler config had its own, so a config made mid-drag lost the finger
and its back), the view is marked off home from the first move until a render puts it home with a hard `setValue(0)`
(so every render with no finger down, a tab change, a route, a resume, puts it home), a finger silent for 1.5 s counts
as none, and a swipe that went back is held for the back's render (T821) at most 1.5 s. Tests: `gestures.test.tsx`
"(T928) the screen never stays part-way" (3 cases; all three fail on the old `EdgeBack.tsx`).

**Found while reproducing, REPRODUCED every time, same class (map drawn in part of the screen, kept through tab
changes):** search a water (the search sheet shortens the browse map), open its page, Back: the browse map draws only
in its top ~60 %, grey below, through pans and a Conditions → Map tab change (`T928-shots/ios-browse-map-part-drawn-
after-search-water-back.png`, 0.8.3 and this lane's build alike). Background/foreground not needed. Likely the native
map resized while kept hidden under the page (T717, `opacity: 0`) and its drawable not following. Not fixed here: it
needs its own ticket (the main chat's call). Whether it is the tester's case (theirs is sideways, with the day bar moved
too) is not known. Ticketed as T938.

**Review follow-up (54b256e06):** the 1.5 s "finger gone quiet" rule sent the screen home under a finger held still (a
feed render mid-hold jumped it to 0; a release there went back with the view at 0): dropped. A lost end is covered by
AppState instead (background / active puts it home; the system ends every touch then). A render puts it home only with
no finger down AND no spring running, so a cancelled drag's spring home is let finish. Tests: held still (not sent
home, its release goes back), background/active (home), a render mid-spring (not cut); all three fail on c71429593.
