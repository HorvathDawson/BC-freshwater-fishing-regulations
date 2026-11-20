# T928 — iPhone: the main map fills only 2/3 of the screen after tab changes

Status: OPEN.

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
