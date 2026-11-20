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
