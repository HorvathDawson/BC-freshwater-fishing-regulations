# view-swipe: moving between a water's views (owner 2026-10-21)

The owner said: "a sideways swipe moves between a water's VIEWS, Regs <-> Conditions <-> Map, NOT sections". This
replaces G1's part swipe; the down/up buttons and the water map still step between sections.

## The order, and which way is which
**Regulations · Conditions · Map**, left to right, comes from `@app/core` `WATER_VIEWS`. The header strip draws that
list and the swipe walks the same list, so the two always agree.
- **Finger moves LEFT**: the view to the right slides in (Regulations → Conditions → Map).
- **Finger moves RIGHT**: the view to the left comes back.
- **At either end**: the view gives a little (0.3×, at most 32 px) and springs back. There is no wrap.
- **Why this order**: it is the owner's order (G1-gestures.md, 2026-10-21). APP-PREP §0 drew the header's actions as
  `[Map] [Conditions here]`; now that the views are one strip, the swipe's order sets the strip's order.
- **Gesture rules**: horizontal movement must beat vertical 2:1, or the drag counts as a scroll. A swipe never starts in
  the 24 px edge zones. Both rules are unchanged from G1 (`viewSwipeStarts`, `classifySwipe`).

## Where the swipe listens
| view | swipe from | why |
|---|---|---|
| Regulations | anywhere on the view | nothing on it pans |
| Conditions | anywhere on the view, except on its maps and the chart's plot | the route map and the gauge minimap pan, and the plot is read with a finger (zoom is coming); those are shielded |
| Map | the header only | the canvas pans |

**Shields** (review fix, 2026-10-21). The reviewer found that on Conditions only the header (about 120 px of an
844 px screen) took the swipe. Now the Conditions body takes it too, both on a Conditions-tab reach and on a water's
Conditions view. The minimap (`MiniMap`, so the route map and the single-gauge map) and the hydrograph's plot (the
`Svg` only, not its key or caption) wear a shield (`useSwipeShield` / `SwipeShield`). A press that lands on a shield
claims the drag for that thing, so the swipe neither follows nor moves. The shield's press and the recogniser's begin
can arrive in either order, so a press within 400 ms of the begin counts.

On the web, the detector for the Regulations body, and the one for the Conditions body, sits *inside* the view's ScrollView. A browser reads `touch-action`
only up to the nearest scroll container, so a detector wrapped around the ScrollView had its `pan-y` ignored. The
browser then cancelled every sideways drag. I found this in the browser run, not in the unit tests.

## From the Conditions tab to the regulations
- **Header**: a reach opened from the Conditions map now carries the same header as a water. It reads
  "‹ Conditions", then the strip **Regulations · Conditions** with Conditions selected.
- **One labelled action**: the strip's **Regulations** opens that water's regulations. So does a swipe to the right on
  the header.
- **The tap is kept**: the reach opens in water mode with the reach the reader tapped and its "you are here". Its
  Conditions view one swipe away is that same reach, not the water's first reach. The earlier door
  ("and the conditions: what the water is doing", which opened `sections[0]`) is removed.
- **Back** follows the one back rule. From Conditions or Map, back goes to Regulations ("‹ Regulations"). From
  Regulations, back goes to where the reader came from ("‹ Conditions" / "‹ Map" / "‹ Search"): the reach, with its
  "you are here", returns whole.
- **Water map view**: until P15, it is the water framed and lit on the map (the search's framing), on its own camera.
  Leaving water mode puts the browse map back where the reader left it.

## Checked
- **Tests**:
  - `core/src/waterViews.test.ts`: the order, the words, every step, and both ends.
  - `core/src/gestures.test.ts`: every threshold.
  - `ui/src/gestures.test.ts`: `viewSwipeAction` for every view, the Conditions reach, and browse states.
  - `ui/src/route.test.ts`: the reach → Regs → Conditions → Map → back → back walk, and that the tap is kept.
  - `ui-native/src/gestures.test.tsx`: `<ViewSwipe>` with `fireGestureHandler`. A drag that begins with a touch or a
    pointer press on a shielded canvas does not move the view; the same drag on the body does.
  - `ui-native/src/SwipeShield.test.tsx`: the minimap and the chart's plot are shielded, while the text around them
    and the chart's caption still swipe. Mutants caught: shield removed from the plot, `held()` always false.
  - `ui-native/src/ConditionsPanel.test.tsx`: the panel takes the body swipe from `WaterViewsContext`.
  - `ui-native/src/WaterMode.test.tsx`: the real Shell on the fixture bundle. It swipes the header and the body
    through all three views and back, and pins that the Conditions view is the tapped reach with its "you are here".
    It also swipes the Conditions body: reach → Regulations, then Conditions → Map, then Conditions → Regulations.
- **Mutation checks**: two mutants were tried and both were caught. One handed the Conditions view no tap; the other
  turned off the header swipe.
- **Web** (`web-check.mjs`, Chromium at 390×844 with touch emulation, against a worktree expo server on :39561; start it without file watching only after the last edit, since `CI=1`
  serves the files as they were at start):
  0. Review fix: from a Conditions reach, a **body** swipe right (on the estimate) opens Regulations. Back on
     Conditions, **a pan on the minimap stays** and **a drag on the chart's plot stays**. A body swipe left goes to Map,
     and a body swipe right goes back to Regulations. With the shield removed, the minimap pan switched to Map, so the
     browser run sees the shield. Screenshot `phone-light-03b-map-from-body`.
  1. From a Conditions reach, a header swipe right opens Regulations.
  2. A body swipe left goes to Conditions, and a header swipe left goes to Map.
  3. **A pan on the map canvas stays on Map.**
  4. A header swipe right goes back to Conditions.
  5. **A vertical scroll on Conditions stays on Conditions.**
  6. A header swipe right goes to Regulations, and a further swipe right stays there (the first view).
  7. ‹ returns to the reach, and the bottom bar is back.
  8. Desktop at 1440×900: the strip moves between all three views. No page errors.
- **Screenshots**: `phone-*` and `desktop-*`, light and dark: `01-reach`, `02-regs`, `03-conditions`, `04-map`, and
  `phone-light-05-back-to-reach`.

## Not done / not verified
- **Midpoint section (`mid_sid`)**: "Conditions here" at the part's midpoint needs P09/P14, which have not landed. A
  water opened from search or a list opens its Conditions view on the water's first reach. A tapped water opens on the
  tapped reach.
- **Date pill**: the water header has no date pill yet (P14/T23).
- **Native feel**: how it feels on Android and iOS is not checked (no emulator). On Android, RNGH inside a ScrollView
  is expected to work the same way, but this is unproven.
- **Desktop layout**: the Conditions view's desktop layout (the chart not filling the width, visible in
  `desktop-*-03-conditions.png`) belongs to conditions-layout.
