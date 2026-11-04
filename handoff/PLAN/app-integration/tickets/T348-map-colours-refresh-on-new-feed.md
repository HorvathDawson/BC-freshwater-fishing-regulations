# T348 — Map colours don't refresh when a new feed arrives (river forecast came in, map stayed blank)

Status: DONE (9907af3c0) 2026-11-04 (was: OPEN — bug (owner 2026-11-02: "when new feeds come in we need the map colors to update if they have new info.
We got the new river forecast in but the colors didn't update and still showed blank")).

A fresh river-forecast feed reached the app, but the map's conditions colouring kept its old state (blank, no data)
until something else forced a redraw.

1. **Find which feeds drive the map paint:** forecast, gauges, DFO salmon, in-season, run timing. For each, check
   whether a newer copy invalidates the paint on every path: foreground fetch, background refresh, reconnect and app
   resume.
2. **Likely causes:**
   - the paint is computed once at map load and keyed to the old feed version;
   - the stored feed changes but the map isn't notified;
   - a memoised selector never sees a changed key.
3. **Fix once for all feeds:** a new feed version bumps one "feeds changed" signal. The map re-applies paint for that
   layer (feature-state, no full style reload), and the open pane re-reads.
4. **Tests:**
   - unit: a new forecast version updates the affected sections' paint;
   - test bed: open the map with an old or empty forecast, deliver a new one, and the colours change without a
     restart.
   Run the same checks for gauges, DFO and in-season.

Do it together with T347. Size: S–M.

## Evidence from T346 (problem report 98935871c9b7548e, 2026-11-02, 0.4.0 build 7, android 37, screen "more")
"The number of reaches coloured in conditions doesn't change when zoom or scroll changes, only when you click a
different forecast then back to the option you were on (change -> +5 -> change)." Owner 2026-11-03: the report is about the
LEGEND'S COUNT TEXT ("12 reaches coloured", Chrome.tsx `LegendRamp`, core `legendCount`: the reaches ON SCREEN with a
standing) and its histogram marks, NOT the map colours. The count and marks keep the old view's numbers after a pan or
zoom and only recompute when the forecast offset changes. Fix: recompute them on the map's region-did-change (once at
rest, not per camera tick: T312), and check the marks move with it. Test: pan/zoom under Conditions -> count changes.
The report held no layer/offset/zoom (T352).

## Done (9907af3c0, 2026-11-04)
- **The legend's count and marks (the report's actual symptom):** on the phone the visible set was asked only on a
  "fully rendered" event, which a settle over tiles the map already holds need not fire, so the count kept the old
  view's. `Map.native.tsx`: a settle with no full render within 700 ms asks itself; still one query per camera move
  (adapter-contract test, mutation-checked). Web asks on `idle`, which fires after every move.
- **New feed → map:** already wired by the owner's 10-29 feed-version work: the update service's `apply` (`feed.offer`)
  then `onFeedChange` bumps the sync `epoch` (gauges; DFO through `feedMoved`; in-season its own epoch), and
  `useConditionsColour` re-asks on it (new index → new values table → relabel). Now pinned by a mounted test
  (`conditionsColour.render.test.tsx`: +1 day blank, the forecast arrives, coloured in place). The web has no update
  service: it re-reads the index on the 5-minute poll (FEED_TTL_MS). The 11-02 "stayed blank" could not be reproduced in
  code; if it recurs on 0.5.0, the report's new map/horizon fields (T352) will say where.
- T347 (the weekend run dropped from the feed) was the other cause of a blank forecast map: fixed with it.
