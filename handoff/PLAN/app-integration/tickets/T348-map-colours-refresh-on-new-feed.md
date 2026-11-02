# T348 — Map colours don't refresh when a new feed arrives (river forecast came in, map stayed blank)

Status: OPEN — bug (owner 2026-11-02: "when new feeds come in we need the map colors to update if they have new info.
We got the new river forecast in but the colors didn't update and still showed blank").

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
different forecast then back to the option you were on (change -> +5 -> change)." So colours also fail to repaint on
zoom/pan under Conditions, and only a forecast-offset toggle redraws them. Same repaint path as this ticket; check it too.
The report held no layer/offset/zoom (T352).
