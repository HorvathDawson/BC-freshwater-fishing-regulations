# T180 — Move the remaining component copy into core words (from T60)

Status: OPEN. Source: T60's data-vs-UI gate (`app/tools/data-vs-ui.test.ts`) froze ~105 hand-written strings still typed
in components as `KNOWN_COPY` (a list that may only shrink).

Where: App.tsx attribution lines (duplicate `sourcesVM` publishers), SpotScreen (36), SpotCapture (13), SearchSheet (8),
GaugeTrace (7), Hydrograph (6), Chrome (3), WaterScreen/WaterViews (4), Sheet Done/Close, Credits "Sources",
FishSpinner "Loading", SpotsScreen "NO PHOTO".

Work: one words table per screen in core (beside the existing words modules); move each string, delete its KNOWN_COPY
line in the same commit; the duplicated attribution reads `sourcesVM`. NO wording changes.

Acceptance: KNOWN_COPY empty (or each remaining line says why it must stay); strict check green.
Size: M. Owner: no.
