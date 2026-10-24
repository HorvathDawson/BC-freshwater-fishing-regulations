# T180 — Move the remaining component copy into core words (from T60)

Status: DONE 71179834, b54c69c1, bc7bc016, 462f8488 (see Outcome). Source: T60's data-vs-UI gate (`app/tools/data-vs-ui.test.ts`) froze ~105 hand-written strings still typed
in components as `KNOWN_COPY` (a list that may only shrink).

Where: App.tsx attribution lines (duplicate `sourcesVM` publishers), SpotScreen (36), SpotCapture (13), SearchSheet (8),
GaugeTrace (7), Hydrograph (6), Chrome (3), WaterScreen/WaterViews (4), Sheet Done/Close, Credits "Sources",
FishSpinner "Loading", SpotsScreen "NO PHOTO".

Work: one words table per screen in core (beside the existing words modules); move each string, delete its KNOWN_COPY
line in the same commit; the duplicated attribution reads `sourcesVM`. NO wording changes.

Acceptance: KNOWN_COPY empty (or each remaining line says why it must stay); strict check green.
Size: M. Owner: no.

## Outcome

DONE 2026-10-24 (71179834, b54c69c1, bc7bc016, 462f8488). `KNOWN_COPY` in `app/tools/data-vs-ui.test.ts` went from 95
strings in 19 files to `{}`. No wording changed: each string moved verbatim, the render tests are unchanged and green,
and `packages/core/src/componentWords.test.ts` pins every line composed from parts as one whole string.

- Shared furniture: `uiWords.ts` (new) holds `SHEET_WORDS` (Done / Close), `CREDITS_WORDS` and `SPINNER_WORDS`. The
  water screen's words are `WATER_SCREEN_WORDS` (waterViews.ts). The dev hint is `INSTALL_WORDS.serve`.
- App.tsx's attribution is now `MAP_CREDITS` (menu/sources.ts, beside `sourcesVM`). It cannot be `sourcesVM`'s `from`
  lines as they stand, because those are worded differently ("© OpenStreetMap contributors. © Protomaps." against
  "Basemap © OpenStreetMap contributors · © Protomaps") and the brief allowed no wording changes. Instead both read
  one `PUBLISHERS` table (menu/words.ts), so each publisher's name is written once. The Province's forecast disclaimer
  stays one whole literal: it is required verbatim, and `pipeline/gauges/feed/forecast.py` holds the same string.
- Search: `SEARCH_BAR` took the sheet's words (pinned whole in search.test.ts).
- Conditions: `FLOW_LEGEND.near` / `.scale` (the ramp's screen-reader lines), `CONDITIONS_WORDS.hydrograph`, and
  `TRACE_WORDS` (trace.ts).
- Spots: `SPOT_WORDS`, `SPOT_CAPTURE_WORDS`, `SPOT_CONFIRM_WORDS`, `SPOT_RECORD_WORDS`, `SPOTS_LIST_WORDS`, and the
  weather window's row names in `SPOT_WINDOW` (spotWords.ts).

Not moved: `packages/map/src/gaugeDots.ts` still has "Hydrometric data: Environment and Climate Change Canada" typed
in as a GeoJSON source attribution. The scan does not cover map (it is not a drawing root), and this string was not on
the frozen list.
