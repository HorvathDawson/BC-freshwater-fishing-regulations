# T173 — A fish obstacles layer (falls, dams, culverts) from the BC Data Catalogue

Status: OPEN — future (owner 2026-10-23: "a fish obstacle layer could be interesting from the bc data catalogue. but
that is a future ticket").

## Sources to evaluate
- Provincial Obstacles to Fish Passage (FISS obstacles — falls, cascades, dams, velocity barriers, with height where
  known) and the Provincial Stream Crossing Inventory (PSCIS: culvert / crossing assessments, passability) — BC Data
  Catalogue, Open Government Licence – BC; fetch via the openmaps WFS like T154's dams layer.
- Already in the repo: the T154 dams layer (WRIS) and the OSM waterfalls layer.

## Work (when it starts)
1. Fetch + licence note + a fetch script (data/fetch_data.py style); match points to FWA lines (as dams.py does).
2. Decide what it is FOR: a map layer (obstacles as small marks with type/height on tap), and/or evidence for run timing
   (T162: a run cannot pass a barrier — the passage walk stops there) and for regulations ("upstream of the falls"
   locators in T164's DFO work and the synopsis).
3. App: a toggle in the layers menu; marks small and distinct from gauges and spots; tap sheet with type, height,
   source, date. Ships: release file + app. Size: M.
