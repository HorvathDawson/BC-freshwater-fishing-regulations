# T154 — A dams layer so "a widening behind a dam reads as a lake" can be decided by data

Status: OPEN

Source: T148 (R2 could not fire on the owner's three "dammed area" marks — Quinsam, South Nanaimo, Bonaparte — because no
dam or obstruction dataset is in the repo; the waterfalls layer is OSM falls only). 2026-10-23.

## Work
Fetch the provincial dams layer (BC Data Catalogue, e.g. WRIS dams public view) into data/source with its licence noted;
match dams to FWA lines/polygons; re-run the T148 revise with R2 using it and report how many candidates move and whether
the three owner marks are caught. No app change until the owner reads the result.
