# needs-pin resolver (24 entries / 28 todo rows)

Offline (FWA-only, no live OSM) resolver + review queue for the last remaining
`status=='todo'` rows in `waterbody-splits.json`. **Nothing here writes curation** —
it produces a review queue for a human to pick/adjust pins, then a follow-up apply step.

## Files
- `fwa_helpers.py` — FWA geometry helpers (EPSG:3005). Key angles:
  - `mainstem(name, mus)` → river line ordered **mouth→source** by `DOWNSTREAM_ROUTE_MEASURE`.
    Distance-from-start == metres upstream of the FWA mouth. **The FWA mouth (measure 0) is
    used as the tidal-boundary reference** (per user: "use last node of stream as default tidal").
  - `walk_from(line, 0, km)` → point `km` upstream of the mouth (for "N km above tidal / from mouth").
  - `fsr_crossings(line)` → Forest-Service-Road bridges crossing the river (ordered by km up).
  - `confluence(line, trib, mus)` → nearest point of a named tributary.
  - `falls_near(line)` → FWA `waterfalls` within 250 m.
  - MU-clip disambiguation via `wmu` polygon bbox (`mu_bbox`).
  - `overpass()/osm_road_cross()` exist but Overpass was **flaky/timeouts in this env** — the
    queue was built offline. Only 2 OSM hits were usable (Capilano footbridge, Hays Cove Circle).
- `generate_pins.py` → writes `offline_pins.json` (`{rid: {method, pins:[[label,[lat,lon]|None]]}}`).
- `build_queue.py` → writes `stream_sections/docs/curation-review-queue.md` (bucketed A–E).
- `offline_pins.json` → generated pin set (checked in so the queue is reproducible without a re-run).

## Reproduce
```
PYTHONPATH=stream_sections/oneoff/needspin .venv/bin/python stream_sections/oneoff/needspin/generate_pins.py
PYTHONPATH=.                               .venv/bin/python stream_sections/oneoff/needspin/build_queue.py
```

## Confidence buckets (in the queue)
- **A. STRONG** — named FWA/FSR match, high confidence: Copper (South Bay Main), Eve (JOINT
  SOUTHMAIN = South Main Br), Artlish (2 FSR bracket 10 km), Mamin (walk-10 km), Mohun (MENZMN.2).
- **B. SPRINGBOARD (correct FWA channel)** — pin is on the right river at the right distance;
  eyeball the exact bridge/pool. Deena, N. Alouette@216, Elk-Coal@7 km, Chowade (sole crossing),
  Kitimat Hwy37, Ptarmigan, Chuckwalla (10 mi), Pinkut, Swift, Thorn, Cranberry, Thompson-Martel, Kitimat-hatchery.
- **C. TOWNSITE/landmark (FWA name/MU miss)** — FWA lookup wrong/absent; pin is a townsite/OSM
  anchor to eyeball. Burton (Hwy 6 @ Woden), Trepanier (97C @ Peachland), Capilano (footbridge),
  Hays (culvert), Ksi X'anmas (remote — needs topo).
- **D. NEEDS FISS/topo** — no offline feature. White River salmon-viewing pool, Ptarmigan falls.
- **E. LIKELY n/a** — Skeena "Section 4" bait-ban applies to the whole (already-bounded) reach; propose n/a.

## After the human picks pins
Apply with the usual `load_curation()`/`save_curation()` pattern (see prior `apply_*.py` in
scratchpad / the curation-handoff), set `reviewed` on entries with no remaining `todo`.
5 rows still have **no coord** and need topo/gazetteer: `thorn-...-b` (500 m u/s), `cranberry-...-b`
(canyon d/s sign), `thompson-...-1c9ce3` (Martel locality), `ksi-x-anmas` (remote coastal),
`skeena-...-c2e0df` (→ n/a).
