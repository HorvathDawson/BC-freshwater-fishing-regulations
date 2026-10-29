"""THE RUN-TIMING FEED (T199, owner 2026-10-28): its own stream, NOT the release. Fetched, bound to the live atlas and
uploaded to R2 `runtiming/` by `.github/workflows/feeds-runtiming.yml` (or by hand, the same commands); the app reads it
lazily in the Map tab's Run timing mode. Nothing in the bundle, the release or `deliver` reads or writes it.

    # owner, on the Mac, once per atlas (after a promote: the base names the atlas's sections)
    python -m pipeline.runtiming.feed.base [--upload]              # -> data/generated/runtiming/feed/base-<handles>.json.gz
    # anywhere (the workflow does this): HTTP + the base, no atlas
    python -m pipeline.runtiming.fetch.explorer && python -m pipeline.runtiming.fetch.boundaries \\
        && python -m pipeline.runtiming.fetch.nuseds
    python -m pipeline.runtiming.parse --out <work>/timing.json --polygons <work>/cu_polygons.geojson
    python -m pipeline.runtiming.feed.build --timing <work>/timing.json --polygons <work>/cu_polygons.geojson \\
        --base <base-<handles>.json.gz> --out <work>/out
    python -m pipeline.runtiming.feed.publish --dir <work>/out --via wrangler|aws [--yes]   # staging only

THE BASE (`base.py`, `runtiming-base/1`) is the atlas reduced to what the binding reads — per section: kind, Strahler
order, length, FWA watershed code, blue line and measures, lake key, names, the sections it flows into, its water's
`item_id` and a point — for the streams of order >= 3, the lakes that drain, and every named water. Built from the
atlas's own graph and the tiles' own geometry, stamped with their `section_handles`, kept PRIVATE on R2
(`private/runtiming/base/<handles>.json.gz`: the data worker never serves `private/`). A workflow run binds against
the base whose handles the live release's tiles carry, and refuses when there is none (the owner builds it).

WHAT THE APP READS (`runtiming/`, every file but the index named by its content and immutable):

    index.json   runtiming-index/1 — the one mutable key (no-cache): {format, built_at, section_handles, tile_version,
                 sources, files: {runs|paint|waters: {url, bytes, sha256}}}. `bytes`/`sha256` are the DECODED file's
                 (the app's `fetchVerified`); files are stored gzipped and served `Content-Encoding: gzip`.
    runs-<sha12>.json     runtiming-runs/1 — every run (a PSF Conservation Unit or a DFO surveyed stream × species ×
                 run): species, slot, population / stream name, run label, peak, window, quality / survey years,
                 its months (12 bits) and pentads (73 bits) in the water, its source; the sources and credits; the
                 slots (Pink split by brood line) and the default (steelhead).
    paint-<sha12>.json    runtiming-paint/1 — the PAINT INDEX, against ONE atlas (`section_handles`): profiles (each a
                 set of run x relation, with its species bitmask and per slot the months local / passing) and per
                 section (sid ascending, delta coded) its profile and tens of km to the sea. The app labels each
                 section from it with no join; it paints nothing when the handles are not the tiles'.
    waters-<sha12>.json   runtiming-waters/1 — the same binding with NO section ids: every bound reach as its
                 water's `item_id` (null for an unnamed stream) + extent (a stream: its FWA blue line and a measure
                 range; a lake: its waterbody key) + its profile (the run x relation set; `run_ids` names the runs).
                 Durable across atlas rebuilds; for any consumer that is not the map (and checking by eye).

THE RELATIONS (`bind.py`, measured on the 2026-10-24 prototype, handoff/PLAN/runtiming/PLAN.md): `spawning` (PSF: in
the CU polygon, order >= 3 or a draining lake), `passage` (PSF: the flow walked down to the sea, never into another
river's code), `observed` (DFO: the surveyed stream's own named sections; replaces that species' PSF spawning there).
PSF dates are RIVER ENTRY: no travel time is invented; `km` says how far up the reader stands.
"""
