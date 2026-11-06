# T305 — A tile rebuild with unchanged inputs gets a new tile_version

Status: DONE 2026-11-06 — 4754c6f8d, 7dd2f991f; proved: two side tile builds with unchanged inputs give the same `tile_version` and byte-identical pmtiles (below).

## Problem
tippecanoe writes its own command line into the pmtiles metadata (`generator_options`). That command line includes a
RANDOM temp dir (`/var/folders/…/tiles-lowband-<random>/low.mbtiles`), so every tile build hashes to a new
`tile_version` even when every tile and layer is the same. The release id hashes the tiles, so a rebuild with nothing
changed also gets a new `data_release`. Proof: the side run reproduced r-51d80e68f056 byte for byte except this
(`r-6474d28c0448`, pmtiles +19 bytes, layers/ identical).

## Work
Use a deterministic temp path for the low band, or strip or normalise `generator_options` before the version is taken
(`pipeline/deliver/tiles/tippe.py`, `version.py`). Prove it with two tile builds into `--side` dirs: same `tile_version`.

Depends: none. Size: S. Owner: no.

## Proof 2026-11-06
Two tile builds back to back from the live atlas (unchanged between them: graph.pkl / registry.json mtimes the same before
and after), each into its own side dir (`python -m pipeline.deliver.tiles --build data/generated/atlas/full --out
~/t305-side-N/tiles --release-root ~/t305-side-N/release`, the side run's tiles command), through heavy.sh:
- build 1: 8m57s, 12.5 GB peak · build 2: 9m59s, 12.4 GB peak;
- **both `tile_version` 822db448a2851809.a4c3f4eb02bd** — the live tiles' id, and the fresh clone's (T416);
- `atlas.pmtiles` sha256 the same (95acd087642d1764…) and `layers/` identical file for file.
