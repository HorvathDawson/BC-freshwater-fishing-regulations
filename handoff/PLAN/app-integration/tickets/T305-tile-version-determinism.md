# T305 — A tile rebuild with unchanged inputs gets a new tile_version

Status: OPEN (found in the 2026-10-31 build pass, `build-pass-2026-10-31.md`).

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
