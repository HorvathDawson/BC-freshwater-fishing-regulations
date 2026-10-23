# T85 — Native install size: atlas z13 overzoom and the basemap landuse filter (D32)

Status: OPEN.

Source: decisions D32 and app-data/QUESTIONS.md: about −345 MB per native install, "scheduled before the first public
native release". T64 names only parcels, basins and "S2 easy wins".

## Work
Measure both changes; decide; build them in the same tile build as T64 if they fit (one tile build). Never replace the
tiles dir: promote file by file. Tiles and bundle share a vintage.

## Acceptance
Size table before/after; no visible loss at z13+ (shots, including the overzoom corner-cutting noted in
implement_progress).

Depends: T64 (same window). Heavy, scripts/heavy.sh. Size: M. Owner: no.
