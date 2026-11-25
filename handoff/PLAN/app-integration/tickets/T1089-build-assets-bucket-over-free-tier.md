# T1089 — The build-assets bucket is over R2's 10 GB free tier: store the big source file compressed, and write down how to refresh it

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1089 | The build-assets bucket is over R2's 10 GB free tier: store the big source file compressed, and a runbook for refreshing the sources | OPEN — 0.10.1, or the first free lane before it | | owner 2026-11-24 |

Source: owner 2026-11-24 (planning chat): the `bc-fishing-build-assets` bucket "has too much data in it puts me
beyond 10gb total free threshold. can we clean it up?" Offered four ways; his answer, verbatim: "yes compress it then
we should have somethign mention hopw to rebuild it to update every so often too."

Measured 2026-11-24 (read-only listing): 12 objects, 11.49 GiB.

| Object | Size | Pinned in `data/source/pins.json`? |
|---|---|---|
| `source/9b35a8a7f394e6c8/bc_fisheries_data.gpkg` (2026-09-15) | 9,746 MB | yes |
| `source/a2c84de258187fb9/hydat.sqlite3` | 1,242 MB | yes |
| `DFO_TIDAL_BOUNDARY.gpkg` (2026-03-10) | 531 MB | no, but `data/fetch_data.py` downloads it |
| `source/6960ed933a82f6f6/basemap.pmtiles` | 215 MB | yes |
| `source/73befc72a74a0877/fish_obstacles.gpkg` | 31 MB | yes |
| `source/…/synopsis_raw_data.json`, `source/…/row_boxes.json` | under 1 MB | yes |
| `parsing/` (4 files, July), `extraction/synopsis_raw_data.json` (July) | 5 MB | no: leftovers |

So the bucket is live build input, not junk; one file is 85% of it.

## Work
1. Compressed pins in `pipeline/tools/source_pins.py`: `pin` uploads a compressed copy (zstd, or what measures
   best), `fetch` downloads and unpacks, and `check` still verifies the sha256 of the UNPACKED file, so a pin stays
   the same bytes the build read. Measure first and write the sizes here: the GeoPackage, `hydat.sqlite3` and
   `DFO_TIDAL_BOUNDARY.gpkg`. Target: the bucket well under 10 GB with room for the next version.
2. Re-pin the three big files compressed; prove a fresh `fetch` + `check` in a clean directory; only then delete the
   uncompressed objects (the owner confirms the delete, or does it). Delete the July `parsing/` and `extraction/`
   leftovers after checking nothing reads them (the pinned copies under `source/` replace them).
3. `DFO_TIDAL_BOUNDARY.gpkg` becomes a pin like the others (today it sits at the bucket root, unversioned).
4. One old version at most stays in the bucket: `pin` lists objects no pin names and says what to delete.
5. A runbook, `handoff/PLAN/runbooks/` (owner: "how to rebuild it to update every so often"): how to refresh
   each source from upstream (`data/fetch_data.py`, the HYDAT release, the tidal boundary scripts), how to tell it
   changed, pin it again, what must be rebuilt afterwards (atlas, tiles, gauges) and which checks prove the new
   version, and how often (a suggested cadence per source; HYDAT is quarterly). A line in the release runbook points
   at it.

## Acceptance
The bucket's size before and after is in the ticket and is under 10 GB; a fresh clone's `fetch` + `check` passes; the
runbook exists and the docs guard reads its paths; `bc.pmtiles` (pinned at 4,158 MB with a production-bucket URL,
not in this bucket) is noted as counted against the same account total.

Depends: none. Severity: low to medium (a few cents a month over the free tier today; it grows with each new pinned
version). Release: 0.9.2, or the first free lane before it. Model: Opus (pins are what builds trust), Sonnet for the
runbook. Size: S–M. Owner: yes (confirms the delete of the old objects).
