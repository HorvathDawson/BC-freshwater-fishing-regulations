# Bathymetry — PREPARED, NOT FED

**Status (owner, 2026-10-22):** kept on purpose. Nothing feeds it and no build stage runs it yet; it comes back
with the depth-chart feature. Do not delete it as dead code — tidy it. **T634 (owner 2026-11-11)** restarted the data
half: first pass below, nothing shipped.

What it is: a survey sheet's waterbody identifier -> the lake it is a chart of. The CODE picks the lake (FWA's
`WATERBODY_KEY_GROUP_CODE_50K`); names, position and size only confirm or refuse it. A name never picks a lake.

## Files

| file | does | runs |
|---|---|---|
| `propose.py` | chart id -> lake PROPOSALS under the owner's rules (rules and buckets in its docstring); writes `GENERATED.base/bathymetry/proposals.json` | `scripts/heavy.sh … -- python -m pipeline.bathymetry.propose` (reads the gpkg, minutes) |
| `accept.py` | the ONLY writer of the frozen file `data/curated/bathymetry/matches.json` (created by the first accept); never rewrites an accepted row, never guesses a lake | the owner: `python -m pipeline.bathymetry.accept --by … --ids …` |
| `fetch_vectors.py` | fetches the 2001 vector depth zips (FIDQ, beside the PDFs) to a directory OUTSIDE git: sequential, resumable, stops on a refusal | `python -m pipeline.bathymetry.fetch_vectors --out ~/data-offgit/bc-bathymetry-vectors` |
| `e00.py` | reader for those zips (ArcInfo export, uncompressed `EXP 0`): arcs + `CONTOUR`, soundings + `DEPTH`, META; units only where the file says "depths in …" | library |
| `place.py` | the sheet's shore (FCODE GA2711…, inlet gaps closed) and a RIGID fit (turn + shift, never a stretch) onto our lake outline, with IoU, area ratio and shore distances | library |

Tests: `pipeline/tests/test_bathymetry_t634.py` (synthetic inputs only; no Province file is in the repo).

The matcher reuses `pipeline.deliver.lake_facts.core` (the `bind` normaliser) and the two-name test of the deleted
`generate/audit.py` (`git show 971f40575^:pipeline/bathymetry/generate/audit.py`): the printed title is the one name on a
chart not made from the code. Curated name variants (`CURATED.waters.name_variants`) count only when their source is not
`bathymetry` / `stocking` (those were written by the v1 matchers from these very rows). Its last output,
`data/generated/bathymetry/audit.json`, is still on this Mac with no generator: a list to compare against, nothing more.

## Licence (T634, catalogue records read 2026-11-10)

Only the open table (`Bathymetric Maps - Open`, the PDFs) is Open Government Licence. The sheet layer, the placed
contours (28 lakes) and the Cariboo line work are "Access Only". The vector zips have no record of their own: they are
linked only from the Access Only sheet layer. Nothing derived from an Access Only record ships until the owner decides.

## What it needs before it is fed

1. **Sources.** `data/source/bc_bathymetry_sheets.json` (`fetch_data.fetch_bathymetry_sheets`) and the WSA reference
   CSV / PDFs (`fetch_data.fetch_bathymetry_pdfs`); the vector zips (above, outside git).
2. **The owner's accepts** into the frozen file; then `config.yaml` `curated.bathymetry.matches` names it (null today)
   and `pipeline/tools/audit/check_curated.py` stops reporting it `not built`.
3. **A bundle writer** for the waiting `chart` table (`pipeline/deliver/bundle/schema.sql`; empty, named by
   `cov.skip` in `bundle/build.py`). Keep it: `app/tools/bundle-schema.test.ts` requires it.
4. **The release:** depth values ride in `lakes/1` (`pipeline/deliver/lake_facts.py`), the one landing point.
