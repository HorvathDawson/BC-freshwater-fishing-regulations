# pipeline/bathymetry: PREPARED, NOT FED

Owner 2026-10-22: kept on purpose; no build stage runs it, nothing feeds it yet (it returns with the depth charts). Do not delete it as dead
code. It maps a sheet's waterbody id to its lake: the CODE picks the lake (FWA's `WATERBODY_KEY_GROUP_CODE_50K`); names, position and size only confirm or refuse; a name never picks a lake.

| file | does |
|---|---|
| `propose.py` | chart id to lake PROPOSALS under the owner's rules, to `GENERATED.bathymetry/proposals.json` (`scripts/heavy.sh ... -- python -m pipeline.bathymetry.propose`) |
| `accept.py` | the ONLY writer of the frozen `data/curated/bathymetry/matches.json`; owner-only; never rewrites an accepted row |
| `fetch_vectors.py` (`python -m pipeline.bathymetry.fetch_vectors --out DIR`) | fetches the 2001 vector depth zips to a directory OUTSIDE git (sequential, resumable) |
| `e00.py`, `place.py`, `place_more.py` (`python -m pipeline.bathymetry.place_more`) | the zips' reader, the rigid fit onto our lake outline, every zip placed by program and graded |
| `refit.py`, `refit_all.py` (`python -m pipeline.bathymetry.refit_all --vec-dir DIR`) | T1188: the wider fitter on every loose or poor placing (every turn and the mirror, a two-lake union, a wrong-lake code by shape, a stretch of 5 % or less as its own flagged class; a bigger stretch only listed; a found lake must be free), then on every placed sheet the best neighbouring lake, the shape's complexity and a `confidence` (high / high_stretched / review: `high` is the repo's own strict placing on its coded lake, unstretched, that nothing flags: no rival turn, mirror or neighbour, not a simple shore), to `GENERATED.bathymetry/refit.json`, `.csv`, `_shapes.json`; `--resummarise` redoes the counts from refit.json |
| `contours.py`, `compress.py` | the placed contours (prototype), chart PDFs to 200 dpi 1-bit (outside git) |

- Reads: `data/source/bc_bathymetry_sheets.json`, `data/source/wsa_bathymetry_maps.csv`. Writes proposals under `data/generated/`; chart zips and PDFs outside git.
- Licence: only the open table (the chart PDFs) is Open Government Licence; the sheet layer, placed contours and vector zips are "Access Only". The owner
  said build on them (2026-11-11) and owes the licence link: every artefact carries the unconfirmed-licence flag; nothing ships until confirmed.
- To be fed it needs the owner's accepts, `CURATED.bathymetry.matches` written, a bundle writer for the waiting `chart` table
  (`pipeline/deliver/bundle/schema.sql`; keep it, `app/tools/bundle-schema.test.ts` requires it) and lake values in `lakes/1` (`deliver/lake_facts.py`).
- Tests: `pipeline/tests/test_bathymetry_t634.py`, `test_bathymetry_t1188.py` (synthetic inputs only); `test_doc_drift.py` keeps any `generate/<file>.py` row listed here real.
