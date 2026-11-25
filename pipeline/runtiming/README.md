# pipeline/runtiming: when each salmon and steelhead run is in fresh water

Run timing reaches the app as its own FEED (not the release): `feed/` (base, build, publish; the data contract is its `__init__.py`), run by
`.github/workflows/feeds-runtiming.yml` and read by the Map tab's Run timing mode from R2 `runtiming/`. Sources: the Pacific Salmon Foundation's
Pacific Salmon Explorer run-timing dataset (river entry, used with the PSF's written permission) and DFO's NuSEDS spawner surveys (in the stream,
Open Government Licence - Canada).

- Entry: `python -m pipeline.runtiming.fetch.explorer` (3 PSF pages), `.fetch.boundaries` (CU vector tiles), `.fetch.nuseds`, then
  `python -m pipeline.runtiming.parse` (seconds, no atlas). Those are the local test bed's path only (HTTP).
- Reads/writes: `data/source/runtiming/` (`timing.json` is TRACKED source data: 329 PSF CU curves and the DFO records, fields explained in
  `timing.py`), `data/generated/runtiming/cu_polygons.geojson`.
- The PUBLISHED feed is built in the workflow's runner, never from this repo's files: a run started with `refresh: true` fetches PSF + NuSEDS afresh and
  keeps the sources privately at R2 `private/runtiming/src/sources.tar.gz`; every other run builds from that kept copy. A new NuSEDS is noticed weekly by
  the feeds worker, which starts the workflow. A feed shrunk by more than 10% fails the run unless `allow_shrink` gives the owner's reason.
- A reach has runs, not a run: a LIST of runs (the Skeena has summer and winter steelhead six months apart; Pink even and odd are separate slots).
  Labels come from `runs.derive_label` or NuSEDS's `RUN_TYPE`; `data/curated/runtiming/run_calls.json` is AUTHORED (labels, coverage gaps): keep it.
- Rule: never a GitHub schedule; the worker starts the workflow.
- Tests: `pipeline/tests/test_runtiming_*.py`.
