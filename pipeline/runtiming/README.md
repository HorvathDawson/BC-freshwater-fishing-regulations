# Run timing — FED AS ITS OWN STREAM (T162 parse, T199 feed 2026-10-28)

When each salmon and steelhead run is in fresh water. T199 (owner 2026-10-28): it reaches the app as ITS OWN FEED,
not the release — `pipeline/runtiming/feed/` (base → build → publish; the data contract is its `__init__.py`), run by
`.github/workflows/feeds-runtiming.yml`, read by the Map tab's Run timing mode from R2 `runtiming/`.
`handoff/PLAN/runtiming/PLAN.md` (T162) is the binding's history; its release file `runs/1` is superseded.

## Sources

| | what | dated at | terms | credit |
|---|---|---|---|---|
| PSF | dataset #90, *Average Run Timing of Salmon and Steelhead Conservation Units*, from the Pacific Salmon Explorer's saved run-timing page | river entry | PSF's written permission (2026-10-24; their reply is not stored or quoted) | Pacific Salmon Foundation (Salmon Watersheds Program) |
| DFO | NuSEDS spawner surveys (arrival, start / peak / end of spawning, per stream and year) | in the stream | Open Government Licence – Canada | Fisheries and Oceans Canada |

## The path

THE PUBLISHED FEED is built in the workflow's runner, never from this repo's files (T412, T485): a run started with
`refresh: true` (or when no copy is kept yet) fetches PSF + NuSEDS afresh, parses and builds, and only after the floor
check passes keeps the sources PRIVATELY at R2 `private/runtiming/src/sources.tar.gz` (T552); every other run (a release's,
the button's without refresh) builds from that kept copy and makes no outside request. A new NuSEDS is noticed weekly by
the feeds worker, which starts the workflow with `refresh: true` and `expect_dfo` (the run fails if its NuSEDS is still
older). A collapsed feed (PSF runs, DFO runs or painted sections down more than 10% against the live one) fails the run
unless `allow_shrink` gives the owner's reason.

THE LOCAL PATH below is for the test bed (`feed.release --local` binds the tracked `timing.json`). Nothing compares the
tracked file with the kept copy: after a `refresh` run, the live feed's `runtiming/index.json` `sources.dfo.updated` /
`sources.psf.updated` can be newer than `timing.json`'s `sources` — re-run the path below to bring the test bed level.

```bash
# by hand, for the local test bed (HTTP only)
python -m pipeline.runtiming.fetch.explorer        # 3 PSF pages -> data/source/runtiming/
python -m pipeline.runtiming.fetch.boundaries      # CU vector tiles -> data/source/runtiming/cu_tiles/
python -m pipeline.runtiming.fetch.nuseds          # NuSEDS + census sites -> data/source/runtiming/dfo/
# seconds, no atlas
python -m pipeline.runtiming.parse                 # -> data/source/runtiming/timing.json (TRACKED; source data, T331)
                                                   #  + data/generated/runtiming/cu_polygons.geojson
```

Q163: "we don't need to save PSF pages, just the data parsed". AS IT STANDS (2026-11-10) the three PSF pages
(`run-timing.html`, `biological-status.html`, `catch-and-run-size.html`) and `fetched.json` under `data/source/runtiming/`
ARE still tracked beside `timing.json`; the CU tiles and NuSEDS are not. Whether to untrack the pages is the owner's call
(T552 item 8). `timing.json` holds
329 PSF CU curves (scaled to their own peak, 0..100 per pentad) and 4,944 DFO records (per site, species, run type and
pink brood line: the percent of the latest ≤25 dated survey years the fish were in the stream, per pentad; ≥3 years;
DFO's "Stream Aggregate" sites are not places and give none). `pipeline/runtiming/timing.py` says what each field is.

## A reach has runs, not a run

The Skeena carries a summer and a winter steelhead run six months apart, so a reach holds a LIST of runs; Pink (even)
and Pink (odd) are separate slots (they never share a year). Run labels come from the CU's name (`runs.derive_label`)
or NuSEDS's `RUN_TYPE`; `data/curated/runtiming/run_calls.json` is AUTHORED (labels, coverage gaps): keep it.

## Older code (deleted, T436)

The 2026-09-08 frozen-index path (`generate/{index,passage,profiles,indicators,crosswalk}.py`, `consume/bundle.py`) had
no importer once the feed replaced it, and was deleted (git keeps it). `generate/extract.py`'s `runs_from_page` stays:
it is what `parse` reads the PSF page with.
