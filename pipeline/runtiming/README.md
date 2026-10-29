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

```bash
# about once a year, owner-run (HTTP only; every fetched file is gitignored)
python -m pipeline.runtiming.fetch.explorer        # 3 PSF pages -> data/source/runtiming/
python -m pipeline.runtiming.fetch.boundaries      # CU vector tiles -> data/source/runtiming/cu_tiles/
python -m pipeline.runtiming.fetch.nuseds          # NuSEDS + census sites -> data/source/runtiming/dfo/
# seconds, no atlas
python -m pipeline.runtiming.parse                 # -> data/curated/runtiming/timing.json (TRACKED)
                                                   #  + data/generated/runtiming/cu_polygons.geojson
```

Only the parsed data is tracked (Q163: "we don't need to save PSF pages, just the data parsed"). `timing.json` holds
329 PSF CU curves (scaled to their own peak, 0..100 per pentad) and 4,944 DFO records (per site, species, run type and
pink brood line: the percent of the latest ≤25 dated survey years the fish were in the stream, per pentad; ≥3 years;
DFO's "Stream Aggregate" sites are not places and give none). `pipeline/runtiming/timing.py` says what each field is.

## A reach has runs, not a run

The Skeena carries a summer and a winter steelhead run six months apart, so a reach holds a LIST of runs; Pink (even)
and Pink (odd) are separate slots (they never share a year). Run labels come from the CU's name (`runs.derive_label`)
or NuSEDS's `RUN_TYPE`; `data/curated/runtiming/review.json` is AUTHORED (labels, coverage gaps): keep it.

## Older code kept

`generate/` (`index`, `passage`, `profiles`) and `consume/bundle.py` are the 2026-09-08 frozen-index path. The plan
replaces it with a binding made at delivery (section ids moved under the frozen index); `generate/extract.py`'s
`runs_from_page` is what `parse` reads the PSF page with.
