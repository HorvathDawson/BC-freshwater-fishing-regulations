# T212 — tidal boundaries: a tidal split on every stream that enters the ocean (research first, no data changes yet)

Status: PARTIAL (41393956 — research done, proposals in the review Tidal tab; no curated data changed)

Source: owner 2026-10-27 — "critical"; start in the next work window.

## Goal
Find the tidal boundary (where freshwater regulation ends and tidal/DFO tidal rules begin) on every stream that enters the
ocean — at least every NAMED one — as candidate splits. Every ocean stream's mouth is labelled as tidal (e.g.
"mouth (tidal boundary)", or a second name/alias "tidal" on the split). Nothing is changed in curated data yet: this
ticket FINDS them all and proposes them, with confidence and sources, for the owner to accept later.

## Inputs to cross-reference
- The owner's earlier DFO tidal polygon: a .gpkg or .geojson holding ONE polygon of the DFO (tidal) boundary, which the
  owner EDITED by hand to match tidal boundaries found on streams. Not found by a quick search on 2026-10-27 (only
  data/source/bc_boundary.geojson and data/curated/gauges/tidal.json turned up). Search thoroughly: the whole repo incl.
  untracked/ignored files (`git status --ignored`, data/source, data/scratch, handoff/), git history of DELETED files
  (`git log --all --diff-filter=D --name-only -- '*.gpkg' '*.geojson'`), and the owner's ~/Downloads, ~/Desktop,
  ~/Documents (read-only). Also check pipeline/atlas/waters/added_streams/coastal.py and the reach run's `tidal`
  diagnostics for what tidal source the build reads today. If it cannot be found, ask the owner for the path.
  FOUND (owner 2026-10-27: "it is fetched from R2"): the `tidal_boundary` layer of data/source/bc_fisheries_data.gpkg
  (the build gpkg from the R2 build-assets bucket), built by data/build_tidal_boundary.py + process_tidal_boundary.py
  (last in git at 5c3319c0) from DFO PFMA subareas (`dfodfooy__dfo_bc_pfma_subareas_chs_v3_gshp`); see
  pipeline/regs/dfo_salmon/DATAFLOW.md (~line 440). The owner's hand edits live in that layer.
- Tidal-boundary text in the DFO salmon notices and the synopsis (e.g. "tidal waters", "tidal boundary signs", "below
  the CPR bridge"), and the tidal limits already curated (Fraser below Mission, Q42).
- Web search per stream where the above disagree or are silent (DFO tidal boundary descriptions, Fisheries Act
  schedules, maps).

## Output
- A candidate list: stream, proposed tidal split location (lat/lon, on the FWA line), source(s), confidence
  (high/medium/low), how the polygon and the text agree or disagree — as JSON + a short .md summary with counts.
- Shown in the curation review (like the DFO suggestions) for the owner to accept; accepted ones become curated splits
  labelled/aliased as tidal.
- Coverage: named ocean-entering streams found / total.

## Outcome (2026-10-27)
- Tool: `python -m pipeline.tools.audit.tidal_candidates --gpkg <bc_fisheries_data.gpkg> --atlas <atlas/full>
  --basemap data/source/bc.pmtiles --out handoff/PLAN/curation-review/tidal` (read only, ~2 min).
- Output: `handoff/PLAN/curation-review/tidal/tidal-candidates.json` (1,163 rows) + `tidal-candidates.md`.
  Coverage: 916 / 966 named ocean-entering streams (FWA 9xx mainstems + Fraser/Skeena/Nass) are met by the polygon;
  50 are missed (proposed at the FWA mouth, low). 197 named streams enter tidal water that is not the sea.
- Words found: the synopsis p.21 box "Tidal Boundaries on Rivers in Region 2" (16 streams), DFO tidal-area pages
  (Area 29 list + Areas 2/12/13/17/23/27), DFO salmon notices and synopsis rows, curated tidal splits, tidal gauges.
  19 streams: polygon and located words agree (within 300 m). Disagree: Capilano, Nimpkish, Copper Creek; Tlell's
  two texts differ (1 km vs 1.5 km) and the polygon misses it.
- Review: curation review **Tidal** tab (`#tidal/`), listed + on the map with the polygon's edge; "accept proposal"
  pre-fills a mark (scope `tidal`, Place y + the cut as a `NewSplit` labelled "<water>: mouth (tidal boundary)",
  alias "tidal boundary" when above the FWA mouth). The mark is the only write; T185 / T166 turn accepted marks
  into splits.
