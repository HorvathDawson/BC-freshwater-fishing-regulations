# added_streams: streams the FWA lacks, minted into the stream graph

Some streams the regs name are absent from the FWA stream layer (Salamander Creek, the Burnaby Lake and
Deer Lake creeks). This package turns stream lines from city layers, OpenStreetMap or a hand drawing into
real graph nodes with their own geometry, each joined to the FWA network by a flow edge. An added stream
is nameable through `name_variants.json`, bindable by regs, and counts as a tributary of the stream it joins.

Reads: `data/source/municipal/*.geojson` (city exports, `osm_streams.geojson`, `manual_streams.geojson`;
`SOURCES.md` lists them), the FWA gpkg, DEM tiles (AWS Terrarium, cached).
Writes: `data/curated/waters/added_streams.json`, the one frozen artifact the atlas build reads, plus
reports under `data/generated/added_streams/`. Entry points: `build_dataset.py` (mint), `mapcheck.py`
(check maps), `harness.py` (standalone demo), `fetch_osm.py` (the only network step, human-run).

```
fetch_osm / hand-drawn line --> data/source/municipal/*.geojson --clean--> merge --> fwa_match --> resolve
                                                                           --> mint --> validate --> data/curated/waters/added_streams.json
atlas build: drop fwa_exclude fids, add the artifact's fids, attach_connectors, feed name_variants
```

## Commands

```
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.atlas.waters.added_streams.fetch_osm --bbox W S E N   # Overpass
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.atlas.waters.added_streams.harness                     # standalone check
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.atlas.waters.added_streams.build_dataset burnaby squamish port_moody
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.atlas.waters.added_streams.mapcheck burnaby --only package
PYTHONPATH="$PWD" .venv/bin/python -m pytest pipeline/atlas/waters/added_streams/tests -q
```

`build_dataset` resolves each source in its own bbox (a combined bbox cross-contaminates), merges with
per-source blk offsets, then the verify maps (`data/generated/added_streams/verify_<source>.html`) are
eyeballed and the artifact committed. The tests are synthetic (no gpkg, no network, no credits) and the root
`pytest.ini` `testpaths` collects them with the main suite.
`tests/data/added_streams.sample.geojson` feeds `harness.py`, not the unit tests.

## The line schema

A FeatureCollection of lon/lat `LineString`s. Per-feature properties:

| prop | meaning |
|---|---|
| `source` | `"osm"` or `"manual"` |
| `blk` | channel id (negative int); features of one continuous stream share it |
| `osm_way_id`, `name` | optional provenance and stream name |
| `connect_to` | receiver: `{gnis_id}`, `{blk}` or `{coord}`; omitted, the nearest FWA or added receiver within about 250 m |
| `wsc`, `stream_order`, `stream_magnitude`, `gnis_id`, `edge_type` | optional overrides, else minted |

## How a stream is minted

- Merge (`merge.py`): same-name touching ways, or the same explicit `blk`, are one channel, one WSC. A fork
  with another name stays a separate channel.
- Classify (`fwa_match.classify`): a line that hugs an FWA stream is a duplicate (dropped, FWA geometry wins);
  one genuinely longer is an extension (only the novel tail, on the FWA stream's own blk and WSC);
  anything else is novel. A differently named line that only partly hugs an FWA line stays novel.
- WSC (`wsc.py`): receiver's WSC plus a 6-digit proportional-distance segment, assigned from the FWA root
  outward, so every added stream prefix-descends the stream it joins. The true length of a partly loaded
  river is recovered from its coded FWA children. No two different streams share a code (`_bump_wsc`);
  same-name fragments of one creek do.
- Order and magnitude: computed over the added network (Shreve sum, Strahler fold), not defaulted. The
  ripple onto FWA nodes downstream is not applied.
- Same-name tributary: a same-name piece joining its mainstem partway (under `_CONTINUATION_FRAC`, 85 %)
  is a distinct tributary named `<mainstem> Trib.N`; one joining at the source continues the mainstem.
- Ingest (`ingest.py`): each channel becomes an FWA-shaped `FidRow` and `BlkChain` (edge type `1000`, never
  a barrier) plus a `ConnectorSpec`; `attach_connectors` adds the flow edge (`connector` for a gap,
  `confluence` for a shared vertex). The synthetic rows go through the same `build_stream_graph` and
  `build_section_geometries` as FWA data.
- Validate (`validate.py`) blocks the write unless every WSC prefix-descends its receiver and every
  chain roots at a real FWA drainage or a 900 tidal root (`coastal.py`).
- Streams with no receiver in tolerance are reported, never guessed.

## DEM flow and the resolver

`dem.py::dem_flow` orients each channel by ground elevation. Its steps, in order: clip a line drawn past the
river it drains into; split at T-junctions; build the node graph (a loop closing two headwaters is
dropped); give an approved lake its FWA or tidal outflow, and drain every mouth within 40 m of its shore
into it; BFS the flow from every node that reaches a river, so each creek flows to its own nearest contact.
`build_dataset._resolve_topology` derives receivers from that tree: `dem_outlet` is checked before
`dem_down`; a tributary joins the stream its mouth touches; a novel the DEM eliminated is not minted;
`_anchor` lets tidal win over a 900 coastal reach and an inland 100 route win over tidal.
A lake's outlet runs through the lake as a central spine (`lake_through_spine`), so its inlets attach at
distinct measures and the under-lake span carries the lake `wbk`.

`build_dataset` also prunes a leaf tributary of 50 m or less (`_SHORT_TRIB_M`) and trims a mouth that
overshoots its receiver (`_OVERSHOOT_TOL`, 40 m). `RELIABLE_SOURCES` lists sources trusted without DEM
orientation (`manual`). Per-source calls live in `data/curated/waters/added_stream_calls.json`
(`clean.calls()`: `exclude_names`, `exclude_src_ids`); `FWA_EXCLUDE_BY_SOURCE` and
`APPROVED_LAKE_NAMES_BY_SOURCE` are in `build_dataset.py`. The regressions for each rule above are in
`tests/test_build.py` and `tests/test_dem.py`.

## What the atlas build consumes

City layers add to FWA and never replace it: `fwa_exclude` is empty, and the build refuses a non-empty
one outside `pipeline.atlas.build.OWNER_APPROVED_FWA_EXCLUDE`. A city line that matches an FWA line is a
name label on it (a `duplicate` name variant). Only a named, real city stream with no FWA equivalent is
minted (`build_dataset.addable`: unnamed, "(Unnamed)", drainage-typed and drainage-named pieces are not).
A re-freeze keeps every blk it can (`_keep_previous_blks`: same source, name and src_id), since splits,
overrides and gauge matches name them. A side channel the owner names (`SIDE_CHANNELS`, today Northrop
Channel from the Stave River) carries edge type 1350 and an `intake`; `pipeline/tests/test_northrop_side_channel.py` pins it.

The artifact's keys: `streams` (blk, wsc, name, `segments` with under-lake `wbk`, `connector`, order,
magnitude, receiver, `base_measure`), `fwa_exclude` (WSC prefixes to drop), `name_variants` (`added`,
`duplicate`, `excluded_fwa`). `atlas/build.py` reads it on every build (`--no-added-streams` skips it,
`--added-streams PATH` points elsewhere): it drops the excluded fids, appends the added ones, attaches
connectors by measure (a node id is `{blk}:{down_m}`), and passes the name variants to `apply_name_variants`.
A bbox build keeps in-bbox streams plus their added-receiver chain (`_streams_in_bbox`).

## Open

About 4 Burnaby streams stay unresolved (one `Eagle Trib.3` sub-branch); `nonlinear_blks` may flag a
source-data bowtie (Dallas 393), reported not raised.
