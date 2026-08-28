# pipeline/added_streams — put non-FWA streams into the stream graph

Some streams referenced by BC fishing regs (and local maps) are **absent from the FWA** stream
layer — e.g. Salamander Creek, and a cluster of Burnaby Lake / Deer Lake creeks. This package ingests
any such stream line — **fetched from OSM or hand-drawn** — as a real graph node with its own
geometry, joined to the FWA network by a **`connector`** flow edge. Once in the graph the added
stream is named (via `name_variants.json`), bindable by regs, and — being a real flow edge — counts
as a tributary of the stream it joins.

Source-neutral: the ingest only cares about a curated GeoJSON of stream lines. OSM is one *populator*
(`fetch_osm`); a hand-drawn LineString is another. Both land in the same file and take the same path.

## Workflow

```
fetch_osm (Overpass, human-run)  ─┐
                                  ├─>  pipeline/added_streams.geojson  ──ingest──> graph nodes + connectors
hand-drawn LineString  ──────────┘        (curated, allowlisted)
```

1. **(optional) Fetch OSM candidates over an area** — human-run, hits Overpass (external network):
   ```
   PYTHONPATH="$PWD" .venv/bin/python -m pipeline.added_streams.fetch_osm \
       --bbox -123.02 49.22 -122.88 49.28 --out output/added_streams/added_candidates.geojson
   ```
   Writes `output/added_streams/added_candidates.geojson` (one Feature per merged channel) + a `.md` review table.
2. **Curate** — copy the keepers into `pipeline/added_streams.geojson`, adding a `connect_to`
   (`{gnis_id}` / `{blk}` / `{coord}`) for anything ambiguous. Hand-drawn streams: author a
   `LineString` with `source:"manual"` and a negative `blk`.
3. **Verify standalone** — no build.py needed:
   ```
   PYTHONPATH="$PWD" .venv/bin/python -m pipeline.added_streams.harness \
       --geojson pipeline/tests/data/added_streams.sample.geojson --out output/added_streams/added_demo.gpkg
   ```
   Prints merged mainstem = one node/blk/wsc, tributaries nesting under it, added nodes ∈
   `ancestors(FWA stream)`; writes a small gpkg to eyeball in QGIS.
4. **Build integration — DONE** (see *Build integration* below): `pipeline/build.py` reads the frozen
   `added_streams.build.json` on every build (on by default) and wires the added streams into the graph.

## Curated file (`pipeline/added_streams.geojson`)

A FeatureCollection of `LineString`s (lon/lat). Per-feature properties:

| prop | meaning |
|---|---|
| `source` | `"osm"` \| `"manual"` |
| `blk` | **channel** id (negative int). Features of one continuous stream share it; each tributary gets its own. fetch_osm fills `-min(way_id)` for same-name-connected OSM ways; author sets it for hand-drawn. |
| `osm_way_id` | OSM provenance (optional) |
| `name` | stream name (optional; can also be added later via `name_variants.json`) |
| `connect_to` | receiver: `{gnis_id}` / `{blk}` (FWA positive, or another added negative) / `{coord}`. Omit to auto-detect the nearest FWA/added receiver within ~250 m. |
| `wsc`, `stream_order`, `stream_magnitude`, `gnis_id`, `edge_type` | optional overrides (else minted) |

## How it works

- **Merge** (`merge.py`) — same-`name` touching ways (or same explicit `blk`) merge into one channel
  = one blue line = one WSC, exactly like FWA merges fids. A fork has a different name, so it stays a
  separate channel (that join becomes an edge, not a merge).
- **WSC** (`wsc.py`) — a channel's watershed code = its receiver's WSC + a 6-digit proportional-
  distance segment (`round(pct, 2)·10⁴`; FWA spec: 528.8 / 2479.5 → 21.33 % → `213300`). Assigned
  top-down from the FWA root outward, so every added stream prefix-descends the stream it joins.
- **BLK** — `-osm_way_id` (or an author-assigned negative). Never collides with real (positive) FWA
  BLKs.
- **Order / magnitude** — computed over the added network, not defaulted: Shreve
  `magnitude(C) = 1 + Σ(magnitude of C's added tributaries)`; Strahler order folds tributaries
  source→mouth (equal order → +1, else keep the max). So a channel formed by joining tributaries gets
  order/magnitude 2+, matching FWA. A feature may override either.
- **Ingest** (`ingest.py`) — synthesizes an FWA-shaped `FidRow` + `BlkChain` per channel (every
  attribute the build reads: order, magnitude, `edge_type="1000"` — never `"2300"`/barrier, route
  measures, geometry) and a `ConnectorSpec`; `attach_connectors` adds the real flow edge
  (`connector` for a mouth↔receiver gap, `confluence` for a shared vertex) and a short bridge line
  keyed under the **receiving mainstem's blk** (the connector shares the mainstem's blk).
- The synthetic records go through the **same** `build_stream_graph` / `build_section_geometries` as
  FWA data — no parallel machinery.

## Known Phase-B decision

An added tributary joining a larger FWA stream does not change the FWA stream's Strahler order (a
lower order joining a higher one keeps the higher — FWA rule c), and its Shreve magnitude would rise
by the tributary's magnitude across every FWA node downstream. That downstream ripple is deliberately
NOT applied in Phase A (it would mutate existing FWA nodes). Decide during the build-integration
(Phase B) whether the +magnitude on the FWA mainstem is worth propagating (it is imperceptible for
line-weight rendering on a large river).

## Municipal batch (bulk sources → minted dataset)

Beyond hand-authored/OSM streams, whole municipal stream layers (Port Moody, Burnaby, Squamish,
Abbotsford — `data/*.geojson`, described in `SOURCES.md`) are turned into the minted map by a one-time
batch:

```
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.added_streams.build_dataset burnaby squamish …
```

Flow: `clean` (uniform schema, strip ArcGIS cruft) → `merge` (same-name connected ways → one channel)
→ `fwa_match` (fuzzy, FWA-favouring) → resolve receiver → `underlake` (tag under-lake wbk) → mint →
`validate` → write `added_streams.build.json` + `output/added_streams/added_name_variant_candidates.json` +
`output/added_streams/added_streams_report.md`.

- **duplicates** (a municipal line that hugs an FWA stream, even a small offset subset) are dropped —
  FWA geometry wins.
- **extensions** (municipal genuinely bigger) keep only the novel tail on the FWA stream's own
  blk+wsc so graph-build merges it into that blue line.
- **novel** streams mint a negative blk + a WSC that prefix-descends their receiver — an FWA stream,
  another added stream, or a **900 tidal root** (`coastal.py`) when they drain to the ocean.
- `validate.py` blocks the write unless every WSC prefix-descends its receiver and every chain roots
  at a real FWA drainage or 900 (primary codes inherited, never invented).
- **matching drives direction**: the FWA/receiver match point is the mouth; the stream expands
  upstream. A municipal name differing from a matched FWA name is reported as a name-variant candidate
  (FWA name is boss; the municipal name is a searchable alias).
- Streams with no receiver within tolerance are **reported, not guessed** (add a `connect_to` or widen
  the tolerance).

Consume the built dataset with `build_dataset.to_graph_inputs(load_build(path))` →
`(added FidRows, ConnectorSpecs)`.

## Build integration (consumed by `pipeline/build.py`)

The build **does not re-run the resolver**. It consumes a single **frozen, vetted** artifact —
`pipeline/hack/added_streams/added_streams.build.json` — that is generated once, eyeballed on the
verify maps, and checked in. `build.py` reads it on **every build, on by default** (skip with
`--no-added-streams`; point elsewhere with `--added-streams PATH`).

**Artifact contents** (top-level keys):
- `streams` — one record per minted stream: `blk` (negative), `wsc`, `name`, `segments`
  (`coords3005` + under-lake `wbk`), `connector` (mouth→receiver, with the pre-merge `mouth`),
  `stream_order`/`stream_magnitude`, `receiver_kind`/`receiver_blk`, `base_measure`.
- `fwa_exclude` — WSC **prefixes** whose FWA blue lines the municipal network supersedes (e.g.
  `100-019698-` = every tributary under Still Creek). The consumer removes them.
- `name_variants` — three kinds: **added** (a stream's own municipal name → its own blk; this is how
  the otherwise-nameless added node gets named + a registry item), **duplicate** (a municipal line
  that hugs a KEPT FWA blue line → its name aliases that FWA blk), **excluded_fwa** (an excluded FWA
  reach's own gnis name → the added stream that superseded it, so the name survives the exclusion).

**What `build.py` does** (all additive, right after `load_stream_fids`):
1. drop FWA fids whose trimmed WSC starts with any `fwa_exclude` prefix (`_apply_fwa_exclude`);
2. `to_graph_inputs(streams)` → append the synthetic added fids so the **same** `build_blk_chains` /
   `build_stream_graph` / `build_section_geometries` ingest them (under-lake segments tie into lake
   nodes via their `wbk`);
3. after the graph is built, `attach_connectors` wires each added stream to its receiver at the
   confluence (resolving nodes **by measure**, since a node id is `{blk}:{down_m}` and a lake-inlet
   mouth under the through-lake spine has no `:0` section);
4. feed the artifact's `name_variants` into the existing `apply_name_variants` call.
A bbox build keeps only in-bbox added streams **plus their added-receiver chain** (`_streams_in_bbox`),
so it never adds another region's streams as orphans nor severs a chain at the bbox edge.

**Regenerate + re-freeze** (Claude-safe: gpkg + DEM only, **no credits**) — resolves each source in its
OWN bbox (a combined bbox cross-contaminates), then merges with per-source blk offsets:
```
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.hack.added_streams.build_dataset burnaby squamish port_moody
```
Then eyeball `output/added_streams/verify_<source>.html` (regenerate with `python -m pipeline.hack.added_streams.verify_map <source>`)
and commit the artifact. It is the checked-off source of truth from then on.

**Guarantees the resolver enforces** for the consumer:
- **Unique WSC** — no two DIFFERENT streams share a code (`_bump_wsc`); same-name *fragments* of one
  creek intentionally share, an unnamed stream shares with nobody.
- **Lake through-flow** — a lake's outlet stream runs THROUGH the lake as a central spine
  (`lake_through_spine`), so its many inlets attach at DISTINCT measures (no pile-up of identical
  `…-999999` codes) and the under-lake span carries the lake `wbk`.
- **Same-name tributaries** — a same-name piece joining its same-name mainstem **partway** (< 85 % up)
  is a distinct tributary: its own descendant code + a `<mainstem> Trib.N` name. One joining at the
  **source** is a fragment continuation and shares the code.

## Notes

- `fetch_osm.py` is the only network step (Overpass) and is human-run; everything else is local.
- Nothing here is Burnaby- or OSM-specific — new area (new bbox), a hand-drawn line, or a municipal
  layer all take the same path.
