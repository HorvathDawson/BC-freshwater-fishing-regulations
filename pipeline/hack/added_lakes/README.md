# pipeline/added_lakes — put non-FWA lake polygons into the graph

Some lakes the fishing regulations name are **absent from the FWA waterbody layer**. Redsand Lake is
one: DFO Region 6 regulates it by name, but FWA has no polygon for it, so no water of that name
exists and an exact lookup returns nothing.

Nearby Treston Lake carries the name `'TRESTON LAKE  REDSAND LAKE'` (double space), which *looks*
like it has swallowed Redsand's name. It has not, twice over:

* the string is a **bathymetry map label** (`source: "bathymetry"` in `name_variants.json`), not an
  FWA gazetted name — a sheet covering both lakes, which is itself evidence they are two waters; and
* names index under their **whole normalised string**, so Treston answers to
  `treston lake redsand lake` and never to `redsand lake`.

A substring search suggests otherwise and is not how the matcher works. 110 registry names contain a
double space and they are not one convention: several lakes (`ELINOR L.  NARAMATA L.`), a qualifier
(`LOON LAKE  NEAR AINSWORTH`), a typo (`Waller  Creek`). **Leave them alone** — Redsand needed a
polygon, not a name rescue.

This package ingests a hand-drawn polygon as a real lake node, the same way
[`added_streams`](../added_streams/README.md) ingests a hand-drawn stream line.

## The one idea

A lake polygon and the stream network are **separate data**. What links them is that the polygon
**cuts** the stream running through it. In this build that link is a single field —
`FidRow.wbk` — and `graph/graph.py::_assign_owners` does the rest:

> *"a fid whose `wbk` is a lake/manmade wbk belongs to that lake node and **BREAKS the current
> piece** (so the fids on each side of a lake become separate pieces)"*

So adding a lake needs **no change to the graph code at all**. Re-stamp the fids that fall inside
the polygon with the new wbk and everything else follows:

| what you might expect to build | what actually happens |
|---|---|
| split the stream at the lake | automatic — the lake wbk breaks the fid run |
| `lake_in` / `lake_out` flow edges | `build_stream_graph` mints them from the lake-owned fids |
| a `lake:{wbk}` boundary regs can bind | `_finalize_lake_bounds` mints it from those edges |
| a registry item | `add_waterbody_items` / `add_curated_wbk_items` |
| a name | a `name_variants.json` entry targeting `{"wbks": ["-1"]}` |
| display geometry + MU assignment | the polygon goes into `wbk_polys`, same as an FWA one |

The lake NODE's own geometry stays what it is for every other lake — the stitched under-lake fid
lines (`build_section_geometries`). The polygon is the separate, display-side artefact.

## Minting ids

Every synthetic id is **negative**, mirroring `added_streams`' negative `blk`: it can never collide
with a real one and it is obvious on sight in a node id (`lake:-1`), an item id (`wbk:-1`), a
boundary (`lake:-1`) or a ref (`gnis:-9000001`). Two bands, so the namespaces stay distinguishable:

| id | band | why |
|----|------|-----|
| `wbk` | `-1, -2, -3, …` | FWA waterbody keys are positive 9-digit integers, so any negative is free |
| `gnis_id` | `-9000001, -9000002, …` (`-9_000_000 - n`) | real gnis ids run **1,642 – 8,000,027**, so this is out of range in MAGNITUDE as well as sign — it cannot be mistaken for a real id even if a sign is dropped somewhere downstream |

The gnis matters because a lake node carries one exactly as a stream node does (`lake_gnis` in
`build_stream_graph`), and it is what lets a gnis-keyed `name_variants` entry or override resolve
onto the lake. Without it the lake would answer only to its wbk.

Assignments are **authored in the file, not computed** — the same choice `added_streams` makes for
hand-drawn lines — so a key is stable across rebuilds and reviewable in a diff. Never renumber an
existing one: a curated split, an override or a saved binding may already point at it.

| wbk | gnis_id | name | added | why FWA lacks it |
|-----|---------|------|-------|------------------|
| `-1` | `-9000001` | Redsand Lake | 2026-09-01 | no FWA polygon; its name is buried in Treston Lake's name tuple |

## Curated file (`pipeline/added_lakes.geojson`)

A FeatureCollection of `Polygon`s in **lon/lat (EPSG:4326)** — convert before adding; the Redsand
polygon arrived as EPSG:3857. Per-feature properties:

| prop | meaning |
|------|---------|
| `wbk` | the negative waterbody key, authored (see above) |
| `gnis_id` | the negative gnis, authored — paired with `name` so the lake node carries it |
| `name` | the lake's name; becomes a gazette name tuple on the node, paired with `gnis_id` |
| `kind` | `lake` \| `manmade` — what `lake_kind` records, and what decides node kind |
| `source` | `manual` \| the layer it came from |
| `note` | why FWA lacks it, where the polygon came from, what it sits on |
| `claims` | (informational) the blks/wscs the polygon overlaps, so a reviewer can check the re-stamp hit what was intended |

`claims` is **documentation, not input**: which fids get re-stamped is decided by geometry at build
time, so the file cannot silently disagree with the map.

## Verify without a full build

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.hack.added_lakes.ingest --check

Prints, per lake, the fids the polygon would claim, the blks they belong to, and the stream pieces
that would be split — without touching the build.
