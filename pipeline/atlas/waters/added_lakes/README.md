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

**Author one number.** A feature carries a single positive `id` — the next integer, `max + 1` — and
both synthetic keys are **derived** from it by `ingest.py`:

| derived key | from | band | why that band |
|---|---|---|---|
| `wbk` | `-id` | `-1, -2, -3, …` | FWA waterbody keys are positive 9-digit integers, so any negative is free |
| `gnis_id` | `-(9_000_000 + id)` | `-9000001, -9000002, …` | real gnis ids run **1,642 – 8,000,027**, so this is out of range in MAGNITUDE as well as sign — it cannot be mistaken for a real id even if a sign is dropped somewhere downstream |

So `id: 3` **is** `wbk:-3` and `gnis:-9000003`, always. Both keys stay negative, mirroring
`added_streams`' negative `blk`: they can never collide with a real one and they are obvious on sight
in a node id (`lake:-3`), an item id (`wbk:-3`), a boundary (`lake:-3`) or a ref (`gnis:-9000003`).

`wbk` and `gnis_id` **must not appear in the file** — `load()` refuses a feature that carries either.
Authoring them separately is how you get a lake whose polygon and whose name resolve to different
things, and a stale pair that disagrees with `id` is precisely the mismatch nothing downstream could
catch. One number, one lake.

The gnis matters because a lake node carries one exactly as a stream node does (`lake_gnis` in
`build_stream_graph`), and it is what lets a gnis-keyed `name_variants` entry or override resolve
onto the lake. Without it the lake would answer only to its wbk.

The `id` itself is **authored, not computed** — the same choice `added_streams` makes for hand-drawn
lines — so a key is stable across rebuilds and reviewable in a diff. Never renumber an existing one:
a curated split, an override or a saved binding may already point at it.

| id | -> wbk | -> gnis_id | name | added | why FWA lacks it |
|----|--------|------------|------|-------|------------------|
| `1` | `-1` | `-9000001` | Redsand Lake | 2026-09-01 | no FWA polygon; its name is buried in Treston Lake's name tuple |
| `2` | `-2` | `-9000002` | Marsh Pond | 2026-09-03 | no FWA polygon; curation held only a KML point (OSM relation 2531058) |
| `3` | `-3` | `-9000003` | Children's Fishing Pond | 2026-09-03 | no FWA polygon; the ungazetted half of the `HALL ROAD (Mission) POND` override (OSM way 302989664) |

## Curated file (`pipeline/added_lakes.geojson`)

A FeatureCollection of `Polygon`s in **lon/lat (EPSG:4326)** — convert before adding; the Redsand
polygon arrived as EPSG:3857. Per-feature properties:

| prop | meaning |
|------|---------|
| `id` | the one authored number: a positive integer, `max + 1`. `wbk` and `gnis_id` are DERIVED from it (see above) and must not be written here |
| `name` | the lake's name; becomes a gazette name tuple on the node, paired with the derived `gnis_id` |
| `kind` | `lake` \| `manmade` — what `lake_kind` records, and what decides node kind |
| `source` | `manual` (hand-drawn) \| `osm` \| the layer it came from |
| `note` | why FWA lacks it, where the polygon came from, what it sits on |
| `claims` | (informational) the blks/wscs the polygon overlaps, so a reviewer can check the re-stamp hit what was intended |
| `part_of` | `{"wbk": "<parent>"}` on a PART of a lake the book splits (Kootenay, Williston, Shannon). The atlas does not read it; the bundle build copies it to `item.part_of` (refusing a part or parent this build lacks), and readers group a lake's parts by that column — never by this file |

`claims` is **documentation, not input**: which fids get re-stamped is decided by geometry at build
time, so the file cannot silently disagree with the map.

## Verify without a full build

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.atlas.waters.added_lakes.ingest --check

Prints, per lake, the fids the polygon would claim, the blks they belong to, and the stream pieces
that would be split — without touching the build.
