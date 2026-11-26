# added_lakes: lakes the FWA lacks, minted into the graph

Some lakes the regs name have no FWA waterbody polygon (Redsand Lake, Marsh Pond). This package ingests a
hand-drawn or OSM polygon as a real lake node, the way [`added_streams`](../added_streams/README.md) does
for a stream line.

Reads: `data/curated/waters/added_lakes.geojson` (`CURATED.waters.added_lakes`). Writes: nothing of its own;
the atlas build takes its lakes. Entry points: `ingest.py` (`--check`), `split_parents.py`.

## How a lake joins the graph

A lake polygon and the stream network are separate data. The link is one field, `FidRow.wbk`: the build
re-stamps the fids inside the polygon with the lake's wbk, and `graph/graph.py::_assign_owners` breaks the
stream there. No graph code changes. The `lake_in` and `lake_out` edges, the `lake:{wbk}` boundary and the
registry item come from the existing build; the polygon goes into `wbk_polys` like an FWA one; the name is a
`name_variants.json` entry targeting `{"wbks": ["-1"]}`. The lake node's own geometry is the stitched
under-lake fid lines.

## Ids

A feature carries one authored positive `id` (the next integer, `max + 1`). `ingest.py` derives
`wbk = -id` and `gnis_id = -(9_000_000 + id)`: negatives never collide with FWA's positive 9-digit wbks, and
the gnis band lies outside real gnis ids (1,642 to 8,000,027) in magnitude as well as sign. `load()` refuses
a feature that carries `wbk` or `gnis_id`. An `id` is never renumbered: a split, an override or a saved
binding may already point at it.

## The file

Polygons in lon/lat (EPSG:4326). Properties:

| prop | meaning |
|---|---|
| `id` | the authored number above |
| `name` | the lake's name, paired with the derived `gnis_id` |
| `kind` | `lake` or `manmade` |
| `source` | `manual`, `osm` or the layer it came from |
| `note` | why FWA lacks it and where the polygon came from |
| `claims` | informational: the blks and wscs the polygon overlaps, for a reviewer; the build decides by geometry |
| `part_of` | `{"wbk": "<parent>"}` on a part of a lake the book splits; the bundle build copies it to `item.part_of` |

A name with a double space in the registry (`'TRESTON LAKE  REDSAND LAKE'`) is a bathymetry label indexed
under its whole string. It does not answer to the second lake's name; add a polygon, not a rename.

## Check and test

```
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.atlas.waters.added_lakes.ingest --check
```

Prints, per lake, the fids the polygon claims and the stream pieces it would split, without a build.
