# Municipal stream layers

Four city GeoJSON exports in `data/source/municipal/`, all WGS84 lon/lat `LineString` with some
`MultiLineString`, each under its city's open government licence. They are candidate pools: `clean.py`
(one cleaner per source, uniform schema) writes `data/generated/municipal/cleaned/<source>.geojson`, and
`build_dataset.py` matches and mints the kept lines (see `README.md`). `clean.py` holds the per-field
handling; the table lists only what each layer is good for.

| file | city layer | features | use |
|---|---|---|---|
| `port_moody_esa_streams.geojson` | Port Moody, Environmentally Sensitive Areas | 484 | `theme` separates Stream from Culvert and Ditch; `description` names a receiver on 14 rows |
| `burnaby_waterways.geojson` | Burnaby, Waterway | 402 | `WATERWAYNAME` carries the hierarchy (`Beaver Trib.1`); `UNITID` and `UNITID2` give upstream and downstream node codes |
| `squamish_watercourses.geojson` | Squamish, Watercourse | 670 | `Contributing_to_DS` names the downstream receiver on 569 rows; few names; species columns are mostly `NA` |
| `abbotsford_streams.geojson` | Abbotsford, Environment Layers | 18,508 | `FISH_CLASS` (A fish-bearing, B potentially, C not); names are ALL-CAPS and mostly `DITCH-nn` codes |

Also in the directory: `osm_streams.geojson` (picks from OpenStreetMap) and `manual_streams.geojson`
(channels drawn by hand); see `README.md` for their schema.

Gotchas: each layer has its own `OBJECTID` space, so a minted stream takes its own negative `blk`; every
`Shape__Length` is metres from a projected original; a `MultiLineString` is flattened before use.
