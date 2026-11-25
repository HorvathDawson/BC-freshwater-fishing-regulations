# pipeline/atlas/waters: waters the FWA does not have

Mints the waters the Freshwater Atlas lacks: `added_streams/` (municipal streams, negative `blk` ids) and `added_lakes/`
(hand-drawn lake polygons the gazetteer never named, negative `wbk`/`gnis` ids). They run seldom (a municipality publishes a new
layer, or a bug is found) and their output is curated forever: the negative ids are a live ABI that splits and name variants
are bound to, so reminting strands every one. Never delete them as one-offs.

- Entry: `python -m pipeline.atlas.waters.added_streams.build_dataset` (builds the dataset the atlas build then consumes by default);
  `added_lakes/ingest.py` for lakes. Each subfolder has its own README (`added_streams/README.md`, `added_lakes/README.md`).
- Reads: the municipal layers and OSM under `data/source/`, FWA matches; `added_streams/SOURCES.md` lists the sources.
- Writes: `data/curated/waters/` (the added-water files; curated, not regenerable), read by `pipeline/atlas/build.py`.
- Rule: authored, reviewed and generated data are different kinds (AGENTS 35); see `data/curated/README.md`.

## Tests

`pipeline/atlas/waters/added_streams/tests/` (listed in `pytest.ini` `testpaths`).
