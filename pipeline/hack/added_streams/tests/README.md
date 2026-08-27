# added_streams tests

Tests for the **one-off** `pipeline/hack/added_streams/` module (municipal stream ingest → DEM flow →
resolver → mint). They live here, next to the module, so they run **separately** from the main
pipeline suite: the root `pytest.ini` sets `testpaths = pipeline/tests`, so a bare `pytest` does
NOT collect these. Run them explicitly:

```bash
PYTHONPATH=. .venv/bin/python -m pytest pipeline/hack/added_streams/tests -q     # 92 pass, ~1 s
```

All fixtures are synthetic (built in-code) — no gpkg, no network, no LLM credits. One file per
module (`test_dem.py`, `test_build.py`, …). `test_dem.py` carries the DEM-flow regression cases
(apex loop, Y-headwaters diamond, lake-sink-over-pit, two-creeks-at-a-river, lake-outflow dual
attach); each was proven to fail before its fix.

`data/added_streams.sample.geojson` is the tiny hand-authored sample used by `harness.py` (the
standalone real-FWA demo), not by these unit tests.
