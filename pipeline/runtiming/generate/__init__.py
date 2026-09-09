"""Turning fetched pages and 2.6 GB of stream geometry into a frozen index.

NEEDS THE ATLAS — `data/generated/tiles/layers/stream.geojsonl`, 1.2 million sections — so
nothing on the build path may import from here. Runs about once a year, by hand, after a
fetch. Same rule as `pipeline/gauges/generate`, for the same reason.
"""
