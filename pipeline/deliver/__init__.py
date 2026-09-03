"""The two artifacts a client reads: the SQLite bundle and the PMTiles atlas.

They split on what changes. Geometry changes when the province republishes the FWA — rarely,
and it invalidates 875 MB. Regulations change every edition and invalidate about 10 MB.
Three clocks, three artifacts, so a client can update the cheap one alone.
"""
