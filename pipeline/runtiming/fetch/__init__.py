"""HTTP and nothing else. No atlas, no geometry, no bundle.

Run timing is a long-term average that the Pacific Salmon Foundation revises about once a
year, so this runs about once a year — unlike `gauges/feed`, which runs every 30 minutes.
It writes verbatim pages to `data/source/runtiming/` and interprets none of them: parsing
belongs to `generate`, so a re-fetch can never change what a build believed.
"""
