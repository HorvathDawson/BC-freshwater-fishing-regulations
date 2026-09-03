"""Depth charts: a survey sheet's waterbody identifier -> the lake it is a chart of.

SAME SHAPE AS GAUGES AND STOCKING. `generate/` proposes matches and needs the atlas;
promotion writes them into `data/curated/bathymetry/`; consumers only ever read that.

The join is an exact key — a sheet's `waterbody_identifier` is FWA's own
`WATERBODY_KEY_GROUP_CODE_50K` — CONFIRMED BY THE GAZETTE NAME, never by a name variant: a
variant may itself have come from a bathymetry sheet, and confirming a match against
something the match produced is circular.
"""
