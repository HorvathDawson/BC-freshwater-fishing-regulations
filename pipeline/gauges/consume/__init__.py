"""Reading the frozen matches: where to cut a river, and which reaches a gauge may speak
for. Topology only — no geometry, no matcher, so this is free to run on every build.

    place.py          the frozen match projected onto this graph (`nodes_for`, `summarise`)
    shed.py           one station per reach, and its trust band
    panel.py          the donor panel's rules (gates, weights, who may lead)
    panels.py         every section's donor panel, built and interned
    basins.py         which station speaks for each watershed group
    tidal.py          tide and backwater stations
    cuts.py           where the atlas cuts a river at its gauge
    bundle_tables.py  the bundle's gauge tables, written in one place (`write`)
"""
