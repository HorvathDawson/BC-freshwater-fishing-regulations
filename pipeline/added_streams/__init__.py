"""Import non-FWA streams (OSM-fetched OR hand-authored) into the stream graph.

A curated GeoJSON of stream lines (``pipeline/added_streams.geojson``) is turned into synthetic
FWA-shaped records (a minted negative BLK, a proportional-distance WSC, and every attribute the
build pipeline reads) and fed through the SAME `build_stream_graph` path FWA streams take. Each
added stream is joined to its receiver by a real ``connector`` flow edge, so it is named
(via ``name_variants.json``), bindable by regs, and counts as a tributary of the stream it joins.

Source-neutral: OSM is one populator (`fetch_osm`), a hand-drawn LineString is another; both land
in the same file and take the same path. See ``README.md``.
"""
