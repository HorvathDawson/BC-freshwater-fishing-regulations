"""THE RELEASE CONTRACT (P08): every shipped file, its format and, for SQLite files, its tables and columns, as code.

The app reads the release through this contract (`python -m pipeline.tools.emit_pack` writes it to
app/packages/data/src/pack.generated.ts; `--check` fails CI when they drift, with no data needed). `test_release.py`
holds a BUILT release to it exactly, so the builder cannot add, drop or rename a column without this file (and so the
app's generated copy) moving with it.
"""
from __future__ import annotations

from pipeline.deliver import lake_facts as LF
from pipeline.deliver.release import paint as PAINT
from pipeline.deliver.release import shards as SH

_FRAME = ["id", "j"]
_REGION_TABLES = {
    "cell": ["akey", "s", "ladder", "rows", "gear", "licence", "display"],
    **{t: _FRAME for t in ("display_frame", "gear_frame", "ladder_frame", "ladder_verdict", "licence_answer",
                           "licence_doc", "licence_frame", "licence_hold", "licence_u16", "rows_decided", "rows_frame",
                           "rows_row")},
    "meta": ["k", "v"],
}

#: the files a release may omit. `lakes` is the lake modes' slot, prepared and not fed (owner 2026-10-21): no producer
#: makes one, `deliver all` ships none, and the app greys Stocked / Depth until a release carries it.
OPTIONAL = frozenset({"lakes"})

#: {file: {"format", "kind": "sqlite" | "bin", "tables": {table: [columns]}}}
FILES = {
    "paint": {"format": PAINT.FORMAT, "kind": "bin", "tables": {}},
    "flow-area": {"format": PAINT.AREA_FORMAT, "kind": "bin", "tables": {}},
    "lakes": {"format": LF.FORMAT, "kind": "bin", "tables": {}},
    "core": {"format": "core/1", "kind": "sqlite", "tables": {
        "alias": ["item_id", "alias"],
        "item": ["ord", "item_id", "name", "kind", "part_of", "area_ha", "size", "gauged", "has_rows", "pieces"],
        "item_alias": ["alias", "item_id"],
        "item_section": ["sid", "ord"],
        "meta": ["k", "v"],
        "place": ["place_id", "osm", "name", "kind", "pop", "lat", "lon"]}},
    "near": {"format": "near/1", "kind": "sqlite", "tables": {
        "meta": ["k", "v"], "place_water": ["place_id", "ckm", "ord"]}},
    "conditions": {"format": "conditions/1", "kind": "sqlite", "tables": {
        "gauge": ["station", "name", "item_id", "sid", "lon", "lat", "area_km2", "mag", "matched_by", "match_m"],
        "gauge_clim": ["station", "parameter", "bands"],
        "lake_gauge": ["item_id", "station"],
        "meta": ["k", "v"],
        "section_down": ["sid", "down_sid"],
        "section_gauge": ["sid", "station", "trust", "mag"]}},
    "water": {"format": "water/1", "kind": "sqlite", "tables": {
        "akey": ["akey", "ruleset", "licensing_set", "steelhead_water", "steelhead", "steelhead_rules",
                 "province_except", "home_region", "kind", "tidal", "seg", "mom", "nseg"],
        "akey_region": ["akey", "region"],
        "display_rule": _FRAME,
        "entry": ["e", "id", "j"],
        "item": ["item_ord", "item_id", "shown", "picker", "unresolved", "name", "kind", "sections", "outside_bc",
                 "entries", "steelhead", "x"],
        "lic": ["lix", "j"],
        "lset": ["set_id", "j"],
        "meta": ["k", "v"],
        "part": ["item_ord", "part_ix", "akey", "ord", "label_s", "short_s", "runs_s", "place_s", "hint_s", "km",
                 "closed_all_year", "paper_licence", "xp", "sections", "xruns"],
        "part_geo": ["ord", "part_ix", "rank", "mid_sid", "mid_lon", "mid_lat", "w", "s", "e", "n"],
        "part_mark": ["ord", "mark_ix", "lon", "lat", "label", "down_part", "up_parts"],
        "part_section": ["ord", "sid", "part_ix"],
        "picker": _FRAME,
        "place": ["place", "regions", "label", "names"],
        "rule": ["rix", "j"],
        "ruleset": ["set_id", "j"],
        "runs": _FRAME,
        "seglist": ["seg", "starts"],
        "segmom": ["mom", "ix"],
        "static": ["k", "j"],
        "str": ["id", "text"],
        "water_continue": ["ord", "part", "next_ord", "next_part"],
        "water_exit": ["ord", "down_ord", "down_part"],
        "xpart": _FRAME,
        "zdict": ["tbl", "d"]}},
    **{f"regs-{r}": {"format": "regs-region/1", "kind": "sqlite", "tables": _REGION_TABLES} for r in SH.REGIONS},
    "regs-sid": {"format": "regs-sid/1", "kind": "sqlite", "tables": {
        "meta": ["k", "v"], "section_akey": ["block", "b"], "section_place": ["block", "b"]}},
}

#: what the units of the transformed columns are (the app's decoders read these)
UNITS = {
    "water.part_geo.mid_lon": "microdegrees", "water.part_geo.mid_lat": "microdegrees",
    "water.part_geo.w": "microdegrees", "water.part_geo.s": "microdegrees", "water.part_geo.e": "microdegrees",
    "water.part_geo.n": "microdegrees", "water.part_mark.lon": "microdegrees", "water.part_mark.lat": "microdegrees",
    "conditions.gauge_clim.bands": "clim/2: [u8 pentad, 5 x float16 LE] records",
    "flow-area": "flow-area/1: 'BCFA', u8 1, one LEB128 varint per paint section (sid order) = round(km2 x 1000)",
    "paint": "paint/2: see pipeline/deliver/release/paint.py",
    "lakes": "lakes/1: 'BCLK', u8 1, 8 bytes section_handles, LEB128 asof+1, LEB128 count, then per section "
             "(sid ascending) LEB128 sid delta, u8 survey (0 none, 1 sheet, 2 digitised), LEB128 stocked day+1 "
             "(0 never); days since 1900-01-01 (pipeline/deliver/lake_facts.py)",
}
REGIONS = SH.REGIONS
