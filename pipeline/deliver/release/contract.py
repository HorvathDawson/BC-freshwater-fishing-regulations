"""THE RELEASE CONTRACT (P08): every shipped file, its format and, for SQLite files, its tables and columns, as code.

The app reads the release through this contract (`python -m pipeline.tools.emit.pack` writes it to
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
    # (T54) the gentler access tiers (permit / reserve / private) a Regulations page notes; paint carries the no-access rows
    "access": {"format": PAINT.ACCESS_FORMAT, "kind": "bin", "tables": {}},
    "lakes": {"format": LF.FORMAT, "kind": "bin", "tables": {}},
    "core": {"format": "core/1", "kind": "sqlite", "tables": {
        # `name_key` / `alias_key` (T114): what the app's SEARCH filters on (`@app/core` `searchKey` of the query). The
        # app's search NEEDS them: a core without them fails every water search, so ship it with the app that reads it.
        # Stored thin (`pipeline.common.search_key.stored_key`): NULL where COALESCE(key, lower(name)) already is it
        "alias": ["item_id", "alias", "alias_key"],
        "item": ["ord", "item_id", "name", "kind", "part_of", "area_ha", "name_key", "size", "gauged", "has_rows",
                 "pieces"],
        "item_alias": ["alias", "item_id"],
        "item_section": ["sid", "ord"],
        "meta": ["k", "v"],
        "place": ["place_id", "osm", "name", "kind", "pop", "lat", "lon"],
        # a group name that only points ("Tee Pee Lakes: see Friday Lake, …"), the bundle's table as it is. ADDITIVE,
        # so still core/1: a core without it is one the app's pointer search answers nothing from (the committed regs
        # fixture's core, like its missing `place`, is cut to what its suite asks)
        "see_pointer": ["entry_id", "name", "ix", "target_entry", "target_name", "item_id"]}},
    "near": {"format": "near/1", "kind": "sqlite", "tables": {
        "meta": ["k", "v"], "place_water": ["place_id", "ckm", "ord"]}},
    "conditions": {"format": "conditions/1", "kind": "sqlite", "tables": {
        # `tidal` (T78), `backwater` (T160): ADDITIVE, last, defaulted — still conditions/1; an older release is read
        # as 0 (no flag)
        "gauge": ["station", "name", "item_id", "sid", "lon", "lat", "area_km2", "mag", "matched_by", "match_m",
                  "tidal", "backwater"],
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
    "paint": "paint/4: see pipeline/deliver/release/paint.py",
    "access": "access/1: 'BCAX', u8 1, 8 bytes section_handles, then an access half (pipeline/deliver/release/paint.py)",
    "lakes": "lakes/1: 'BCLK', u8 1, 8 bytes section_handles, LEB128 asof+1, LEB128 count, then per section "
             "(sid ascending) LEB128 sid delta, u8 survey (0 none, 1 sheet, 2 digitised), LEB128 stocked day+1 "
             "(0 never); days since 1900-01-01 (pipeline/deliver/lake_facts.py)",
}
REGIONS = SH.REGIONS

#: (T129) THE WEB REQUEST BUDGET. The release total (`build.TOTAL_GATE`) is what a PHONE holds; a web visitor fetches
#: lazily (MINIMAL-PACKAGING §2, the app's `release/loader.ts`), and nothing gated those paths. Per moment of a first
#: visit: the release files it fetches, by manifest name ("manifest" is manifest.json itself; "mask" the tile set's
#: bc_outside.geojson, sent as is; "regs-{region}" the LARGEST regs-<region> file; every other file its gzip bytes),
#: and the most they may weigh. The file lists are what the browser walk pins (`app/tools/request-walk.mjs`,
#: `request-walk.pin.json`); `build.gate_problems` fails the release over a gate, and `app/tools/release-budget.test.ts`
#: holds the live release and the pin to it. Gates set 2026-10-24 from r-bb4331929428 with 10-15 % headroom (measured in
#: the comments; the T129 ticket's table). The sql.js engine (sql-wasm, 659 kB) rides with the first SQLite file opened
#: and is not a release file: it is not priced here.
WEB_BUDGET = {
    # the coloured map, Map tab (measured 1,024,306: manifest 5,888 + paint 955,855 + mask 62,563)
    "first_paint": {"files": ["manifest", "paint", "mask"], "gate": 1_150_000},
    # the first tap on a NAMED water on the map, largest region: sid -> water (core), the page (water + the region
    # file) and the tapped section's access note (measured 4,436,131, regs-6)
    "first_tap": {"files": ["core", "water", "access", "regs-{region}"], "gate": 5_000_000},
    # the first tap on an UNNAMED stream (most streams): + its key (regs-sid) and conditions, whose `section_down` the
    # header's "Tributary of ..." walks (measured 8,530,859, regs-6)
    "first_tap_unnamed": {"files": ["core", "water", "access", "regs-{region}", "regs-sid", "conditions"],
                          "gate": 9_500_000},
    # the Conditions tab: gauges + climatology, the panels' areas, core for the lakes' gauges (measured 4,156,368)
    "conditions_open": {"files": ["core", "flow-area", "conditions"], "gate": 4_700_000},
}
