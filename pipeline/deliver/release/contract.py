"""THE RELEASE CONTRACT (P08): every shipped file, its format and, for SQLite files, its tables and columns, as code.

The app reads the release through this contract (`python -m pipeline.tools.emit.pack` writes it to
app/packages/data/src/pack.generated.ts; `--check` fails CI when they drift, with no data needed). `test_release.py`
holds a BUILT release to it exactly, so the builder cannot add, drop or rename a column without this file (and so the
app's generated copy) moving with it.
"""
from __future__ import annotations

from pipeline.deliver import lake_facts as LF
from pipeline.deliver import method as METHOD
from pipeline.deliver import salmon as SALMON
from pipeline.deliver.release import packed as PACKED
from pipeline.deliver.release import paint as PAINT
from pipeline.deliver.release import shards as SH

_FRAME = ["id", "j"]
#: (T810 ticket L, the trout cutover) `cell.trout` (LAST, nullable: no table at that key and segment, Q-D3) and the trout
#: section's four tables are REQUIRED: in every `regs-<region>` file, with the format `regs-region/2` (below). The store's
#: and the shards' lists (`store.build.SECTIONS` + `ADDED_SECTIONS` / `FAMILIES`, `shards.TOP` + `ADDED_TOP` /
#: `SHARD_TABLES` + `ADDED_SHARD_TABLES`) are held equal to it by `test_trout_section.py`.
_REGION_TABLES = {
    "cell": ["akey", "s", "ladder", "rows", "gear", "licence", "display", "trout"],
    **{t: _FRAME for t in ("display_frame", "gear_frame", "ladder_frame", "ladder_verdict", "licence_answer",
                           "licence_alien", "licence_doc", "licence_frame", "licence_hold", "licence_u16", "rows_decided",
                           "rows_frame", "rows_row", "trout_dates", "trout_frame", "trout_sheet", "trout_table")},
    "meta": ["k", "v"],
}

#: (T810 ticket I, trout table contract 2.1) THE OPTIONAL ANSWERS SECTIONS' TABLES: a section a release may carry or not,
#: ALL OR NOTHING — its version in the water file's layout (`meta.layout.versions`) and the manifest's
#: `regulations.versions`, its `cell` column (LAST, nullable: no value at that key and segment) and its tables in EVERY
#: `regs-<region>` file; or none of the three. A release without it is exactly the release before it existed (an
#: all-NULL column is never shipped: "absent" and "nothing here" must not look the same). Emitted for the app as
#: `PACK_OPTIONAL_SECTION_TABLES` (pack.py), held by `test_release.py`. EMPTY since the cutover (ticket L): `trout` moved
#: into `_REGION_TABLES`; the rule stays for the next additive section. Kept apart from `OPTIONAL` below (optional FILES).
OPTIONAL_SECTION_TABLES: dict = {}

#: (T810 ticket L) THE REGS FILES' FORMAT. regs-region/2: rows 34 and the trout section required (contract 2.7) — an app
#: that reads regs-region/1 cannot open them, so the held-set path installs the new set over the old one and
#: `app_gate.oldest_reader` derives `minAppVersion` from the format (`app_reads.json`), never a stamp typed by hand.
REGS_REGION_FORMAT = "regs-region/2"


def region_tables(sections=()) -> dict:
    """A `regs-<region>` file's tables and columns with the optional sections `sections` (`OPTIONAL_SECTION_TABLES`):
    `_REGION_TABLES`, each section's `cell` columns appended and its tables added."""
    out = {t: list(cols) for t, cols in _REGION_TABLES.items()}
    for n in sections:
        spec = OPTIONAL_SECTION_TABLES[n]
        out["cell"] += spec["cell"]
        out.update({t: list(cols) for t, cols in spec["tables"].items()})
    return out


#: the files a release may omit. `lakes` is the lake modes' slot, prepared and not fed (owner 2026-10-21): no producer
#: makes one, `deliver all` ships none, and the app greys Stocked / Depth until a release carries it.
#: (T823) `flow-small` is BUILT into every release from now, and optional only so an app built from this contract still
#: reads a release cut before it (staging, 2026-11-16): the app reads its absence as "no warning bits", by the file's
#: presence in the manifest, never by catching an error. Required once a release carrying it is live and the app reads it.
#: (T679 #8) `flow-own` too: built only once `packed.SHIP_OWN_GAUGE_FLAG` is on; its absence is "decide as before".
#: (T973) `cw-through` too: app 0.8.4 reads it; an older app's contract does not know it, so it reads nothing new and
#: never shows a carried unit as a plain requirement. Absent: no "may need" note anywhere.
OPTIONAL = frozenset({"lakes", "flow-small", "flow-own", "cw-through"})

#: {file: {"format", "kind": "sqlite" | "bin", "tables": {table: [columns]}}}
FILES = {
    # (T679 #16) paint/6 (+ the one access flag); (T975) the only paint form a release writes
    "paint": {"format": PAINT.FORMAT, "kind": "bin", "tables": {}},
    "flow-area": {"format": PAINT.AREA_FORMAT, "kind": "bin", "tables": {}},
    # (T823) per paint section, which of its panel's members drain so much more than it that their reading carries the
    # small-water warning (the conditions meta's `small_water_text`): pipeline/gauges/consume/small_water.py
    "flow-small": {"format": PAINT.SMALL_FORMAT, "kind": "bin", "tables": {}},
    # (T679 #8) per paint section, which of its panel's members stand on its own water (the ranking's rule): the gauge
    # list's "ON THIS WATER" heading reads it (paint.py `encode_own`; behind `packed.SHIP_OWN_GAUGE_FLAG`)
    "flow-own": {"format": PAINT.OWN_FORMAT, "kind": "bin", "tables": {}},
    # (T973, owner 2026-11-20 12:11) the Classified Waters units carried through a rowed no-unit water: per section one
    # note (the carried designation's unit name, class, period, and the rowed waters passed) and its licence year as if
    # the unit held; app 0.8.4's licence card draws an ordinary Classified Waters row with the why behind an expand
    # (build.cw_through). OPTIONAL
    "cw-through": {"format": "cw-through/1", "kind": "sqlite", "tables": {
        "meta": ["k", "v"], "cw_note": ["ix", "unit_name", "classified", "dates", "through"],
        # (owner 13:22) each carried section's licence year as if its unit held (`answers.licence.carried_year`)
        "cw_year": ["yx", "j"], "section_cw": ["sid", "ix", "yx"]}},
    # (T54) the gentler access tiers (permit / reserve / private) a Regulations page notes; paint carries the no-access rows
    "access": {"format": PAINT.ACCESS_FORMAT, "kind": "bin", "tables": {}},
    "lakes": {"format": LF.FORMAT, "kind": "bin", "tables": {}},
    # (T211) the DFO salmon: per chain of DFO locations and day, the species' verdicts, gear lines and map status; each
    # placed section's chain (a section in no run: the UNLISTED chain, closed). The Salmon card and the Salmon colouring.
    "salmon": {"format": SALMON.FORMAT, "kind": "bin", "tables": {}},
    # (owner 2026-10-28) its runs, one file per bundle region (`x`: a section in none): a page's card reads its own
    **{SALMON.shard_name(r): {"format": SALMON.SHARD_FORMAT, "kind": "bin", "tables": {}}
       for r in (*SH.REGIONS, SALMON.NO_REGION)},
    # (T213) bait ban / fly fishing only per section and day — the map's "Bait & fly" colouring, projected from
    # the regs store's open-days strips (pipeline/deliver/method.py)
    "method": {"format": METHOD.FORMAT, "kind": "bin", "tables": {}},
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
        "see_pointer": ["entry_id", "name", "ix", "target_entry", "target_name", "item_id"],
        # (T224) where a non-official shown name / alias came from (a city layer, OSM, an ECCC station…). ADDITIVE,
        # so still core/1: a core without it shows no source (the app asks `HAS_NAME_SOURCE` first)
        "name_source": ["item_id", "name", "src", "label"],
        # (T182) each water's "near X" town(s) (build.core_near): a search's result line reads core alone. ADDITIVE,
        # so still core/1: a core without it is read the old way (near's place_water)
        "item_near": ["ord", "place_id", "ckm"],
        # (T247) the regions a water lies in and the management units it touches (build.core_area), comma-joined: search
        # by "2-8" / "Region 2" and the "Region 2 · MU 2-8" that tells two waters of one name apart. ADDITIVE, so still
        # core/1: a core without it searches by name and town only (the app asks `HAS_ITEM_AREA` first)
        "item_area": ["ord", "regions", "mus"],
        # (T796, owner 2026-11-16) a section's note, one sentence said once on its page ("Mostly outside Gladys Lake
        # Ecological Reserve: its closure (B.C. Synopsis p.9) does not apply here.", `bundle.outside_note`). ADDITIVE, so
        # still core/1: a core without it has no notes
        "section_note": ["sid", "note"],
        # (T927, owner 2026-11-19) the book's boundary places search finds (build.core_cut_place): a curated cut on its
        # water, its words, other names, whether the words name nothing of their own, and its PLACE (stream key +
        # metres, AGENTS 5; a map mark's microdegrees where one stands). ADDITIVE, so still core/1: a core without it
        # finds no places (the app asks `HAS_CUT_PLACE` first)
        "cut_place": ["ord", "words", "alt", "generic", "blk", "m", "lon", "lat"]}},
    # (T686) near/2: one row per town, its waters packed (`packed.py`: ords ascending as gaps, then ckm as byte planes).
    # near/1 was (place_id, ckm, ord), one row per town and water; (T975) no longer written or read
    "near": {"format": PACKED.NEAR_FORMAT, "kind": "sqlite", "tables": {
        "meta": ["k", "v"], "place_water": PACKED.NEAR_COLUMNS}},
    # (T182) the tables T129 measured riding in a file a moment did not otherwise need moved into files of their own:
    # `section_down` (down), `gauge_clim` (climate) out of conditions, `part_geo` (geo) out of water; and conditions
    # gained `lake_section` (build.lake_section), so it answers the lake road without core. The app reads a release cut
    # before the move the old way: a moved table is read from its old file when the manifest names no new one
    # (`app/packages/data/src/release/queries.ts` `MOVED_FROM`).
    "conditions": {"format": "conditions/1", "kind": "sqlite", "tables": {
        # `tidal` (T78), `backwater` (T160), `diversion` (T696): ADDITIVE, last, defaulted — still conditions/1; an
        # older release is read as 0 (no flag)
        "gauge": ["station", "name", "item_id", "sid", "lon", "lat", "area_km2", "mag", "matched_by", "match_m",
                  "tidal", "backwater", "diversion"],
        "lake_gauge": ["item_id", "station"],
        "lake_section": ["sid", "station"],
        "meta": ["k", "v"],
        # `rel`, `dist_b` (T93): where the station stands to the reach (a GAUGE_RELS index) and a channel-distance bucket
        # (shed.DIST_STEP). ADDITIVE, last, nullable — still conditions/1; an older release has neither and the app says
        # no distance
        "section_gauge": ["sid", "station", "trust", "mag", "rel", "dist_b"],
        # (T376) the watershed groups by name and by place (pipeline/deliver/bundle/basin_grid.py): ADDITIVE — still
        # conditions/1; a release cut before them answers no basin, and the app says so (`basinAt` throws)
        "basin": ["ix", "basin_id", "name"],
        "basin_frame": ["w", "s", "dlon", "dlat", "cols", "rows"],
        "basin_grid": ["row", "b"]}},
    # (T686) down/2: blocks of 4096 sids (`packed.py`), each row's down_sid, len and lake inside. down/1 was (sid,
    # down_sid, len, lake) per row (`len` T252, `lake` T296 additive in it); (T975) no longer written or read
    "down": {"format": PACKED.DOWN_FORMAT, "kind": "sqlite", "tables": {
        "meta": ["k", "v"], "section_down": PACKED.DOWN_COLUMNS}},
    # (T686) climate/2: the same table, `bands` clim/3 (clim/2's float16 bits transposed + delta, `packed.py`).
    # climate/1 carried clim/2 as it is; (T975) no longer written or read
    "climate": {"format": PACKED.CLIMATE_FORMAT, "kind": "sqlite", "tables": {
        "meta": ["k", "v"], "gauge_clim": ["station", "parameter", "bands"]}},
    # (T517) geo/2: a part is one connected run numbered in river order, so `part_stretch` is gone and `rank` (always
    # part_ix) left the table
    "geo": {"format": "geo/2", "kind": "sqlite", "tables": {
        "meta": ["k", "v"], "part_geo": ["ord", "part_ix", "mid_sid", "mid_lon", "mid_lat", "w", "s", "e", "n"]}},
    # (T517) water/3 (review finding 15): one part per connected run (`grp`, `run`; `lo_s` / `hi_s` its END TOKENS), part_ix renumbered in river
    # order; `part.hint_s` / `short_s`, `part_mark.down_stretch` / `up_stretches` / `label` (now `token`) and
    # `part_line.stretch` gone. An app reading
    # water/1 refuses a water/2 release and the other way round (no reader of the old schema is kept)
    "water": {"format": "water/3", "kind": "sqlite", "tables": {
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
        "part": ["item_ord", "part_ix", "akey", "ord", "label_s", "runs_s", "place_s", "km",
                 "closed_all_year", "paper_licence", "lo_s", "hi_s", "xp", "sections", "xruns", "grp",
                 "run"],
        # `confidence` (T207), `alt` (T517: the place's other names, a JSON list)
        "part_mark": ["ord", "mark_ix", "lon", "lat", "token", "down_part", "up_parts", "confidence", "alt"],
        # (T388) each part's own stretch of stream as runs on its blue lines (part_line.py): the place a named link
        # carries, so a named water never needs `lines`
        "part_line": ["ord", "part_ix", "blk", "m0", "m1"],
        # `m` (T387: the section's length along the water, metres; NULL unmeasured)
        "part_section": ["ord", "sid", "part_ix", "m"],
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
        # (T985) `lon` / `lat`: the mouth's place (microdegrees), where the map labels "Flows into <water>". ADDITIVE,
        # last, so still water/3: an app before it reads only the columns it names; a release cut before it has none,
        # and the app draws no mouth label (`@app/data/regs` navRows reads the row's own columns)
        "water_exit": ["ord", "down_ord", "down_part", "lon", "lat"],
        "xpart": _FRAME,
        "zdict": ["tbl", "d"]}},
    **{f"regs-{r}": {"format": REGS_REGION_FORMAT, "kind": "sqlite", "tables": _REGION_TABLES} for r in SH.REGIONS},
    # (T256) THE ANSWER SLICES (slices.py), web-only: per region, the rows of core / water a tap there reads, complete
    # for what they cover; the manifest marks each (`slice_of`, `region`) and a phone never installs one
    **{f"core-{r}": {"format": "core-slice/1", "kind": "sqlite", "tables": {
        "cover_sid": ["block", "b"],
        "item": ["ord", "item_id", "name", "kind", "part_of", "area_ha", "name_key", "size", "gauged", "has_rows",
                 "pieces"],
        "item_area": ["ord", "regions", "mus"], "item_near": ["ord", "place_id", "ckm"],
        "item_section": ["sid", "ord"], "meta": ["k", "v"], "name_source": ["item_id", "name", "src", "label"],
        "place": ["place_id", "osm", "name", "kind", "pop", "lat", "lon"],
        # (T796) ADDITIVE, still core-slice/1: the whole core's `section_note`, in every slice (slices.py)
        "section_note": ["sid", "note"]}} for r in SH.REGIONS},
    # (T370) a water slice holds only its region's own rows; the tables every region reads whole are `water-shared`
    **{f"water-{r}": {"format": "water-slice/4", "kind": "sqlite", "tables": {
        "item": ["item_ord", "item_id", "shown", "picker", "unresolved", "name", "kind", "sections", "outside_bc",
                 "entries", "steelhead", "x"],
        "meta": ["k", "v"],
        "part": ["item_ord", "part_ix", "akey", "ord", "label_s", "runs_s", "place_s", "km",
                 "closed_all_year", "paper_licence", "lo_s", "hi_s", "xp", "sections", "xruns", "grp",
                 "run"],
        "part_mark": ["ord", "mark_ix", "lon", "lat", "token", "down_part", "up_parts", "confidence", "alt"],
        "part_line": ["ord", "part_ix", "blk", "m0", "m1"],                        # (T388) by ord, as part_section
        "part_section": ["ord", "sid", "part_ix", "m"], "picker": _FRAME, "str": ["id", "text"],
        "water_continue": ["ord", "part", "next_ord", "next_part"],
        # (T985) ADDITIVE, still water-slice/4: the whole file's rows, columns and all (slices.py copies them)
        "water_exit": ["ord", "down_ord", "down_part", "lon", "lat"], "xpart": _FRAME}} for r in SH.REGIONS},
    # (T370) THE REGION-WIDE WATER TABLES, ONE WEB FILE: byte-identical in all nine water slices before (623 of ~780 kB
    # gz each, 2026-11-04); fetched once, a second region then costs only its own rows. Web-only (`slice_of` water).
    "water-shared": {"format": "water-shared/1", "kind": "sqlite", "tables": {
        "akey": ["akey", "ruleset", "licensing_set", "steelhead_water", "steelhead", "steelhead_rules",
                 "province_except", "home_region", "kind", "tidal", "seg", "mom", "nseg"],
        "akey_region": ["akey", "region"], "display_rule": _FRAME, "lic": ["lix", "j"], "meta": ["k", "v"],
        "place": ["place", "regions", "label", "names"], "rule": ["rix", "j"], "ruleset": ["set_id", "j"],
        "seglist": ["seg", "starts"], "segmom": ["mom", "ix"], "static": ["k", "j"], "str": ["id", "text"],
        "zdict": ["tbl", "d"]}},
    "regs-sid": {"format": "regs-sid/1", "kind": "sqlite", "tables": {
        # (T672 1b) `section_tidal`: 1 per tidal section (the bundle's `tidal`), so a key-less tidal section is told
        # from one outside B.C. ADDITIVE, so still regs-sid/1: a regs-sid without it is read as before (the app asks
        # `hasSectionTidal` first; a key-less section of such a release reads "outside B.C." as it always did)
        "meta": ["k", "v"], "section_akey": ["block", "b"], "section_place": ["block", "b"],
        "section_tidal": ["block", "b"]}},
    # (T369) every stream section's stream key and measure span, so a link's PLACE resolves to a section of this release
    # (release/lines.py; the app's regs/line.ts). Its own file, fetched only when a link is made or opened: in no
    # WEB_BUDGET moment. A release cut before it has none, and then no link carries a place.
    "lines": {"format": "lines/1", "kind": "sqlite", "tables": {
        "meta": ["k", "v"], "section_line": ["block", "n", "blk0", "m0", "b"]}},
}

#: FILES ONLY THE WEB FETCHES: a phone never installs or holds one (the manifest marks each `web_only`, the app's install
#: set leaves it out, and the offline total does not count it). The answer slices are web-only the same way, marked by
#: `slice_of` (slices.py). (T386, owner 2026-11-04) EMPTY NOW: `lines` was web-only (T369, links a web thing); a phone
#: holds it too, so an open unnamed reach is carried across a data update and a link's place resolves the same way on
#: both (+~1.3 MB gz on the offline total). The mechanism stays for a file that is the web's alone.
WEB_ONLY: frozenset = frozenset()

#: what the units of the transformed columns are (the app's decoders read these)
UNITS = {
    "geo.part_geo.mid_lon": "microdegrees", "geo.part_geo.mid_lat": "microdegrees",
    "geo.part_geo.w": "microdegrees", "geo.part_geo.s": "microdegrees", "geo.part_geo.e": "microdegrees",
    "geo.part_geo.n": "microdegrees",
    "water.part_mark.lon": "microdegrees", "water.part_mark.lat": "microdegrees",
    "water.water_exit.lon": "microdegrees", "water.water_exit.lat": "microdegrees",
    "climate.gauge_clim.bands": "clim/3: u8 N, N pentad bytes, then per p10/p25/p50/p75/p90 the N float16 bit deltas "
                                "(mod 2^16, the first from 0) as 5N low bytes then 5N high bytes; decoded, clim/2's "
                                "[u8 pentad, 5 x float16 LE] records (pipeline/deliver/release/packed.py)",
    "near.place_water.b": "near/2: LEB128 N, N x (ord - previous ord - 1, the first from -1), N ckm low bytes, N ckm high "
                          "bytes; read sorted by (ckm, ord) (pipeline/deliver/release/packed.py)",
    "down.section_down.b": "down/2: per block of 4096 sids, LEB128 N, N x sid gap (from block<<12 - 1), N x zz(down_sid "
                           "delta, from block<<12), N x (len + 1, 0 NULL), L, L x lake slot gap (from -1) "
                           "(pipeline/deliver/release/packed.py)",
    "flow-area": "flow-area/1: 'BCFA', u8 1, one LEB128 varint per paint section (sid order) = round(km2 x 1000)",
    "flow-small": "flow-small/1: 'BCFS', u8 1, one LEB128 varint per paint section (sid order) = bitmask over its panel's "
                  "members by ord: bit i set where member i drains at least the conditions meta's small_water_ratio times "
                  "the section's target area (show small_water_text beside that member's reading)",
    "paint": "paint/6: see pipeline/deliver/release/paint.py",
    "flow-own": "flow-own/1: 'BCFO', u8 1, one LEB128 varint per paint section (sid order) = bitmask over its panel's "
                "members by ord: bit i set where member i stands on the section's own water (along the water, across a "
                "water_continue, a lake on the way ends it: the bundle's section_own)",
    "access": "access/1: 'BCAX', u8 1, 8 bytes section_handles, then an access half (pipeline/deliver/release/paint.py)",
    "lakes": "lakes/1: 'BCLK', u8 1, 8 bytes section_handles, LEB128 asof+1, LEB128 count, then per section "
             "(sid ascending) LEB128 sid delta, u8 survey (0 none, 1 sheet, 2 digitised), LEB128 stocked day+1 "
             "(0 never); days since 1900-01-01 (pipeline/deliver/lake_facts.py)",
    "salmon": "salmon/4: 'BCSA', u8 4, 8 bytes section_handles, u32le JSON length + UTF-8 JSON (credit, default, "
              "regions (+ the header notes, the bucket's quota tree, T356 shared + sizes), verdicts (+ T332 an open day's "
              "quota tree, T356 c a header closure; u 1: both regions' words, no number; T796 f 1 on a closed verdict from "
              "DFO's Finfish closure: all fishing closed, trout included), gears, chains (r, w, l the "
              "places narrowest first, sp, g, m, + T356 c; T770 o another fish's closure lines (\"Eulachon closed all "
              "year: No fishing for eulachon\"), day runs of an index into gears (-1 none), apart from the gear lines g, "
              "absent where none; u the places on its water not drawn yet; T668 s on a section "
              "in two DFO regions: each region's places as indices into l, the most strict answer per fish speaks, and "
              "h the region slugs whose fishing-hours line shows; a per species day runs of the index into l of the "
              "place that answers), unmapped, scopes, x, shards; T549 off: every curated place no chain names, id -> "
              "{why, k, r, w, items}, shown as a text row on its water's page; p: each chain place's {r, water, sec}, "
              "what a row is named by; T796 a B.C. Synopsis closure is a place book:<entry_id>::<rule_id>[~n], first in "
              "the chains it closes, its p {r?, water?, static 1, book {rule, pages}}; static: each such place's year "
              "{sp: 5 runs of verdict indexes (-1 nothing), g}, kept by the app under a live feed) "
              "(pipeline/deliver/salmon.py, salmon_who.py)",
    **{SALMON.shard_name(r): "salmon-shard/1: 'BCSR', u8 1, 8 bytes section_handles, u8 region length + region, LEB128 "
                             "run count, then per run LEB128 sid gap, count, chain (pipeline/deliver/salmon.py)"
       for r in (*SH.REGIONS, SALMON.NO_REGION)},
    "method": "method/1: 'BCMT', u8 1, 8 bytes section_handles, LEB128 profile count, then per profile LEB128 run count + "
              "runs (length << 2 | code: 0 none or closed, 1 bait ban, 2 fly only; 366 days), then LEB128 run count and "
              "three columns sid gap[], count[], profile[] (pipeline/deliver/method.py)",
}
REGIONS = SH.REGIONS

#: (T182) the kind handicaps (km) a search row's "near X" weighs a town by — `@app/core` search.ts `HANDICAP_KM`, the
#: smallest distance + handicap wins (an unknown kind: `NEAR_HANDICAP_UNKNOWN_KM`). `build.core_near` ships each
#: water's winner(s) by them into core's `item_near`; emitted into pack.generated.ts and held EQUAL to the app's by
#: `packages/data/src/release/nearTown.test.ts`, so the two cannot drift apart.
NEAR_HANDICAP_KM = {"city": 0, "town": 0.5, "village": 3, "suburb": 3, "neighbourhood": 4, "quarter": 4,
                    "hamlet": 5, "locality": 6}
NEAR_HANDICAP_UNKNOWN_KM = 6

#: (T129) THE WEB REQUEST BUDGET. The release total (`build.OFFLINE_GATE`) is what a PHONE holds; a web visitor fetches
#: lazily (MINIMAL-PACKAGING §2, the app's `release/loader.ts`), and nothing gated those paths. Per moment of a first
#: visit: the release files it fetches, by manifest name ("manifest" is manifest.json itself; "mask" the tile set's
#: bc_outside.geojson, sent as is; "regs-{region}" the LARGEST regs-<region> file; every other file its gzip bytes),
#: and the most they may weigh. The file lists are what the browser walk pins (`app/tools/request-walk.mjs`,
#: `request-walk.pin.json`); `build.gate_problems` fails the release over a gate, and `app/tools/release-budget.test.ts`
#: holds the live release and the pin to it. Gates set 2026-10-24 from r-bb4331929428 with 10-15 % headroom (measured in
#: the comments; the T129 ticket's table). The sql.js engine (sql-wasm, 659 kB) rides with the first SQLite file opened
#: and is not a release file: it is not priced here.
#: (T182) re-cut for the split files. Measured by cutting the new files from r-98c43091c440's own (the builder's code
#: on that release's tables; T182 ticket): before → after, gates ~10-15 % over the after.
WEB_BUDGET = {
    # the coloured map, Map tab (measured 1,024,306: manifest 5,888 + paint 955,855 + mask 62,563)
    "first_paint": {"files": ["manifest", "paint", "mask"], "gate": 1_150_000},
    # (T182) THE COLD LOAD — what every web visit downloads unasked: first paint, then the idle prefetch (T186: core and
    # the region in view's file, priced as the largest region) and the Standard map's bait ban / fly only marks (`method`,
    # T213). The web reads `salmon` only on the first press of Salmon (owner 2026-10-28), so it is not here. The web's cap
    # on the release is this, not the release total. Measured on r-bda3fea28838: 2,981,794 (manifest 7,578 + paint
    # 956,798 + mask 62,563 + core 1,217,943 + regs-6 428,356 + method 308,556); the sql.js wasm (659 kB) rides along
    # and is not a release file.
    "cold_load": {"files": ["manifest", "paint", "mask", "core", "regs-{region}", "method"], "gate": 3_350_000},
    # the first search: core alone — each result's "near X" is core's `item_near` (was core + near: 3,082,554 →
    # 1,217,684)
    # (T247) + `item_area` (each water's regions and management units: search by "2-8" / "Region 2"): core 1,271,866 →
    # ~1,381,000 on r-0fa7927b16e5's tables, so 1,400,000 → 1,500,000
    # (T678, owner 2026-11-12, RULINGS O5: "raise now, shrink later"; T686) the D1 pond fold: core 1,388,309 → 1,508,127
    # gz on the T678 candidate (`item_section` +16,120 rows), so 1,500,000 → 1,650,000 (the first search ~120 kB more)
    "first_search": {"files": ["core"], "gate": 1_650_000},
    # the first tap on a NAMED water on the map, largest region: sid -> water (core), the page (water + the region
    # file), the water map's boxes (geo, read with the page's access note) and the tapped section's access note
    # (4,501,206 → 4,627,390, regs-6: core + 126 kB for item_near; part_geo moved, not added)
    # (T182, 2026-10-28) + salmon: the page's Salmon card (T211) reads it — the prefetch no longer hides it (5,053,227)
    # (owner 2026-10-28) the card reads the salmon head and its region's slice (`salmon-{region}`, priced as the largest,
    # R6 305,569): r-70f3ce0effc2 4,987,754 (was 5,061,679 with the whole file; an R2 tap reads 26 kB of salmon, not 405)
    # (T256) THE ANSWER SLICES (slices.py): the tap reads its region's `water-{region}` (priced as the largest, R6) instead
    # of the whole water — r-fa1915a56adb's tables: water 2,221,528 → water-6 858,978 (an R2 tap: water-2 777,894). Core
    # is the idle prefetch's whole file here (a visit on a good line has it before it taps); `first_tap_low` is the tap
    # with nothing ahead of it.
    # (T370) + `water-shared`: the slice's region-wide tables, now one file (the same bytes, two fetches)
    "first_tap": {"files": ["core", "water-{region}", "water-shared", "geo", "access", "regs-{region}", "salmon", "salmon-{region}"],
                  # (owner 2026-11-16 ~14:55, T796 A5: "but yes you can do the raise"; RULINGS O5, O10) 4,200,000 →
                  # 4,400,000: the book closures in the salmon head (+59,968) and salmon-6 (+53,225) on r-46e4355a7277's
                  # 4,198,757 → ~4,312,000
                  "gate": 4_400_000},
    # (T256) THE TAP WITH NOTHING AHEAD OF IT — Save-Data or the low-bandwidth mode (T254): no idle prefetch of core, the
    # Bait & fly marks (`method`) come with the tap; the tap reads its region's `core-{region}` + `water-{region}`, never the
    # whole files. Priced as the largest region (R6): ~3,090,000 (it was core + water + … ≈ 5,300,000)
    "first_tap_low": {"files": ["core-{region}", "water-{region}", "water-shared", "geo", "access", "regs-{region}", "salmon",
                                "salmon-{region}", "method"],
                      # (owner 2026-11-16 ~14:55, T796 A5; RULINGS O5, O10) 3,400,000 → 3,600,000: the same +113 kB on
                      # r-46e4355a7277's 3,359,382 → ~3,472,600
                      "gate": 3_600_000},
    # the first tap on an UNNAMED stream (most streams): + its key (regs-sid) and the flow pointers (down) the header's
    # "Tributary of ..." walks — no longer all of conditions (8,590,503 → 5,892,308, regs-6)
    # (T182, 2026-10-28) + salmon for the page's Salmon card, as the named tap (6,282,328)
    # (T256) water → the region's `water-{region}` (its rules, statics and keys are whole in every slice): ~6,715,000 →
    # ~5,550,000. The background prefetch (T253) reads this list as WHOLE files (it walks the province): `water`.
    "first_tap_unnamed": {"files": ["core", "water-{region}", "water-shared", "access", "regs-{region}", "regs-sid", "down", "salmon",
                                    "salmon-{region}"],
                          # (owner 2026-10-28) r-70f3ce0effc2 6,216,855 (was 6,290,780)
                          # (T252) + ~498 kB on `down` (closed downstream, named panel seeds, reach lengths: 624,913 →
                          # 1,122,957) ≈ 6,715,000: the gate 6,850,000 → 7,200,000
                          # (T256) water-{region}: 7,200,000 → 6,200,000
                          # (T686) down/2 (`down` 1,169,858 → 608,697): 5,473,323 on the side release of 2026-11-20.
                          # Kept at 6,200,000 while the live release (6,034,484, down/1) is the one
                          # `app/tools/release-budget.test.ts` holds to it; ~6,000,000 once a down/2 release is LATEST
                          "gate": 6_200_000},
    # the Conditions tab: gauges and the lake road (conditions' `lake_section`, so no core), the panels' areas; the
    # climatology (climate) waits for a chart (4,151,534 → 997,511)
    # (T823) + `flow-small` (21,396 B gz on the 2026-11-16 bundle, ~1,095,000 with it): the app reads it with flow-area
    # (the map's classes and the sheet's panel carry the small-water warning), and the walk's pin says so
    # (`app/tools/release-budget.test.ts` holds the pin to this list); under the 1,150,000 gate as it stands
    "conditions_open": {"files": ["flow-area", "flow-small", "conditions"], "gate": 1_150_000},
}
