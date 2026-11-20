"""`python -m pipeline.deliver release`: THE SHIPPED SET (P08, MINIMAL-PACKAGING §1–§2), written to
`data/release/<data_release>/` and nowhere else. It holds no regulation logic: it copies tables, cuts the region shards,
writes paint, derives three per-water flags for search, converts the climatology to float16, VACUUMs, gzips and hashes.

  file           holds                                                            source
  paint          the status index + the flow paint + the no-access rows (paint.py) status_index.bin + bundle
  access         the gentler access tiers: permit, reserve, private (T54)        bundle (section_access)
  core           item (+ size, gauged, has_rows), alias, item_alias, item_section, place, bundle
                 see_pointer (a group name that only points: search leads to the waters it names),
                 item_near (T182: the town each search row names, so the first search needs no `near`)
  near           place_water, one packed row per town: near/2 (T686, packed.py)   bundle
  water          the regs store's common tables + display_rule + akey_region,     regs store + bundle
                 part_section, part_mark, water_exit,
                 water_continue (which stream goes on as another: the bundle's
                 own table, bundle/continuation.py), part_line (T388: each part's
                 own stretch of stream, the place a named link carries —
                 part_line.py, from section_measure); meta `store_meta` = the regs
                 store's own meta, so water + the shards + regs-sid ARE the store
                 (store/release.py; nothing reads regs.sqlite, P20)
  regs-<region>  cell + the frames its akeys reach (shards.py), × 9               regs store
  regs-sid       section_akey, section_place, section_tidal (T672 1b)            regs store
  lines          section_line (T369: the section a link's place falls on, lines.py)  bundle (section_measure)
  conditions     gauge, section_gauge, lake_gauge, lake_section (T182: the lake   bundle
                 road per section, so Conditions needs no core); basin, basin_frame,
                 basin_grid (T376: the watershed groups' names, and which one a
                 point is in — bundle/basin_grid.py)
  down           section_down (T182: an unnamed tap's "Tributary of ..." walk),   bundle
                 in packed blocks of 4096 sids: down/2 (T686, packed.py)
  climate        gauge_clim, bands float16 transposed + delta: clim/3 (T686;      bundle
                 packed.py; T182: off the Conditions map)
  geo            part_geo (T182: "show on the map" without the whole water file), bundle
  flow-area      the panel target areas, one varint per paint section (paint.py)  bundle
  flow-small     (T823) per paint section, the panel members much bigger than it  bundle
  lakes          OPTIONAL, absent today: per lake section, surveyed / last        a lakes/1 file, only when
                 stocked — the Depth and Stocked modes' values (lakes/1)          handed one (`lakes=`); no feed
                                                                                  makes one yet (lake_facts.py)

Every SQLite file carries a `meta` table: format, section_handles, reach_digest (+ rule_ids_sha256 and the card-gate
stamp where they apply). Each file ships as `<name>-<sha12>.<ext>.gz` (gzip -9, no timestamp); the manifest
(`manifest.py`) lists url, bytes, gz bytes, sha256 (of the raw file) and format. Two runs on the same inputs write the
same bytes (`test_release.py` pins it).
"""
from __future__ import annotations

import gzip
import hashlib
import json
import re
import shutil
import sqlite3
import struct
import tempfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from pipeline.deliver import lake_facts as LF
from pipeline.deliver.release import contract as C
from pipeline.deliver.release import lines as LINES
from pipeline.deliver.release import packed as PACKED
from pipeline.deliver.release import part_line as PART_LINE
from pipeline.deliver.release import paint as PAINT
from pipeline.deliver.release import shards as SH
from pipeline.deliver.release import slices as SLICES

#: every file and the format a release WRITES (bumped when a reader must refuse an older one); (T686) near / down /
#: climate follow `packed.SHIP_PACKED_FORMATS` (`contract.written_files`), read at call time by `formats()`
def formats() -> Dict[str, str]:
    return {name: spec["format"] for name, spec in C.written_files().items()}


FORMATS = formats()
#: the gzip size gates (MINIMAL-PACKAGING §2; P08 test 7)
#: (T211) `salmon`: a provisional gate, set before any release carried the file (the owner's build measures it)
#: (T213) `method`: 308,222 gzipped measured on the live store 2026-10-27 (433,483 with closed days: over the total gate)
#: (T182) EVERY shipped file has a gate: the web fetches each file lazily (the moment that needs it, `contract.WEB_BUDGET`),
#: so a file's own size and the moments it rides in are what a visitor pays — a file with no gate fails the release, so a
#: new file (T199 runs, T200 stocking, T201/T218 depth) states its size when it is added. The gates T182 added are ~10-15 %
#: over r-bda3fea28838 (gz: water 2,196,090, near 1,991,899, climate 1,435,910, regs-sid 1,217,035, conditions 807,994,
#: down 624,863, geo 612,797, flow-area 188,672).
#: (owner 2026-10-28) `salmon` is split per bundle region: the head (payload, ~25 kB gz) and `salmon-<r>` (each region's
#: sid runs; measured on r-098f810f6e0a's sections: R6 277,024, R5 102,502, R1 51,828, R2 51,000, the rest < 25 kB)
#: (T252) `down` 720,000 → 1,250,000: the pointers closed downstream and seeded with named panel sections too
#: (107,731 → 141,324 rows; the added are mostly lakes) and each reach's length (`len`, 100 m): 624,913 → 1,122,957 gz,
#: measured on the 2026-10-28 bundle's atlas (full.prev, handles db853e9556df81a6). The first unnamed tap pays it
#: (`contract.WEB_BUDGET["first_tap_unnamed"]`).
#: (T678, owner 2026-11-12, RULINGS O5: "raise now, shrink later"; shrinking is T686) `core` 1,450,000 → 1,650,000 and
#: `conditions` 950,000 → 1,060,000: the D1 pond fold (16,119 ponds into their rivers' items) measured on the T678
#: candidate (atlas b5cb0aa4345a6af0) against r-483b295de57c — core 1,388,309 → 1,508,127 gz (`item_section` 65,237 →
#: 81,357 rows), conditions 916,049 → 963,726 gz (`section_gauge` 108,075 → 114,140 rows: a folded pond takes its river's
#: gauge). Each gate ~10 % over the measurement, as T182 set them.
#: (owner 2026-11-15, round 5: "Raise to 420 kB (Recommended)"; RULINGS O5 again, shrinking is T686) `salmon-6`
#: 320,000 → 420,000: the Stikine and Taku "including tributaries" places bound (92,460 sections off the region
#: default; no_extent in r-f749475d0962) measured 379,295 B gz on the 2026-11-15 hold-latest build (reach d8cefe97)
#: against LATEST's 314,700. The other regions keep 320,000.
#: (owner 2026-11-16, T796 A5, question dialog: "Raise to 100 kB (Recommended)"; RULINGS O5 "raise now, shrink later",
#: O9; shrinking is T686) `salmon` 60,000 → 100,000: the rule book's full closures first in every DFO-listed chain they
#: close (`deliver.salmon.with_book_closures`) — the head 31,847 → ~92,000 B gz on r-46e4355a7277's salmon step.
#: (owner 2026-11-16 ~14:55, T796 A5: "but yes you can do the raise"; RULINGS O5, O10) `salmon-6` 420,000 → 460,000: the
#: book places in every listed chain fragment Region 6's runs, +53,225 B gz on r-46e4355a7277's numbers (379,396 →
#: ~432,600).
#: (T686, owner 2026-11-16: "(a) Three lossless changes") ON THE PACKED PATH ONLY (`PACKED_GATES`, owner 2026-11-20:
#: formats later; the old forms keep `UNPACKED_GATES`): `near` 2,250,000 → 1,000,000, `down` 1,250,000 → 700,000,
#: `climate` 1,650,000 → 1,100,000: near/2, down/2, climate/2 (`packed.py`) measured on a side release of the live
#: bundle (reach 6ca9a300dad3d95e, 2026-11-20) against r-6258ffded676 — near 2,129,181 → 884,236, down 1,169,858 →
#: 608,697, climate 1,433,007 → 985,939 B gz, every value decoded equal; each gate ~11-15 % over, as T182 set them.
#: `salmon-6` stays 460,000: nothing here shrinks it (415,307 on r-6258ffded676, ~11 % under the gate; 420,000 would
#: leave 1 %).
#: (T686) the three files' gates follow the switch (`size_gates()`): the old forms keep today's gates until it flips
PACKED_GATES = {"near": 1_000_000, "down": 700_000, "climate": 1_100_000}
UNPACKED_GATES = {"near": 2_250_000, "down": 1_250_000, "climate": 1_650_000}
SIZE_GATES = {"paint": 1_000_000, "access": 300_000, "core": 1_650_000, "lakes": 100_000, "salmon": 100_000,
              **{f"salmon-{r}": 320_000 for r in (*SH.REGIONS, "x")}, "salmon-6": 460_000, "method": 600_000,
              # (T388) water + `part_line` (79,253 B gz measured alone on the 2026-11-04 bundle; water was 2,409,579 in
              # r-b86b60ca533b → ~2.49 MB, too close to 2.5 MB) → 2,650,000
              "water": 2_650_000, **UNPACKED_GATES, "regs-sid": 1_400_000, "conditions": 1_060_000, "geo": 700_000, "flow-area": 220_000,
              # (T823) `flow-small`: one bitmask varint per paint section, measured 2026-11-16 on the live bundle (141,189
              # sections): 21,396 B gz at x50 (x200: 27,444)
              "flow-small": 32_000,
              # (T679 #8) `flow-own`: one bitmask varint per paint section, as flow-small (measured below when built)
              "flow-own": 40_000,
              # (T973) `cw-through`: the carried units and (owner 13:22) each one's licence year as if the unit held:
              # 7,080 sections, 21 notes, 15 years, measured 76,015 B gz on the 2026-11-20 side bundle
              "cw-through": 120_000,
              # (T369) `lines`: ~0.99-1.08 MB gz estimated 2026-11-03 on the atlas's handles (ends proxied from the FWA
              # stream extents; the build measures the real one); (T386) phone-held too since 2026-11-04 (in the offline
              # total), fetched once a page is drawn (its place in the address) or when a link with a place is opened
              # measured 2026-11-03 on the batch-3 bundle: 1,283,173 B gz (the estimate was low) → 1,400,000
              "lines": 1_400_000, **{f"regs-{r}": 1_500_000 for r in SH.REGIONS},
              # (T256) the answer slices (slices.py), web-only: measured on r-fa1915a56adb's tables, core-<r> 197-287 kB,
              # water-<r> ~0.75 MB (rules, display rules and the dictionaries are whole in every one)
              # (T370) those moved to `water-shared` (623 kB measured 2026-11-04): water-<r> 1.1 MB → 400 kB (its own rows
              # measured 100-220 kB)
              **{f"core-{r}": 400_000 for r in SH.REGIONS}, **{f"water-{r}": 400_000 for r in SH.REGIONS},
              "water-shared": 850_000}
def size_gates() -> Dict[str, int]:
    """`SIZE_GATES` with near / down / climate at their packed gates when `packed.SHIP_PACKED_FORMATS` is on (call time)."""
    return {**SIZE_GATES, **(PACKED_GATES if PACKED.SHIP_PACKED_FORMATS else UNPACKED_GATES)}


#: (T182) The release total is no longer the web's cap — a web visitor never fetches the whole release (the cold load is
#: `WEB_BUDGET["cold_load"]`); it is what a PHONE holds offline, beside ~450 MB of tiles + basemap. It stays as a
#: runaway-growth backstop, raised 15 → 20 MB so the last band's files (T199 runs ~0.87 MB, T200, T201/T218) fit.
#: History: 14 → 15 MB, owner 2026-10-22 (Q55).
OFFLINE_GATE = 20_000_000

#: (T796) `section_note` (`bundle.outside_note`): a water's note said once on its page, e.g. "Mostly outside <reserve>"
CORE_TABLES = ("item", "alias", "item_alias", "item_section", "place", "see_pointer", "name_source", "section_note")
NEAR_TABLES = ("place_water",)
COND_TABLES = ("gauge", "section_gauge", "lake_gauge", "basin", "basin_frame", "basin_grid")
DOWN_TABLES = ("section_down",)
CLIMATE_TABLES = ("gauge_clim",)
WATER_BUNDLE_TABLES = ("part_section", "part_mark", "water_exit")
F16_MAX = 65504.0


class ReleaseError(SystemExit):
    pass


def _ddl(src: sqlite3.Connection, schema: str, tables: Iterable[str]) -> List[str]:
    out = []
    for t in tables:
        row = src.execute(f"SELECT sql FROM {schema}.sqlite_master WHERE type='table' AND name=?", (t,)).fetchone()
        if row is None:
            raise ReleaseError(f"release: {schema} has no table {t}")
        out.append(row[0])
        out += [s for (s,) in src.execute(f"SELECT sql FROM {schema}.sqlite_master WHERE type='index' AND tbl_name=? "
                                          "AND sql IS NOT NULL ORDER BY name", (t,))]
    return out


def _pk_order(con: sqlite3.Connection, schema: str, t: str) -> str:
    cols = [r for r in con.execute(f"PRAGMA {schema}.table_info({t})")]
    pk = [c[1] for c in sorted(cols, key=lambda c: c[5]) if c[5]]
    return ", ".join(pk or [c[1] for c in cols])


def _meta(con: sqlite3.Connection, rows: Dict[str, str]) -> None:
    con.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL) WITHOUT ROWID")
    con.execute("DELETE FROM meta")
    con.executemany("INSERT INTO meta VALUES (?, ?)", sorted(rows.items()))


def _finish(tmp: Path, out: Path) -> None:
    con = sqlite3.connect(tmp)
    con.execute("PRAGMA user_version = 0")
    con.commit()
    if out.exists():
        out.unlink()
    con.execute(f"VACUUM INTO '{out}'")
    con.close()
    tmp.unlink()


def copy_file(out: Path, sources: Dict[str, Path], plan: Iterable[tuple], meta: Dict[str, str],
              extra=None) -> Path:
    """Write `out` from `plan` = [(schema, table, where-or-None)] over the attached `sources` {schema: path}."""
    tmp = out.with_name(out.name + ".tmp")
    if tmp.exists():
        tmp.unlink()
    con = sqlite3.connect(tmp)
    for name, p in sources.items():
        con.execute(f"ATTACH ? AS {name}", (str(p),))          # read only by use: nothing here writes to it
    for schema, t, where in plan:
        for stmt in _ddl(con, schema, [t]):
            con.execute(stmt)
        con.execute(f"INSERT INTO main.{t} SELECT * FROM {schema}.{t} {where or ''} ORDER BY {_pk_order(con, schema, t)}")
    if extra:
        extra(con)
    _meta(con, meta)
    con.commit()
    for name in sources:
        con.execute(f"DETACH {name}")
    con.close()
    _finish(tmp, out)
    return out


def _core_flags(con: sqlite3.Connection) -> None:
    """Per-water facts search needs that live in other files (`entry`, `section_gauge`, `gauge`, `lake_gauge`), derived
    once here so `core` answers search alone: `size` (the biggest reach's stream magnitude), `gauged`, `has_rows`."""
    con.execute("ALTER TABLE item ADD COLUMN size INTEGER")
    con.execute("ALTER TABLE item ADD COLUMN gauged INTEGER NOT NULL DEFAULT 0")
    con.execute("ALTER TABLE item ADD COLUMN has_rows INTEGER NOT NULL DEFAULT 0")
    # search's two per-match subqueries (does it own a section, how many), answered once here (A5's budget)
    con.execute("ALTER TABLE item ADD COLUMN pieces INTEGER NOT NULL DEFAULT 0")
    con.execute("UPDATE item SET pieces = (SELECT count(*) FROM item_section s WHERE s.ord = item.ord)")
    con.execute("UPDATE item SET size = (SELECT max(sg.mag) FROM item_section s JOIN b.section_gauge sg ON sg.sid = s.sid "
                "WHERE s.ord = item.ord)")
    con.execute("UPDATE item SET gauged = (EXISTS (SELECT 1 FROM item_section s JOIN b.gauge g ON g.sid = s.sid "
                "WHERE s.ord = item.ord) OR EXISTS (SELECT 1 FROM b.lake_gauge lg WHERE lg.item_id = item.item_id))")
    # EVERY water a row matched (`entry.matched`), not only its first (`entry.item_id`): North Cameron's second
    # polygon and Norbury's are rowed waters too
    con.execute("CREATE TEMP TABLE rowed (item_id TEXT PRIMARY KEY) WITHOUT ROWID")
    con.execute("INSERT OR IGNORE INTO rowed SELECT j.value FROM b.entry e, json_each(e.matched) j")
    con.execute("UPDATE item SET has_rows = item_id IN (SELECT item_id FROM rowed)")
    con.execute("DROP TABLE rowed")
    # no name index on item / alias (T114): water SEARCH is a contains-LIKE on the search keys, which no index serves
    # (the old item_name_nc / alias_nc cost ~200 KB gzip and served nothing)
    con.execute("CREATE INDEX see_pointer_nc ON see_pointer(name COLLATE NOCASE)")
    # the case-sensitive name indexes are the bundle's; pointer search is a prefix LIKE: the NOCASE one serves it
    con.execute("DROP INDEX IF EXISTS item_by_name")
    con.execute("DROP INDEX IF EXISTS alias_by_text")
    # places: nothing in the app reads them by OSM id; search reads the name case-insensitively
    con.execute("DROP INDEX IF EXISTS place_by_osm")
    con.execute("DROP INDEX IF EXISTS place_by_name")
    con.execute("CREATE INDEX place_name_nc ON place(name COLLATE NOCASE)")
    # item_section keyed by the tap (sid → water, A1), with the one index the water → sections read needs
    con.execute("CREATE TABLE item_section_k (sid INTEGER NOT NULL, ord INTEGER NOT NULL, PRIMARY KEY (sid, ord)) "
                "WITHOUT ROWID")
    con.execute("INSERT INTO item_section_k SELECT sid, ord FROM item_section ORDER BY sid, ord")
    con.execute("DROP TABLE item_section")
    con.execute("ALTER TABLE item_section_k RENAME TO item_section")
    con.execute("CREATE INDEX section_by_item ON item_section(ord)")
    core_near(con)
    core_area(con)


SECTION_NOTE_DDL = "CREATE TABLE section_note (sid INTEGER PRIMARY KEY, note TEXT NOT NULL) WITHOUT ROWID"
NAME_SOURCE_DDL = ("CREATE TABLE name_source (item_id TEXT NOT NULL, name TEXT NOT NULL, src TEXT NOT NULL, "
                   "label TEXT NOT NULL, PRIMARY KEY (item_id, name)) WITHOUT ROWID")


def _core_flags_empty_name_source(con: sqlite3.Connection) -> None:
    """`_core_flags`, for a bundle cut before T224's `name_source`: the table, empty."""
    con.execute(NAME_SOURCE_DDL)
    _core_flags(con)


def core_near(con: sqlite3.Connection, schema: str = "b") -> None:
    """(T182) `item_near`: per water, the town(s) a search row's "near X" names, so the first search reads core alone,
    not the 2 MB `near` file. The app's `describeBy` (`@app/core` search.ts) names the ring town with the smallest
    distance + kind handicap; this ships every town AT that smallest cost, in exact hundredths of a km
    (`contract.NEAR_HANDICAP_KM`, held equal to the app's by a test), and the app's `describeBy` still picks among them
    (a tie is its name order to break) — so the row reads exactly as it did from every ring, at one row per water. A
    town with no position never names a water (the app's ring read skips it). `schema` holds `place_water`; `place` is
    core's own."""
    cost = " ".join(f"WHEN '{k}' THEN {int(round(v * 100))}" for k, v in sorted(C.NEAR_HANDICAP_KM.items()))
    con.execute("CREATE TABLE item_near (ord INTEGER NOT NULL, place_id INTEGER NOT NULL, ckm INTEGER NOT NULL, "
                "PRIMARY KEY (ord, place_id)) WITHOUT ROWID")
    con.execute(f"""
        WITH ring AS (SELECT pw.ord, pw.place_id, pw.ckm,
                             pw.ckm + (CASE p.kind {cost} ELSE {int(round(C.NEAR_HANDICAP_UNKNOWN_KM * 100))} END) AS c
                      FROM {schema}.place_water pw JOIN main.place p ON p.place_id = pw.place_id AND p.lat IS NOT NULL),
             best AS (SELECT ord, min(c) AS m FROM ring GROUP BY ord)
        INSERT INTO item_near SELECT r.ord, r.place_id, min(r.ckm) FROM ring r JOIN best USING (ord)
        WHERE r.c = best.m GROUP BY r.ord, r.place_id ORDER BY r.ord, r.place_id""")


def _region_key(r: str) -> tuple:
    """Regions in the book's order: 1..6, 7A, 7B, 8."""
    return (int(r[0]), r[1:]) if r[:1].isdigit() else (99, r)


def _mu_key(mu: str) -> tuple:
    """Management units numerically: 2-8 before 2-18."""
    try:
        a, b = mu.split("-", 1)
        return (int(a), int(b))
    except ValueError:
        return (99, 0)


def core_area(con: sqlite3.Connection, schema: str = "b") -> None:
    """(T247) `item_area`: per water, the regions it lies in and the management units it touches — the union over its
    sections of the bundle's own `section_ruleset.place` / `.mus` (`region_place.regions`, `mu_set.codes`) — so search can
    answer "2-8" and "Region 2" and tell two Elk Rivers apart by where they are, from core alone. Comma-joined, in the
    book's order. ADDITIVE: a bundle without those tables (a hand-built one, cut before them) gives no table, and the app
    searches without areas. `schema` holds the bundle; `item_section` is core's own."""
    have = {r[0] for r in con.execute(f"SELECT name FROM {schema}.sqlite_master WHERE type='table'")}
    if not {"section_ruleset", "region_place", "mu_set"} <= have:
        return
    if not {"place", "mus"} <= {r[1] for r in con.execute(f"PRAGMA {schema}.table_info(section_ruleset)")}:
        return
    con.execute("CREATE TABLE item_area (ord INTEGER PRIMARY KEY, regions TEXT NOT NULL, mus TEXT NOT NULL) "
                "WITHOUT ROWID")
    regions: Dict[int, set] = {}
    mus: Dict[int, set] = {}
    for ord_, rg, mu in con.execute(
            f"SELECT DISTINCT s.ord, r.regions, m.codes FROM main.item_section s "
            f"JOIN {schema}.section_ruleset q ON q.sid = s.sid "
            f"LEFT JOIN {schema}.region_place r ON r.place = q.place LEFT JOIN {schema}.mu_set m ON m.mus = q.mus"):
        regions.setdefault(ord_, set()).update(x for x in (rg or "").split(",") if x)
        mus.setdefault(ord_, set()).update(x for x in (mu or "").split(",") if x)
    con.executemany("INSERT INTO item_area VALUES (?, ?, ?)",
                    [(o, ",".join(sorted(regions.get(o, ()), key=_region_key)), ",".join(sorted(mus.get(o, ()), key=_mu_key)))
                     for o in sorted(set(regions) | set(mus))])


#: (T927 Pitfalls) a place whose words NAME nothing of their own ("lower falls", "boundary signs", "tidal boundary (DFO
#: Subarea 3-18)"): no capital letter outside its brackets. Search offers it only with its water's name in the query.
_BRACKETS = re.compile(r"\([^)]*\)")


def cut_words(text: str) -> str:
    """(T927) A cut's words as search shows them, spaces collapsed. (The part ends' words carry no confidence clause since
    T780: `display.ends` says the place alone.)"""
    return re.sub(r"\s+", " ", text or "").strip()


def cut_generic(words: str) -> bool:
    """(T927) The words name no place of their own: no capital letter outside brackets ("lower falls")."""
    return not any(c.isupper() for c in _BRACKETS.sub(" ", words))


def core_cut_place(con: sqlite3.Connection, ends: Dict[str, List[str]], schema: str = "b") -> List[str]:
    """(T927, owner 2026-11-19 23:49) `cut_place`: THE BOOK'S BOUNDARY PLACES search finds — every curated cut (the
    bundle's `cut_place`) on each named water it stands on: `ord` the water, `words` what the place is called (the part
    ends' own words, `display.ends`; the `split` name where nothing ends there), `alt` its other
    names (JSON, NULL: none), `generic` 1 when the words name nothing of their own (`cut_generic`: search wants the
    water's name too), `blk` / `m` THE PLACE (the FWA stream key and metres along it, AGENTS 5 — the app opens the water
    at the part holding it, `@app/ui` place.ts), `lon` / `lat` in microdegrees where a map mark stands there (the
    bundle's `part_mark.token`), else NULL. The waters: the split's main-stem positions (`split.at`), else the waters
    whose sections hold the place; a cut on no water is COUNTED AND PRINTED (T927 review: never dropped silently).
    ADDITIVE, still core/1: a bundle without `cut_place` gives no table, and search finds no places. `schema` holds the
    bundle; `item` / `item_section` are core's own. Returns the split ids on no water."""
    have = {r[0] for r in con.execute(f"SELECT name FROM {schema}.sqlite_master WHERE type='table'")}
    if "cut_place" not in have:
        return
    con.execute("CREATE TABLE cut_place (ord INTEGER NOT NULL, words TEXT NOT NULL, alt TEXT, generic INTEGER NOT NULL, "
                "blk INTEGER NOT NULL, m INTEGER NOT NULL, lon INTEGER, lat INTEGER, PRIMARY KEY (ord, blk, m, words)) "
                "WITHOUT ROWID")
    ord_of = dict(con.execute("SELECT item_id, ord FROM main.item"))
    mark: Dict[str, Tuple[int, int]] = {}
    if "part_mark" in have and "token" in {r[1] for r in con.execute(f"PRAGMA {schema}.table_info(part_mark)")}:
        for tok, lon, lat in con.execute(f"SELECT token, lon, lat FROM {schema}.part_mark WHERE token IS NOT NULL "
                                         "ORDER BY ord, mark_ix"):
            mark.setdefault(tok, (int(round(lon * 1_000_000)), int(round(lat * 1_000_000))))
    rows: set = set()
    unplaced: List[str] = []
    for sid, blk, m, name, at, same in con.execute(
            f"SELECT c.split_id, c.blk, c.m, s.name, s.at, s.same_place_as FROM {schema}.cut_place c "
            f"JOIN {schema}.split s ON s.split_id = c.split_id ORDER BY c.split_id"):
        said = [w for w in (ends.get(sid) or []) if not w.startswith("to:")]
        words = cut_words(said[0] if said else name)
        if not words:
            raise ReleaseError(f"cut_place: {sid} has no words")
        alt = []
        for w in [*said[1:], name]:
            w = cut_words(w)
            if w and w.lower() != words.lower() and w.lower() not in (a.lower() for a in alt):
                alt.append(w)
        ords = sorted({ord_of[i] for i, _ in json.loads(at or "[]") if i in ord_of})
        if not ords:                     # no main-stem position: the waters whose sections hold it, a stream first
            held = con.execute(
                f"SELECT DISTINCT s.ord, i.kind = 'stream' FROM {schema}.section_measure sm JOIN main.item_section s "
                f"ON s.sid = sm.sid JOIN main.item i ON i.ord = s.ord WHERE sm.blk = ? AND sm.from_m <= ? AND ? <= sm.to_m",
                (blk, m, m)).fetchall()
            ords = sorted({o for o, st in held if st} or {o for o, _ in held})
        if not ords:
            unplaced.append(sid)
        lon, lat = mark.get(sid) or (mark.get(same) if same else None) or (None, None)
        for o in ords:
            # generic by the cut's own label (`split.name`): the part words capitalise their first letter
            rows.add((o, words, json.dumps(alt, ensure_ascii=False) if alt else None, int(cut_generic(name)),
                      blk, m, lon, lat))
    con.executemany("INSERT OR IGNORE INTO cut_place VALUES (?, ?, ?, ?, ?, ?, ?, ?)", sorted(rows, key=lambda r: r[:6]))
    print(f"  cut_place: {len(rows)} row(s); {len(unplaced)} curated cut(s) on no water, not searchable"
          + (f": {', '.join(unplaced)}" if unplaced else ""))
    return unplaced


def display_ends(store: sqlite3.Connection) -> Dict[str, List[str]]:
    """(T927) The regs store's `display.ends` (T517: {token: [words, other names…]}), decoded."""
    from pipeline.deliver.store.common import unpack
    row = store.execute("SELECT j FROM static WHERE k = 'display.ends'").fetchone()
    if row is None:
        raise ReleaseError("release: the regs store has no static display.ends — the cut places cannot be named")
    zd = dict(store.execute("SELECT tbl, d FROM zdict"))
    return json.loads(unpack(row[0], zd.get("static")))


def cw_through(con: sqlite3.Connection, schema: str = "b", bundle: Optional[Path] = None, year_of=None) -> None:
    """(T973, owner 2026-11-20 12:11 + 13:22) `cw-through/1`: the bundle's `designation_carried`, as the app draws it.
    Per section (`section_cw`): its note (`cw_note`: the carried designation's licence unit name and class, its period
    — the record's `when`, JSON, NULL all year — and the rowed no-unit waters passed, `through` JSON [[item_id, name], …]
    up from the classified water) and its LICENCE YEAR (`cw_year`: JSON [[start_day, licence value], …], the licence
    frame's own shape, `answers.licence.carried_year` — the section's answer as if the carried designation held, so the
    card draws an ordinary Classified Waters row). Python resolves; the app selects the day and words the why.
    `year_of(sid, source_sid, designation, set_id)` computes a year (default: `carried_year` over `bundle`). A bundle
    without the table is refused."""
    if not con.execute(f"SELECT 1 FROM {schema}.sqlite_master WHERE type = 'table' AND name = 'designation_carried'"
                       ).fetchone():
        raise ValueError("cw-through: the bundle has no designation_carried table (T973); rebuild it: "
                         "python -m pipeline.deliver bundle")
    con.execute("CREATE TABLE cw_note (ix INTEGER PRIMARY KEY, unit_name TEXT NOT NULL, classified TEXT NOT NULL, "
                "dates TEXT, through TEXT NOT NULL)")
    con.execute("CREATE TABLE cw_year (yx INTEGER PRIMARY KEY, j TEXT NOT NULL)")
    con.execute("CREATE TABLE section_cw (sid INTEGER PRIMARY KEY, ix INTEGER NOT NULL, yx INTEGER NOT NULL) WITHOUT ROWID")
    rows = con.execute(
        f"SELECT c.sid, d.unit_name, d.classified, json_extract(d.record, '$.when'), c.through, c.entry_id, "
        f"c.designation_id, (SELECT sl.set_id FROM {schema}.section_licensing sl WHERE sl.sid = c.sid) "
        f"FROM {schema}.designation_carried c JOIN {schema}.designation d "
        f"ON d.entry_id = c.entry_id AND d.designation_id = c.designation_id ORDER BY c.sid").fetchall()
    (n,) = con.execute(f"SELECT count(*) FROM {schema}.designation_carried").fetchone()
    if len(rows) != n:
        raise ValueError(f"cw-through: {n - len(rows)} carried unit(s) name a designation the bundle does not hold")
    source: Dict[str, int] = {}
    for e, d in sorted({(r[5], r[6]) for r in rows}):
        got = con.execute(f"SELECT min(sid) FROM {schema}.designation_section WHERE entry_id = ? AND designation_id = ? "
                          f"AND via = 'reach'", (e, d)).fetchone()[0] or con.execute(
            f"SELECT min(sid) FROM {schema}.designation_section WHERE entry_id = ? AND designation_id = ?", (e, d)
        ).fetchone()[0]
        if got is None:
            raise ValueError(f"cw-through: the carried designation {e}#{d} binds no section of its own")
        source[f"{e}#{d}"] = got
    if year_of is None:
        if bundle is None:
            raise ValueError("cw-through: no bundle to compute the carried licence years from")
        from pipeline.deliver.answers import licence as LA
        from pipeline.deliver.answers.common import connect
        bdb = connect(str(bundle))
        C, P, days, contested, memo = LA.corpus(bdb), LA.profiles(), LA.change_days(bdb), LA.contested_sets(bdb), {}

        def year_of(sid, source_sid, designation, set_id):
            return LA.carried_year(bdb, C, sid, source_sid, designation, set_id, None, days, P, contested, memo)
    notes: Dict[tuple, int] = {}
    years: Dict[str, int] = {}
    by_group: Dict[tuple, int] = {}
    out = []
    from pipeline.deliver.answers.licence import section_licence_keys
    keys = section_licence_keys(con, (r[0] for r in rows), schema)
    for sid, unit_name, cls, when, through, e, d, set_id in rows:
        key = (unit_name, cls, when if when not in ("{}", "null") else None, through)
        ix = notes.setdefault(key, len(notes))
        # every section of one LICENCE KEY answers alike (T973 review, LF): the whole key — licensing set, province
        # exceptions, tidal, water kind, a keyed rule set — never the licensing set alone (a lake, a wetland or a tidal
        # section carried beside a stream would take the stream's year)
        g = (keys[sid], f"{e}#{d}")
        if g not in by_group:
            y = year_of(sid, source[g[1]], g[1], set_id)
            j = json.dumps([[k, y[k]] for k in sorted(y)], separators=(",", ":"), sort_keys=True)
            by_group[g] = years.setdefault(j, len(years))
        out.append((sid, ix, by_group[g]))
    con.executemany("INSERT INTO section_cw VALUES (?, ?, ?)", out)
    con.executemany("INSERT INTO cw_note VALUES (?, ?, ?, ?, ?)", [(ix, *k) for k, ix in notes.items()])
    con.executemany("INSERT INTO cw_year VALUES (?, ?)", [(yx, j) for j, yx in years.items()])


def lake_section(con: sqlite3.Connection, schema: str = "b") -> None:
    """(T182) `lake_section`: THE LAKE ROAD per section — the lowest station over every gauged lake holding it (the
    rule the app's `lakeStationsFor` and the Conditions map's lake classes apply), so Conditions opens without core.
    `schema` holds `item`, `item_section` and `lake_gauge`."""
    con.execute("CREATE TABLE lake_section (sid INTEGER PRIMARY KEY, station TEXT NOT NULL)")
    con.execute(f"INSERT INTO lake_section SELECT s.sid, min(lg.station) FROM {schema}.item_section s "
                f"JOIN {schema}.item i ON i.ord = s.ord JOIN {schema}.lake_gauge lg ON lg.item_id = i.item_id "
                "GROUP BY s.sid ORDER BY s.sid")


def geo_e6(con: sqlite3.Connection) -> None:
    """`part_geo` (copied from the bundle in degrees) as integer microdegrees (~0.1 m): a REAL is 8 bytes, these ~4."""
    con.execute("CREATE TABLE part_geo_e6 (ord INTEGER NOT NULL, part_ix INTEGER NOT NULL, "
                "mid_sid INTEGER NOT NULL, mid_lon INTEGER NOT NULL, mid_lat INTEGER NOT NULL, w INTEGER, "
                "s INTEGER, e INTEGER, n INTEGER, PRIMARY KEY (ord, part_ix)) WITHOUT ROWID")
    e6 = lambda c: f"CAST(round({c} * 1000000) AS INTEGER)"  # noqa: E731
    con.execute(f"INSERT INTO part_geo_e6 SELECT ord, part_ix, mid_sid, {e6('mid_lon')}, {e6('mid_lat')}, "
                f"{e6('w')}, {e6('s')}, {e6('e')}, {e6('n')} FROM part_geo ORDER BY ord, part_ix")
    con.execute("DROP TABLE part_geo")
    con.execute("ALTER TABLE part_geo_e6 RENAME TO part_geo")


def clim_f16(bands: bytes) -> bytes:
    """[u8 pentad, 5 × f32] records → [u8 pentad, 5 × f16] (clim/2). A value past float16's range is refused."""
    if len(bands) % 21:
        raise ReleaseError(f"release: a gauge_clim blob of {len(bands)} bytes is not whole records")
    out = bytearray()
    for i in range(0, len(bands), 21):
        vals = struct.unpack_from("<5f", bands, i + 1)
        if any(abs(v) > F16_MAX for v in vals):
            raise ReleaseError(f"release: a climatology value {max(vals)} does not fit float16")
        out.append(bands[i])
        out += struct.pack("<5e", *vals)
    return bytes(out)


def _climate_f16(con: sqlite3.Connection) -> None:
    """float32 → float16 (clim/2): climate/1, what a release writes while `packed.SHIP_PACKED_FORMATS` is off."""
    rows = con.execute("SELECT rowid, bands FROM gauge_clim").fetchall()
    con.executemany("UPDATE gauge_clim SET bands = ? WHERE rowid = ?", [(clim_f16(b), r) for r, b in rows])


def _climate_extra(con: sqlite3.Connection) -> None:
    """float32 → float16 (clim/2), then transposed + delta (clim/3, T686: the same float16 bits, `packed.py`)."""
    rows = con.execute("SELECT rowid, bands FROM gauge_clim").fetchall()
    con.executemany("UPDATE gauge_clim SET bands = ? WHERE rowid = ?", [(PACKED.clim3(clim_f16(b)), r) for r, b in rows])



def lakes_bytes(raw: bytes, handles: str) -> bytes:
    """The lake facts as shipped (the file IS the release file): refused unless built against this release's atlas."""
    got = LF.handles_of(raw)
    if got != handles:
        raise ReleaseError(f"release: the lake facts are from another atlas ({got}, the bundle's is {handles})")
    LF.decode(raw)                                  # whole, or refused here rather than in the app
    return raw


def _sha(p: Path, n: Optional[int] = None) -> str:
    h = hashlib.sha256(p.read_bytes()).hexdigest()
    return h[:n] if n else h


def sizes(release_dir: Path) -> Dict[str, dict]:
    """{file: {bytes, gz}} from a release's manifest (the size gates read this)."""
    files = json.loads((Path(release_dir) / "manifest.json").read_text())["files"]
    return {k: {"bytes": v["bytes"], "gz": v["gz"]} for k, v in sorted(files.items())}


def gate_problems(release_dir: Path) -> List[str]:
    """The size gates (P08 test 7): each file's gzip under its gate (a file with none fails: T182), the web moments
    (`contract.WEB_BUDGET`), and the offline total (what a phone holds)."""
    sz, gates = sizes(release_dir), size_gates()
    out = [f"{k}: no size gate (build.SIZE_GATES)" for k in sz if k not in gates]
    out += [f"{k}: {v['gz']:,} B gzip > {gates[k]:,}" for k, v in sz.items() if k in gates and v["gz"] > gates[k]]
    # (T256) the answer slices are web-only (a phone installs the whole files), (T369) and `lines`: not what a phone holds
    total = sum(v["gz"] for k, v in sz.items() if k not in SLICES.slice_marks() and k not in C.WEB_ONLY)
    if total > OFFLINE_GATE:
        out.append(f"offline total: {total:,} B gzip > {OFFLINE_GATE:,}")
    return out + [f"web {k}: {n:,} B > {C.WEB_BUDGET[k]['gate']:,} ({', '.join(f)})"
                  for k, (n, f) in web_moments(release_dir).items() if n > C.WEB_BUDGET[k]["gate"]]


def web_moments(release_dir: Path) -> Dict[str, tuple]:
    """(T129) {moment: (bytes a web visitor fetches for it, the files priced)} for every `contract.WEB_BUDGET` moment:
    a file's gzip bytes from the manifest, the manifest's own size, the tile set's mask as sent (`tiles.mask.bytes`), and
    `regs-{region}` as the largest region file."""
    d = Path(release_dir)
    m = json.loads((d / "manifest.json").read_text())
    files = m["files"]
    region = max((k for k in files if k.startswith("regs-") and k != "regs-sid"), key=lambda k: files[k]["gz"])
    # (owner 2026-10-28) a page's Salmon card reads its region's salmon slice: priced as the largest
    salmon_r = max((k for k in files if k.startswith("salmon-")), key=lambda k: files[k]["gz"], default="salmon-{region}")

    def price(name: str) -> tuple:
        if name == "manifest":
            return name, (d / "manifest.json").stat().st_size
        if name == "mask":
            return name, int(((m.get("tiles") or {}).get("mask") or {}).get("bytes") or 0)
        name = region if name == "regs-{region}" else salmon_r if name == "salmon-{region}" else name
        if name in ("core-{region}", "water-{region}"):                # (T256) the largest region's slice
            stem = name.split("-")[0]
            name = max((k for k in files if k in {f"{stem}-{r}" for r in SH.REGIONS}), key=lambda k: files[k]["gz"],
                       default=name)                              # (T370) never `water-shared`
        if name == "salmon-{all}":                               # the Salmon map: every slice
            return name, sum(int(f["gz"]) for k, f in files.items() if k.startswith("salmon-"))
        return name, int(files[name]["gz"]) if name in files else 0
    out = {}
    for k, spec in C.WEB_BUDGET.items():
        priced = [price(f) for f in spec["files"]]
        out[k] = (sum(n for _, n in priced), [f for f, _ in priced])
    return out


def mark_token(down_hi: Optional[str], up_lo: Optional[str], down_side: bool = False, up_side: bool = False,
               down_keyed: bool = True, up_keyed: bool = True) -> Optional[str]:
    """(T517 review finding 2) THE PLACE A MARK STANDS AT, as an end token — never stored words:
      * the token the lower part's upper end and the upper part's lower end share (the answers say both ends of a
        place alike, `builders._names`);
      * a side that ships no ends (an UNKEYED part: outside B.C., tidal water) — the other side's end there;
      * across a lake (the lower one's top its outlet, the upper one's bottom its inlet) — the upper one's (the lake);
      * two main-stem parts whose ends are two cuts at one place (Little Qualicum's signs 35 m below the hatchery
        fence and the fence) — the lower part's upper end;
      * else None — and the release ships the token the parts' RUNS share there (`topology.mark_words`, the bundle's
        `part_mark.token`; a part with no main stem ships no ends of its own), or none: a side channel meeting its
        river at no named place."""
    if down_hi is not None and down_hi == up_lo:
        return down_hi
    if not up_keyed and down_hi is not None:
        return down_hi
    if not down_keyed and up_lo is not None:
        return up_lo
    if (down_hi or "").startswith("lake_outlet") and (up_lo or "").startswith("lake_inlet"):
        return up_lo
    if down_side or up_side or down_hi is None or up_lo is None:
        return None
    return down_hi


def mark_tokens(con: sqlite3.Connection) -> Dict[Tuple[int, int], str]:
    """{(ord, mark_ix): token} for the water file's marks (`mark_token` over each up part, the first that says one)."""
    s = dict(con.execute("SELECT id, text FROM str"))
    part = {(o, ix): (s.get(lo), s.get(hi), km is None, akey is not None) for o, ix, lo, hi, km, akey in con.execute(
        "SELECT item_ord, part_ix, lo_s, hi_s, km, akey FROM part")}
    out: Dict[Tuple[int, int], str] = {}
    for o, k, d, ups in con.execute("SELECT ord, mark_ix, down_part, up_parts FROM part_mark"):
        dn = part[(o, d)]
        for u in json.loads(ups):
            up = part[(o, u)]
            t = mark_token(dn[1], up[0], dn[2] and dn[3], up[2] and up[3], dn[3], up[3])
            if t is not None:
                out[(o, k)] = t
                break
    return out


def bundle_continues(b: sqlite3.Connection) -> List[tuple]:
    """`water_continue` as the bundle wrote it (`pipeline.deliver.bundle.continuation`): copied, never derived here — the
    release reads nothing past the bundle (audit F07). A bundle without the table predates it and is refused."""
    if not b.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'water_continue'").fetchone():
        raise ReleaseError("release: the bundle has no water_continue table — it predates the continuation links; "
                           "rebuild it: python -m pipeline.deliver bundle")
    return b.execute("SELECT ord, part, next_ord, next_part FROM water_continue ORDER BY ord").fetchall()


def bundle_section_measure(b: sqlite3.Connection) -> List[tuple]:
    """(T369) `section_measure` as the bundle wrote it (`bundle/section_measure.py`, from the atlas graph): copied into
    regs-sid `section_line`, never derived here (audit F07). A bundle without the table predates it and is refused."""
    if not b.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'section_measure'").fetchone():
        raise ReleaseError("release: the bundle has no section_measure table — it predates the place links (T369); "
                           "rebuild it: python -m pipeline.deliver bundle")
    return b.execute("SELECT sid, blk, from_m, to_m FROM section_measure ORDER BY sid").fetchall()


def build(*, bundle: Path, store: Path, status_index: Path, export_dir: Path, out_root: Path,
          data_release: Optional[str] = None, tiles: Optional[dict] = None, basemap: Optional[dict] = None,
          lakes: Optional[Path] = None, sources: Optional[dict] = None, synopsis: Optional[Path] = None,
          reports: Optional[Path] = None) -> Path:
    """Write the release; return its directory (only the shipped files + manifest.json). `tiles`/`basemap`/`sources`
    are the manifest's blocks (manifest.py); `lakes` an optional lakes/1 file (pipeline/deliver/lake_facts.py), shipped
    as the file `lakes`; None (the default, and `deliver all`) ships none. Reads nothing past the bundle: the continuation
    links are the bundle's own `water_continue`. `reports` (T936): the build reports' root — the release's own lists are
    written to `<reports>/<release id>/` (never shipped); None writes none (the warnings are still printed)."""
    from pipeline.deliver.answers.card_check import STAMP_NAME, stamp_problems
    answers = export_dir / "ui-rules-answers.json"
    bad = stamp_problems(answers, answers.read_bytes())
    if bad:
        raise ReleaseError("release: the answers are not proved by the card gate — " + "; ".join(bad))
    stamp = json.loads((export_dir / STAMP_NAME).read_text())["card_check"]
    b = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    st = sqlite3.connect(f"file:{store}?mode=ro", uri=True)
    bmeta, smeta = dict(b.execute("SELECT k, v FROM meta")), dict(st.execute("SELECT k, v FROM meta"))
    if bmeta.get("section_handles") != smeta.get("section_handles"):
        raise ReleaseError("release: the bundle and the regs store are from different atlases")
    if smeta.get("answers_sha256") != stamp["answers_sha256"]:
        raise ReleaseError("release: the regs store holds other answers than the card gate proved")
    base = {"section_handles": bmeta["section_handles"], "reach_digest": bmeta["reach_digest"]}
    regs = {**base, "rule_ids_sha256": smeta["rule_ids_sha256"]}
    continues = bundle_continues(b)
    # (T369) every stream section's span on its blue line: packed into `lines`, and (T388) each named part's runs of it
    # into the water file's `part_line`
    sm = bundle_section_measure(b)
    part_lines = PART_LINE.from_bundle(b, sm)

    work = Path(tempfile.mkdtemp(prefix=".release-", dir=out_root if out_root.exists() else None))
    try:
        files: Dict[str, Path] = bundle_files(work, bundle, b, base, status_index.read_bytes(),
                                              ends=display_ends(st))
        if lakes is not None:
            p = work / "lakes.bin"
            p.write_bytes(lakes_bytes(lakes.read_bytes(), base["section_handles"]))
            files["lakes"] = p
        regions = SH.akey_regions(st)
        # (T211) the DFO salmon, from the bundle's salmon tables (pipeline/deliver/salmon.py): a head and one file per
        # bundle region (owner 2026-10-28: a water page's card fetches its own region's slice), cut by the store's places
        from pipeline.deliver import salmon as SALMON
        _sid_regions = SH.sid_regions(st)
        # (T796 A5) the rule book's full closures, FIRST in the chains of the stretches they close (a static place:
        # the stored verdicts beside the bundle say which rule closes which section on which days)
        _payload, _secs = SALMON.read_bundle(b)
        _payload, _secs = SALMON.with_book_closures(_payload, _secs, *SALMON.book_inputs(bundle, _secs))
        # (T936) a WARNING, never a stop: game fish closed by species closures alone, DFO salmon open or release, and a
        # B.C. bait ban / fly rule the closed day's strip does not draw (salmon_gear.py); the whole list goes to `reports`
        from pipeline.deliver.release import salmon_gear as SALMON_GEAR
        from pipeline.deliver.store.decode import Store as _Store
        try:                                            # (T936 review) a warning never stops the build
            _gear_hidden = SALMON_GEAR.rows(_Store(None, verify=False, con=st), _payload, _secs,
                                            lambda: SALMON_GEAR.bundle_parts(b))
            print(SALMON_GEAR.say(_gear_hidden))
        except Exception as e:                          # shown, not swallowed: the build log names it
            _gear_hidden = None
            print(f"WARNING (T936): the hidden-gear check could not run: {type(e).__name__}: {e}")
        for name, blob in SALMON.encode_files(_payload, _secs, base["section_handles"],
                                               lambda sid: _sid_regions.get(sid, ())).items():
            (work / f"{name}.bin").write_bytes(blob)
            files[name] = work / f"{name}.bin"
        shard_tables = SH.shard_tables(st)              # (T810 tickets I, L) + an added section's, where the store holds it
        common = [t for (t,) in st.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                  if t not in shard_tables and t not in SH.SID_TABLES and t != "meta"]

        def water_extra(con):
            # camera hints as integer microdegrees (~0.1 m): a REAL is 8 bytes, these are ~4 (part_geo: the geo file)
            e6 = lambda c: f"CAST(round({c} * 1000000) AS INTEGER)"  # noqa: E731
            con.execute("CREATE TABLE part_mark_e6 (ord INTEGER NOT NULL, mark_ix INTEGER NOT NULL, lon INTEGER NOT NULL, "
                        "lat INTEGER NOT NULL, token TEXT, down_part INTEGER NOT NULL, up_parts TEXT NOT NULL, "
                        "confidence TEXT, alt TEXT, "
                        "PRIMARY KEY (ord, mark_ix)) WITHOUT ROWID")
            # `confidence` (T207: NULL for high), `alt` (T517). (T517 review finding 2) a mark ships the END TOKEN its
            # two parts share there (`mark_token`), named by the app through `display.ends` like every part: no stored words
            tok = mark_tokens(con)
            con.executemany("INSERT INTO part_mark_e6 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            [(o, k, lon, lat, tok.get((o, k), bt), d, u, c, a) for o, k, lon, lat, d, u, c, a, bt in
                             con.execute(f"SELECT ord, mark_ix, {e6('lon')}, {e6('lat')}, down_part, up_parts, "
                                         "confidence, alt, token FROM part_mark ORDER BY ord, mark_ix").fetchall()])
            con.execute("DROP TABLE part_mark")
            con.execute("ALTER TABLE part_mark_e6 RENAME TO part_mark")
            # the exit's label is the receiving water's name, which `item` already holds
            con.execute("CREATE TABLE water_exit_k (ord INTEGER PRIMARY KEY, down_ord INTEGER NOT NULL, down_part INTEGER)")
            con.execute("INSERT INTO water_exit_k SELECT ord, down_ord, down_part FROM water_exit ORDER BY ord")
            con.execute("DROP TABLE water_exit")
            con.execute("ALTER TABLE water_exit_k RENAME TO water_exit")
            # which stream goes on as another (the Chilliwack as the Vedder): keyed both ways, for ▲ and ▼
            con.execute("CREATE TABLE water_continue (ord INTEGER PRIMARY KEY, part INTEGER NOT NULL, "
                        "next_ord INTEGER NOT NULL UNIQUE, next_part INTEGER NOT NULL)")
            con.executemany("INSERT INTO water_continue VALUES (?, ?, ?, ?)", sorted(continues))
            con.execute("CREATE TABLE akey_region (akey INTEGER NOT NULL, region TEXT NOT NULL, "
                        "PRIMARY KEY (akey, region)) WITHOUT ROWID")
            con.executemany("INSERT INTO akey_region VALUES (?, ?)",
                            sorted((a, r) for a, rs in regions.items() for r in rs))
            # (T388) each part's own stretch of stream (part_line.py): a named link's place, without `lines`
            PART_LINE.write(con, part_lines)
        water_meta = {**regs, "format": formats()["water"], "card_check": json.dumps(stamp, sort_keys=True),
                      **{k: smeta[k] for k in ("layout", "about_bundle", "blob_codec", "answers_sha256")},
                      # the store's own meta, whole: the store is put back together from the release and held to its
                      # store_digest (store/release.py `province_store`)
                      "store_meta": json.dumps(smeta, sort_keys=True)}
        files["water"] = copy_file(work / "water.sqlite", {"s": store, "b": bundle},
                                   [("s", t, None) for t in common] + [("b", t, None) for t in WATER_BUNDLE_TABLES],
                                   water_meta, extra=water_extra)
        for r in SH.REGIONS:
            mine = [a for a, rs in regions.items() if r in rs]
            ids = SH.shard_rows(store, mine)
            plan = []
            for t in shard_tables:
                keep = sorted(ids[t])
                col = "akey" if t == "cell" else "id"
                plan.append(("s", t, f"WHERE {col} IN ({','.join(map(str, keep)) or 'NULL'})"))
            files[f"regs-{r}"] = copy_file(work / f"regs-{r}.sqlite", {"s": store}, plan,
                                           {**regs, "format": formats()[f"regs-{r}"], "region": r})
        files["regs-sid"] = copy_file(work / "regs-sid.sqlite", {"s": store}, [("s", t, None) for t in SH.SID_TABLES],
                                      {**regs, "format": formats()["regs-sid"]})
        # (T369) `lines`: every stream section's stream key and measure span (release/lines.py), so a link's PLACE
        # resolves to a section of this release — packed from the bundle's `section_measure` (read above)
        files["lines"] = copy_file(work / "lines.sqlite", {}, [], {**base, "format": formats()["lines"]},
                                   extra=lambda con: LINES.write(con, sm))
        del sm, part_lines
        # (T256) the answer slices: a web tap in region r reads core-<r> + water-<r>, not the whole files
        files.update(SLICES.build_slices(work, files["core"], files["water"], _sid_regions))
        # (T213) bait ban / fly fishing only per section and day: a projection of the store's open-days strips
        from pipeline.deliver import method as METHOD
        p = work / "method.bin"
        p.write_bytes(METHOD.encode(METHOD.compute(st)))
        files["method"] = p

        final = ship(work, files, base["section_handles"], out_root, data_release=data_release, tiles=tiles,
                     basemap=basemap, valid_until=bmeta.get("valid_until") or None, sources=sources, synopsis=synopsis,
                     bundle=bmeta, regs_versions=json.loads(smeta["layout"]).get("versions") if "layout" in smeta else None)
        # T517 (owner 2026-11-08: the parts and the painted colours "can never diverge"): held on the SHIPPED files,
        # before any pointer names the release
        from pipeline.deliver.release.paint_sync import check as _paint_sync
        _paint_sync(final, bundle)
        # T517 (owner 2026-11-09: fail fast; review finding 8): the names and run rules on the SHIPPED water file too
        from pipeline.deliver.answers.display.name_check import check_release as _name_check
        _name_check(final)
        if reports is not None and _gear_hidden is not None:   # (T936) beside the release's other reports, by its id
            try:                                        # after ship(), before LATEST: a failed report never strands it
                SALMON_GEAR.write(_gear_hidden, Path(reports) / final.name)
            except OSError as e:
                print(f"WARNING (T936): the hidden-gear report could not be written: {e}")
        from pipeline.deliver.release.manifest import write_latest
        write_latest(out_root, final.name)              # T72: the pointer every reader asks, not the newest mtime
        return final
    finally:
        b.close()
        st.close()
        shutil.rmtree(work, ignore_errors=True)


def bundle_files(work: Path, bundle: Path, b: sqlite3.Connection, base: Dict[str, str],
                 status_index: bytes, ends: Optional[Dict[str, List[str]]] = None) -> Dict[str, Path]:
    """The bundle's half of the release — paint, flow-area, access, core, near, conditions, down, climate, geo —
    written into `work`. `base` is
    every file's meta (section_handles, reach_digest); `ends` the regs store's `display.ends` (T927: core's `cut_place`
    words; None: no table). The app's committed fixture releases
    (`app/packages/data/dev/fixture-release/`) were cut once by this code and are frozen (T61)."""
    files: Dict[str, Path] = {}
    packed = PACKED.SHIP_PACKED_FORMATS          # (T686) near/2, down/2, climate/2 only once the switch is on
    p = work / "paint.bin"
    p.write_bytes(PAINT.encode(status_index, b))
    files["paint"] = p
    p = work / "flow-area.bin"
    p.write_bytes(PAINT.encode_area(b))
    files["flow-area"] = p
    p = work / "access.bin"
    p.write_bytes(PAINT.encode_access(b, handles=base["section_handles"]))
    files["access"] = p
    # (T224) `name_source` is additive: a bundle built before it gives an empty table (no name shows a source)
    with sqlite3.connect(f"file:{bundle}?mode=ro", uri=True) as _b:
        _has_ns = _b.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='name_source'").fetchone()
        # (T796) `section_note` is additive too: a bundle built before it ships an empty table (no page has a note)
        _has_sn = _b.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='section_note'").fetchone()
    _flags = _core_flags if _has_ns else _core_flags_empty_name_source

    def _core_extra(con: sqlite3.Connection) -> None:
        from pipeline.deliver.bundle.place_names import case_core_names
        case_core_names(con)   # (T941) a search row / neighbour name reads as the water page does
        _flags(con)
        if not _has_sn:
            con.execute(SECTION_NOTE_DDL)
        if ends is not None:
            core_cut_place(con, ends)
    files["core"] = copy_file(work / "core.sqlite", {"b": bundle},
                              [("b", t, None) for t in CORE_TABLES
                               if (t != "name_source" or _has_ns) and (t != "section_note" or _has_sn)],
                              {**base, "format": formats()["core"]}, extra=_core_extra)
    files["near"] = copy_file(work / "near.sqlite", {"b": bundle}, [("b", t, None) for t in NEAR_TABLES],
                              {**base, "format": formats()["near"]}, extra=PACKED.pack_near if packed else None)
    # (T823) THE SMALL-WATER WARNING follows the speaker: one bit per panel member per paint section (`flow-small`,
    # paint.py), the sentence and the threshold in the conditions meta (pipeline/gauges/consume/small_water.py)
    from pipeline.gauges.consume.small_water import META_RATIO_KEY, META_TEXT_KEY, SMALL_WATER_TEXT, ratio_for
    p = work / "flow-small.bin"
    p.write_bytes(PAINT.encode_small(b, ratio_for()))
    files["flow-small"] = p
    # (T679 #8) the members on each section's own water, once the owner has seen the list (`packed.SHIP_OWN_GAUGE_FLAG`)
    if PACKED.SHIP_OWN_GAUGE_FLAG:
        p = work / "flow-own.bin"
        p.write_bytes(PAINT.encode_own(b))
        files["flow-own"] = p
    # (T973) the Classified Waters units carried through a rowed no-unit water: app 0.8.4's "may need" note (OPTIONAL in
    # the contract: an older app knows no such file and reads nothing new)
    files["cw-through"] = copy_file(work / "cw-through.sqlite", {"b": bundle}, [],
                                    {**base, "format": formats()["cw-through"]},
                                    extra=lambda con: cw_through(con, bundle=bundle))
    files["conditions"] = copy_file(work / "conditions.sqlite", {"b": bundle},
                                    [("b", t, None) for t in COND_TABLES],
                                    {**base, "format": formats()["conditions"], META_TEXT_KEY: SMALL_WATER_TEXT,
                                     META_RATIO_KEY: f"{ratio_for():g}"}, extra=lake_section)
    files["down"] = copy_file(work / "down.sqlite", {"b": bundle}, [("b", t, None) for t in DOWN_TABLES],
                              {**base, "format": formats()["down"]}, extra=PACKED.pack_down if packed else None)
    files["climate"] = copy_file(work / "climate.sqlite", {"b": bundle}, [("b", t, None) for t in CLIMATE_TABLES],
                                 {**base, "format": formats()["climate"], "clim": PACKED.CLIM_CODEC if packed else "clim/2"},
                                 extra=_climate_extra if packed else _climate_f16)
    files["geo"] = copy_file(work / "geo.sqlite", {"b": bundle}, [("b", "part_geo", None)],
                             {**base, "format": formats()["geo"]}, extra=geo_e6)
    return files


def ship(work: Path, files: Dict[str, Path], handles: str, out_root: Path, *, data_release: Optional[str] = None,
         tiles: Optional[dict] = None, basemap: Optional[dict] = None, valid_until: Optional[str] = None,
         regs_versions: Optional[dict] = None, sources: Optional[dict] = None,
         bundle: Optional[dict] = None, synopsis: Optional[Path] = None) -> Path:
    """Gzip + sha-name every file, write the manifest, and move `work` to `out_root/<release id>`; the id is the
    content's digest unless one is given. `bundle` is the bundle's `meta`: its corpus and reach-run digests are
    stated in the manifest (`manifest.bundle_block`, T133) and are part of what the id covers."""
    from pipeline.deliver.release import manifest as MAN
    said_bundle = MAN.bundle_block(bundle) if bundle is not None else None
    entries = {}
    for name, raw in sorted(files.items()):
        sha = _sha(raw)
        gz = gzip.compress(raw.read_bytes(), compresslevel=9, mtime=0)
        fname = f"{name}-{sha[:12]}{raw.suffix}.gz"
        (work / fname).write_bytes(gz)
        entries[name] = {"url": fname, "bytes": raw.stat().st_size, "gz": len(gz), "sha256": sha,
                         "format": formats()[name], "section_handles": handles,
                         **SLICES.slice_marks().get(name, {}),       # (T256) web-only: what it is cut from
                         **({"web_only": True} if name in C.WEB_ONLY else {})}   # (T369) a phone never holds it
        raw.unlink()
    # T215: OUR COPY of the synopsis PDF, beside the data files (raw: a PDF is already compressed), named by content. The
    # boxes `rules[].book_box` were cut from these bytes; the app opens THIS file with the highlight, never gov.bc.ca's.
    # (T354) the caller hands the downsampled copy (`synopsis.shipped`: page boxes untouched, so the boxes hold); the
    # manifest carries the index the app's append needs (`synopsis.append`): the cited page opens first, the book never read into JS.
    said_synopsis = None
    if synopsis is not None:
        from pipeline.deliver.release import synopsis as SYN
        ssha = _sha(synopsis)
        sname = f"synopsis-{ssha[:12]}.pdf"
        shutil.copyfile(synopsis, work / sname)
        said_synopsis = {"id": ssha[:12], "url": sname, "bytes": synopsis.stat().st_size, "sha256": ssha,
                         "append": SYN.append_index(synopsis)}
    # The id covers EVERYTHING the manifest says, not only the files: an app keeps a release by its id, so a
    # manifest-only change (valid_until, a source date, the tiles pairing) under the same id would never be fetched.
    said = {"tiles": tiles, "basemap": basemap, "valid_until": valid_until, "regs_versions": regs_versions,
            "sources": sources, "synopsis": said_synopsis, **({"bundle": said_bundle} if said_bundle is not None else {})}
    from pipeline.deliver.release.rebase import id_of            # (T435) the one formula; rebase recomputes with it
    rid = data_release or id_of(entries, said)
    for e in entries.values():
        e["url"] = f"{rid}/{e['url']}"
    if said_synopsis is not None:
        said_synopsis = {**said_synopsis, "url": f"{rid}/{said_synopsis['url']}"}
    man = MAN.manifest(rid, entries, handles, tiles=tiles, basemap=basemap, valid_until=valid_until,
                       regs_versions=regs_versions, sources=sources, bundle=bundle, synopsis=said_synopsis)
    (work / "manifest.json").write_text(json.dumps(man, indent=1, sort_keys=True) + "\n")
    final = out_root / rid
    if final.exists():
        shutil.rmtree(final)                # a re-run of identical inputs writes identical bytes; replace them
    out_root.mkdir(parents=True, exist_ok=True)
    work.rename(final)
    return final
