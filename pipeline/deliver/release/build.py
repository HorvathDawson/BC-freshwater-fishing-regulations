"""`python -m pipeline.deliver release`: THE SHIPPED SET (P08, MINIMAL-PACKAGING §1–§2), written to
`data/release/<data_release>/` and nowhere else. It holds no regulation logic: it copies tables, cuts the region shards,
writes paint, derives three per-water flags for search, converts the climatology to float16, VACUUMs, gzips and hashes.

  file           holds                                                            source
  paint          the status index + the flow paint + the no-access rows (paint.py) status_index.bin + bundle
  access         the gentler access tiers: permit, reserve, private (T54)        bundle (section_access)
  core           item (+ size, gauged, has_rows), alias, item_alias, item_section, place, bundle
                 see_pointer (a group name that only points: search leads to the waters it names)
  near           place_water                                                      bundle
  water          the regs store's common tables + display_rule + akey_region,     regs store + bundle
                 part_section, part_geo, part_mark, water_exit,
                 water_continue (which stream goes on as another: the bundle's
                 own table, bundle/continuation.py)
  regs-<region>  cell + the frames its akeys reach (shards.py), × 9               regs store
  regs-sid       section_akey, section_place                                      regs store
  conditions     gauge, section_gauge, lake_gauge, section_down, gauge_clim       bundle
                 (bands float16: clim/2)
  flow-area      the panel target areas, one varint per paint section (paint.py)  bundle
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
import shutil
import sqlite3
import struct
import tempfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from pipeline.deliver import lake_facts as LF
from pipeline.deliver.release import paint as PAINT
from pipeline.deliver.release import shards as SH

#: every file and its format (bumped when a reader must refuse an older one)
FORMATS = {"paint": PAINT.FORMAT, "flow-area": PAINT.AREA_FORMAT, "access": PAINT.ACCESS_FORMAT,"core": "core/1", "near": "near/1", "water": "water/1", "regs-sid": "regs-sid/1",
           "conditions": "conditions/1", "lakes": LF.FORMAT, **{f"regs-{r}": "regs-region/1" for r in SH.REGIONS}}
#: the gzip size gates (MINIMAL-PACKAGING §2; P08 test 7)
SIZE_GATES = {"paint": 1_000_000, "access": 300_000, "core": 1_300_000, "lakes": 100_000,**{f"regs-{r}": 1_500_000 for r in SH.REGIONS}}
TOTAL_GATE = 14_000_000

CORE_TABLES = ("item", "alias", "item_alias", "item_section", "place", "see_pointer")
NEAR_TABLES = ("place_water",)
COND_TABLES = ("gauge", "section_gauge", "lake_gauge", "section_down", "gauge_clim")
WATER_BUNDLE_TABLES = ("part_section", "part_geo", "part_mark", "water_exit")
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
    con.execute("CREATE INDEX item_name_nc ON item(name COLLATE NOCASE)")
    con.execute("CREATE INDEX alias_nc ON alias(alias COLLATE NOCASE)")
    con.execute("CREATE INDEX see_pointer_nc ON see_pointer(name COLLATE NOCASE)")
    # the case-sensitive name indexes are the bundle's; search is LIKE (case-insensitive): the NOCASE ones serve it
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


def _conditions_extra(con: sqlite3.Connection) -> None:
    rows = con.execute("SELECT rowid, bands FROM gauge_clim").fetchall()
    con.executemany("UPDATE gauge_clim SET bands = ? WHERE rowid = ?", [(clim_f16(b), r) for r, b in rows])



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
    """The size gates (P08 test 7): each file's gzip under its gate, and the total."""
    sz = sizes(release_dir)
    out = [f"{k}: {v['gz']:,} B gzip > {SIZE_GATES[k]:,}" for k, v in sz.items() if k in SIZE_GATES and v["gz"] > SIZE_GATES[k]]
    total = sum(v["gz"] for v in sz.values())
    if total > TOTAL_GATE:
        out.append(f"total: {total:,} B gzip > {TOTAL_GATE:,}")
    return out


def bundle_continues(b: sqlite3.Connection) -> List[tuple]:
    """`water_continue` as the bundle wrote it (`pipeline.deliver.bundle.continuation`): copied, never derived here — the
    release reads nothing past the bundle (audit F07). A bundle without the table predates it and is refused."""
    if not b.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'water_continue'").fetchone():
        raise ReleaseError("release: the bundle has no water_continue table — it predates the continuation links; "
                           "rebuild it: python -m pipeline.deliver bundle")
    return b.execute("SELECT ord, part, next_ord, next_part FROM water_continue ORDER BY ord").fetchall()


def build(*, bundle: Path, store: Path, status_index: Path, export_dir: Path, out_root: Path,
          data_release: Optional[str] = None, tiles: Optional[dict] = None, basemap: Optional[dict] = None,
          lakes: Optional[Path] = None, sources: Optional[dict] = None) -> Path:
    """Write the release; return its directory (only the shipped files + manifest.json). `tiles`/`basemap`/`sources`
    are the manifest's blocks (manifest.py); `lakes` an optional lakes/1 file (pipeline/deliver/lake_facts.py), shipped
    as the file `lakes`; None (the default, and `deliver all`) ships none. Reads nothing past the bundle: the continuation
    links are the bundle's own `water_continue`."""
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

    work = Path(tempfile.mkdtemp(prefix=".release-", dir=out_root if out_root.exists() else None))
    try:
        files: Dict[str, Path] = bundle_files(work, bundle, b, base, status_index.read_bytes())
        if lakes is not None:
            p = work / "lakes.bin"
            p.write_bytes(lakes_bytes(lakes.read_bytes(), base["section_handles"]))
            files["lakes"] = p
        regions = SH.akey_regions(st)
        common = [t for (t,) in st.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                  if t not in SH.SHARD_TABLES and t not in SH.SID_TABLES and t != "meta"]

        def water_extra(con):
            # camera hints as integer microdegrees (~0.1 m): a REAL is 8 bytes, these are ~4
            con.execute("CREATE TABLE part_geo_e6 (ord INTEGER NOT NULL, part_ix INTEGER NOT NULL, rank INTEGER NOT NULL, "
                        "mid_sid INTEGER NOT NULL, mid_lon INTEGER NOT NULL, mid_lat INTEGER NOT NULL, w INTEGER, "
                        "s INTEGER, e INTEGER, n INTEGER, PRIMARY KEY (ord, part_ix)) WITHOUT ROWID")
            e6 = lambda c: f"CAST(round({c} * 1000000) AS INTEGER)"  # noqa: E731
            con.execute(f"INSERT INTO part_geo_e6 SELECT ord, part_ix, rank, mid_sid, {e6('mid_lon')}, {e6('mid_lat')}, "
                        f"{e6('w')}, {e6('s')}, {e6('e')}, {e6('n')} FROM part_geo ORDER BY ord, part_ix")
            con.execute("DROP TABLE part_geo")
            con.execute("ALTER TABLE part_geo_e6 RENAME TO part_geo")
            con.execute("CREATE TABLE part_mark_e6 (ord INTEGER NOT NULL, mark_ix INTEGER NOT NULL, lon INTEGER NOT NULL, "
                        "lat INTEGER NOT NULL, label TEXT, down_part INTEGER NOT NULL, up_parts TEXT NOT NULL, "
                        "PRIMARY KEY (ord, mark_ix)) WITHOUT ROWID")
            con.execute(f"INSERT INTO part_mark_e6 SELECT ord, mark_ix, {e6('lon')}, {e6('lat')}, label, down_part, up_parts "
                        "FROM part_mark ORDER BY ord, mark_ix")
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
        water_meta = {**regs, "format": FORMATS["water"], "card_check": json.dumps(stamp, sort_keys=True),
                      **{k: smeta[k] for k in ("layout", "about_bundle", "blob_codec", "answers_sha256")}}
        files["water"] = copy_file(work / "water.sqlite", {"s": store, "b": bundle},
                                   [("s", t, None) for t in common] + [("b", t, None) for t in WATER_BUNDLE_TABLES],
                                   water_meta, extra=water_extra)
        for r in SH.REGIONS:
            mine = [a for a, rs in regions.items() if r in rs]
            ids = SH.shard_rows(store, mine)
            plan = []
            for t in SH.SHARD_TABLES:
                keep = sorted(ids[t])
                col = "akey" if t == "cell" else "id"
                plan.append(("s", t, f"WHERE {col} IN ({','.join(map(str, keep)) or 'NULL'})"))
            files[f"regs-{r}"] = copy_file(work / f"regs-{r}.sqlite", {"s": store}, plan,
                                           {**regs, "format": FORMATS[f"regs-{r}"], "region": r})
        files["regs-sid"] = copy_file(work / "regs-sid.sqlite", {"s": store}, [("s", t, None) for t in SH.SID_TABLES],
                                      {**regs, "format": FORMATS["regs-sid"]})

        return ship(work, files, base["section_handles"], out_root, data_release=data_release, tiles=tiles,
                    basemap=basemap, valid_until=bmeta.get("valid_until") or None, sources=sources,
                    regs_versions=json.loads(smeta["layout"]).get("versions") if "layout" in smeta else None)
    finally:
        b.close()
        st.close()
        shutil.rmtree(work, ignore_errors=True)


def bundle_files(work: Path, bundle: Path, b: sqlite3.Connection, base: Dict[str, str],
                 status_index: bytes) -> Dict[str, Path]:
    """The bundle's half of the release — paint, flow-area, core, near, conditions — written into `work`. `base` is
    every file's meta (section_handles, reach_digest). Shared by `build` and the app's fixture release
    (`pipeline.tools.fixture_release`), so the fixture is cut by the same code as the shipped files."""
    files: Dict[str, Path] = {}
    p = work / "paint.bin"
    p.write_bytes(PAINT.encode(status_index, b))
    files["paint"] = p
    p = work / "flow-area.bin"
    p.write_bytes(PAINT.encode_area(b))
    files["flow-area"] = p
    p = work / "access.bin"
    p.write_bytes(PAINT.encode_access(b, handles=base["section_handles"]))
    files["access"] = p
    files["core"] = copy_file(work / "core.sqlite", {"b": bundle}, [("b", t, None) for t in CORE_TABLES],
                              {**base, "format": FORMATS["core"]}, extra=_core_flags)
    files["near"] = copy_file(work / "near.sqlite", {"b": bundle}, [("b", t, None) for t in NEAR_TABLES],
                              {**base, "format": FORMATS["near"]})
    files["conditions"] = copy_file(work / "conditions.sqlite", {"b": bundle},
                                    [("b", t, None) for t in COND_TABLES],
                                    {**base, "format": FORMATS["conditions"], "clim": "clim/2"},
                                    extra=_conditions_extra)
    return files


def ship(work: Path, files: Dict[str, Path], handles: str, out_root: Path, *, data_release: Optional[str] = None,
         tiles: Optional[dict] = None, basemap: Optional[dict] = None, valid_until: Optional[str] = None,
         regs_versions: Optional[dict] = None, sources: Optional[dict] = None) -> Path:
    """Gzip + sha-name every file, write the manifest, and move `work` to `out_root/<release id>`; the id is the
    content's digest unless one is given."""
    from pipeline.deliver.release import manifest as MAN
    entries = {}
    for name, raw in sorted(files.items()):
        sha = _sha(raw)
        gz = gzip.compress(raw.read_bytes(), compresslevel=9, mtime=0)
        fname = f"{name}-{sha[:12]}{raw.suffix}.gz"
        (work / fname).write_bytes(gz)
        entries[name] = {"url": fname, "bytes": raw.stat().st_size, "gz": len(gz), "sha256": sha,
                         "format": FORMATS[name], "section_handles": handles}
        raw.unlink()
    # The id covers EVERYTHING the manifest says, not only the files: an app keeps a release by its id, so a
    # manifest-only change (valid_until, a source date, the tiles pairing) under the same id would never be fetched.
    said = {"tiles": tiles, "basemap": basemap, "valid_until": valid_until, "regs_versions": regs_versions,
            "sources": sources}
    rid = data_release or "r-" + hashlib.sha256(
        json.dumps({"files": {k: v["sha256"] for k, v in sorted(entries.items())}, "said": said},
                   sort_keys=True).encode()).hexdigest()[:12]
    for e in entries.values():
        e["url"] = f"{rid}/{e['url']}"
    man = MAN.manifest(rid, entries, handles, tiles=tiles, basemap=basemap, valid_until=valid_until,
                       regs_versions=regs_versions, sources=sources)
    (work / "manifest.json").write_text(json.dumps(man, indent=1, sort_keys=True) + "\n")
    final = out_root / rid
    if final.exists():
        shutil.rmtree(final)                # a re-run of identical inputs writes identical bytes; replace them
    out_root.mkdir(parents=True, exist_ok=True)
    work.rename(final)
    return final
