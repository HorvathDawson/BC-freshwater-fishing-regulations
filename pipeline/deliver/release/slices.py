"""THE ANSWER SLICES (T256 item 1): `core-<r>` and `water-<r>`, one pair per region — what a web tap in region r reads
instead of the whole `core` (1.4 MB gz) and `water` (2.2 MB gz). On one bar the answer itself was the cost (T254): a
named water's page read core + water + regs-<r> ≈ 3.9 MB whatever the order.

A slice is a SUBSET OF ITS WHOLE FILE'S ROWS, never a new answer: the same tables, the same columns, the same bytes per
row (blobs keep the whole file's dictionaries), so the app's SQL runs unchanged on either. What it holds is COMPLETE for
what it COVERS, and the app reads a slice only for keys it covers (`app/packages/data/src/release/focus.ts`):

  sids      every section whose place names region r (`shards.sid_regions`), as a bitmap per 4096-sid block
            (`cover_sid`, core only). A covered sid's water — or that it has none (an unnamed stream) — is the slice's.
  waters    (ords) every water holding a covered sid, + the waters those flow into or go on as (`water_exit`,
            `water_continue` both ways: the water map names them), + the parts of a covered water (`item.part_of`: a
            lake cut into closure areas finds its parts). A covered water has EVERY row of its own in every table:
            core item / item_section / item_near / item_area / name_source; water item / part (+ its xpart) /
            part_section / part_line (T388) / part_mark / water_exit / water_continue (either end) / picker.
  ids       water `str`: every string a kept row names (no blob names a str: every str id is a column's, checked
            below); core `place`: the towns the kept `item_near` rows name (a place_id present is that town's row).
  shared    (T370) `water-shared`, ONE web file beside the slices: the water tables every region reads whole — `akey`,
            `akey_region`, `ruleset`, `seglist`, `segmom`, `rule`, `display_rule`, `lic`, `static`, `place`, `zdict`
            (the store reads rules / lics / statics whole) and the `str` rows their columns name. They were byte-
            identical in all nine water slices (623 of ~780 kB gz each, measured 2026-11-04); a second region now costs
            only its own rows. A water slice holds none of them.
  not held  core alias / item_alias / see_pointer (search reads the whole core); water entry / lset / runs (no app
            read: the seo build and the pipeline's store re-assembly read the whole water).

The app keys a read to a slice by PRESENCE: an ord / item_id / str id present in the slice is covered (complete), absent
is "ask the whole file" — so a key the slice does not cover costs bytes, never a wrong answer. Slices are web-only:
the manifest marks them (`slice_of`, `region`), a phone never installs them (it holds the whole files), and the
release's offline total does not count them.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Set

from pipeline.deliver.release import shards as SH

CORE_SLICE_FORMAT = "core-slice/1"
WATER_SLICE_FORMAT = "water-slice/2"
WATER_SHARED_FORMAT = "water-shared/1"
#: (T370) the web file the region-wide water tables live in, beside the slices
WATER_SHARED = "water-shared"

#: core tables a slice carries; keyed by ord (`ord`) or by item_id (`item_id`), `None` = whole
CORE_SLICE_TABLES: Dict[str, str | None] = {"item": "ord", "item_section": "ord", "item_near": "ord",
                                            "item_area": "ord", "name_source": "item_id", "place": "place_id"}
#: water tables a slice carries; the column a covered water is keyed by, "*" = special (below)
WATER_SLICE_TABLES: Dict[str, str] = {
    "item": "item_ord", "part": "item_ord", "part_section": "ord", "part_line": "ord", "part_mark": "ord",
    "water_exit": "ord", "water_continue": "*", "xpart": "*", "picker": "*", "str": "*"}
#: tables a water cut before them lacks: a slice of it has none either (water_continue; T388 part_line)
WATER_SLICE_ADDITIVE = ("water_continue", "part_line")
#: (T370) the water tables every region reads whole: in `water-shared` only, never in a slice
WATER_SHARED_TABLES = ("akey", "akey_region", "ruleset", "seglist", "segmom", "rule", "display_rule", "lic", "static",
                       "place", "zdict")
#: every column of water that names a `str` id (`str` keeps only these ids' rows)
STR_COLUMNS = (("item", "item_id"), ("item", "kind"), ("part", "label_s"), ("part", "short_s"), ("part", "runs_s"),
               ("part", "place_s"), ("part", "hint_s"), ("akey", "kind"), ("akey", "province_except"),
               ("akey", "home_region"), ("place", "label"))
BLOCK_BITS, BLOCK = SH.BLOCK_BITS, SH.BLOCK


class SliceError(SystemExit):
    pass


def region_sids(sid_regions: Dict[int, List[str]], region: str) -> Set[int]:
    return {s for s, rs in sid_regions.items() if region in rs}


def covered_ords(core: sqlite3.Connection, water: sqlite3.Connection, sids: Set[int]) -> Set[int]:
    """The waters a slice holds whole (module docstring: `waters`). `core`/`water` are the release's built files."""
    pairs = core.execute("SELECT sid, ord FROM item_section").fetchall()
    if len({s for s, _ in pairs}) != len(pairs):
        raise SliceError("slices: a section held by two waters — a covered sid would not name ONE water")
    own = {o for s, o in pairs if s in sids}
    cov = set(own)
    for o, d in water.execute("SELECT ord, down_ord FROM water_exit"):
        if o in own:
            cov.add(d)
    for o, nxt in water.execute("SELECT ord, next_ord FROM water_continue"):
        if o in own:
            cov.add(nxt)
        if nxt in own:
            cov.add(o)
    ids = {i for o, i in core.execute("SELECT ord, item_id FROM item") if o in cov}
    cov |= {o for o, p in core.execute("SELECT ord, part_of FROM item WHERE part_of IS NOT NULL") if p in ids}
    return cov


def sid_bitmap(sids: Iterable[int]) -> List[tuple]:
    """[(block, 512-byte bitmap)] for the blocks holding any of `sids`, bit i of block b = sid (b << 12) | i."""
    out: Dict[int, bytearray] = {}
    for s in sids:
        b = out.setdefault(s >> BLOCK_BITS, bytearray(BLOCK // 8))
        i = s & (BLOCK - 1)
        b[i >> 3] |= 1 << (i & 7)
    return [(k, bytes(v)) for k, v in sorted(out.items())]


def _copy(con: sqlite3.Connection, src: str, table: str, where: str = "", order: str = "") -> None:
    sql = con.execute(f"SELECT sql FROM {src}.sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    if sql is None:
        raise SliceError(f"slices: the whole file has no table {table}")
    con.execute(sql[0])
    for (s,) in con.execute(f"SELECT sql FROM {src}.sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL "
                            "ORDER BY name", (table,)).fetchall():
        con.execute(s)
    con.execute(f"INSERT INTO main.{table} SELECT * FROM {src}.{table} {where} {order}")


def _pk(con: sqlite3.Connection, src: str, t: str) -> str:
    cols = list(con.execute(f"PRAGMA {src}.table_info({t})"))
    pk = [c[1] for c in sorted(cols, key=lambda c: c[5]) if c[5]]
    return "ORDER BY " + ", ".join(pk or [c[1] for c in cols])


def _meta(con: sqlite3.Connection, src: str, fmt: str, whole: str, region: str | None, n: int | None) -> None:
    rows = dict(con.execute(f"SELECT k, v FROM {src}.meta"))
    rows.update({"format": fmt, "slice_of": whole, "whole_format": rows.get("format", ""),
                 **({"region": region} if region is not None else {}), **({"covers": str(n)} if n is not None else {})})
    con.execute("CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL) WITHOUT ROWID")
    con.executemany("INSERT INTO meta VALUES (?, ?)", sorted(rows.items()))


def _finish(con: sqlite3.Connection, tmp: Path, out: Path) -> Path:
    con.commit()
    con.execute("DETACH s")
    con.close()
    c = sqlite3.connect(tmp)
    c.execute("PRAGMA user_version = 0")
    c.commit()
    if out.exists():
        out.unlink()
    c.execute(f"VACUUM INTO '{out}'")
    c.close()
    tmp.unlink()
    return out


def _start(out: Path, whole: Path, ords: Set[int]) -> sqlite3.Connection:
    tmp = out.with_name(out.name + ".tmp")
    if tmp.exists():
        tmp.unlink()
    con = sqlite3.connect(tmp)
    con.execute("ATTACH ? AS s", (str(whole),))
    con.execute("CREATE TEMP TABLE k (o INTEGER PRIMARY KEY)")
    con.executemany("INSERT INTO temp.k VALUES (?)", [(o,) for o in sorted(ords)])
    return con


def core_slice(whole: Path, out: Path, region: str, ords: Set[int], sids: Set[int]) -> Path:
    con = _start(out, whole, ords)
    con.execute("CREATE TEMP TABLE ki (i TEXT PRIMARY KEY)")
    con.execute("INSERT INTO temp.ki SELECT item_id FROM s.item WHERE ord IN (SELECT o FROM temp.k)")
    for t, key in CORE_SLICE_TABLES.items():
        if not con.execute("SELECT 1 FROM s.sqlite_master WHERE type='table' AND name=?", (t,)).fetchone():
            continue                                # ADDITIVE tables a cut before them lacks (item_near, item_area, …)
        where = ("" if key is None else f"WHERE {key} IN (SELECT o FROM temp.k)" if key == "ord"
                 else "WHERE place_id IN (SELECT place_id FROM main.item_near)" if key == "place_id"
                 else f"WHERE {key} IN (SELECT i FROM temp.ki)")
        _copy(con, "s", t, where, _pk(con, "s", t))
    con.execute("CREATE TABLE cover_sid (block INTEGER PRIMARY KEY, b BLOB NOT NULL)")
    con.executemany("INSERT INTO cover_sid VALUES (?, ?)", sid_bitmap(sids))
    _meta(con, "s", CORE_SLICE_FORMAT, "core", region, len(ords))
    return _finish(con, out.with_name(out.name + ".tmp"), out)


def water_slice(whole: Path, out: Path, region: str, ords: Set[int]) -> Path:
    con = _start(out, whole, ords)
    for t, key in WATER_SLICE_TABLES.items():
        if not con.execute("SELECT 1 FROM s.sqlite_master WHERE type='table' AND name=?", (t,)).fetchone():
            if t in WATER_SLICE_ADDITIVE:
                continue                            # ADDITIVE: a water cut before the links / part_line has none
            raise SliceError(f"slices: the whole water has no table {t}")
        if key == "*":
            continue
        _copy(con, "s", t, f"WHERE {key} IN (SELECT o FROM temp.k)", _pk(con, "s", t))
    has = lambda t: con.execute("SELECT 1 FROM s.sqlite_master WHERE type='table' AND name=?", (t,)).fetchone()  # noqa: E731
    if has("water_continue"):
        _copy(con, "s", "water_continue", "WHERE ord IN (SELECT o FROM temp.k) OR next_ord IN (SELECT o FROM temp.k)",
              _pk(con, "s", "water_continue"))
    _copy(con, "s", "xpart", "WHERE id IN (SELECT xp FROM main.part)", _pk(con, "s", "xpart"))
    _copy(con, "s", "picker", "WHERE id IN (SELECT picker FROM main.item WHERE picker IS NOT NULL)",
          _pk(con, "s", "picker"))
    _strs(con, [(t, c) for t, c in STR_COLUMNS if t in WATER_SLICE_TABLES])
    _meta(con, "s", WATER_SLICE_FORMAT, "water", region, len(ords))
    return _finish(con, out.with_name(out.name + ".tmp"), out)


def water_shared(whole: Path, out: Path) -> Path:
    """(T370) `water-shared`: the region-wide water tables whole, and the `str` rows their columns name."""
    con = _start(out, whole, set())
    for t in WATER_SHARED_TABLES:
        if not con.execute("SELECT 1 FROM s.sqlite_master WHERE type='table' AND name=?", (t,)).fetchone():
            raise SliceError(f"slices: the whole water has no table {t}")
        _copy(con, "s", t, "", _pk(con, "s", t))
    _strs(con, [(t, c) for t, c in STR_COLUMNS if t in WATER_SHARED_TABLES])
    _meta(con, "s", WATER_SHARED_FORMAT, "water", None, None)
    return _finish(con, out.with_name(out.name + ".tmp"), out)


def _strs(con: sqlite3.Connection, cols: List[tuple]) -> None:
    """The `str` rows the kept rows of `cols` name (a str id present is that string's row)."""
    used = " UNION ".join(f"SELECT {c} FROM main.{t} WHERE {c} IS NOT NULL" for t, c in cols)
    _copy(con, "s", "str", f"WHERE id IN ({used})", _pk(con, "s", "str"))


def str_ids_are_column_ids(water: sqlite3.Connection) -> List[int]:
    """The str ids no column names (a blob naming a str would be one): must be none, or `str` cannot be cut by
    columns. Returns them (empty = fine)."""
    used = " UNION ".join(f"SELECT {c} FROM {t} WHERE {c} IS NOT NULL" for t, c in STR_COLUMNS)
    return [i for (i,) in water.execute(f"SELECT id FROM str WHERE id NOT IN ({used}) ORDER BY id")]


def build_slices(work: Path, core: Path, water: Path, sid_regions: Dict[int, List[str]]) -> Dict[str, Path]:
    """{`core-<r>` / `water-<r>`: path} for every region, written into `work` from the built `core` and `water`."""
    c = sqlite3.connect(f"file:{core}?mode=ro", uri=True)
    w = sqlite3.connect(f"file:{water}?mode=ro", uri=True)
    try:
        loose = str_ids_are_column_ids(w)
        if loose:
            raise SliceError(f"slices: {len(loose)} str ids no column names (first {loose[:5]}) — a blob names a "
                             "str; the water slice cannot cut str by columns")
        out: Dict[str, Path] = {}
        for r in SH.REGIONS:
            sids = region_sids(sid_regions, r)
            ords = covered_ords(c, w, sids)
            out[f"core-{r}"] = core_slice(core, work / f"core-{r}.sqlite", r, ords, sids)
            out[f"water-{r}"] = water_slice(water, work / f"water-{r}.sqlite", r, ords)
        out[WATER_SHARED] = water_shared(water, work / f"{WATER_SHARED}.sqlite")
        return out
    finally:
        c.close()
        w.close()


#: the manifest's mark on a slice entry (`ship`): the whole file it is cut from and its region
def slice_marks() -> Dict[str, dict]:
    return {**{f"core-{r}": {"slice_of": "core", "region": r} for r in SH.REGIONS},
            **{f"water-{r}": {"slice_of": "water", "region": r} for r in SH.REGIONS},
            WATER_SHARED: {"slice_of": "water"}}                 # (T370) no region: every one reads it


def sid_regions_of_release(regs_sid: Path, water: Path) -> Dict[int, List[str]]:
    """`shards.sid_regions` read off a release's own files (regs-sid's `section_place`, water's `place`) — the store's
    tables as the release ships them, so a release can be given slices without its store (P20: none is kept)."""
    st = sqlite3.connect(":memory:")
    try:
        st.execute("ATTACH ? AS s", (str(regs_sid),))
        st.execute("ATTACH ? AS w", (str(water),))
        st.execute("CREATE TABLE section_place AS SELECT * FROM s.section_place")
        st.execute("CREATE TABLE place AS SELECT place, regions FROM w.place")
        return SH.sid_regions(st)
    finally:
        st.close()


def add_to_release(src: Path, out_root: Path) -> Path:
    """A release with the answer slices, cut from release `src` (one built before them): every file of `src` as it is,
    + `core-<r>` / `water-<r>`, through the release's own `ship` (gzip, sha names, manifest, the id over all of it).
    Writes `<out_root>/<new id>/` and LATEST there; `src` is never touched."""
    import gzip
    import json
    import shutil
    import tempfile
    from pipeline.deliver.release import build as B
    src = Path(src)
    m = json.loads((src / "manifest.json").read_text())
    if any(k in m["files"] for k in slice_marks()):
        raise SliceError(f"slices: {src.name} already has its slices")
    out_root.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix=".slices-", dir=out_root))
    try:
        files: Dict[str, Path] = {}
        for name, e in m["files"].items():
            raw = gzip.decompress((src.parent / e["url"]).read_bytes())
            p = work / f"{name}{Path(Path(e['url']).stem).suffix}"
            p.write_bytes(raw)
            files[name] = p
        files.update(build_slices(work, files["core"], files["water"],
                                  sid_regions_of_release(files["regs-sid"], files["water"])))
        syn = None
        if m.get("synopsis"):
            syn = work / ".synopsis.pdf"
            shutil.copyfile(src.parent / m["synopsis"]["url"], syn)
        handles = next(iter(m["files"].values()))["section_handles"]
        final = B.ship(work, files, handles, out_root, tiles={k: v for k, v in m["tiles"].items() if k != "compatible"},
                       basemap=m.get("basemap"), valid_until=(m.get("regulations") or {}).get("valid_until"),
                       regs_versions=(m.get("regulations") or {}).get("versions"), sources=m.get("sources"),
                       bundle=m.get("bundle"), synopsis=syn)
        if syn is not None:
            (final / syn.name).unlink(missing_ok=True)
        from pipeline.deliver.release.manifest import write_latest
        write_latest(out_root, final.name)
        return final
    finally:
        shutil.rmtree(work, ignore_errors=True)


def main(argv=None) -> int:
    """python -m pipeline.deliver.release.slices --from data/release/<r> --out-root <dir>"""
    import argparse
    ap = argparse.ArgumentParser(description="cut a release with the answer slices (T256) from one built before them")
    ap.add_argument("--from", dest="src", type=Path, required=True)
    ap.add_argument("--out-root", type=Path, required=True)
    a = ap.parse_args(argv)
    from pipeline.deliver.release import build as B
    d = add_to_release(a.src, a.out_root)
    print(d)
    bad = B.gate_problems(d)
    for p in bad:
        print(f"SIZE GATE: {p}")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
