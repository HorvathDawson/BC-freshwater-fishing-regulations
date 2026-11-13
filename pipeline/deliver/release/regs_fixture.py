"""THE REGS FIXTURE RELEASE (T10, D13 amended by D22): the release's REGULATIONS half — water, regs-<region>, regs-sid
and the `core` rows `itemOrd` reads — cut from a built release to the waters the golden states name, so the app's regs
conformance runs in `pnpm check` (and CI) with no live data. Committed under `app/packages/data/dev/regs-release/`.

    python -m pipeline.deliver.release.regs_fixture [--release <data/release/r-…>] [--taps <taps.json>] [--item ID …]
                                                  [--out app/packages/data/dev/regs-release]

It decides nothing and derives nothing: every row is the release's, copied byte for byte (blobs keep their codec and
the water file keeps every dictionary), by the release builder's own `copy_file` / `ship`, so the fixture is the same
formats the app reads. What it keeps:

  water          every rule, display_rule, static, str, zdict, entry, ruleset, lset, picker, runs, xpart, seglist,
                 segmom, place, lic — whole (the app's `rules()` sees all of them); `item`, `part`, `part_section`,
                 `part_geo`, `part_mark`, `water_exit` of the kept waters; `water_continue` where BOTH waters are
                 kept (the app opens the other one); `akey` + `akey_region` of the kept keys
  regs-<region>  the cells of the kept keys in that region and every frame row they reach (the shard rule of
                 `pipeline.deliver.release.shards`: ladder → verdicts; rows → decided + rows; licence → holds,
                 documents, under-16, the documents' answers), only for regions holding a kept key
  regs-sid       the blocks holding the kept waters' sections and the golden unnamed handles, every OTHER slot zeroed
                 (a slot that is kept always names a kept key)
  core           `item`, `item_section`, `alias`, `item_alias` of the kept waters (and the `item` row of each water
                 a kept water exits into); no places. The water each kept water flows into is kept whole (its map
                 group: T459 keeps it in colour)
  geo            (T182, when the source release has one) `part_geo` of the kept waters — water then has none

Kept keys = the kept waters' parts' keys ∪ the keys of the kept section slots. Kept waters = taps.json `states[*].item`
∪ `leap[*].item` ∪ `--item`. Every file's meta is the source's (format, digests, layout, codec, answers sha) plus
`fixture_of = <source release id>`; the manifest names no tile set (the fixture is paired with none). Two runs on the
same release write the same bytes and the same release id. The output root holds this one release: older `r-*`
folders under it are removed after a successful build.
"""
from __future__ import annotations

import argparse
import gzip
import json
import shutil
import sqlite3
import tempfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set

from pipeline.deliver.release import build as B
from pipeline.deliver.release import lines as L
from pipeline.deliver.release import shards as SH
from pipeline.deliver.store.common import unpack
from pipeline.deliver.store.release import ADDITIVE_TABLES

REPO = Path(__file__).resolve().parents[3]
TAPS = REPO / "app" / "fixtures" / "golden" / "taps.json"
OUT = REPO / "app" / "packages" / "data" / "dev" / "regs-release"
#: (T441) the fixture's stream lines, beside it (`@app/data/release/node` `regsFixtureLines`)
LINES_OUT = REPO / "app" / "packages" / "data" / "dev" / "section-lines.json"
SID_WIDTH = {"section_akey": 2, "section_place": 1, "section_tidal": 1}
#: water tables cut to the kept waters (by their ord column); every other water table is copied whole
WATER_BY_ORD = {"item": "item_ord", "part": "item_ord", "part_section": "ord", "part_geo": "ord", "part_mark": "ord",
                "water_exit": "ord"}
WATER_BY_AKEY = ("akey", "akey_region")


class FixtureError(SystemExit):
    pass


def golden_items(taps: Path) -> List[str]:
    t = json.loads(taps.read_text())
    return sorted({s["item"] for s in t["states"].values()} | {s["item"] for s in t.get("leap", [])})


def golden_handles(taps: Path) -> List[int]:
    return sorted(u["handle"] for u in json.loads(taps.read_text())["unnamed"])


def newest_release(root: Path) -> Optional[Path]:
    """THE release on disk (`release.manifest.latest`, the one answer — T133)."""
    from pipeline.deliver.release.manifest import latest
    return latest(root)


def _ids(xs: Iterable[int]) -> str:
    return ",".join(str(int(x)) for x in sorted(set(xs))) or "NULL"


def _strs(xs: Iterable[str]) -> str:
    return ",".join("'" + x.replace("'", "''") + "'" for x in sorted(set(xs))) or "NULL"


def unnamed_at(lines_out: Path) -> List[int]:
    """The unnamed reach the tests walk, as [blue_line_key, from_m] — from the committed file (its `unnamed_at`, or,
    for a file written before T441, its `unnamed` handle's own row)."""
    d = json.loads(lines_out.read_text())
    if d.get("unnamed_at"):
        return list(d["unnamed_at"])
    row = next((r for r in d["lines"] if r[0] == d["unnamed"]), None)
    if row is None:
        raise FixtureError(f"regs fixture: {lines_out} names unnamed {d['unnamed']} but lists no line for it")
    return [row[1], row[2]]


def fixture_lines(release: Path, sids: Iterable[int], unnamed_at: Sequence[int], out: Path) -> int:
    """(T441) THE FIXTURE'S STREAM LINES, cut from the release's own `lines` file (`lines.py`, the atlas graph's exact
    lines): `[handle, blue_line_key, from_m, to_m)` of every stream section among `sids`, plus the one unnamed reach the
    tests walk — named by its line and start (`unnamed_at` = [blk, from_m], stable across a renumbering), its handle
    looked up here. A slot that spans nothing (a filler: not a stream section) is left out; an open end stays open
    (0xFFFFFFFF, `LINE_OPEN`). Returns the rows written. Was a hand re-map through section_handles.txt."""
    man = json.loads((release / "manifest.json").read_text())
    e = man["files"].get("lines")
    if e is None:
        raise FixtureError(f"regs fixture: release {man['data_release']} has no lines file")
    with tempfile.NamedTemporaryFile(suffix=".sqlite") as f:
        f.write(gzip.decompress((release.parent / e["url"]).read_bytes()))
        f.flush()
        con = sqlite3.connect(f"file:{f.name}?mode=ro", uri=True)
        decoded: Dict[int, List] = {}

        def line(s: int):
            k = s >> L.BLOCK_BITS
            if k not in decoded:
                r = con.execute("SELECT n, b FROM section_line WHERE block = ?", (k,)).fetchone()
                decoded[k] = L.decode_block(r[1], r[0]) if r else []
            b = decoded[k]
            return b[s & (L.SLOTS - 1)] if (s & (L.SLOTS - 1)) < len(b) else None

        rows = []
        for s in sorted(set(sids)):
            ln = line(s)
            if ln is not None and ln[1] != ln[2]:
                rows.append([s, ln[0], ln[1], ln[2]])
        blk, m = unnamed_at
        hit = con.execute("SELECT block, n, b FROM section_line WHERE blk0 <= ? ORDER BY blk0 DESC, m0 DESC LIMIT 64",
                          (blk,)).fetchall()
        unnamed = None
        for k, n, b in hit:
            for i, (lb, lo, hi) in enumerate(L.decode_block(b, n)):
                if lb == blk and lo == m and lo != hi:
                    unnamed = (k << L.BLOCK_BITS) | i
                    rows.append([unnamed, lb, lo, hi])
                    break
            if unnamed is not None:
                break
        con.close()
    if unnamed is None:
        raise FixtureError(f"regs fixture: no section starts at {m} m on line {blk} (the unnamed reach)")
    rows = sorted({r[0]: r for r in rows}.values())
    doc = {"about": ("(T369, T441) TEST FIXTURE, WRITTEN by pipeline.deliver.release.regs_fixture: the stream line "
                     "[handle, blue_line_key, from_m, to_m) of every stream section of the regs fixture's waters "
                     "(dev/regs-release), plus one unnamed reach (`unnamed`, found by `unnamed_at` = [blue_line_key, "
                     "from_m]), cut from the release's own `lines` file. 4294967295 = open end. Never hand-edited."),
           "release": man["data_release"], "unnamed": unnamed, "unnamed_at": [blk, m], "lines": rows}
    out.write_text(json.dumps(doc, separators=(",", ":")).replace("],[", "],\n[") + "\n")
    return len(rows)


def build(release: Path, items: Sequence[str], handles: Sequence[int], out_root: Path,
          lines_out: Optional[Path] = None) -> Path:
    man = json.loads((release / "manifest.json").read_text())
    src_id = man["data_release"]
    for need in ("water", "core", "regs-sid"):
        if need not in man["files"]:
            raise FixtureError(f"regs fixture: release {src_id} has no {need} file")
    tmp = Path(tempfile.mkdtemp(prefix=".regs-fixture-src-"))
    out_root.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix=".regs-fixture-", dir=out_root))
    try:
        src: Dict[str, Path] = {}
        for name, e in man["files"].items():
            if name in ("water", "core", "regs-sid", "geo") or name.startswith("regs-"):
                p = tmp / f"{name}.sqlite"
                p.write_bytes(gzip.decompress((release.parent / e["url"]).read_bytes()))
                src[name] = p
        con = {k: sqlite3.connect(f"file:{p}?mode=ro", uri=True) for k, p in src.items()}
        meta = {k: dict(c.execute("SELECT k, v FROM meta")) for k, c in con.items()}
        # the province store's own meta (its store_digest) does not describe a subset of it
        meta["water"].pop("store_meta", None)

        # the kept waters, by core's ord (= water's item_ord, P08)
        ords = dict(con["core"].execute(f"SELECT item_id, ord FROM item WHERE item_id IN ({_strs(items)})"))
        missing = sorted(set(items) - set(ords))
        if missing:
            raise FixtureError(f"regs fixture: release {src_id} has no water {', '.join(missing)}")
        kept_ords = set(ords.values())
        # (T517 review sweep) the water each kept water's mouth flows into is kept whole too: the map keeps it in colour
        # (T459, `useReceivingSections` reads its group: item, parts, sections, marks, exit, geo) — one level
        kept_ords |= {o for (o,) in con["water"].execute(
            f"SELECT down_ord FROM water_exit WHERE ord IN ({_ids(kept_ords)})")}
        akeys: Set[int] = {a for (a,) in con["water"].execute(
            f"SELECT akey FROM part WHERE item_ord IN ({_ids(kept_ords)}) AND akey IS NOT NULL")}

        # the section slots: the kept waters' sections + the golden unnamed handles
        sids = {s for (s,) in con["core"].execute(f"SELECT sid FROM item_section WHERE ord IN ({_ids(kept_ords)})")}
        sids |= set(handles)
        # (T672 1b) `section_tidal` is ADDITIVE: a release cut before it has none, and its fixture none either
        have_sid = {t for (t,) in con["regs-sid"].execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        sid_tables = [t for t in SH.SID_TABLES if t in have_sid or t not in ADDITIVE_TABLES]
        blocks: Dict[str, Dict[int, bytearray]] = {t: {} for t in sid_tables}
        for t in sid_tables:
            w = SID_WIDTH[t]
            full = dict(con["regs-sid"].execute(
                f"SELECT block, b FROM {t} WHERE block IN ({_ids(s >> SH.BLOCK_BITS for s in sids)})"))
            for s in sids:
                b = full.get(s >> SH.BLOCK_BITS)
                if b is None:
                    continue
                mine = blocks[t].setdefault(s >> SH.BLOCK_BITS, bytearray(len(b)))
                o = w * (s & (SH.BLOCK - 1))
                mine[o:o + w] = b[o:o + w]
                if t == "section_akey":
                    a = int.from_bytes(b[o:o + w], "little")
                    if a:
                        akeys.add(a)

        regions: Dict[str, Set[int]] = {}
        for a, r in con["water"].execute(f"SELECT akey, region FROM akey_region WHERE akey IN ({_ids(akeys)})"):
            regions.setdefault(r, set()).add(a)
        unplaced = sorted(akeys - {a for v in regions.values() for a in v})
        if unplaced:
            raise FixtureError(f"regs fixture: keys {unplaced} are in no region file")

        fix = {"fixture_of": src_id}
        files: Dict[str, Path] = {}
        wtables = [t for (t,) in con["water"].execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name != 'meta' ORDER BY name")]
        plan = []
        for t in wtables:
            if t in WATER_BY_ORD:
                plan.append(("s", t, f"WHERE {WATER_BY_ORD[t]} IN ({_ids(kept_ords)})"))
            elif t == "water_continue":
                plan.append(("s", t, f"WHERE ord IN ({_ids(kept_ords)}) AND next_ord IN ({_ids(kept_ords)})"))
            elif t in WATER_BY_AKEY:
                plan.append(("s", t, f"WHERE akey IN ({_ids(akeys)})"))
            else:
                plan.append(("s", t, None))
        files["water"] = B.copy_file(work / "water.sqlite", {"s": src["water"]}, plan, {**meta["water"], **fix})
        # (T182) a release cut with the split files holds the water map's boxes in `geo`: the kept waters' rows
        if "geo" in src:
            files["geo"] = B.copy_file(work / "geo.sqlite", {"s": src["geo"]},
                                       [("s", "part_geo", f"WHERE ord IN ({_ids(kept_ords)})")],
                                       {**meta["geo"], **fix})

        zd = dict(con["water"].execute("SELECT tbl, d FROM zdict"))
        for r in sorted(regions):
            name = f"regs-{r}"
            if name not in con:
                raise FixtureError(f"regs fixture: release {src_id} has no {name} file")
            ids = _shard_ids(con[name], zd, regions[r])
            plan = [("s", t, f"WHERE {'akey' if t == 'cell' else 'id'} IN ({_ids(ids[t])})") for t in SH.SHARD_TABLES]
            files[name] = B.copy_file(work / f"{name}.sqlite", {"s": src[name]}, plan, {**meta[name], **fix})

        def sid_extra(c: sqlite3.Connection) -> None:
            for t in sid_tables:
                c.executemany(f"UPDATE {t} SET b = ? WHERE block = ?",
                              [(bytes(b), blk) for blk, b in sorted(blocks[t].items())])
        files["regs-sid"] = B.copy_file(
            work / "regs-sid.sqlite", {"s": src["regs-sid"]},
            [("s", t, f"WHERE block IN ({_ids(blocks[t])})") for t in sid_tables], {**meta["regs-sid"], **fix},
            extra=sid_extra)

        ids_ = sorted(ords)
        # (T517 review sweep) a kept water's exit names another water: core holds that water's `item` row too (its id
        # and name — for the receiving waters kept above, the waters THEY flow into), or the app refuses the cut as a
        # damaged release (`navRows`: an exit into an item core lacks)
        exit_ords = {o for (o,) in con["water"].execute(
            f"SELECT down_ord FROM water_exit WHERE ord IN ({_ids(kept_ords)})")}
        files["core"] = B.copy_file(work / "core.sqlite", {"s": src["core"]}, [
            ("s", "item", f"WHERE ord IN ({_ids(kept_ords | exit_ords)})"),
            ("s", "item_section", f"WHERE ord IN ({_ids(kept_ords)})"),
            ("s", "alias", f"WHERE item_id IN ({_strs(ids_)})"),
            ("s", "item_alias", f"WHERE item_id IN ({_strs(ids_)})"),
            ("s", "place", "WHERE 0"),
        ], {**meta["core"], **fix})
        for c in con.values():
            c.close()

        handles16 = meta["water"]["section_handles"]
        tiles = {"compatible": list(man["tiles"]["compatible"])}
        regs = man.get("regulations") or {}
        final = B.ship(work, files, handles16, out_root, tiles=tiles, valid_until=regs.get("valid_until"),
                       regs_versions=regs.get("versions"))
        for old in out_root.glob("r-*"):
            if old != final:
                shutil.rmtree(old)
        if lines_out is not None:
            fixture_lines(release, sids, unnamed_at(lines_out), lines_out)
        return final
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        if work.exists():
            shutil.rmtree(work, ignore_errors=True)


def _shard_ids(con: sqlite3.Connection, zd: Dict[str, bytes], akeys: Set[int]) -> Dict[str, Set[int]]:
    """{table: the ids a shard of `akeys` holds}: `shards.shard_rows`' rule, read from a region file."""
    val = lambda t, b: json.loads(unpack(b, zd.get(t)))  # noqa: E731
    out: Dict[str, Set[int]] = {t: set() for t in SH.SHARD_TABLES}
    out["cell"] = set(akeys)
    tops: Dict[str, Set[int]] = {}
    for row in con.execute(f"SELECT {', '.join(SH.TOP)} FROM cell WHERE akey IN ({_ids(akeys)})"):
        for col, i in zip(SH.TOP, row):
            tops.setdefault(SH.TOP[col], set()).add(i)
    for tbl, ids in tops.items():
        out[tbl] |= ids
        for _i, blob in con.execute(f"SELECT id, j FROM {tbl} WHERE id IN ({_ids(ids)})"):
            for sub, refs in SH.frame_refs(tbl, val(tbl, blob)).items():
                out[sub].update(refs)
    for _i, blob in con.execute(f"SELECT id, j FROM licence_doc WHERE id IN ({_ids(out['licence_doc'])})"):
        out["licence_answer"].update(val("licence_doc", blob))
    return out


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--release", type=Path, default=None, help="default: the newest data/release/r-*")
    ap.add_argument("--taps", type=Path, default=TAPS)
    ap.add_argument("--item", action="append", default=[], help="another water to keep (item id); repeatable")
    ap.add_argument("--out", type=Path, default=OUT)
    a = ap.parse_args(argv)
    rel = a.release
    if rel is None:
        from pipeline.common.curated import GENERATED
        rel = newest_release(GENERATED.release)
    if rel is None or not (rel / "manifest.json").exists():
        raise FixtureError(f"regs fixture: no release at {rel or 'data/release/r-*'}")
    d = build(rel, sorted(set(golden_items(a.taps)) | set(a.item)), golden_handles(a.taps), a.out,
              lines_out=LINES_OUT if a.out == OUT else None)
    sz = B.sizes(d)
    print(json.dumps({"release": str(d), "gz_total": sum(v["gz"] for v in sz.values()), "files": sz}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
