"""T667 slice 6 (D3): the live counts printed in the release diff (`pipeline/deliver/release/live_counts.py`), on release
folders built here (a manifest and a gzipped core with the tables the counts read)."""
from __future__ import annotations

import gzip
import json
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.release import live_counts as LC


def _release(root: Path, rid: str, items: dict[str, int], aliases: int) -> Path:
    """A release folder whose core holds `items` (item_id -> its number of sections) and `aliases` alias rows."""
    d = root / rid
    d.mkdir(parents=True)
    db = root / f"{rid}.sqlite"
    con = sqlite3.connect(db)
    con.executescript("CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT, name TEXT);"
                      "CREATE TABLE item_section (sid INTEGER, ord INTEGER);"
                      "CREATE TABLE item_alias (alias TEXT PRIMARY KEY, item_id TEXT);")
    sid = 0
    for ord_, (item, n) in enumerate(items.items()):
        con.execute("INSERT INTO item VALUES (?, ?, ?)", (ord_, item, item))
        for _ in range(n):
            sid += 1
            con.execute("INSERT INTO item_section VALUES (?, ?)", (sid, ord_))
    for i in range(aliases):
        con.execute("INSERT INTO item_alias VALUES (?, ?)", (f"wbk:{i}", next(iter(items))))
    con.commit()
    con.close()
    (root / f"core-{rid}.sqlite.gz").write_bytes(gzip.compress(db.read_bytes()))
    (d / "manifest.json").write_text(json.dumps({"files": {"core": {"url": f"core-{rid}.sqlite.gz"}}}))
    return d


def test_the_release_counts_read_the_core(tmp_path):
    d = _release(tmp_path, "r-a", {"gnis:11267": 137, "gnis:7836": 9, "gnis:1": 4}, aliases=117)
    got = LC.release_counts(d)
    assert got == {"items": 3, "absorbed ids (item_alias)": 117,
                   "sections: Rancheria River (gnis:11267)": 137, "sections: East Gribbell Creek (wsc:915-679587-924307)": 0,
                   "sections: Stellako River (gnis:7836)": 9, "sections: Six Mile Slough (gnis:6438)": 0}


def test_the_diff_text_whole():
    before = {"items": 10, "tier: P4": 747}
    after = {"items": 12, "tier: P4": 747, "tier: P1": 224}
    assert LC.diff_text(before, after, "r-old", "r-new") == "\n".join([
        "live counts, r-old -> r-new (listed, never a stop; T667 D3):",
        "  items: 10 -> 12 (+2)",
        "  tier: P1: 224 (not recorded for r-old)",
        "  tier: P4: 747 -> 747"])
    assert LC.diff_text(None, {"items": 1}, "(no live release)", "r-new").splitlines()[1] == \
        "  items: 1 (not recorded for (no live release))"


def test_a_folder_that_is_not_a_release_is_refused_naming_it(tmp_path):
    with pytest.raises(FileNotFoundError, match="manifest.json is missing"):
        LC.release_counts(tmp_path)
    d = _release(tmp_path, "r-b", {"gnis:1": 1}, aliases=0)
    (tmp_path / "core-r-b.sqlite.gz").unlink()
    with pytest.raises(FileNotFoundError, match="core-r-b.sqlite.gz is missing"):
        LC.release_counts(d)


def test_every_count_the_tests_used_to_pin_is_printed():
    """The pins D3 retired (test_one_water_kind: four waters' sections, the absorbed ids; test_review_queue: every
    pre-check code and tier, the closures' undrawn parts and ambiguous cuts) each have a line."""
    from pipeline.regs.review import precheck as P
    from pipeline.regs.review import tiers as T
    assert set(LC.WATCHED) == {"gnis:11267", "wsc:915-679587-924307", "gnis:7836", "gnis:6438"}
    src = Path(LC.__file__).read_text(encoding="utf-8")
    for needle in ("P.CHECKS", "T.TIERS", "closures with an undrawn part", "closures with an ambiguous cut",
                   "absorbed ids (item_alias)"):
        assert needle in src, needle
    assert {"binds_nothing", "unresolved", "ambiguous_cut", "undrawn_part"} <= set(P.CHECKS)
    assert {"P1", "P4", "sweep"} <= set(T.TIERS)
