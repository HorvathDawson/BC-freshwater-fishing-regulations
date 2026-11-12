"""T555 item 5: the release check that lists item ids gone from the release before with no alias (on releases it builds)."""
import gzip
import json
import sqlite3

import pytest

from pipeline.deliver.release import gone_ids as G


def _release(root, rid, items, alias=None):
    """A release folder holding only what the check reads: a manifest naming a gzipped core with `item` (+ `item_alias`)."""
    d = root / rid
    d.mkdir(parents=True)
    db = root / f"{rid}.sqlite"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT UNIQUE, name TEXT)")
    con.executemany("INSERT INTO item (item_id, name) VALUES (?, ?)", sorted(items.items()))
    if alias is not None:
        con.execute("CREATE TABLE item_alias (alias TEXT PRIMARY KEY, item_id TEXT NOT NULL)")
        con.executemany("INSERT INTO item_alias VALUES (?, ?)", sorted(alias.items()))
    con.commit()
    con.close()
    (d / "core-x.sqlite.gz").write_bytes(gzip.compress(db.read_bytes()))
    (d / "manifest.json").write_text(json.dumps({"files": {"core": {"url": f"{rid}/core-x.sqlite.gz"}}}))
    return d


BEFORE = {"gnis:1": "Camp Slough", "wbk:2": "Side Channel", "wbk:3": "Lost Pond", "wbk:4": ""}


def test_an_absorbed_id_is_followed_and_a_dropped_one_is_listed_by_name(tmp_path):
    b = _release(tmp_path, "r-before", BEFORE)
    a = _release(tmp_path, "r-after", {"gnis:1": "Camp Slough"}, alias={"wbk:2": "gnis:1"})
    rep = G.compare(b, a)
    assert rep["before"] == 4 and rep["kept"] == 1
    assert rep["aliased"] == [{"id": "wbk:2", "now": "gnis:1", "name": "Side Channel"}]
    assert rep["gone"] == [{"id": "wbk:3", "name": "Lost Pond"}, {"id": "wbk:4", "name": ""}]
    assert G.summary(rep, "r-before", "r-after") == (
        "item ids, r-before -> r-after: 1 of 4 kept, 1 absorbed (followed by item_alias), 2 gone with no alias\n"
        "  gone  wbk:3  Lost Pond\n"
        "  gone  wbk:4  (unnamed)")


def test_an_alias_to_an_id_the_release_does_not_hold_is_gone_and_says_so(tmp_path):
    b = _release(tmp_path, "r-before", BEFORE)
    a = _release(tmp_path, "r-after", {"gnis:1": "Camp Slough", "wbk:3": "Lost Pond", "wbk:4": ""}, alias={"wbk:2": "gnis:9"})
    rep = G.compare(b, a)
    assert rep["gone"] == [{"id": "wbk:2", "name": "Side Channel", "alias_to_missing": "gnis:9"}]
    assert G.summary(rep, "b", "a").splitlines()[1] == (
        "  gone  wbk:2  Side Channel  (item_alias names gnis:9, which is not an item)")


def test_nothing_gone_is_one_line_and_a_core_cut_before_item_alias_has_no_aliases(tmp_path):
    b = _release(tmp_path, "r-before", BEFORE)
    same = _release(tmp_path, "r-same", BEFORE, alias={})
    assert G.summary(G.compare(b, same), "r-before", "r-same") == (
        "item ids, r-before -> r-same: 4 of 4 kept, 0 absorbed (followed by item_alias), 0 gone with no alias")
    old = _release(tmp_path, "r-old", {"gnis:1": "Camp Slough"})          # no item_alias table at all
    assert [g["id"] for g in G.compare(b, old)["gone"]] == ["wbk:2", "wbk:3", "wbk:4"]


def test_a_missing_release_folder_or_core_file_raises_naming_it(tmp_path):
    b = _release(tmp_path, "r-before", BEFORE)
    with pytest.raises(FileNotFoundError, match="is not a release folder"):
        G.compare(b, tmp_path / "r-nowhere")
    (b / "core-x.sqlite.gz").unlink()
    with pytest.raises(FileNotFoundError, match="core-x.sqlite.gz is missing"):
        G.compare(b, b)


def test_the_command_prints_the_list_and_never_refuses(tmp_path, capsys):
    b = _release(tmp_path, "r-before", BEFORE)
    a = _release(tmp_path, "r-after", {"gnis:1": "Camp Slough"}, alias={"wbk:2": "gnis:1"})
    out = tmp_path / "rep.json"
    assert G.main([str(b), str(a), "--json", str(out)]) == 0
    assert "2 gone with no alias" in capsys.readouterr().out
    assert [g["id"] for g in json.loads(out.read_text())["gone"]] == ["wbk:3", "wbk:4"]
