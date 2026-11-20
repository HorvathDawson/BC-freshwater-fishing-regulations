"""T941: the release's core `item.name` is cased with the one caser (`place_names.display_case`), so a search row and a
neighbour water's name read as the water page does ("Kitwanga Mill Pond", never "KITWANGA MILL POND"). Cased on the core
file's copy (`release.build` `_core_extra`), never in the registry: the search key stays the stored spelling's, and the
water's `name_source` row follows its shown name. The end-to-end pin on a built release is
`test_release.py::test_cores_water_names_are_cased_as_the_water_page_cases_them`."""
from __future__ import annotations

import sqlite3

import pytest

from pipeline.common.search_key import search_key, stored_key
from pipeline.deliver.bundle import place_names as P


def _core(rows, sources=()):
    con = sqlite3.connect(":memory:")
    con.executescript("CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT NOT NULL UNIQUE, name TEXT NOT NULL,"
                      " name_key TEXT);"
                      "CREATE TABLE name_source (item_id TEXT NOT NULL, name TEXT NOT NULL, src TEXT NOT NULL,"
                      " label TEXT NOT NULL, PRIMARY KEY (item_id, name)) WITHOUT ROWID;")
    con.executemany("INSERT INTO item (item_id, name, name_key) VALUES (?, ?, ?)",
                    [(i, n, stored_key(n)) for i, n in rows])
    con.executemany("INSERT INTO name_source VALUES (?, ?, 'override', 'Project curation')", sources)
    return con


@pytest.mark.smoke
def test_a_shouting_name_is_cased_and_keeps_its_search_key():
    con = _core([("a", "KITWANGA MILL POND"), ("b", "O'ROURKE LAKE"), ("c", "HART LAKE (Fort St. James)"),
                 ("d", "Mabel Lake"), ("e", "CPR"), ("f", "")],
                [("b", "O'ROURKE LAKE"), ("b", "Rourke Pond")])
    moved = P.case_core_names(con)
    assert moved == [("a", "KITWANGA MILL POND", "Kitwanga Mill Pond"), ("b", "O'ROURKE LAKE", "O'Rourke Lake"),
                     ("c", "HART LAKE (Fort St. James)", "Hart Lake (Fort St. James)")]
    got = {i: (n, k) for i, n, k in con.execute("SELECT item_id, name, COALESCE(name_key, lower(name)) FROM item")}
    assert got == {"a": ("Kitwanga Mill Pond", "kitwanga mill pond"), "b": ("O'Rourke Lake", "orourke lake"),
                   "c": ("Hart Lake (Fort St. James)", "hart lake (fort st. james)"), "d": ("Mabel Lake", "mabel lake"),
                   "e": ("CPR", "cpr"), "f": ("", "")}
    assert all(k == search_key(n) for n, k in got.values())
    # the water's source row follows its shown name (the app joins name_source.name = item.name); an alias's stays
    assert sorted(con.execute("SELECT item_id, name FROM name_source")) == [("b", "O'Rourke Lake"), ("b", "Rourke Pond")]


def test_mutation_a_caser_that_rewrites_words_is_refused(monkeypatch):
    """A caser that changed the words ("L." -> "Lake") would change what search finds: refused, never shipped."""
    monkeypatch.setattr(P, "display_case", lambda n: n.replace("L.", "Lake").title())
    with pytest.raises(ValueError, match="search key"):
        P.case_core_names(_core([("a", "TEANOOK L.")]))
