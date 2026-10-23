"""T114: the search key — the Python half of one rule the app's `searchKey` also follows.

The cases are `pipeline/common/search_key_cases.json`, which `app/tools/search-key-parity.test.ts` reads too: both
languages are held to one table, so the key the bundle STORES (`item.name_key`, `alias.alias_key`) is the key the app
makes of a query. Mutations, each failing a case below: drop the accent fold ("barrière lake"), drop the apostrophe
delete ("children's fishing pond"), use Python's `\\s` ("\\ufeffmabel lake"), use `unicodedata.combining` instead of
U+0300-U+036F (no case yet; it agrees on every BC name) — see `test_the_key_is_written_onto_the_bundle` for the store.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from pipeline.common.search_key import search_key, stored_key

CASES = json.loads((Path(__file__).resolve().parents[1] / "common" / "search_key_cases.json").read_text())["cases"]


@pytest.mark.parametrize("text,key", CASES, ids=[repr(c[0]) for c in CASES])
def test_the_shared_table(text: str, key: str) -> None:
    assert search_key(text) == key


def test_the_table_holds_the_tickets_cases() -> None:
    # T114's acceptance names these two; the table must keep them
    assert ["Barrière Lake", "barriere lake"] in CASES
    assert ["Children's Fishing Pond", "childrens fishing pond"] in CASES


def test_the_key_is_written_onto_the_bundle(monkeypatch: pytest.MonkeyPatch) -> None:
    """`bundle.build._items` writes name_key / alias_key with this function, through the bundle's own schema.
    Mutation: write `name` (or `normalise(name)`) into `name_key` and the first assertion fails."""
    from pipeline.common import section_handles
    import importlib

    BB = importlib.import_module("pipeline.deliver.bundle.build")   # the package re-exports `build`, the function

    monkeypatch.setattr(section_handles, "read", lambda _dir: ({}, {}))   # no sections: no handle table needed
    schema = (Path(BB.__file__).parent / "schema.sql").read_text()
    db = sqlite3.connect(":memory:")
    db.executescript(schema)
    items = [{"id": "gnis:1", "name": "Barrière River", "kind": "stream", "variants": ["Children's Creek"],
              "section_ids": []},
             {"id": "gnis:2", "name": "Mabel Creek", "kind": "stream", "variants": ["Mable Creek"], "section_ids": []}]
    BB._items(db, Path("."), items, BB.Coverage())
    assert db.execute("SELECT name, name_key FROM item ORDER BY ord").fetchall() == [
        ("Barrière River", "barriere river"), ("Mabel Creek", None)]
    assert db.execute("SELECT alias, alias_key FROM alias ORDER BY alias").fetchall() == [
        ("Children's Creek", "childrens creek"), ("Mable Creek", None)]
    # what the app reads (`NAME_KEY` / `ALIAS_KEY`) is the search key on every row
    assert all(k == search_key(n) for n, k in db.execute("SELECT name, COALESCE(name_key, lower(name)) FROM item"))
    assert all(k == search_key(a) for a, k in db.execute("SELECT alias, COALESCE(alias_key, lower(alias)) FROM alias"))


@pytest.mark.parametrize("text", [c[0] for c in CASES] + ["Mabel Lake", "MABEL  LAKE", "Barrière", "Ölund Lake", "ÆB"])
def test_a_stored_key_is_left_out_only_where_lower_already_is_the_key(text: str) -> None:
    """`stored_key` is NULL only for ASCII text whose key is its lower case — where SQLite's `lower()` (ASCII-exact
    with or without ICU) gives the key. Mutation: drop `isascii()` and "ÆB"-like text could fall to an engine's lower."""
    k = stored_key(text)
    if k is None:
        assert text.isascii() and text.lower() == search_key(text)
    else:
        assert k == search_key(text)
    assert (k if k is not None else text.lower()) == search_key(text)


def test_most_names_store_no_key() -> None:
    assert stored_key("Mabel Lake") is None and stored_key("Children's Fishing Pond") == "childrens fishing pond"
    assert stored_key("Barrière Lake") == "barriere lake" and stored_key("Mabel  Lake") == "mabel lake"
