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

from pipeline.common.search_key import search_key

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
              "section_ids": []}]
    BB._items(db, Path("."), items, BB.Coverage())
    assert db.execute("SELECT name, name_key FROM item").fetchall() == [("Barrière River", "barriere river")]
    assert db.execute("SELECT alias, alias_key FROM alias").fetchall() == [("Children's Creek", "childrens creek")]
