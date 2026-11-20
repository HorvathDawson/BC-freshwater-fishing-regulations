"""A hand-built bundle's DERIVED tables (DATAFLOW P2), written by the bundle's own code: the rule
keys (`derived.rule_keys`, no steelhead facts) and the parts (`derived.write`)."""
from __future__ import annotations

import sqlite3

from pipeline.deliver.bundle import derived


class _Cov:
    def filled(self, *a):
        pass

    def skip(self, *a):
        pass


def section_sets(db: sqlite3.Connection, sets: dict[int, int]) -> None:
    """`rule_key` and `section_ruleset` (with its `key_ix`) for {sid: set_id}."""
    kinds = dict(db.execute("SELECT s.sid, i.kind FROM item_section s JOIN item i ON i.ord = s.ord"))
    rows, key_of = derived.rule_keys(sets, set(), set(), kinds)
    db.executemany("INSERT INTO rule_key VALUES (?,?,?,?,?,?,?)", rows)
    db.executemany("INSERT INTO section_ruleset (sid, set_id, key_ix) VALUES (?,?,?)",
                   [(s, v, key_of[s]) for s, v in sorted(sets.items())])


def finish(db: sqlite3.Connection) -> None:
    """`rule_ix`, `rule.closure_grade`, `part`, `part_section` — once every other row is in."""
    derived.write(db, _Cov())


def no_notices(monkeypatch, tmp_path) -> None:
    """(T978) the export reads the in-season notices' end tokens (`inseason.matches.load`), whose resolution is
    generated on a machine that ran `resolve` (T679 #11) and is absent on CI: a test of a HAND-MADE bundle points the
    export at a notice file with no matches instead of the live one (it tests parts, not notices)."""
    from pipeline.regs.inseason import matches as _M
    f = tmp_path / "notice_waters.json"
    f.write_text('{"format": "inseason-matches/3", "matches": {}, "history": []}\n', encoding="utf-8")
    monkeypatch.setattr(_M, "default_path", lambda: f)
