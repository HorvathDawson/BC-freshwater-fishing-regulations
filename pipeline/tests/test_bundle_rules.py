"""The rules, from the reach builder into the bundle.

THE ONE TEST THAT MATTERS IS AT THE BOTTOM and it is not a row count. Interning 1,720,243
(section, rule) pairs into 1,905 sets is a compression, and the only thing worth proving
about a compression is that it is LOSSLESS: expand the sets again and you must get back
every pair, and no pair you did not start with. A row count agrees with itself.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from pipeline.common.curated import GENERATED
from pipeline.deliver.bundle.rules import intern_sets

ROWS = [
    {"section_id": "s1", "entry_id": "e1", "rule_id": "r1", "scope": "reach"},
    {"section_id": "s2", "entry_id": "e1", "rule_id": "r1", "scope": "reach"},
    {"section_id": "s3", "entry_id": "e1", "rule_id": "r1", "scope": "trib"},
    {"section_id": "s4", "entry_id": "e1", "rule_id": "r1", "scope": "reach"},
    {"section_id": "s4", "entry_id": "e2", "rule_id": "r9", "scope": "reach"},
]


def test_sections_covered_by_the_same_rules_share_one_set():
    """The whole design in one assertion: a river with one closure on it repeats that
    closure down every section of its length, and those are not different facts."""
    section_set, sets = intern_sets(ROWS)
    assert section_set["s1"] == section_set["s2"]
    assert len(sets) == 3          # {r1 reach}, {r1 trib}, {r1 reach + r9 reach}


def test_the_same_rule_reached_two_ways_is_two_sets():
    """`scope` is part of the set's identity. s1 and s3 carry the same RULE and a reader
    must be told different things about them — one names this water, the other says a
    closure upstream reaches it. Collapsing them would lose the only distinction."""
    section_set, _ = intern_sets(ROWS)
    assert section_set["s1"] != section_set["s3"]


def test_set_ids_are_stable_across_runs():
    """A bundle whose set numbering moved between builds would diff as though every section
    in the province had changed."""
    a, _ = intern_sets(ROWS)
    b, _ = intern_sets(list(reversed(ROWS)))
    assert a == b


def test_a_section_with_no_rule_gets_no_row():
    """Absent, not an empty set. "No rule covers this water" and "a set that happens to be
    empty" would read the same in the client and only one of them is a fact we have."""
    section_set, _ = intern_sets(ROWS)
    assert "s99" not in section_set


# --------------------------------------------------------------------------- #
# The acceptance test

def _latest_run() -> Path | None:
    runs = [(p.stat().st_mtime, p.parent)
            for p in GENERATED.reaches.glob("*/report.json")]
    return max(runs)[1] if runs else None


@pytest.mark.slow
def test_expanding_the_sets_reproduces_every_binding_exactly():
    """Ground truth is the reach builder's own output; the bundle must agree pair for pair.

    Not "the same number of rows" — the SAME ROWS. A compression that drops one binding
    drops a closure on a real river, and a compression that invents one closes water that is
    open. Both are invisible to a count.
    """
    run = _latest_run()
    if run is None:
        pytest.skip("no reach run on disk")
    bundle = GENERATED.bundle / "bundle.sqlite"
    if not bundle.exists():
        pytest.skip("no bundle built")

    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    if not db.execute("SELECT 1 FROM sqlite_master "
                      "WHERE type='table' AND name='ruleset'").fetchone():
        pytest.skip("bundle predates the ruleset tables")

    truth = set()
    for line in (run / "rule_section.jsonl").open(encoding="utf-8"):
        r = json.loads(line)
        truth.add((r["section_id"], r["entry_id"], r["rule_id"], r["scope"]))

    got = set(db.execute(
        "SELECT s.section_id, r.entry_id, r.rule_id, r.via "
        "FROM section_ruleset s JOIN ruleset r USING(set_id)"))

    missing = truth - got
    invented = got - truth
    assert not missing, (f"{len(missing):,} bindings lost in the bundle, "
                         f"e.g. {sorted(missing)[:3]}")
    assert not invented, (f"{len(invented):,} bindings the reach builder never emitted, "
                          f"e.g. {sorted(invented)[:3]}")
    assert len(got) == len(truth)


@pytest.mark.slow
def test_every_bound_rule_reaches_at_least_one_section_in_the_bundle():
    """A rule that binds and then covers nothing renders as "no regulations here" on water
    a curator wrote a rule for — the failure mode this whole table exists to prevent."""
    bundle = GENERATED.bundle / "bundle.sqlite"
    if not bundle.exists():
        pytest.skip("no bundle built")
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    if not db.execute("SELECT 1 FROM sqlite_master "
                      "WHERE type='table' AND name='ruleset'").fetchone():
        pytest.skip("bundle predates the ruleset tables")
    orphans = db.execute(
        "SELECT entry_id, rule_id FROM rule WHERE uncertain = 0 "
        "AND NOT EXISTS (SELECT 1 FROM ruleset x "
        "                WHERE x.entry_id = rule.entry_id AND x.rule_id = rule.rule_id)"
    ).fetchall()
    assert not orphans, f"{len(orphans)} bound rules cover no section, e.g. {orphans[:3]}"
